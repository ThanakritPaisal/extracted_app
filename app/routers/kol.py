from fastapi import APIRouter, Depends, HTTPException, Query, Body
from psycopg2.extensions import connection as _conn
from app.db import get_conn
from app.services.scraper import scrape_and_store, scrape_and_return, kalodata_scraper, fetch_creators, parse_video_stats
from bs4 import BeautifulSoup
from typing import List, Optional
from pydantic import BaseModel, HttpUrl
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import re, json, time, os, logging
from tiktok_captcha_solver import make_undetected_chromedriver_solver
import requests

API_KEY = "1c5f034b5674c55173fda99041be9b77"
logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

def make_driver():
    opts = Options()
    # Cloud Run friendly flags:
    opts.add_argument("--headless=new")      # modern headless
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    # Optional: lower logging noise
    # opts.add_argument("--log-level=3")

    # If Chrome is at a custom path (usually not needed with google-chrome-stable):
    # opts.binary_location = "/usr/bin/google-chrome"

    # Let Selenium Manager find the matching chromedriver automatically:
    driver = webdriver.Chrome(options=opts)
    return driver

def _parse_compact_number(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    cleaned = value.strip().replace(',', '')
    if not cleaned:
        return None
    multiplier = 1
    suffix = cleaned[-1].upper()
    if suffix in {"K", "M", "B"}:
        cleaned = cleaned[:-1]
        if suffix == "K":
            multiplier = 1_000
        elif suffix == "M":
            multiplier = 1_000_000
        elif suffix == "B":
            multiplier = 1_000_000_000
    try:
        return int(float(cleaned) * multiplier)
    except ValueError:
        return None


def _extract_username_from_url(url: str) -> Optional[str]:
    from urllib.parse import urlparse

    parsed = urlparse(url)
    path = parsed.path or ""
    if not path.startswith("/@"):
        return None
    segment = path.split("/", 2)[1]
    username = segment.lstrip("@")
    return username or None


def _extract_basic_posts_from_html(soup: BeautifulSoup, max_posts: int = 10):
    """Return lightweight post data (cover, text, hashtags) from the profile grid."""
    posts = []
    for card in soup.select("[data-e2e='user-post-item']")[:max_posts]:
        cover = None
        cover_img = card.select_one("[data-e2e='video-cover'] img")
        if cover_img and cover_img.has_attr("src"):
            cover = cover_img["src"]

        caption = ""
        caption_el = card.select_one("[data-e2e='user-post-item-desc']") or card
        if caption_el:
            caption = caption_el.get_text(" ", strip=True)

        hashtags = []
        for tag in card.select("[data-e2e='video-tag']"):
            text = tag.get_text(strip=True)
            if text.startswith("#"):
                hashtags.append(text.lstrip("#"))

        if not hashtags and caption:
            import re as _re

            hashtags = [match.group(1) for match in _re.finditer(r"#(\w+)", caption)]

        posts.append({
            "text": caption,
            "cover": cover,
            "likes": None,
            "comments": None,
            "shares": None,
            "views": None,
            "hashtags": hashtags,
        })
    return posts

router = APIRouter(prefix="/kol", tags=["KOL"])

@router.post("/scrape")
def run_scraper(
    pages: int = 10,
    conn: _conn = Depends(get_conn)
):
    """
    Trigger scraping of `pages` pages and store into kol_gmv.
    Returns: {"inserted": <number_of_rows>}
    """
    try:
        inserted = scrape_and_store(conn, pages)
        return {"inserted": inserted}
    except Exception as e:
        # rollback is handled inside service if needed
        raise HTTPException(500, str(e))


@router.post("/scrape_and_return")
def run_scraper_return(
    pages: int = 10,
    kol_type="Fashion",
    left_bound=5000,
    right_bound=200000,
    conn: _conn = Depends(get_conn)
):
    inserted = scrape_and_return(conn, kol_type, pages, left_bound, right_bound)
    return {"data": inserted}
    
@router.post("/kalodata_scraper")
def run_kalodata_return(
    pages: int = 10,
    type: str = "Pet Supplies",
    followers_filter: str = "5000-8000",
    conn: _conn = Depends(get_conn)
):
    try:
        inserted = kalodata_scraper(conn, pages, type, followers_filter)
        return {"data": inserted}
    except Exception as e:
        # rollback is handled inside service if needed
        raise HTTPException(500, str(e))
    

@router.get("/find_creators")
async def find_creators(
    query: str = "",
    page: int = 1,
    size: int = 12,
    conn: _conn = Depends(get_conn)
):
    try:
        result = fetch_creators(conn=conn, query=query, page=page, size=size)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/latest_videos")
async def fetch_latest_video(
    username: str = Query(..., description="TikTok username, e.g. 'khamkhomnews'"),
    count_videos: int = Query(1, description="Number of most recent videos to fetch"),
):
    profile_url = f"https://www.tiktok.com/@{username}"

    # 1) Spin up your undetected driver
    chrome_bin  = os.getenv("CHROME_BIN")
    driver_path = os.getenv("CHROMEDRIVER_PATH")
    opts = Options()
    if chrome_bin:
        opts.binary_location = chrome_bin
    opts.add_argument("--headless")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")

    try:
        driver = make_undetected_chromedriver_solver(API_KEY, options=opts, no_warn=True)
    except WebDriverException as exc:
        raise HTTPException(500, f"ChromeDriver error: {exc}")

    try:
        # 2) Load the profile and scroll to reveal all thumbnails
        driver.get(profile_url)
        time.sleep(2)
        driver.execute_script("document.body.style.overflow = 'auto';")
        last_h = driver.execute_script("return document.body.scrollHeight")
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            new_h = driver.execute_script("return document.body.scrollHeight")
            if new_h == last_h:
                break
            last_h = new_h

        # grab all the video links
        soup = BeautifulSoup(driver.page_source, "html.parser")
        thumbs = [
            a["href"] if a["href"].startswith("http") else "https://www.tiktok.com"+a["href"]
            for a in soup.select("a[href*='/video/']")
        ]
        # de-dupe and limit
        seen = set()
        vids = []
        for u in thumbs:
            if u not in seen:
                seen.add(u)
                vids.append(u)
            if len(vids) >= count_videos:
                break

    finally:
        driver.quit()

    results = []
    # 3) For each video URL, fetch its page and parse
    for video_url in vids:
        try:
            opts_video = Options()
            if chrome_bin:
                opts_video.binary_location = chrome_bin
            opts_video.add_argument("--headless")
            opts_video.add_argument("--disable-gpu")
            opts_video.add_argument("--no-sandbox")
            driver_video = make_undetected_chromedriver_solver(
                API_KEY,
                options=opts_video,
                no_warn=True
            )
            try:
                driver_video.get(video_url)
                time.sleep(5)
                html = driver_video.page_source
            except TimeoutException:
                raise HTTPException(500, f"Timeout loading video page: {video_url}")
            finally:
                driver_video.quit()

            video_data = parse_video_stats(html, video_url)
            results.append(video_data)
        except Exception as e:
            # if one fails, continue on
            print(f"Failed to fetch or parse {video_url}: {e}")

    return {
        "account": username,
        "videos":  results
    }

@router.get("/search_api_data")
async def get_search_api_data(
    keyword: str = Query(..., description="Keyword to search on TikTok, e.g. 'ไก่'"),
):
    from seleniumwire import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    import requests
    import os, json, time

    search_url = f"https://www.tiktok.com/search?lang=en&q={keyword}"

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(service=Service(), options=options)

    try:
        driver.get(search_url)
        time.sleep(15)  # wait for network calls to populate

        seen_urls = set()
        results = []

        for request_data in driver.requests:
            try:
                url = request_data.url
                if (
                    request_data.response
                    and "www.tiktok.com/api/search/general/full" in url
                    and url not in seen_urls
                ):
                    seen_urls.add(url)
                    headers = dict(request_data.headers)
                    headers.pop("Content-Length", None)  # remove if present
                    response = requests.get(url, headers=headers)
                    if response.status_code == 200:
                        results.append(response.json())
            except Exception as e:
                print(f"Error scraping TikTok API: {e}")
                continue
    finally:
        driver.quit()

    if not results:
        raise HTTPException(status_code=404, detail="No TikTok search API data found.")

    return {"keyword": keyword, "results": results}

@router.get("/search_creator_snippets")
async def search_creator_snippets(
    keyword: str = Query(..., description="Keyword to search on TikTok, e.g. 'ไก่'")
):
    """Return up to 50 unique creators (name, uniqueId, url) from TikTok search, paginating if needed."""
    from seleniumwire import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    import requests
    import time
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

    search_url = f"https://www.tiktok.com/search?lang=en&q={keyword}"

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(service=Service(), options=options)

    creators = {}
    try:
        driver.get(search_url)
        time.sleep(5)

        # simulate user scrolling to trigger additional search requests
        scroll_pause = 1.5
        last_height = driver.execute_script("return document.body.scrollHeight")
        for _ in range(6):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(scroll_pause)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        seen_urls = []
        headers_template = None
        cookies_template = {cookie['name']: cookie['value'] for cookie in driver.get_cookies()}
        payload_queue = []

        def fetch_payload(url: str):
            nonlocal headers_template, cookies_template
            headers = dict(headers_template or {})
            headers.pop("Content-Length", None)
            headers.pop("content-length", None)
            try:
                resp = requests.get(url, headers=headers, cookies=cookies_template, timeout=20)
                if resp.status_code == 200:
                    data = resp.json()
                    data['__source_url__'] = url
                    return data
            except Exception as err:
                print(f"Error fetching TikTok search API: {err}")
            return None

        # capture initial API calls triggered during page load/scroll
        for request_data in driver.requests:
            url = getattr(request_data, "url", "")
            if not url or not request_data.response:
                continue
            if "www.tiktok.com/api/search/general/full" not in url:
                continue
            if url in seen_urls:
                continue
            seen_urls.append(url)
            headers_template = dict(getattr(request_data, "headers", {}) or {})
            payload = fetch_payload(url)
            if payload:
                payload_queue.append(payload)

        # paginate until 50 creators gathered or no further data
        while len(creators) < 50 and payload_queue:
            payload = payload_queue.pop(0)
            last_url = payload.get('__source_url__')
            data_blocks = payload.get("data") or []
            for block in data_blocks:
                if len(creators) >= 50:
                    break
                if not isinstance(block, dict):
                    continue
                item = block.get("item") or {}
                author = item.get("author") or {}
                unique_id = author.get("uniqueId") or author.get("secUid")
                nickname = author.get("nickname") or author.get("authorName")
                if not unique_id and not nickname:
                    continue
                profile_url = f"https://www.tiktok.com/@{unique_id}" if unique_id else (f"https://www.tiktok.com/@{nickname}" if nickname else "")
                key = unique_id or nickname
                if key and key not in creators:
                    creators[key] = profile_url
            has_more = payload.get("has_more")
            cursor = payload.get("cursor")
            if (
                has_more and has_more != 0 and cursor is not None and len(creators) < 50 and last_url
            ):
                parsed = urlparse(last_url)
                query = parse_qs(parsed.query)
                query['cursor'] = [str(cursor)]
                next_query = urlencode({k: v[0] if isinstance(v, list) else v for k, v in query.items()})
                next_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, next_query, parsed.fragment))
                next_payload = fetch_payload(next_url)
                if next_payload:
                    payload_queue.append(next_payload)
                else:
                    break
            else:
                continue
    finally:
        driver.quit()

    if not creators:
        raise HTTPException(status_code=404, detail="No creators found for given keyword.")

    return {
        "keyword": keyword,
        "creators": list(creators.values())[:50]
    }


@router.get("/search_top_creators")
async def search_top_creators(
    keyword: str = Query(..., description="Keyword to search on TikTok, e.g. 'ไก่'"),
    limit: int = Query(100, ge=1, le=500, description="Maximum creators to return")
):
    """Collect creator profile URLs from TikTok's general search feed using cursor pagination."""
    from seleniumwire import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    import requests
    import time
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

    limit_value = getattr(limit, 'default', limit)
    try:
        limit_value = int(limit_value)
    except (TypeError, ValueError):
        limit_value = 100
    limit = max(1, min(500, limit_value))

    search_url = f"https://www.tiktok.com/search?lang=en&q={keyword}"

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(service=Service(), options=options)

    creators: dict[str, str] = {}
    try:
        driver.get(search_url)
        time.sleep(5)

        scroll_pause = 1.5
        last_height = driver.execute_script("return document.body.scrollHeight")
        for _ in range(6):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(scroll_pause)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        cookies_template = {cookie['name']: cookie['value'] for cookie in driver.get_cookies()}
        headers_template = None
        payload_queue = []
        seen_urls: set[str] = set()
        seen_cursors: set[str] = set()

        def fetch_payload(url: str):
            headers = dict(headers_template or {})
            headers.pop("Content-Length", None)
            headers.pop("content-length", None)
            try:
                resp = requests.get(url, headers=headers, cookies=cookies_template, timeout=20)
                if resp.status_code == 200:
                    data = resp.json()
                    data['__source_url__'] = url
                    return data
            except Exception as err:
                print(f"Error fetching TikTok general search API: {err}")
            return None

        for request_data in driver.requests:
            url = getattr(request_data, "url", "")
            if not url or not request_data.response:
                continue
            if "www.tiktok.com/api/search/general/full" not in url:
                continue
            if url in seen_urls:
                continue
            seen_urls.add(url)
            headers_template = dict(getattr(request_data, "headers", {}) or {})
            payload = fetch_payload(url)
            if payload:
                payload_queue.append(payload)

        while len(creators) < limit and payload_queue:
            payload = payload_queue.pop(0)
            source_url = payload.get("__source_url__")
            data_blocks = payload.get("data") or []
            for block in data_blocks:
                if len(creators) >= limit:
                    break
                if not isinstance(block, dict):
                    continue
                item = block.get("item") or {}
                author = item.get("author") or {}
                unique_id = author.get("uniqueId") or author.get("secUid")
                nickname = author.get("nickname") or author.get("authorName")
                if not unique_id and not nickname:
                    continue
                profile_url = f"https://www.tiktok.com/@{unique_id}" if unique_id else (f"https://www.tiktok.com/@{nickname}" if nickname else "")
                key = unique_id or nickname
                if key and key not in creators:
                    creators[key] = profile_url

            cursor = payload.get("cursor")
            has_more = payload.get("has_more")
            data_section = payload.get("data") if isinstance(payload.get("data"), dict) else None
            if data_section:
                cursor = data_section.get("cursor") or cursor
                has_more = data_section.get("has_more", has_more)

            if (
                has_more and has_more != 0 and cursor is not None and source_url and len(creators) < limit
            ):
                cursor_key = str(cursor)
                if cursor_key in seen_cursors:
                    continue
                seen_cursors.add(cursor_key)
                parsed = urlparse(source_url)
                query = parse_qs(parsed.query)
                query['cursor'] = [cursor_key]
                if 'offset' in query:
                    query['offset'] = [cursor_key]
                next_query = urlencode({k: v[0] if isinstance(v, list) else v for k, v in query.items()})
                next_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, next_query, parsed.fragment))
                if next_url not in seen_urls:
                    next_payload = fetch_payload(next_url)
                    if next_payload:
                        seen_urls.add(next_url)
                        payload_queue.append(next_payload)
    finally:
        driver.quit()

    if not creators:
        raise HTTPException(status_code=404, detail="No creators found for given keyword.")

    urls = [url for url in creators.values() if url]
    return urls[:limit]


@router.post("/tiktok_profiles_batch")
async def tiktok_profiles_batch(
    urls: List[HttpUrl] = Body(..., embed=True, description="TikTok profile URLs, e.g. https://www.tiktok.com/@username")
):
    """Fetch profile metadata and up to 10 posts for each TikTok profile URL, reusing a single browser session."""
    from seleniumwire import webdriver as wire_webdriver
    import requests
    from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
    from http.cookies import SimpleCookie

    results = {"kol_metadata": {}, "kol_post_data": {}}
    failures: dict[str, str] = {}
    usernames = []

    seen = set()
    for raw_url in urls:
        url_str = str(raw_url)
        username = _extract_username_from_url(url_str)
        if not username:
            failures[url_str] = "Unable to extract username from URL"
            continue
        if username in seen:
            continue
        seen.add(username)
        usernames.append(username)

    if not usernames:
        raise HTTPException(status_code=400, detail="No valid TikTok usernames found in request.")

    chrome_bin = os.getenv("CHROME_BIN")
    driver_path = os.getenv("CHROMEDRIVER_PATH")

    opts = Options()
    if chrome_bin:
        opts.binary_location = chrome_bin
    opts.add_argument("--headless")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--window-size=1920,1080")

    service = Service(driver_path) if driver_path else Service(ChromeDriverManager().install())

    try:
        driver = wire_webdriver.Chrome(service=service, options=opts)
    except WebDriverException as exc:
        raise HTTPException(500, f"ChromeDriver error: {exc}")

    def _safe_get_html(driver_instance):
        """Attempt to get page HTML via JS (safer than page_source in some crash cases)."""
        try:
            return driver_instance.execute_script("return document.documentElement.outerHTML;")
        except Exception:
            # fallback to page_source; may raise WebDriverException on crashed tab
            try:
                return driver_instance.page_source
            except Exception:
                raise

    try:
        for username in usernames:
            profile_url = f"https://www.tiktok.com/@{username}"

            try:
                driver.requests.clear()
            except Exception:
                pass

            # We'll allow one retry in case the tab crashes; recreate driver if needed
            attempt = 0
            html = None
            while attempt < 2 and html is None:
                attempt += 1
                try:
                    driver.get(profile_url)
                except Exception as exc:
                    failures[profile_url] = f"Error loading page: {exc}"
                    break

                # allow dynamic content to load and trigger API requests
                time.sleep(4)
                try:
                    for i in range(3):
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        logger.info("Scrolled profile %s iteration %d", username, i + 1)
                        time.sleep(1.5)
                except Exception as exc:
                    logger.warning("Scrolling error for %s: %s", username, exc)

                try:
                    html = _safe_get_html(driver)
                except Exception as exc:
                    # Common sign: tab crashed / session lost. Try to recreate driver once.
                    msg = str(exc)
                    logger.warning("Failed to fetch HTML for %s on attempt %d: %s", username, attempt, msg)
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    if attempt < 2:
                        try:
                            driver = wire_webdriver.Chrome(service=service, options=opts)
                            logger.info("Recreated Chrome driver to recover from crash for %s", username)
                        except Exception as exc2:
                            failures[profile_url] = f"Failed to recreate driver after crash: {exc2}"
                            html = None
                            break
                    else:
                        failures[profile_url] = f"Failed to fetch page HTML after retries: {exc}"
                        break

            if not html:
                # skip processing this username
                continue

            cookies = {cookie['name']: cookie['value'] for cookie in driver.get_cookies()}
            if "msToken" not in cookies:
                try:
                    cookie_str = driver.execute_script("return document.cookie;") or ""
                except Exception:
                    cookie_str = ""
                for part in cookie_str.split(";"):
                    if "=" not in part:
                        continue
                    k, v = part.split("=", 1)
                    k = k.strip()
                    v = v.strip()
                    if k and k not in cookies:
                        cookies[k] = v
            api_payload = None

            def _sanitize_headers(raw_headers: dict[str, str]) -> dict[str, str]:
                """Drop pseudo headers & Content-Length so requests can reuse them."""
                sanitized = {}
                for key, value in (raw_headers or {}).items():
                    if not key:
                        continue
                    if key.startswith(":"):
                        continue
                    if key.lower() == "content-length":
                        continue
                    sanitized[key] = value
                if "User-Agent" not in sanitized:
                    try:
                        ua = driver.execute_script("return navigator.userAgent;")
                    except Exception:
                        ua = None
                    sanitized["User-Agent"] = ua or (
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                    )
                return sanitized

            def _ensure_ms_token_in_url(target_url: str) -> str:
                token = cookies.get("msToken")
                if not token:
                    return target_url
                parsed = urlparse(target_url)
                query = parse_qs(parsed.query, keep_blank_values=True)
                current = query.get("msToken")
                if current and current[0]:
                    return target_url
                query["msToken"] = [token]
                flattened = []
                for key, values in query.items():
                    if isinstance(values, list):
                        for val in values:
                            flattened.append((key, val))
                    else:
                        flattened.append((key, values))
                new_query = urlencode(flattened)
                return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))

            saw_item_list_request = False
           
            for request_data in driver.requests:
                
                url = getattr(request_data, "url", "") or ""
                if "www.tiktok.com/api/post/item_list" not in url:
                    continue
                if not request_data.response:
                    continue
                saw_item_list_request = True

                return request_data.url
                
                # try:
                resp = requests.get(request_data.url)
                resp.raise_for_status()
                api_payload = resp.json()
                # except Exception as exc:
                #     logger.warning("Failed to fetch/parse TikTok item_list API for %s: %s", username, exc)
                #     continue

                
                
                

            if not saw_item_list_request:
                logger.warning(
                    "No TikTok item_list request captured for %s (profile_url=%s)",
                    username,
                    profile_url,
                )
            elif api_payload is None:
                logger.warning(
                    "TikTok item_list request captured but no JSON decoded for %s",
                    username,
                )
      
            soup = BeautifulSoup(html, "html.parser")
            logger.info("Profile %s page length %d chars", username, len(html))
            avatar_el = soup.select_one("[data-e2e='user-avatar'] img")
            title_el = soup.select_one("[data-e2e='user-title']")
            bio_el = soup.select_one("[data-e2e='user-bio']")
            followers_el = soup.select_one("[data-e2e='followers-count']")
            following_el = soup.select_one("[data-e2e='following-count']")
            likes_el = soup.select_one("[data-e2e='likes-count']")

            metadata = {
                "name": title_el.get_text(strip=True) if title_el else username,
                "account": username,
                "bio": bio_el.get_text("\n", strip=True) if bio_el else "",
                "url": profile_url,
                "followers": _parse_compact_number(followers_el.get_text(strip=True) if followers_el else None),
                "following": _parse_compact_number(following_el.get_text(strip=True) if following_el else None),
                "total_likes": _parse_compact_number(likes_el.get_text(strip=True) if likes_el else None),
                "region": None,
                "avatar": avatar_el["src"] if avatar_el and avatar_el.has_attr("src") else "",
            }

            items = []
            if isinstance(api_payload, dict):
                for key in ("itemList", "item_list", "items", "aweme_list"):
                    candidate = api_payload.get(key)
                    if isinstance(candidate, list):
                        items = candidate
                        break
                if not items:
                    data_section = api_payload.get("data")
                    if isinstance(data_section, dict):
                        for key in ("itemList", "item_list", "items", "aweme_list"):
                            candidate = data_section.get(key)
                            if isinstance(candidate, list):
                                items = candidate
                                break

            # fallback: parse SIGI_STATE JSON for ItemModule data when API fetch fails
            if not items:
                sigi_state = soup.find("script", id="SIGI_STATE")
                if sigi_state and sigi_state.string:
                    try:
                        sigi_data = json.loads(sigi_state.string)
                        item_module = sigi_data.get("ItemModule")
                        if isinstance(item_module, dict):
                            items = list(item_module.values())
                    except Exception:
                        pass

            metadata["total_videos"] = min(len(items), 10)
            metadata = {k: v for k, v in metadata.items() if v not in (None, "")}
            results["kol_metadata"][username] = metadata

            post_entries = []
            for item in (items or [])[:10]:
                if not isinstance(item, dict):
                    continue
                video = item.get("video") or {}
                stats = item.get("stats") or {}
                caption = item.get("desc") or ""
                hashtags = []
                for extra in item.get("textExtra") or []:
                    name = extra.get("hashtagName")
                    if name:
                        hashtags.append(name)

                post_entry = {
                    "text": caption,
                    "cover": video.get("cover") or video.get("originCover") or video.get("dynamicCover"),
                    "likes": stats.get("diggCount"),
                    "comments": stats.get("commentCount"),
                    "shares": stats.get("shareCount"),
                    "views": stats.get("playCount"),
                    "hashtags": hashtags,
                }
                post_entry = {k: v for k, v in post_entry.items() if v not in (None, "") or k == "hashtags"}
                if "hashtags" not in post_entry:
                    post_entry["hashtags"] = []
                post_entries.append(post_entry)

            if not post_entries:
                post_entries = []

            results["kol_post_data"][username] = post_entries

    finally:
        driver.quit()

    if not results["kol_metadata"]:
        raise HTTPException(status_code=500, detail="Failed to scrape any provided profiles.")

    if failures:
        results["errors"] = failures

    return results


@router.post("/tiktok_profiles_basic_posts")
async def tiktok_profiles_basic_posts(
    urls: List[HttpUrl] = Body(..., embed=True, description="TikTok profile URLs, e.g. https://www.tiktok.com/@username")
):
    """Fetch profile metadata and lightweight post info (cover/text/hashtags) without relying on TikTok APIs."""
    results = {"kol_metadata": {}, "kol_post_data": {}}
    failures: dict[str, str] = {}
    usernames = []

    seen = set()
    for raw_url in urls:
        url_str = str(raw_url)
        username = _extract_username_from_url(url_str)
        if not username:
            failures[url_str] = "Unable to extract username from URL"
            continue
        if username in seen:
            continue
        seen.add(username)
        usernames.append(username)

    if not usernames:
        raise HTTPException(status_code=400, detail="No valid TikTok usernames found in request.")

    chrome_bin = os.getenv("CHROME_BIN")
    driver_path = os.getenv("CHROMEDRIVER_PATH")

    opts = Options()
    profile_dir = os.getenv("CHROME_PROFILE_DIR")
    if chrome_bin:
        opts.binary_location = chrome_bin
    logger.info("Launching Selenium for basic posts with profile_dir=%s exists=%s", profile_dir, os.path.exists(profile_dir) if profile_dir else False)
    # run headless by default; override externally when you need manual login
    opts.add_argument("--headless")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1920,1080")
    if profile_dir:
        required = ["Cookies", "Preferences"]
        missing = [name for name in required if not os.path.exists(os.path.join(profile_dir, name))]
        if missing:
            logger.warning("Chrome profile %s missing files: %s", profile_dir, ", ".join(missing))
        opts.add_argument(f"--user-data-dir={profile_dir}")
        opts.add_argument("--profile-directory=Default")

    service = Service(driver_path) if driver_path else Service(ChromeDriverManager().install())

    try:
        driver = webdriver.Chrome(service=service, options=opts)
    except WebDriverException as exc:
        raise HTTPException(500, f"ChromeDriver error: {exc}")

    try:
        for username in usernames:
            profile_url = f"https://www.tiktok.com/@{username}"
            try:
                driver.get(profile_url)
            except Exception as exc:
                failures[profile_url] = f"Error loading page: {exc}"
                continue

            time.sleep(4)
            try:
                for _ in range(3):
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(1.5)
            except Exception:
                pass
            html = driver.page_source
            dump_dir = os.getenv("HTML_DUMP_DIR")
            # dump_dir = r"C:\Users\fin_t\OneDrive\เอกสาร\job_docs\Impower\apify_free\extracted_app\tiktok_html_dumps"
            # dump_dir = "/data/html-dumps"
            if dump_dir:
                os.makedirs(dump_dir, exist_ok=True)
                dump_path = os.path.join(dump_dir, f"{username}.html")
                try:
                    with open(dump_path, "w", encoding="utf-8") as fh:
                        fh.write(html)
                    logger.info("Wrote HTML dump for %s to %s", username, dump_path)
                except Exception as exc:
                    logger.warning("Failed to write HTML dump for %s: %s", username, exc)
            soup = BeautifulSoup(html, "html.parser")

            avatar_el = soup.select_one("[data-e2e='user-avatar'] img")
            title_el = soup.select_one("[data-e2e='user-title']")
            bio_el = soup.select_one("[data-e2e='user-bio']")
            followers_el = soup.select_one("[data-e2e='followers-count']")
            following_el = soup.select_one("[data-e2e='following-count']")
            likes_el = soup.select_one("[data-e2e='likes-count']")

            metadata = {
                "name": title_el.get_text(strip=True) if title_el else username,
                "account": username,
                "bio": bio_el.get_text("\n", strip=True) if bio_el else "",
                "url": profile_url,
                "followers": _parse_compact_number(followers_el.get_text(strip=True) if followers_el else None),
                "following": _parse_compact_number(following_el.get_text(strip=True) if following_el else None),
                "total_likes": _parse_compact_number(likes_el.get_text(strip=True) if likes_el else None),
                "region": None,
                "avatar": avatar_el["src"] if avatar_el and avatar_el.has_attr("src") else "",
            }
            metadata = {k: v for k, v in metadata.items() if v not in (None, "")}
            results["kol_metadata"][username] = metadata

            cards = soup.select("[data-e2e='user-post-item']")
            logger.info("Found %d user-post-item cards for %s", len(cards), username)
            cookies_banner = soup.find(attrs={"id": "cookie-banner"})
            if cookies_banner:
                logger.warning("Cookie/banner detected for %s; may block content", username)
            basic_posts = _extract_basic_posts_from_html(soup)
            if not basic_posts:
                sigi_state = soup.find("script", id="SIGI_STATE")
                if sigi_state and sigi_state.string:
                    try:
                        sigi_data = json.loads(sigi_state.string)
                        item_module = sigi_data.get("ItemModule")
                        if isinstance(item_module, dict):
                            for item in list(item_module.values())[:10]:
                                caption = item.get("desc") or ""
                                hashtags = []
                                for extra in item.get("textExtra") or []:
                                    name = extra.get("hashtagName")
                                    if name:
                                        hashtags.append(name)
                                basic_posts.append({
                                    "text": caption,
                                    "cover": item.get("video", {}).get("cover"),
                                    "likes": None,
                                    "comments": None,
                                    "shares": None,
                                    "views": None,
                                    "hashtags": hashtags,
                                })
                    except Exception:
                        pass
            if not basic_posts:
                logger.warning("No basic posts extracted for %s via HTML fallback", username)
            results["kol_post_data"][username] = basic_posts

    finally:
        driver.quit()

    if not results["kol_metadata"]:
        raise HTTPException(status_code=500, detail="Failed to scrape any provided profiles.")

    if failures:
        results["errors"] = failures

    return results


@router.get("/tiktok_profile_search_scraper")
async def tiktok_profile_search_scraper(
    username: str = Query(..., description="TikTok username, e.g. 'khamkhomnews'")
):
    profile_url = f"https://www.tiktok.com/@{username}"

    # Chrome & driver paths
    chrome_bin  = os.getenv("CHROME_BIN")
    driver_path = os.getenv("CHROMEDRIVER_PATH")
    opts = Options()
    if chrome_bin:
        opts.binary_location = chrome_bin
    opts.add_argument("--headless")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")

    try:
        driver = make_undetected_chromedriver_solver(
            API_KEY,
            options=opts,
            no_warn=True
        )
    except WebDriverException as exc:
        raise HTTPException(500, f"ChromeDriver error: {exc}")

    try:
        driver.get(profile_url)
        html = driver.page_source

    except Exception as exc:
        raise HTTPException(500, f"Error loading page: {exc}")
    finally:
        driver.quit()

    # Parse unchanged
    soup     = BeautifulSoup(html, "html.parser")
    avatar   = soup.select_one("[data-e2e='user-avatar'] img")["src"]
    title    = soup.select_one("[data-e2e='user-title']").get_text(strip=True)
    subtitle = soup.select_one("[data-e2e='user-subtitle']").get_text(strip=True)
    fol_cnt  = soup.select_one("[data-e2e='followers-count']").get_text(strip=True)
    ing_cnt  = soup.select_one("[data-e2e='following-count']").get_text(strip=True)
    likes    = soup.select_one("[data-e2e='likes-count']").get_text(strip=True)
    bio      = soup.select_one("[data-e2e='user-bio']").get_text("\n", strip=True)

    return { title:{
        "name":         title,
        "account":      f"@{username}",
        "bio":          bio,
        "url":          profile_url,
        "followers":    fol_cnt,
        "following":    ing_cnt,
        "subtitle":     subtitle,
        "total_likes":  likes,
        "region": "TH",
        "avatar":       avatar,
    }}



@router.get("/tiktok_profile_search_with_posts")
async def tiktok_profile_search_with_posts(
    username: str = Query(..., description="TikTok username, e.g. 'khamkhomnews'")
):
    from seleniumwire import webdriver as wire_webdriver
    import requests

    profile_url = f"https://www.tiktok.com/@{username}"

    chrome_bin = os.getenv("CHROME_BIN")
    driver_path = os.getenv("CHROMEDRIVER_PATH")
    opts = Options()
    if chrome_bin:
        opts.binary_location = chrome_bin
    # opts.add_argument("--headless")
    # opts.add_argument("--disable-gpu")
    # opts.add_argument("--no-sandbox")
    # opts.add_argument("--disable-blink-features=AutomationControlled")
    # opts.add_argument("--window-size=1920,1080")
    # opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1200,800")        # smaller viewport → less to render
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--no-zygote")
    opts.add_argument("--single-process")              # helps in low-RAM containers
    opts.add_argument("--mute-audio")
    opts.add_argument("--blink-settings=imagesEnabled=false") 

    service = Service(driver_path) if driver_path else Service(ChromeDriverManager().install())

    try:
        driver = wire_webdriver.Chrome(service=service, options=opts)
    except WebDriverException as exc:
        raise HTTPException(500, f"ChromeDriver error: {exc}")

    try:
        driver.get(profile_url)
        time.sleep(8)
        html = driver.page_source

        api_payload = None
        seen_urls = set()
        for request_data in driver.requests:
            url = getattr(request_data, "url", "") or ""
            if not url:
                continue
            if not request_data.response:
                continue
            if "www.tiktok.com/api/post/item_list" not in url:
                continue
            if url in seen_urls:
                continue
            seen_urls.add(url)
            headers = dict(getattr(request_data, "headers", {}) or {})
            headers.pop("Content-Length", None)
            headers.pop("content-length", None)
            cookies = {cookie['name']: cookie['value'] for cookie in driver.get_cookies()}
            try:
                resp = requests.get(url, headers=headers, cookies=cookies, timeout=20)
                if resp.status_code == 200:
                    api_payload = resp.json()
                    break
            except Exception as err:
                print(f"Error fetching TikTok profile API: {err}")
                continue
    except Exception as exc:
        raise HTTPException(500, f"Error loading page: {exc}")
    finally:
        driver.quit()

    soup = BeautifulSoup(html, "html.parser")
    avatar = soup.select_one("[data-e2e='user-avatar'] img")
    title_el = soup.select_one("[data-e2e='user-title']")
    subtitle_el = soup.select_one("[data-e2e='user-subtitle']")
    fol_cnt_el = soup.select_one("[data-e2e='followers-count']")
    ing_cnt_el = soup.select_one("[data-e2e='following-count']")
    likes_el = soup.select_one("[data-e2e='likes-count']")
    bio_el = soup.select_one("[data-e2e='user-bio']")

    profile = {
        "name": title_el.get_text(strip=True) if title_el else "",
        "account": f"@{username}",
        "bio": bio_el.get_text("\n", strip=True) if bio_el else "",
        "url": profile_url,
        "subtitle": subtitle_el.get_text(strip=True) if subtitle_el else "",
        "followers": fol_cnt_el.get_text(strip=True) if fol_cnt_el else "",
        "following": ing_cnt_el.get_text(strip=True) if ing_cnt_el else "",
        "total_likes": likes_el.get_text(strip=True) if likes_el else "",
        "region": "TH",
        "avatar": avatar["src"] if avatar else "",
    }

    posts = []
    items = []
    if api_payload:
        if isinstance(api_payload, dict):
            for key in ("itemList", "item_list", "items", "aweme_list"):
                candidate = api_payload.get(key)
                if isinstance(candidate, list):
                    items = candidate
                    break
            if not items:
                data_section = api_payload.get("data")
                if isinstance(data_section, dict):
                    for key in ("itemList", "item_list", "items", "aweme_list"):
                        candidate = data_section.get(key)
                        if isinstance(candidate, list):
                            items = candidate
                            break

    for item in items or []:
        if not isinstance(item, dict):
            continue
        caption = item.get("desc") or ""
        hashtags = []
        for extra in item.get("textExtra") or []:
            name = extra.get("hashtagName")
            if name:
                hashtags.append(name)
        if not hashtags:
            for challenge in item.get("challenges") or []:
                if isinstance(challenge, dict):
                    name = challenge.get("title") or challenge.get("chaName")
                    if name:
                        hashtags.append(name)
        video_data = item.get("video") or {}
        cover = video_data.get("cover") or video_data.get("originCover") or video_data.get("dynamicCover") or ""
        posts.append({
            "caption": caption,
            "hashtags": hashtags,
            "cover": cover,
        })

    if not posts:
        seen = set()
        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            if "/video/" in href and href not in seen:
                seen.add(href)
                posts.append({
                    "caption": "",
                    "hashtags": [],
                    "cover": "",
                })

    return {
        "profile": profile,
        "posts": posts,
    }
@router.get("/tiktok_profile_with_videos")
async def tiktok_profile_with_videos(
    username: str = Query(..., description="TikTok username, e.g. 'khamkhomnews'")
):
    profile_url = f"https://www.tiktok.com/@{username}"

    # Chrome & driver paths
    chrome_bin  = os.getenv("CHROME_BIN")
    driver_path = os.getenv("CHROMEDRIVER_PATH")
    opts = Options()
    if chrome_bin:
        opts.binary_location = chrome_bin
    opts.add_argument("--headless")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")

    try:
        service = Service(driver_path) if driver_path else Service(ChromeDriverManager().install())
        driver = make_undetected_chromedriver_solver(
            API_KEY,
            options=opts,
            # you can suppress the startup warning:
            no_warn=True
        )
    except WebDriverException as exc:
        raise HTTPException(500, f"ChromeDriver error: {exc}")

    try:
        driver.get(profile_url)
        time.sleep(2)

        # REMOVE any overlay that blocks scrolling/clicks
        driver.execute_script("""
            document.body.style.overflow = 'auto';
            const modal = document.querySelector('.TUXModal-overlay');
            if (modal) modal.remove();
        """)

        # SCROLL full page until no more new content
        SCROLL_PAUSE = 1.0
        last_h = driver.execute_script("return document.body.scrollHeight")
        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(SCROLL_PAUSE)
            new_h = driver.execute_script("return document.body.scrollHeight")
            if new_h == last_h:
                break
            last_h = new_h

        html = driver.page_source

    except Exception as exc:
        raise HTTPException(500, f"Error loading page: {exc}")
    finally:
        driver.quit()

    # Parse unchanged
    soup     = BeautifulSoup(html, "html.parser")
    avatar   = soup.select_one("[data-e2e='user-avatar'] img")["src"]
    title    = soup.select_one("[data-e2e='user-title']").get_text(strip=True)
    subtitle = soup.select_one("[data-e2e='user-subtitle']").get_text(strip=True)
    fol_cnt  = soup.select_one("[data-e2e='followers-count']").get_text(strip=True)
    ing_cnt  = soup.select_one("[data-e2e='following-count']").get_text(strip=True)
    likes    = soup.select_one("[data-e2e='likes-count']").get_text(strip=True)
    bio      = soup.select_one("[data-e2e='user-bio']").get_text("\n", strip=True)

    vids = []
    for a in soup.find_all("a", href=True):
        h = a["href"]
        if "/video/" in h:
            if h.startswith("/"):
                h = "https://www.tiktok.com" + h
            vids.append(h)

    # dedupe
    seen   = set()
    deduped = [u for u in vids if not (u in seen or seen.add(u))]

    return {
        "name":         title,
        "account":      f"@{username}",
        "bio":          bio,
        "url":          profile_url,
        "subtitle":     subtitle,
        "followers":    fol_cnt,
        "following":    ing_cnt,
        "total_likes":  likes,
        "total_videos": len(deduped),
        "avatar":       avatar,
        "videos":       deduped
    }

class CommentItem(BaseModel):
    created_at: Optional[str]
    username:   Optional[str]
    text:       Optional[str]
    likes:      Optional[str]

class VideoCommentsResponse(BaseModel):
    post_full_url: HttpUrl
    list:          List[CommentItem]


@router.get("/tiktok_comments", response_model=VideoCommentsResponse)
async def fetch_video_comments(
    video_url: HttpUrl = Query(
        ..., description="Full TikTok video URL, e.g. https://www.tiktok.com/@user/video/1234567890"
    )
):
    # 1) Selenium setup
    chrome_bin  = os.getenv("CHROME_BIN")
    driver_path = os.getenv("CHROMEDRIVER_PATH")
    opts = Options()
    if chrome_bin:
        opts.binary_location = chrome_bin
    opts.add_argument("--headless")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")

    try:
        service = Service(driver_path) if driver_path else Service(ChromeDriverManager().install())
        driver = make_undetected_chromedriver_solver(
            API_KEY,
            options=opts,
            no_warn=True
        )
    except Exception as exc:
        raise HTTPException(500, f"ChromeDriver start error: {exc}")

    try:
        driver.get(str(video_url))
        time.sleep(3)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
        html = driver.page_source
    except Exception as exc:
        raise HTTPException(500, f"Error loading video page: {exc}")
    finally:
        driver.quit()

    # 2) Parse with BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")

    # 1. Extract video like count from the aria-label
    like_elem = soup.find(attrs={"aria-label": re.compile(r"Like video\s*\n.* likes")})
    video_likes = None
    if like_elem:
        aria = like_elem["aria-label"]
        parts = aria.splitlines()
        if len(parts) > 1:
            video_likes = parts[1].replace(" likes", "").strip()

    # 2. Extract BreadcrumbList JSON-LD
    breadcrumb_data = None
    tag = soup.find(id="BreadcrumbList")
    if tag:
        try:
            breadcrumb_data = json.loads(tag.get_text())
        except Exception as e:
            breadcrumb_data = {"error": str(e)}

# 3. Extract comments
    rows = []
    for item in soup.select("div.css-13wx63w-DivCommentObjectWrapper"):
        # Username & comment
        user = item.select_one("div.css-13x3qpp-DivUsernameContentWrapper a")
        username = user["href"].lstrip("/") if user else None

        comment_span = item.select_one("span[data-e2e='comment-level-1']")
        comment = comment_span.get_text(strip=True) if comment_span else None

        # Find the sub-content wrapper immediately beneath this comment-item
        sub = item.select_one("div.css-1ivw6bb-DivCommentSubContentSplitWrapper")
        timestamp = None
        comment_likes = None
        if sub:
            # timestamp is the first span with weight-normal
            ts = sub.select_one("span.TUXText--weight-normal")
            timestamp = ts.get_text(strip=True) if ts else None

            # the like-button div has aria-label “Like video\nXXX likes”
            like_div = sub.select_one("div[aria-label^='Like video']")
            if like_div:
                # its child span contains the numeric count
                count = like_div.select_one("span")
                comment_likes = count.get_text(strip=True) if count else None
        print(f"Comment by {username}: {comment} (likes: {comment_likes})")
        rows.append({
            "created_at":     timestamp,
            "username":      username,
            "text":       comment,
            "likes": comment_likes
        })
            
    print(f"Found {len(rows)} comments for video: {video_url}")
    return VideoCommentsResponse(
        post_full_url=video_url,
        list=rows
    )




