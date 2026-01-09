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
