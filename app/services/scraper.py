# app/services/scraper.py
import json
import requests
from psycopg2.extras import execute_values
from bs4 import BeautifulSoup
from app.config import TIKTOK_URL, HEADERS, BASE_PARAMS, CATEGORY_CONFIG, CONTACT_URL, CONTACT_PARAMS, SUGGEST_URL, SUGGEST_PARAMS, PROFILE_URL, PROFILE_PARAMS, USER_DETAIL_URL, USER_DETAIL_PARAMS
import re
from app.config_kalo_category import KALO_CATEGORY_CONFIG
from datetime import datetime
import calendar

def fetch_cookie(conn) -> str:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM tiktok_cookie LIMIT 1")
        row = cur.fetchone()
        if not row:
            raise RuntimeError("No TikTok cookie in DB")
        return row[1]

def user_detail(user_id) -> list:

    HEADERS["Cookie"] = "appVersion=2.0; _fbp=fb.1.1709114027746.1761991795; _ga=GA1.1.626448943.1715654259; _tt_enable_cookie=1; _ga_Q21FRKKG88=deleted; _ttp=71MLCAzCYadBlYdwyNckNJUVhAd.tt.1; deviceType=pc; AGL_USER_ID=ce0b5598-4acc-45a3-a29d-80e0d9f5355d; deviceId=8b998e610e4b93c91482408e0ed2c787; _bl_uid=jam2X7vCsOOcnXsRw5R9nLC8Fpjm; _c_WBKFRo=sdD4AasaWjGl1o447C2UiFMB8PdIy4LDlnFIMmoJ; SESSION=NTY5YTM1ZmYtNzQ5NC00Mzc1LTg3YTYtY2M3Yzg3NjQ3NmFl; _gcl_au=1.1.707545305.1747804965.604770605.1750733031.1750733030; _clck=1olr74l%7C2%7Cfxg%7C0%7C1877; page_session=efa48960-a11b-4365-9552-db4548b787ef; Hm_lvt_8aa1693861618ac63989ae373e684811=1751857420,1751872441,1752027559,1752097402; Hm_lpvt_8aa1693861618ac63989ae373e684811=1752097402; HMACCOUNT=573BF2868BF9E327; _clsk=lwr3h7%7C1752097443108%7C4%7C1%7Cn.clarity.ms%2Fcollect; _ga_Q21FRKKG88=GS2.1.s1752097400$o75$g1$t1752097629$j56$l0$h0; _uetsid=1e4b4c905c6b11f08b09654bcc52aed7; _uetvid=ffe876c0efc211ef9a1aa181645708c3; ttcsid=1752097402483::qZFworVLaJcUEa4_Qjz9.26.1752097629278; ttcsid_CM9SHDBC77U4KJBR96OG=1752097402482::0_Ko_QFgO3DBWzQDKIRW.26.1752097629568"
    today = datetime.now().strftime("%Y-%m-%d")
    body = {
        "id": user_id,
        "cateIds": [],
        "startDate": "2024-01-01",
        "endDate": today,
        "sellerId": "",
        "authority": True
    }
    resp = requests.post(
        "https://www.kalodata.com/creator/detail", headers=HEADERS, json=body)
    resp.raise_for_status()
    return resp.json()


def kalodata_scraper(conn, pages: int = 10, type: str = "Pet Supplies", followers_filter: str = "5000-8000") -> list:

    HEADERS["Cookie"] = "appVersion=2.0; _fbp=fb.1.1709114027746.1761991795; _ga=GA1.1.626448943.1715654259; _tt_enable_cookie=1; _ga_Q21FRKKG88=deleted; _ttp=71MLCAzCYadBlYdwyNckNJUVhAd.tt.1; deviceType=pc; AGL_USER_ID=ce0b5598-4acc-45a3-a29d-80e0d9f5355d; deviceId=8b998e610e4b93c91482408e0ed2c787; _bl_uid=jam2X7vCsOOcnXsRw5R9nLC8Fpjm; _c_WBKFRo=sdD4AasaWjGl1o447C2UiFMB8PdIy4LDlnFIMmoJ; SESSION=NTY5YTM1ZmYtNzQ5NC00Mzc1LTg3YTYtY2M3Yzg3NjQ3NmFl; _gcl_au=1.1.707545305.1747804965.604770605.1750733031.1750733030; _clck=1olr74l%7C2%7Cfxg%7C0%7C1877; page_session=efa48960-a11b-4365-9552-db4548b787ef; Hm_lvt_8aa1693861618ac63989ae373e684811=1751857420,1751872441,1752027559,1752097402; Hm_lpvt_8aa1693861618ac63989ae373e684811=1752097402; HMACCOUNT=573BF2868BF9E327; _clsk=lwr3h7%7C1752097443108%7C4%7C1%7Cn.clarity.ms%2Fcollect; _ga_Q21FRKKG88=GS2.1.s1752097400$o75$g1$t1752097629$j56$l0$h0; _uetsid=1e4b4c905c6b11f08b09654bcc52aed7; _uetvid=ffe876c0efc211ef9a1aa181645708c3; ttcsid=1752097402483::qZFworVLaJcUEa4_Qjz9.26.1752097629278; ttcsid_CM9SHDBC77U4KJBR96OG=1752097402482::0_Ko_QFgO3DBWzQDKIRW.26.1752097629568"
    cate_id = None
    for category in KALO_CATEGORY_CONFIG:
        if category["label"] == type:
            cate_id = category["value"]
            break
    
    enriched_data = []

    # Calculate date range: startDate = today - 1 month (clamped), endDate = today
    today_date = datetime.now().date()
    prev_year = today_date.year if today_date.month > 1 else today_date.year - 1
    prev_month = today_date.month - 1 if today_date.month > 1 else 12
    last_day_prev_month = calendar.monthrange(prev_year, prev_month)[1]
    start_date = today_date.replace(
        year=prev_year,
        month=prev_month,
        day=min(today_date.day, last_day_prev_month)
    )
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = today_date.strftime("%Y-%m-%d")
    for i in range(1, pages + 1):
        body = {
        "country": "TH",
        "startDate": start_date_str,
        "endDate": end_date_str,
        "creator.filter.followers": followers_filter,
        "cateIds": [
            cate_id
        ],
        "showCateIds": [
            cate_id
        ],
        "pageNo": i,
        "pageSize": 10,
        "sort": [
            {
            "field": "revenue",
            "type": "DESC"
            }
        ]
        }
        resp = requests.post(
            "https://www.kalodata.com/creator/queryList", headers=HEADERS, json=body)
        resp.raise_for_status()
        page_data = resp.json()

        if page_data.get("success") and "data" in page_data:
            for creator in page_data["data"]:
                user_id = creator.get("id")
                print(user_id)
                if user_id:
                    print(f"Fetching details for user ID: {user_id}")
                    try:
                        detail = user_detail(user_id)
                        enriched_data.append({
                            "summary": creator,
                            "detail": detail
                        })
                    except Exception as e:
                        print(f"Failed to fetch detail for {user_id}: {e}")
        else:
            print(f"No valid data on page {i}")

    return enriched_data


def scrape_and_store(conn, pages: int = 10) -> int:
    """
    Scrape `pages` pages and insert into kol_gmv.
    Returns total rows inserted.
    """
    cookie = fetch_cookie(conn)
    HEADERS["Cookie"] = cookie

    to_insert = []
    total = 0

    for page in range(1, pages + 1):
        body = {
            "query": "",
            "pagination": {"size": 12, "page": page},
            "filter_params": {},
            "algorithm": 18
        }
        resp = requests.post(TIKTOK_URL, headers=HEADERS,
                             params=BASE_PARAMS, json=body)
        resp.raise_for_status()
        data = resp.json().get("creator_profile_list", [])

        for item in data:
            creator_id = item["creator_oecuid"]["value"]
            raw_json   = json.dumps(item)
            video_gmv  = item.get("video_gmv", {}).get("value", {}).get("value", "")
            live_gmv   = item.get("live_gmv", {}).get("value", {}).get("value", "")
            user_id = item.get("handle", {}).get("value", "")
            to_insert.append((creator_id, raw_json, video_gmv, live_gmv, user_id))

        total += len(data)

    # bulk insert
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO public.kol_gmv
              (creator_id, raw_data, video_gmv, live_gmv, user_id)
            VALUES %s
        """, to_insert)
    conn.commit()
    return total


def fetch_contact_types(creator_id: str) -> list:
    """
    Fetches available contact types for a given creator.
    Returns list of type definitions (e.g., fields and titles).
    Handles cases where the response body is empty.
    """
    resp = requests.get(
        CONTACT_URL,
        headers=HEADERS,
        params={**CONTACT_PARAMS, "creator_oecuid": creator_id}
    )
    print(resp)
    resp.raise_for_status()

    # Parse JSON safely
    try:
        data = resp.json()
    except ValueError as e:
        print(f"Failed to parse contact types JSON for {creator_id}: {e}")
        return []

    return data
    

def scrape_and_return(conn, kol_type: str, pages: int = 10, left_bound: int = 5000, right_bound: int = 200000) -> list:
    """
    Scrape `pages` pages for a given category and return enriched records.
    """
    cookie = fetch_cookie(conn)
    HEADERS["Cookie"] = cookie

    to_insert = []
    cat_list = CATEGORY_CONFIG.get(kol_type, [])
    print(f"Scraping {kol_type} with categories: {cat_list}")
    for page in range(1, pages + 1):
        body = {
            "query": "",
            "pagination": {
                "size": 12,
                "next_item_cursor": page * 12,
                "page": page
            },
            "filter_params": {
                "category_list": cat_list,
                "follower_filter": {
                    "left_bound": int(left_bound),
                    "right_bound": int(right_bound)
                }
            },
            "algorithm": 1
        }
        resp = requests.post(
            TIKTOK_URL,
            headers=HEADERS,
            params=BASE_PARAMS,
            json=body
        )
        resp.raise_for_status()
        creator_list = resp.json().get("creator_profile_list", [])

        for item in creator_list:
            creator_id = item.get("creator_oecuid", {}).get("value")

            # Fetch contact info
            contact_data = []
            try:
                contact_data = fetch_contact_types(creator_id)
            except requests.HTTPError as e:
                print(f"Failed to fetch contact types for {creator_id}: {e}")
            
            item["contact_info"] = contact_data
            to_insert.append(item)

    return to_insert

def fetch_creators(conn, query: str = "", page: int = 1, size: int = 20) -> list[dict]:
    """
    Fetch creators via the SUGGEST API and return only those
    whose handle.value exactly equals the query.
    """
    cookie = fetch_cookie(conn)
    HEADERS["Cookie"] = cookie

    body = {
        "request": {
            "query": query,
            "sug_scene": page,
            "size": size
        }
    }

    resp = requests.post(
        SUGGEST_URL,
        headers=HEADERS,
        params=SUGGEST_PARAMS,
        json=body
    )
    resp.raise_for_status()
    data = resp.json().get("data", {}).get("sug_contents", [])

    # filter for exact handle match
    matches = []
    for item in data:
        creator = item.get("creator", {})
        handle = creator.get("handle", {}).get("value", "")
        if handle == query:
            creator_oecuid = creator.get("creator_oecuid", {}).get("value", "")
            profile_body = {
                "creator_oec_id": creator_oecuid,
                "profile_types": [
                    2
                ]
            }
            resp_profile = requests.post(
                PROFILE_URL,
                headers=HEADERS,
                params=PROFILE_PARAMS,
                json=profile_body
            )
            
            resp_profile.raise_for_status()
            creator['profile'] = resp_profile.json()


            USER_DETAIL_PARAMS['uniqueId'] = handle
            resp_user_info = requests.get(
                USER_DETAIL_URL,
                params=USER_DETAIL_PARAMS,
                headers=HEADERS
            )

            resp_user_info.raise_for_status()
            creator['user_info'] = resp_user_info

            matches.append(creator)

    if data == []:
        USER_DETAIL_PARAMS['uniqueId'] = query
        resp_user_info = requests.get(
            USER_DETAIL_URL,
            params=USER_DETAIL_PARAMS,
            headers=HEADERS
        )

        content_type = resp.headers.get("Content-Type", "")
        print("Content-Type:", content_type)
        snippet = resp.text[:300].replace("\n", "\\n")
        print("Body snippet:", snippet)

        # 3) Only parse JSON if it looks like JSON
        if "application/json" in content_type:
            try:
                user_info = resp.json()
                if user_info:              # not empty dict/list
                    matches.append(user_info)
                else:
                    print("→ empty JSON, no data for this user.")
            except ValueError:
                print("→ JSON parse failed despite JSON content-type.")
        else:
            print("→ Non-JSON response, skipping.")


    return matches

def parse_video_stats(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    # helper to find data-e2e spans (or other tags)
    def extract_by_attr(name: str):
        el = soup.find(attrs={"data-e2e": name})
        return el.get_text(strip=True) if el else None

    likes    = extract_by_attr("like-count")
    comments = extract_by_attr("comment-count")
    shares   = extract_by_attr("share-count")

    # fallback: meta description "54.1K Likes, 219 Comments, 24.1K Shares..."
    if not (likes and comments and shares):
        m = soup.find("meta", {"name": "description"})
        if m and m.get("content"):
            desc = m["content"]
            nums = re.findall(
                r"([\d,.KM]+)\s+Likes.*?([\d,.KM]+)\s+Comments.*?([\d,.KM]+)\s+Shares",
                desc,
            )
            if nums:
                l, c, s = nums[0]
                likes    = likes    or l
                comments = comments or c
                shares   = shares   or s

    # extract the raw caption (hashtags + text) from BreadcrumbList JSON-LD
    hashtag_str = None
    tag = soup.find("script", id="BreadcrumbList")
    if tag:
        try:
            bc = json.loads(tag.string)
            hashtag_str = bc["itemListElement"][-1]["item"]["name"]
        except Exception:
            pass

    # now pull *only* the #tags out of that string
    hashtag_list = re.findall(r"#\S+", hashtag_str or "")

    # extract the 'created_at' timestamp from profile header
    created_at = None
    nick = soup.find(attrs={"data-e2e": "browser-nickname"})
    if nick:
        parts = nick.find_all("span")
        if parts:
            created_at = parts[-1].get_text(strip=True)

    img = soup.select_one("div.css-sq145r picture img")
    cover = img and img.get("src")

    return {
        "created_at": created_at,
        "url":        url,
        "text":       hashtag_str,
        "cover": cover,
        "likes":      likes,
        "comments":   comments,
        "shares":     shares,
        "views":      None,
        "hashtags":   hashtag_list,
    }
