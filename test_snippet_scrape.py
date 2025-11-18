import undetected_chromedriver as uc
# Patch fragile destructor to avoid WinError 6 during interpreter shutdown on Windows
if hasattr(uc.Chrome, '__del__'):
    try:
        uc.Chrome.__del__ = lambda self: None
    except Exception:
        pass

import asyncio
import json
import sys

from app.routers.kol import search_creator_snippets, search_users_api


async def _run_search(keyword: str) -> None:
    #result = await search_creator_snippets(keyword=keyword)
    result = await search_users_api(keyword=keyword)
    filename = f"{keyword}_result.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    print(f"Saved search result to {filename}")

if __name__ == "__main__":
    search_keyword = sys.argv[1] if len(sys.argv) > 1 else "coffee"
    asyncio.run(_run_search(search_keyword))