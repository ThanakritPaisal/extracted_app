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

from app.routers.kol import tiktok_profile_search_with_posts


async def _run_profile_search(username: str) -> None:
    result = await tiktok_profile_search_with_posts(username=username)
    filename = f"{username}_result.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    print(f"Saved search result to {filename}")
    # print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    target_username = sys.argv[1] if len(sys.argv) > 1 else "khamkhomnews"
    asyncio.run(_run_profile_search(target_username))