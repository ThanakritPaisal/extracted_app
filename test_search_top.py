import asyncio
import json

from app.routers.kol import search_top_creators

KEYWORD = "coffee"
KEYWORD = "คอนเท้นจัดระเบียบบ้าน ทำความสะอาด คู่รักที่มีคอนเท้นเรื่องงานบ้าน"
LIMIT = 100

async def main():
    result = await search_top_creators(keyword=KEYWORD, limit=LIMIT)
    #filename = f"{KEYWORD}_result.json"
    filename = f"top_creators_list.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"Saved top creators to {filename}")

if __name__ == "__main__":
    asyncio.run(main())
