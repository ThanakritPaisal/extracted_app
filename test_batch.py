import asyncio
import json

from app.routers.kol import tiktok_profiles_batch

URLS = [
    "https://www.tiktok.com/@coffee_dfn",
    "https://www.tiktok.com/@saebom_cafe",
]

OUTPUT_FILE = "batch_profiles.json"

async def main():
    result = await tiktok_profiles_batch(urls=URLS)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"Saved batch scrape to {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(main())
