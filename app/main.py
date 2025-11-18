# app/main.py
from fastapi import FastAPI
from app.db import init_db_pool
from app.routers import kol

app = FastAPI(title="KOL GMV Scraper")

@app.on_event("startup")
def startup():
    init_db_pool()

app.include_router(kol.router)
