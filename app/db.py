# app/db.py
import os
import psycopg2
from psycopg2 import pool

# DATABASE_URL = os.getenv(
#     "DATABASE_URL",
#     "postgresql://doadmin:…@gradient-kol-do-user-…?sslmode=require"
# )

DATABASE_URL = os.getenv(
    "DATABASE_URL"
    
)

# Create a thread-safe pool at startup
db_pool: pool.ThreadedConnectionPool = None

def init_db_pool():
    global db_pool
    db_pool = psycopg2.pool.ThreadedConnectionPool(
        minconn=1, maxconn=5, dsn=DATABASE_URL
    )

def get_conn():
    """
    FastAPI dependency: yields a connection, then returns it to the pool.
    """
    conn = db_pool.getconn()
    try:
        yield conn
    finally:
        db_pool.putconn(conn)
