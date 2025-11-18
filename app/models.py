from sqlalchemy import Column, BigInteger, Text
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class TikTokCookie(Base):
    __tablename__ = "tiktok_cookie"
    id     = Column(BigInteger, primary_key=True, index=True)
    cookie = Column(Text, nullable=False)
