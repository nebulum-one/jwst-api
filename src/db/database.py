from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base

# ⬅️ IMPORTANT: use config.py instead of raw os.getenv
from src.config import DATABASE_URL

if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL is missing! Check your .env file or Railway variables.")

# Railway sometimes uses old postgres:// format — still good to keep this guard.
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db() -> Session:
    """Dependency for getting database sessions"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Initialize database tables"""
    from src.db.models import Base
    Base.metadata.create_all(bind=engine)
