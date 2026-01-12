import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

POSTGRES_DSN = os.getenv("POSTGRES_DSN")  # if None, DB is disabled

engine = None
SessionLocal = None

def init_db():
    global engine, SessionLocal
    if not POSTGRES_DSN:
        return False
    engine = create_engine(POSTGRES_DSN, pool_pre_ping=True)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    return True
