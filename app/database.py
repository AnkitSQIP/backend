from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from app.config import settings


# asyncpg does not accept sslmode= in the URL — SSL must be passed via connect_args.
# Strip any ?sslmode=... or &channel_binding=... from the URL before passing to engine.
import re as _re

_raw_url = settings.database_url
_needs_ssl = "sslmode=require" in _raw_url

# Strip asyncpg-incompatible params from URL
_db_url = _re.sub(r'[?&](sslmode|channel_binding)=[^&]*', '', _raw_url)
_db_url = _db_url.rstrip('?&')

# Neon: SSL required. PgBouncer (local): no SSL, but disable prepared statements
# (asyncpg prepared statements don't work with PgBouncer transaction mode)
_connect_args: dict = {"ssl": "require"} if _needs_ssl else {"ssl": False, "statement_cache_size": 0}

engine = create_async_engine(
    _db_url,
    echo=False,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    connect_args=_connect_args,
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)


class Base(DeclarativeBase):
    pass
