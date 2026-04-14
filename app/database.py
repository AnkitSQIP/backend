from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from app.config import settings


# asyncpg does not accept sslmode= in the URL — SSL must be passed via connect_args.
# Strip any ?sslmode=... or &channel_binding=... from the URL before passing to engine.
import re as _re
_db_url = _re.sub(r'[?&](sslmode|channel_binding)=[^&]*', '', settings.database_url)
_db_url = _db_url.rstrip('?&')

engine = create_async_engine(
    _db_url,
    echo=False,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    connect_args={"ssl": "require"},
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
