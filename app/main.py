"""
IPWatch FastAPI application.
Replaces the monolithic server.py with a clean router-based architecture.
"""
import asyncio
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.database import engine
from app.models import Base
from app.routers import users, workspaces, patents, analytics, taxonomy, watchlists

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Squark IP - Patent Intelligence Platform")

# CORS — read allowed origins from CORS_ORIGINS env var (comma-separated)
# Falls back to "*" if not set (safe since credentials are not used)
from app.config import settings as _settings
_cors_origins = [o.strip() for o in _settings.cors_origins.split(",") if o.strip()] or ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routers
app.include_router(users.router)
app.include_router(workspaces.router)
app.include_router(patents.router)
app.include_router(analytics.router)
app.include_router(taxonomy.router)
app.include_router(watchlists.router)


@app.get("/health")
async def root_health():
    return {"status": "healthy"}


@app.get("/api/health")
async def api_health():
    return {"status": "healthy"}


_keepalive_lock = asyncio.Lock()

async def _keepalive_loop():
    """Ping DB every 10s to prevent Neon cold starts. Skips if a ping is already running."""
    from app.database import AsyncSessionLocal
    from sqlalchemy import text
    while True:
        await asyncio.sleep(10)
        if _keepalive_lock.locked():
            continue
        async with _keepalive_lock:
            try:
                async with AsyncSessionLocal() as db:
                    await asyncio.wait_for(db.execute(text("SELECT 1")), timeout=5)
            except Exception:
                pass


@app.on_event("startup")
async def startup():
    # Create tables (safe: does nothing if they already exist)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    await _seed_default_users()
    asyncio.create_task(_keepalive_loop())
    logger.info("IPWatch backend started")


async def _seed_default_users():
    """Create default users on first start if they don't exist."""
    from app.database import AsyncSessionLocal
    from app.models import User
    from app.auth import get_password_hash
    from sqlalchemy import select

    seed_users = [
        {"email": "admin@squarkip.com", "password": "admin", "full_name": "Admin", "role": "ADMIN"},
        {"email": "basant@squarkip.com", "password": "analystpassword", "full_name": "Basant Analyst", "role": "ANALYST"},
    ]

    async with AsyncSessionLocal() as db:
        for u in seed_users:
            existing = await db.scalar(select(User).where(User.email == u["email"]))
            if not existing:
                db.add(User(
                    email=u["email"],
                    password_hash=get_password_hash(u["password"]),
                    full_name=u["full_name"],
                    role=u["role"],
                    is_active=True,
                ))
        await db.commit()
    logger.info("Default users seeded")
