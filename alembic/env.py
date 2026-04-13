import os
import asyncio
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
from logging.config import fileConfig
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config
from alembic import context

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Override sqlalchemy.url from environment
database_url = os.environ.get("DATABASE_URL", "")
db_connect_args = {}
if database_url:
    parsed = urlsplit(database_url)
    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)

    # asyncpg does not support sslmode in URL query params.
    sslmode_value = None
    filtered_pairs = []
    for key, value in query_pairs:
        key_lower = key.lower()
        if key_lower == "sslmode":
            sslmode_value = value.lower()
            continue
        if key_lower == "channel_binding":
            continue
        filtered_pairs.append((key, value))

    if sslmode_value in {"require", "verify-ca", "verify-full"}:
        db_connect_args["ssl"] = "require"

    sanitized_url = urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, urlencode(filtered_pairs), parsed.fragment)
    )
    database_url = sanitized_url
    config.set_main_option("sqlalchemy.url", database_url)

# Import all models so Alembic can detect them
from app.database import Base
import app.models  # noqa: F401 — registers all ORM models

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        connect_args=db_connect_args,
    )
    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await connectable.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
