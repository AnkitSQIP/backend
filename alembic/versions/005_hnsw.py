"""HNSW vector index for fast cosine similarity search

Revision ID: 005
Revises: 004
Create Date: 2026-04-24

HNSW is better than IVFFlat for <1M vectors: no training needed,
better recall, faster queries. m=16, ef_construction=64 is the
standard production configuration.
"""
from alembic import op
import sqlalchemy as sa

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # CONCURRENTLY must run outside a transaction — autocommit_block handles this.
    # IF NOT EXISTS makes it safe to re-run. On Neon this is a no-op (no embeddings yet);
    # run this migration again after migrating to local Postgres where HNSW is useful.
    with op.get_context().autocommit_block():
        op.execute(sa.text("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_patents_hnsw
            ON patents USING hnsw (embedding vector_cosine_ops)
            WITH (m = 16, ef_construction = 64)
        """))


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(sa.text("DROP INDEX CONCURRENTLY IF EXISTS ix_patents_hnsw"))
