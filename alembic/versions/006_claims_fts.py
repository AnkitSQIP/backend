"""Expand search_vector to include claims_text (weight D)

Revision ID: 006
Revises: 005
Create Date: 2026-04-29
"""
from alembic import op

revision = "006"
down_revision = "005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Drop GIN index on search_vector first (index must go before column)
    op.execute("DROP INDEX IF EXISTS ix_patents_fts")
    # Drop the generated column (cannot ALTER GENERATED columns)
    op.execute("ALTER TABLE patents DROP COLUMN IF EXISTS search_vector")
    # Re-add with claims_text at weight D (lowest priority)
    op.execute("""
        ALTER TABLE patents ADD COLUMN search_vector tsvector
        GENERATED ALWAYS AS (
            setweight(to_tsvector('english', coalesce(title, '')), 'A') ||
            setweight(to_tsvector('english', coalesce(abstract, '')), 'B') ||
            setweight(to_tsvector('english', coalesce(first_claim, '')), 'C') ||
            setweight(to_tsvector('english', coalesce(claims_text, '')), 'D')
        ) STORED
    """)
    op.execute("CREATE INDEX ix_patents_fts ON patents USING GIN(search_vector)")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_patents_fts")
    op.execute("ALTER TABLE patents DROP COLUMN IF EXISTS search_vector")
    op.execute("""
        ALTER TABLE patents ADD COLUMN search_vector tsvector
        GENERATED ALWAYS AS (
            setweight(to_tsvector('english', coalesce(title, '')), 'A') ||
            setweight(to_tsvector('english', coalesce(abstract, '')), 'B') ||
            setweight(to_tsvector('english', coalesce(first_claim, '')), 'C')
        ) STORED
    """)
    op.execute("CREATE INDEX ix_patents_fts ON patents USING GIN(search_vector)")
