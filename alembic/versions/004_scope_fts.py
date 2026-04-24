"""workspace_scope table, FTS search_vector, pg_trgm, partial indexes

Revision ID: 004
Revises: 003
Create Date: 2026-04-24
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB

revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Enable trigram extension for fuzzy search
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")

    # workspace_scope: per-workspace scope filter state
    op.create_table(
        "workspace_scope",
        sa.Column("workspace_id", UUID(as_uuid=True), sa.ForeignKey("workspaces.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("search_strings", JSONB, nullable=False, server_default="[]"),
        sa.Column("taxonomy_node_ids", JSONB, nullable=False, server_default="[]"),
        sa.Column("expanded_terms", JSONB, nullable=False, server_default="[]"),
        sa.Column("include_all", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # Generated FTS column on patents (auto-maintained by Postgres — no trigger needed)
    # Weighted: title=A (highest), abstract=B, first_claim=C
    op.execute("""
        ALTER TABLE patents ADD COLUMN search_vector tsvector
        GENERATED ALWAYS AS (
            setweight(to_tsvector('english', coalesce(title, '')), 'A') ||
            setweight(to_tsvector('english', coalesce(abstract, '')), 'B') ||
            setweight(to_tsvector('english', coalesce(first_claim, '')), 'C')
        ) STORED
    """)

    # GIN index on FTS vector — fast @@ queries
    op.execute("CREATE INDEX ix_patents_fts ON patents USING GIN(search_vector)")

    # Trigram index on title — fuzzy similarity search
    op.execute("CREATE INDEX ix_patents_title_trgm ON patents USING GIN(title gin_trgm_ops)")

    # Partial indexes — only scan pending/reviewed subset per workspace
    # These replace ix_patents_workspace_review_status for pending/reviewed queries
    op.execute("""
        CREATE INDEX ix_patents_pending ON patents(workspace_id, updated_at DESC)
        WHERE review_status = 'pending'
    """)
    op.execute("""
        CREATE INDEX ix_patents_reviewed ON patents(workspace_id, updated_at DESC)
        WHERE review_status = 'reviewed'
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_patents_reviewed")
    op.execute("DROP INDEX IF EXISTS ix_patents_pending")
    op.execute("DROP INDEX IF EXISTS ix_patents_title_trgm")
    op.execute("DROP INDEX IF EXISTS ix_patents_fts")
    op.execute("ALTER TABLE patents DROP COLUMN IF EXISTS search_vector")
    op.drop_table("workspace_scope")
