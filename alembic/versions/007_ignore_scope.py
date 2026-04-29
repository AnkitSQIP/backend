"""Add ignore_strings and ignore_taxonomy_node_ids to workspace_scope

Revision ID: 007
Revises: 006
Create Date: 2026-04-29
"""
from alembic import op

revision = "007"
down_revision = "006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE workspace_scope "
        "ADD COLUMN IF NOT EXISTS ignore_strings JSONB NOT NULL DEFAULT '[]'"
    )
    op.execute(
        "ALTER TABLE workspace_scope "
        "ADD COLUMN IF NOT EXISTS ignore_taxonomy_node_ids JSONB NOT NULL DEFAULT '[]'"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE workspace_scope DROP COLUMN IF EXISTS ignore_strings")
    op.execute("ALTER TABLE workspace_scope DROP COLUMN IF EXISTS ignore_taxonomy_node_ids")
