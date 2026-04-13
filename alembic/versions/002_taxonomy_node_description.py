"""add description to taxonomy_nodes

Revision ID: 002
Revises: 001
Create Date: 2026-04-08
"""
from alembic import op
import sqlalchemy as sa

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "taxonomy_nodes",
        sa.Column("description", sa.Text, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("taxonomy_nodes", "description")
