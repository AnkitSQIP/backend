"""add missing indexes for performance

Revision ID: 003
Revises: 002
Create Date: 2026-04-20
"""
from alembic import op

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index("ix_investigation_queue_workspace_id", "investigation_queue", ["workspace_id"])
    op.create_index("ix_investigation_queue_status", "investigation_queue", ["status"])
    op.create_index(
        "ix_investigation_queue_workspace_status",
        "investigation_queue",
        ["workspace_id", "status"],
    )
    op.create_index("ix_patents_review_status", "patents", ["review_status"])
    op.create_index(
        "ix_patents_workspace_review_status",
        "patents",
        ["workspace_id", "review_status"],
    )


def downgrade() -> None:
    op.drop_index("ix_patents_workspace_review_status", "patents")
    op.drop_index("ix_patents_review_status", "patents")
    op.drop_index("ix_investigation_queue_workspace_status", "investigation_queue")
    op.drop_index("ix_investigation_queue_status", "investigation_queue")
    op.drop_index("ix_investigation_queue_workspace_id", "investigation_queue")
