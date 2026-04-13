"""initial schema

Revision ID: 001
Revises:
Create Date: 2026-03-26
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB, ARRAY

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Enable pgvector extension
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    # users
    op.create_table(
        "users",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("email", sa.String(255), nullable=False, unique=True),
        sa.Column("password_hash", sa.String(255), nullable=False),
        sa.Column("full_name", sa.String(255)),
        sa.Column("role", sa.String(20), nullable=False, server_default="ANALYST"),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_users_email", "users", ["email"], unique=True)

    # workspaces
    op.create_table(
        "workspaces",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("workspace_code", sa.String(50), nullable=False, unique=True),
        sa.Column("description", sa.Text),
        sa.Column("created_by", UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # workspace_users
    op.create_table(
        "workspace_users",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("workspace_id", UUID(as_uuid=True), sa.ForeignKey("workspaces.id"), nullable=False),
        sa.Column("user_id", UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=False),
        sa.Column("role", sa.String(50), nullable=False, server_default="member"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("workspace_id", "user_id"),
    )

    # patents
    op.create_table(
        "patents",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("workspace_id", UUID(as_uuid=True), sa.ForeignKey("workspaces.id"), nullable=False),
        sa.Column("patent_number", sa.String(100), nullable=False),
        sa.Column("title", sa.Text),
        sa.Column("abstract", sa.Text),
        sa.Column("combined_text", sa.Text),
        sa.Column("assignee", sa.String(500)),
        sa.Column("inventor", sa.Text),
        sa.Column("jurisdiction", sa.String(50)),
        sa.Column("filing_date", sa.DateTime(timezone=True)),
        sa.Column("publication_date", sa.DateTime(timezone=True)),
        sa.Column("grant_date", sa.DateTime(timezone=True)),
        sa.Column("priority_date", sa.DateTime(timezone=True)),
        sa.Column("legal_status", sa.String(100)),
        sa.Column("cpc_class", sa.String(200)),
        sa.Column("ipc_class", sa.String(200)),
        sa.Column("claims_count", sa.Integer),
        sa.Column("independent_claims_count", sa.Integer),
        sa.Column("claims_text", sa.Text),
        sa.Column("first_claim", sa.Text),
        sa.Column("patent_family_id", sa.String(100)),
        sa.Column("family_members", sa.Text),
        sa.Column("family_members_count", sa.Integer),
        sa.Column("backward_citation_count", sa.Integer),
        sa.Column("forward_citation_count", sa.Integer),
        sa.Column("patent_url", sa.Text),
        sa.Column("publication_country", sa.String(50)),
        sa.Column("num_claims", sa.Integer),
        sa.Column("review_status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column("reviewed_by", UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=True),
        sa.Column("reviewed_at", sa.DateTime(timezone=True)),
        sa.Column("embedding", sa.Text),  # placeholder — replaced below
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("workspace_id", "patent_number"),
    )
    # Replace placeholder column with real vector type
    op.execute("ALTER TABLE patents DROP COLUMN embedding")
    op.execute("ALTER TABLE patents ADD COLUMN embedding vector(768)")

    op.create_index("ix_patents_workspace_id", "patents", ["workspace_id"])
    op.create_index("ix_patents_patent_number", "patents", ["patent_number"])
    op.create_index("ix_patents_assignee", "patents", ["assignee"])
    op.create_index("ix_patents_filing_date", "patents", ["filing_date"])
    op.create_index("ix_patents_publication_date", "patents", ["publication_date"])
    op.create_index("ix_patents_legal_status", "patents", ["legal_status"])

    # taxonomy_nodes
    op.create_table(
        "taxonomy_nodes",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("node_id", sa.String(100), nullable=False, unique=True),
        sa.Column("label", sa.String(255), nullable=False),
        sa.Column("workspace_id", UUID(as_uuid=True), sa.ForeignKey("workspaces.id"), nullable=False),
        sa.Column("parent_id", sa.String(100), sa.ForeignKey("taxonomy_nodes.node_id"), nullable=True),
        sa.Column("level", sa.Integer, nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_taxonomy_nodes_node_id", "taxonomy_nodes", ["node_id"], unique=True)
    op.create_index("ix_taxonomy_nodes_workspace_id", "taxonomy_nodes", ["workspace_id"])

    # patent_taxonomy
    op.create_table(
        "patent_taxonomy",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("patent_number", sa.String(100), nullable=False),
        sa.Column("taxonomy_node_id", sa.String(100), sa.ForeignKey("taxonomy_nodes.node_id"), nullable=False),
        sa.Column("taxonomy_label", sa.String(255)),
        sa.Column("assigned_by", sa.String(255)),
        sa.Column("confidence", sa.Float, nullable=False, server_default="1.0"),
        sa.Column("assigned_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("patent_number", "taxonomy_node_id"),
    )
    op.create_index("ix_patent_taxonomy_patent_number", "patent_taxonomy", ["patent_number"])
    op.create_index("ix_patent_taxonomy_node_id", "patent_taxonomy", ["taxonomy_node_id"])

    # watchlist_rules
    op.create_table(
        "watchlist_rules",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("workspace_id", UUID(as_uuid=True), sa.ForeignKey("workspaces.id"), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("rule_type", sa.String(50), nullable=False),
        sa.Column("rule_config", JSONB, nullable=False, server_default="{}"),
        sa.Column("is_active", sa.Boolean, nullable=False, server_default="true"),
        sa.Column("created_by", UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_watchlist_rules_workspace_id", "watchlist_rules", ["workspace_id"])

    # watchlist_alerts
    op.create_table(
        "watchlist_alerts",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("rule_id", sa.String(50), nullable=False),
        sa.Column("rule_name", sa.String(255)),
        sa.Column("alert_type", sa.String(50)),
        sa.Column("workspace_id", UUID(as_uuid=True), sa.ForeignKey("workspaces.id"), nullable=False),
        sa.Column("matched_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("matched_patents", ARRAY(sa.Text)),
        sa.Column("matched_assignees", ARRAY(sa.Text)),
        sa.Column("navigation", JSONB),
        sa.Column("is_read", sa.Boolean, nullable=False, server_default="false"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True)),
        sa.Column("read_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_watchlist_alerts_rule_id", "watchlist_alerts", ["rule_id"])
    op.create_index("ix_watchlist_alerts_workspace_id", "watchlist_alerts", ["workspace_id"])

    # investigation_queue
    op.create_table(
        "investigation_queue",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("patent_number", sa.String(100), nullable=False),
        sa.Column("workspace_id", UUID(as_uuid=True), sa.ForeignKey("workspaces.id"), nullable=False),
        sa.Column("assigned_to", UUID(as_uuid=True), sa.ForeignKey("users.id"), nullable=True),
        sa.Column("priority", sa.String(20), nullable=False, server_default="medium"),
        sa.Column("reason", sa.Text),
        sa.Column("status", sa.String(20), nullable=False, server_default="pending"),
        sa.Column("reviewed_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )

    # pgvector ivfflat index — created after table, needs data to train
    # Run: SELECT create_ivfflat_index() after uploading first batch of patents
    # op.execute(
    #     "CREATE INDEX patents_embedding_idx ON patents USING ivfflat (embedding vector_cosine_ops) WITH (lists=100)"
    # )
    # Uncomment after first patent batch is loaded


def downgrade() -> None:
    op.drop_table("investigation_queue")
    op.drop_table("watchlist_alerts")
    op.drop_table("watchlist_rules")
    op.drop_table("patent_taxonomy")
    op.drop_table("taxonomy_nodes")
    op.drop_table("patents")
    op.drop_table("workspace_users")
    op.drop_table("workspaces")
    op.drop_table("users")
    op.execute("DROP EXTENSION IF EXISTS vector")
