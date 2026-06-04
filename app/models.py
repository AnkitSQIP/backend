import uuid
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import (
    String, Boolean, DateTime, Integer, Text, Float,
    ForeignKey, UniqueConstraint, func
)
from sqlalchemy.dialects.postgresql import UUID as PgUUID, JSONB, ARRAY
from sqlalchemy.orm import Mapped, mapped_column, relationship
from pgvector.sqlalchemy import Vector
from app.database import Base


def utcnow():
    return datetime.now(timezone.utc)


class User(Base):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    full_name: Mapped[Optional[str]] = mapped_column(String(255))
    role: Mapped[str] = mapped_column(String(20), nullable=False, default="ANALYST")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Workspace(Base):
    __tablename__ = "workspaces"

    id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    workspace_code: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    created_by: Mapped[Optional[uuid.UUID]] = mapped_column(PgUUID(as_uuid=True), ForeignKey("users.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class WorkspaceUser(Base):
    __tablename__ = "workspace_users"
    __table_args__ = (UniqueConstraint("workspace_id", "user_id"),)

    id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    workspace_id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("workspaces.id"), nullable=False)
    user_id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("users.id"), nullable=False)
    role: Mapped[str] = mapped_column(String(50), nullable=False, default="member")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class Patent(Base):
    __tablename__ = "patents"
    __table_args__ = (UniqueConstraint("workspace_id", "patent_number"),)

    id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    workspace_id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("workspaces.id"), nullable=False, index=True)
    patent_number: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    title: Mapped[Optional[str]] = mapped_column(Text)
    abstract: Mapped[Optional[str]] = mapped_column(Text)
    combined_text: Mapped[Optional[str]] = mapped_column(Text)
    assignee: Mapped[Optional[str]] = mapped_column(String(500), index=True)
    inventor: Mapped[Optional[str]] = mapped_column(Text)
    jurisdiction: Mapped[Optional[str]] = mapped_column(String(50))
    filing_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True)
    publication_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), index=True)
    grant_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    priority_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    legal_status: Mapped[Optional[str]] = mapped_column(String(100), index=True)
    cpc_class: Mapped[Optional[str]] = mapped_column(String(200))
    ipc_class: Mapped[Optional[str]] = mapped_column(String(200))
    claims_count: Mapped[Optional[int]] = mapped_column(Integer)
    independent_claims_count: Mapped[Optional[int]] = mapped_column(Integer)
    claims_text: Mapped[Optional[str]] = mapped_column(Text)
    first_claim: Mapped[Optional[str]] = mapped_column(Text)
    patent_family_id: Mapped[Optional[str]] = mapped_column(String(100))
    family_members: Mapped[Optional[str]] = mapped_column(Text)
    family_members_count: Mapped[Optional[int]] = mapped_column(Integer)
    backward_citation_count: Mapped[Optional[int]] = mapped_column(Integer)
    forward_citation_count: Mapped[Optional[int]] = mapped_column(Integer)
    patent_url: Mapped[Optional[str]] = mapped_column(Text)
    publication_country: Mapped[Optional[str]] = mapped_column(String(50))
    num_claims: Mapped[Optional[int]] = mapped_column(Integer)
    review_status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    reviewed_by: Mapped[Optional[uuid.UUID]] = mapped_column(PgUUID(as_uuid=True), ForeignKey("users.id"), nullable=True)
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    review_note: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    embedding: Mapped[Optional[list]] = mapped_column(Vector(768), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class TaxonomyNode(Base):
    __tablename__ = "taxonomy_nodes"

    id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    node_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False, index=True)
    label: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    workspace_id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("workspaces.id"), nullable=False, index=True)
    parent_id: Mapped[Optional[str]] = mapped_column(String(100), ForeignKey("taxonomy_nodes.node_id"), nullable=True)
    level: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class PatentTaxonomy(Base):
    __tablename__ = "patent_taxonomy"
    __table_args__ = (UniqueConstraint("patent_number", "taxonomy_node_id"),)

    id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    patent_number: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    taxonomy_node_id: Mapped[str] = mapped_column(String(100), ForeignKey("taxonomy_nodes.node_id"), nullable=False, index=True)
    taxonomy_label: Mapped[Optional[str]] = mapped_column(String(255))
    assigned_by: Mapped[Optional[str]] = mapped_column(String(255))
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    assigned_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class WatchlistRule(Base):
    __tablename__ = "watchlist_rules"

    id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    workspace_id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("workspaces.id"), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    rule_type: Mapped[str] = mapped_column(String(50), nullable=False)
    rule_config: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_by: Mapped[Optional[uuid.UUID]] = mapped_column(PgUUID(as_uuid=True), ForeignKey("users.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)


class WatchlistAlert(Base):
    __tablename__ = "watchlist_alerts"

    id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    rule_id: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    rule_name: Mapped[Optional[str]] = mapped_column(String(255))
    alert_type: Mapped[Optional[str]] = mapped_column(String(50))
    workspace_id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("workspaces.id"), nullable=False, index=True)
    matched_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    matched_patents: Mapped[Optional[list]] = mapped_column(ARRAY(Text))
    matched_assignees: Mapped[Optional[list]] = mapped_column(ARRAY(Text))
    navigation: Mapped[Optional[dict]] = mapped_column(JSONB)
    is_read: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    read_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


class WorkspaceScope(Base):
    __tablename__ = "workspace_scope"

    workspace_id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("workspaces.id", ondelete="CASCADE"), primary_key=True)
    search_strings: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    taxonomy_node_ids: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    expanded_terms: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    include_all: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    ignore_strings: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    ignore_taxonomy_node_ids: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class DashboardView(Base):
    """A user-saved custom chart for the analytics dashboard.

    `config` holds the full chart spec built in the UI: chart type, dimension,
    measure, filters, bucketing, time window, etc. Scoped per (workspace, user).
    """
    __tablename__ = "dashboard_views"

    id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    workspace_id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("workspaces.id", ondelete="CASCADE"), nullable=False, index=True)
    user_id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    config: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, onupdate=utcnow)


class InvestigationQueueItem(Base):
    __tablename__ = "investigation_queue"

    id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    patent_number: Mapped[str] = mapped_column(String(100), nullable=False)
    workspace_id: Mapped[uuid.UUID] = mapped_column(PgUUID(as_uuid=True), ForeignKey("workspaces.id"), nullable=False)
    assigned_to: Mapped[Optional[uuid.UUID]] = mapped_column(PgUUID(as_uuid=True), ForeignKey("users.id"), nullable=True)
    priority: Mapped[str] = mapped_column(String(20), nullable=False, default="medium")
    reason: Mapped[Optional[str]] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="pending")
    reviewed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)
