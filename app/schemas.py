from pydantic import BaseModel, EmailStr, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
import uuid


# ============ AUTH / USER SCHEMAS ============

class UserLogin(BaseModel):
    email: str
    password: str


class UserCreate(BaseModel):
    email: str
    password: str
    full_name: str
    role: str


class UserOut(BaseModel):
    id: str
    email: str
    full_name: Optional[str] = None
    role: str
    is_active: bool

    model_config = {"from_attributes": True}


class TokenResponse(BaseModel):
    access_token: str
    token_type: str
    user: UserOut


# ============ WORKSPACE SCHEMAS ============

class WorkspaceCreate(BaseModel):
    name: str
    description: Optional[str] = None


class WorkspaceOut(BaseModel):
    id: str
    name: str
    workspace_code: str
    description: Optional[str] = None
    patent_count: Optional[int] = 0

    model_config = {"from_attributes": True}


class WorkspaceMemberAdd(BaseModel):
    user_id: str
    role: str = "member"


class WorkspaceMemberOut(BaseModel):
    user_id: str
    email: Optional[str]
    full_name: Optional[str]
    role: str
    user_role: Optional[str]


# ============ PATENT SCHEMAS ============

class PatentListItem(BaseModel):
    id: str
    patent_number: str
    title: Optional[str]
    assignee: Optional[str]
    filing_date: Optional[str]
    publication_date: Optional[str]
    legal_status: Optional[str]


class PatentDetail(BaseModel):
    id: str
    patent_number: str
    title: Optional[str]
    abstract: Optional[str]
    assignee: Optional[str]
    inventor: Optional[str]
    jurisdiction: Optional[str]
    filing_date: Optional[str]
    publication_date: Optional[str]
    grant_date: Optional[str]
    priority_date: Optional[str]
    legal_status: Optional[str]
    cpc_class: Optional[str]
    ipc_class: Optional[str]
    claims_count: Optional[int]
    independent_claims_count: Optional[int]
    first_claim: Optional[str]
    claims_text: Optional[str]
    patent_family_id: Optional[str]
    family_members: Optional[str]
    family_members_count: Optional[int]
    backward_citation_count: Optional[int]
    forward_citation_count: Optional[int]
    patent_url: Optional[str]
    publication_country: Optional[str]
    num_claims: Optional[int]
    review_status: Optional[str]
    taxonomy: Optional[List[dict]] = []


class PatentListResponse(BaseModel):
    patents: List[PatentListItem]
    total: int
    skip: int
    limit: int


class UploadResponse(BaseModel):
    message: str
    created: int
    skipped_duplicates: int
    total_in_file: int
    alerts_generated: int
    duplicate_examples: Optional[List[str]] = None


# ============ TAXONOMY SCHEMAS ============

class TaxonomyNodeCreate(BaseModel):
    node_id: str
    label: str
    workspace_id: str
    parent_id: Optional[str] = None
    level: int = 0


class TaxonomyNodeOut(BaseModel):
    node_id: str
    label: str
    parent_id: Optional[str]
    level: int
    workspace_id: str


class TaxonomyAssignmentOut(BaseModel):
    id: str
    patent_number: str
    taxonomy_node_id: str
    taxonomy_label: Optional[str]
    assigned_by: Optional[str]
    assigned_at: Optional[str]


# ============ WATCHLIST SCHEMAS ============

class WatchlistRuleCreate(BaseModel):
    workspace_id: str
    rule_type: str
    rule_config: str  # JSON string from form


class WatchlistRuleOut(BaseModel):
    id: str
    name: str
    workspace_id: str
    rule_type: str
    rule_config: dict
    is_active: bool
    created_by: Optional[str]
    created_at: Optional[str]


class WatchlistAlertOut(BaseModel):
    id: str
    rule_id: str
    rule_name: Optional[str]
    alert_type: Optional[str]
    alert_title: str
    matched_count: int
    matched_patents: List[str]
    matched_assignees: List[str]
    navigation: Optional[dict]
    workspace_id: str
    created_at: Optional[str]
    is_read: bool


# ============ INVESTIGATION QUEUE SCHEMAS ============

class QueueItemCreate(BaseModel):
    patent_number: str
    workspace_id: str
    priority: str = "medium"
    reason: Optional[str] = None


class QueueItemOut(BaseModel):
    id: str
    patent_number: str
    workspace_id: str
    priority: str
    reason: Optional[str]
    status: str


# ============ SEARCH SCHEMAS ============

class SearchResult(BaseModel):
    patent_number: str
    title: Optional[str]
    abstract: Optional[str]
    assignee: Optional[str]
    publication_date: Optional[str]
    legal_status: Optional[str]
    patent_url: Optional[str]
