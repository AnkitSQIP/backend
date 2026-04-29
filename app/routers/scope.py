"""Workspace scope CRUD endpoints."""
import logging
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.deps import get_db, get_current_user
from app.services.scope import get_scope, save_scope

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["scope"])


class ScopeUpdate(BaseModel):
    search_strings: list[str] = []
    taxonomy_node_ids: list[str] = []
    include_all: bool = True
    ignore_strings: list[str] = []
    ignore_taxonomy_node_ids: list[str] = []


class ScopeResponse(BaseModel):
    workspace_id: str
    search_strings: list[str]
    taxonomy_node_ids: list[str]
    expanded_terms: list[str]
    include_all: bool
    ignore_strings: list[str]
    ignore_taxonomy_node_ids: list[str]


def _to_response(scope) -> ScopeResponse:
    return ScopeResponse(
        workspace_id=str(scope.workspace_id),
        search_strings=scope.search_strings or [],
        taxonomy_node_ids=scope.taxonomy_node_ids or [],
        expanded_terms=scope.expanded_terms or [],
        include_all=scope.include_all,
        ignore_strings=scope.ignore_strings or [],
        ignore_taxonomy_node_ids=scope.ignore_taxonomy_node_ids or [],
    )


@router.get("/workspaces/{workspace_id}/scope", response_model=ScopeResponse)
async def get_workspace_scope(
    workspace_id: str,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    scope = await get_scope(workspace_id, db)
    return _to_response(scope)


@router.put("/workspaces/{workspace_id}/scope", response_model=ScopeResponse)
async def update_workspace_scope(
    workspace_id: str,
    body: ScopeUpdate,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    scope = await save_scope(
        workspace_id=workspace_id,
        search_strings=body.search_strings,
        taxonomy_node_ids=body.taxonomy_node_ids,
        include_all=body.include_all,
        ignore_strings=body.ignore_strings,
        ignore_taxonomy_node_ids=body.ignore_taxonomy_node_ids,
        db=db,
    )
    return _to_response(scope)
