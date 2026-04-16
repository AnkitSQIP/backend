import uuid
import logging
from fastapi import APIRouter, Depends, HTTPException, Form
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, delete, outerjoin
from app.models import Workspace, WorkspaceUser, Patent, PatentTaxonomy, TaxonomyNode, User, WatchlistRule, WatchlistAlert, InvestigationQueueItem
from app.deps import get_db, get_current_user, require_role

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["workspaces"])


def _parse_ws_uuid(workspace_id: str) -> uuid.UUID:
    try:
        return uuid.UUID(workspace_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400, detail="Invalid workspace_id")


@router.post("/workspaces")
async def create_workspace_endpoint(
    name: str = Form(...),
    description: str = Form(None),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    workspace_code = f"WS-{uuid.uuid4().hex[:8].upper()}"
    ws = Workspace(
        name=name,
        workspace_code=workspace_code,
        description=description,
        created_by=None,
    )
    db.add(ws)
    await db.flush()
    # Only add workspace_user row if the user actually exists in the users table
    try:
        uid = uuid.UUID(current_user["user_id"])
        user_exists = await db.scalar(select(User).where(User.id == uid))
        if user_exists:
            db.add(WorkspaceUser(workspace_id=ws.id, user_id=uid, role="owner"))
    except (ValueError, AttributeError):
        pass
    await db.commit()
    return {"id": str(ws.id), "name": name, "workspace_code": workspace_code}


@router.get("/workspaces")
async def list_workspaces(
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    # Single query: workspaces + patent counts via LEFT JOIN GROUP BY
    count_subq = (
        select(Patent.workspace_id, func.count(Patent.id).label("cnt"))
        .group_by(Patent.workspace_id)
        .subquery()
    )

    if current_user.get("role") in ["ADMIN", "admin"]:
        rows = await db.execute(
            select(Workspace, func.coalesce(count_subq.c.cnt, 0).label("patent_count"))
            .outerjoin(count_subq, count_subq.c.workspace_id == Workspace.id)
        )
    else:
        uid = uuid.UUID(current_user["user_id"])
        rows = await db.execute(
            select(Workspace, func.coalesce(count_subq.c.cnt, 0).label("patent_count"))
            .join(WorkspaceUser, WorkspaceUser.workspace_id == Workspace.id)
            .outerjoin(count_subq, count_subq.c.workspace_id == Workspace.id)
            .where(WorkspaceUser.user_id == uid)
        )

    return {
        "workspaces": [
            {
                "id": str(ws.id),
                "name": ws.name,
                "workspace_code": ws.workspace_code,
                "description": ws.description,
                "patent_count": int(cnt),
            }
            for ws, cnt in rows
        ]
    }


@router.get("/workspaces/{workspace_id}/members")
async def get_workspace_members(
    workspace_id: str,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _parse_ws_uuid(workspace_id)
    result = await db.scalars(select(WorkspaceUser).where(WorkspaceUser.workspace_id == ws_uuid))
    members_list = list(result.all())

    members = []
    for m in members_list:
        user = await db.scalar(select(User).where(User.id == m.user_id))
        if user:
            members.append({
                "user_id": str(m.user_id),
                "email": user.email,
                "full_name": user.full_name or "",
                "role": m.role,
                "user_role": user.role,
            })
    return {"members": members}


@router.post("/workspaces/{workspace_id}/members")
async def add_workspace_member(
    workspace_id: str,
    user_id: str = Form(...),
    role: str = Form("member"),
    current_user: dict = Depends(require_role(["ADMIN", "admin"])),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _parse_ws_uuid(workspace_id)
    try:
        uid = uuid.UUID(user_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400, detail="Invalid user_id")
    existing = await db.scalar(
        select(WorkspaceUser).where(
            WorkspaceUser.workspace_id == ws_uuid,
            WorkspaceUser.user_id == uid,
        )
    )
    if existing:
        raise HTTPException(status_code=400, detail="User is already a member")
    db.add(WorkspaceUser(workspace_id=ws_uuid, user_id=uid, role=role))
    await db.commit()
    return {"message": "Member added successfully"}


@router.delete("/workspaces/{workspace_id}/members/{user_id}")
async def remove_workspace_member(
    workspace_id: str,
    user_id: str,
    current_user: dict = Depends(require_role(["ADMIN", "admin"])),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _parse_ws_uuid(workspace_id)
    try:
        uid = uuid.UUID(user_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400, detail="Invalid user_id")
    result = await db.execute(
        delete(WorkspaceUser).where(
            WorkspaceUser.workspace_id == ws_uuid,
            WorkspaceUser.user_id == uid,
        )
    )
    await db.commit()
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Member not found")
    return {"message": "Member removed successfully"}


@router.delete("/workspaces/{workspace_id}")
async def delete_workspace(
    workspace_id: str,
    current_user: dict = Depends(require_role(["ADMIN", "admin"])),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _parse_ws_uuid(workspace_id)
    # Delete in FK-safe order:
    # 1. WatchlistAlerts (references workspace)
    # 2. WatchlistRules (references workspace)
    # 3. InvestigationQueueItems (references workspace)
    # 4. PatentTaxonomy (references patent_number + taxonomy_node_id)
    # 5. Patents (references workspace)
    # 6. TaxonomyNode children (level > 0, references parent node_id)
    # 7. TaxonomyNode roots (level == 0)
    # 8. WorkspaceUsers (references workspace)
    # 9. Workspace
    await db.execute(delete(WatchlistAlert).where(WatchlistAlert.workspace_id == ws_uuid))
    await db.execute(delete(WatchlistRule).where(WatchlistRule.workspace_id == ws_uuid))
    await db.execute(delete(InvestigationQueueItem).where(InvestigationQueueItem.workspace_id == ws_uuid))
    pn_result = await db.scalars(select(Patent.patent_number).where(Patent.workspace_id == ws_uuid))
    patent_numbers = list(pn_result.all())
    # Chunk IN clause to avoid DB parameter limits on large workspaces
    CHUNK = 500
    for i in range(0, len(patent_numbers), CHUNK):
        chunk = patent_numbers[i:i+CHUNK]
        await db.execute(delete(PatentTaxonomy).where(PatentTaxonomy.patent_number.in_(chunk)))
    await db.execute(delete(Patent).where(Patent.workspace_id == ws_uuid))
    # Delete children before parents (parent_id FK)
    await db.execute(delete(TaxonomyNode).where(TaxonomyNode.workspace_id == ws_uuid, TaxonomyNode.parent_id.isnot(None)))
    await db.execute(delete(TaxonomyNode).where(TaxonomyNode.workspace_id == ws_uuid))
    await db.execute(delete(WorkspaceUser).where(WorkspaceUser.workspace_id == ws_uuid))
    result = await db.execute(delete(Workspace).where(Workspace.id == ws_uuid))
    await db.commit()
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Workspace not found")
    return {"message": "Workspace deleted", "workspace_id": workspace_id}


@router.delete("/workspaces/{workspace_id}/patents")
async def empty_workspace_patents(
    workspace_id: str,
    current_user: dict = Depends(require_role(["ADMIN", "admin"])),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _parse_ws_uuid(workspace_id)
    pn_result = await db.scalars(
        select(Patent.patent_number).where(Patent.workspace_id == ws_uuid)
    )
    patent_numbers = list(pn_result.all())
    if patent_numbers:
        await db.execute(
            delete(PatentTaxonomy).where(PatentTaxonomy.patent_number.in_(patent_numbers))
        )
    result = await db.execute(delete(Patent).where(Patent.workspace_id == ws_uuid))
    await db.commit()
    return {"message": "Workspace emptied successfully", "deleted_patents": result.rowcount}


@router.delete("/workspaces/{workspace_id}/taxonomy")
async def reset_workspace_taxonomy(
    workspace_id: str,
    current_user: dict = Depends(require_role(["ADMIN", "admin"])),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _parse_ws_uuid(workspace_id)
    pn_result = await db.scalars(
        select(Patent.patent_number).where(Patent.workspace_id == ws_uuid)
    )
    patent_numbers = list(pn_result.all())
    taxonomy_deleted = 0
    if patent_numbers:
        r = await db.execute(
            delete(PatentTaxonomy).where(PatentTaxonomy.patent_number.in_(patent_numbers))
        )
        taxonomy_deleted = r.rowcount
    r2 = await db.execute(delete(TaxonomyNode).where(TaxonomyNode.workspace_id == ws_uuid))
    await db.commit()
    return {
        "message": "Taxonomy reset successfully",
        "deleted_nodes": r2.rowcount,
        "deleted_assignments": taxonomy_deleted,
    }
