import uuid
import json
import logging
from datetime import datetime, timezone
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Form
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, update, and_, or_, func

from app.models import WatchlistRule, WatchlistAlert, Patent
from app.deps import get_db, get_current_user, require_role
from app.services.watchlist import (
    check_watchlist_rules_batch,
    get_navigation_for_rule,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["watchlists"])


def _parse_ws_uuid(workspace_id: str) -> uuid.UUID:
    try:
        return uuid.UUID(workspace_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400, detail="Invalid workspace_id")


def _rule_name(rule_type: str, config: dict) -> str:
    if rule_type == "competitor":
        return f"Competitor: {config.get('competitor_name', config.get('competitor', 'Unknown'))}"
    elif rule_type == "taxonomy":
        return f"Taxonomy: {config.get('taxonomy_label', 'Unknown')}"
    elif rule_type == "legal_status":
        return f"Legal Status: {config.get('legal_status', 'Unknown')}"
    elif rule_type == "family_count":
        return f"Family Members \u2265 {config.get('min_family_members', 2)}"
    elif rule_type == "combination":
        c1 = config.get("condition1", {})
        c2 = config.get("condition2", {})
        op = config.get("operator", "AND")
        return f"{c1.get('type', 'condition1')} {op} {c2.get('type', 'condition2')}"
    return f"Rule: {rule_type}"


# ── RULES ─────────────────────────────────────────────────────────────────────

@router.get("/watchlists/rules")
async def list_watchlist_rules(
    workspace_id: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(WatchlistRule)
    if workspace_id:
        ws_uuid = _parse_ws_uuid(workspace_id)
        stmt = stmt.where(WatchlistRule.workspace_id == ws_uuid)
    result = await db.scalars(stmt.order_by(WatchlistRule.created_at.desc()))
    rules = list(result.all())
    return {
        "rules": [
            {
                "id": str(r.id),
                "name": r.name,
                "rule_type": r.rule_type,
                "rule_config": r.rule_config,
                "is_active": r.is_active,
                "workspace_id": str(r.workspace_id),
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in rules
        ]
    }


@router.post("/watchlists/rules")
async def create_rule(
    workspace_id: str = Form(...),
    rule_type: str = Form(...),
    rule_config: str = Form(...),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _parse_ws_uuid(workspace_id)
    try:
        config = json.loads(rule_config)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid rule_config JSON")

    name = _rule_name(rule_type, config)
    rule = WatchlistRule(
        workspace_id=ws_uuid,
        name=name,
        rule_type=rule_type,
        rule_config=config,
        created_by=uuid.UUID(current_user["user_id"]),
    )
    db.add(rule)
    await db.commit()
    await db.refresh(rule)
    return {
        "message": "Rule created",
        "rule": {
            "id": str(rule.id),
            "name": rule.name,
            "rule_type": rule.rule_type,
            "rule_config": rule.rule_config,
            "is_active": rule.is_active,
        },
    }


@router.delete("/watchlists/rules/{rule_id}")
async def delete_rule(
    rule_id: str,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    try:
        rid = uuid.UUID(rule_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=404, detail="Rule not found")
    result = await db.execute(delete(WatchlistRule).where(WatchlistRule.id == rid))
    await db.commit()
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Rule not found")
    return {"message": "Rule deleted successfully"}


# ── RECHECK ───────────────────────────────────────────────────────────────────

@router.post("/watchlists/recheck/{workspace_id}")
async def recheck_watchlist_rules(
    workspace_id: str,
    current_user: dict = Depends(require_role(["ADMIN", "admin"])),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _parse_ws_uuid(workspace_id)
    result = await db.scalars(select(Patent).where(Patent.workspace_id == ws_uuid))
    patents = list(result.all())
    if not patents:
        return {"message": "No patents found in workspace", "alerts_generated": 0}

    patent_dicts = [
        {
            "patent_number": p.patent_number,
            "title": p.title,
            "abstract": p.abstract,
            "assignee": p.assignee,
            "legal_status": p.legal_status,
            "family_members_count": p.family_members_count or 0,
        }
        for p in patents
    ]
    alerts_info = await check_watchlist_rules_batch(db, patent_dicts, str(ws_uuid))
    return {
        "message": f"Rechecked {len(patents)} patents against watchlist rules",
        "patents_checked": len(patents),
        "alerts_generated": len(alerts_info),
        "alerts_detail": alerts_info,
    }


# ── ALERTS ────────────────────────────────────────────────────────────────────

@router.get("/watchlists/alerts")
async def list_alerts(
    workspace_id: Optional[str] = None,
    rule_id: Optional[str] = None,
    is_read: Optional[bool] = None,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(WatchlistAlert).order_by(WatchlistAlert.created_at.desc()).limit(100)

    if workspace_id:
        ws_uuid = _parse_ws_uuid(workspace_id)
        stmt = stmt.where(WatchlistAlert.workspace_id == ws_uuid)
    if rule_id:
        stmt = stmt.where(WatchlistAlert.rule_id == rule_id)
    if is_read is not None:
        stmt = stmt.where(WatchlistAlert.is_read == is_read)

    result = await db.scalars(stmt)
    alerts = list(result.all())

    # Unread count
    unread_stmt = select(func.count()).select_from(WatchlistAlert).where(WatchlistAlert.is_read == False)
    if workspace_id:
        ws_uuid = _parse_ws_uuid(workspace_id)
        unread_stmt = unread_stmt.where(WatchlistAlert.workspace_id == ws_uuid)
    unread_count = await db.scalar(unread_stmt) or 0

    out = []
    for alert in alerts:
        matched_count = alert.matched_count or 1
        matched_patents = alert.matched_patents or []
        navigation = alert.navigation or {}

        # Regenerate navigation if missing
        if not navigation.get("tab"):
            rule = await db.scalar(
                select(WatchlistRule).where(WatchlistRule.id == uuid.UUID(alert.rule_id))
            ) if alert.rule_id else None
            if rule:
                navigation = get_navigation_for_rule(rule.rule_type, rule.rule_config)
                alert.navigation = navigation
                await db.commit()

        alert_title = (
            f"{matched_count} patents matched rule: {alert.rule_name}"
            if matched_count > 1
            else f"Patent matched rule: {alert.rule_name}"
        )
        out.append({
            "id": str(alert.id),
            "rule_id": alert.rule_id,
            "rule_name": alert.rule_name,
            "alert_type": alert.alert_type or "competitor",
            "alert_title": alert_title,
            "matched_count": matched_count,
            "matched_patents": matched_patents[:10],
            "matched_assignees": alert.matched_assignees or [],
            "navigation": navigation,
            "workspace_id": str(alert.workspace_id),
            "created_at": alert.created_at.isoformat() if alert.created_at else None,
            "is_read": alert.is_read,
        })

    return {"alerts": out, "total": len(out), "unread_count": unread_count}


@router.put("/watchlists/alerts/mark-all-read")
async def mark_all_alerts_read(
    workspace_id: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(WatchlistAlert).where(WatchlistAlert.is_read == False)
    if workspace_id:
        ws_uuid = _parse_ws_uuid(workspace_id)
        stmt = stmt.where(WatchlistAlert.workspace_id == ws_uuid)
    result = await db.scalars(stmt)
    alerts = list(result.all())
    now = datetime.now(timezone.utc)
    count = 0
    for alert in alerts:
        alert.is_read = True
        alert.read_at = now
        count += 1
    await db.commit()
    return {"message": f"Marked {count} alerts as read", "count": count}


@router.put("/watchlists/alerts/{alert_id}/read")
async def mark_alert_read(
    alert_id: str,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    try:
        aid = uuid.UUID(alert_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=404, detail="Alert not found")
    alert = await db.scalar(select(WatchlistAlert).where(WatchlistAlert.id == aid))
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    alert.is_read = True
    alert.read_at = datetime.now(timezone.utc)
    await db.commit()
    return {"message": "Alert marked as read"}
