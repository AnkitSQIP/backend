"""
Watchlist rule evaluation and alert consolidation.
Ported from database.py's check_watchlist_rules_for_patents_batch and related functions.
All MongoDB queries replaced with SQLAlchemy ORM equivalents.
"""
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, and_, or_, func
from app.models import WatchlistRule, WatchlistAlert, PatentTaxonomy

logger = logging.getLogger(__name__)


def get_navigation_for_rule(rule_type: str, rule_config: dict, matched_values: dict = None) -> dict:
    """Generate navigation info for an alert based on rule type."""
    navigation = {"tab": None, "filters": {}}
    matched_assignees = matched_values.get("assignees", []) if matched_values else []

    if rule_type == "competitor":
        navigation["tab"] = "competitor-intelligence"
        competitor = rule_config.get("competitor") or rule_config.get("competitor_name", "")
        if competitor:
            navigation["filters"]["assignee"] = competitor
            if matched_assignees:
                navigation["filters"]["matched_assignees"] = matched_assignees

    elif rule_type == "legal_status":
        navigation["tab"] = "legal-status"
        status = rule_config.get("legal_status", "")
        if status:
            navigation["filters"]["legal_status"] = status

    elif rule_type == "family_count":
        navigation["tab"] = "patent-families"
        navigation["filters"]["min_family_count"] = rule_config.get("min_family_members", 2)

    elif rule_type == "taxonomy":
        navigation["tab"] = "technology"
        label = rule_config.get("taxonomy_label", "")
        if label:
            navigation["filters"]["taxonomy"] = label

    elif rule_type == "combination":
        c1 = rule_config.get("condition1", {})
        c2 = rule_config.get("condition2", {})
        c1_type = c1.get("type", "")

        if c1_type == "competitor":
            navigation["tab"] = "competitor-intelligence"
        elif c1_type == "legal_status":
            navigation["tab"] = "legal-status"
        elif c1_type == "family_count":
            navigation["tab"] = "patent-families"
        elif c1_type == "taxonomy":
            navigation["tab"] = "technology"
        else:
            navigation["tab"] = "overview"

        for cond in [c1, c2]:
            cond_type = cond.get("type", "")
            cond_value = cond.get("value", "")
            if cond_type == "competitor" and cond_value:
                navigation["filters"]["assignee"] = cond_value
                if matched_assignees:
                    navigation["filters"]["matched_assignees"] = matched_assignees
            elif cond_type == "legal_status" and cond_value:
                navigation["filters"]["legal_status"] = cond_value
            elif cond_type == "family_count" and cond_value:
                try:
                    navigation["filters"]["min_family_count"] = int(cond_value)
                except (ValueError, TypeError):
                    pass
            elif cond_type == "taxonomy" and cond_value:
                navigation["filters"]["taxonomy"] = cond_value

    return navigation


def _check_single_condition(patent: dict, condition: dict, taxonomy_map: dict) -> bool:
    cond_type = condition.get("type", "")
    cond_value = (condition.get("value") or "").lower()
    if not cond_value:
        return False

    if cond_type == "competitor":
        return cond_value in (patent.get("assignee") or "").lower()
    elif cond_type == "legal_status":
        return cond_value in (patent.get("legal_status") or "").lower()
    elif cond_type == "taxonomy":
        labels = [l.lower() for l in taxonomy_map.get(patent.get("patent_number"), [])]
        return any(cond_value in l for l in labels)
    elif cond_type == "family_count":
        try:
            return (patent.get("family_members_count") or 0) >= int(cond_value)
        except (ValueError, TypeError):
            return False
    return False


async def check_and_consolidate_alerts(
    db: AsyncSession,
    matched_patents: List[dict],
    rule: WatchlistRule,
) -> dict:
    """Create or update a consolidated daily alert for a rule."""
    rule_id = str(rule.id)
    matched_assignees = list(set(p.get("assignee") for p in matched_patents if p.get("assignee")))
    matched_numbers = [p.get("patent_number") for p in matched_patents]
    navigation = get_navigation_for_rule(rule.rule_type, rule.rule_config, {"assignees": matched_assignees})

    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    existing = await db.scalar(
        select(WatchlistAlert).where(
            and_(
                WatchlistAlert.rule_id == rule_id,
                WatchlistAlert.workspace_id == rule.workspace_id,
                WatchlistAlert.is_read == False,
                WatchlistAlert.created_at >= today_start,
            )
        )
    )

    if existing:
        existing_pns = existing.matched_patents or []
        new_pns = [p for p in matched_numbers if p not in existing_pns]
        if new_pns:
            existing.matched_count = len(existing_pns) + len(new_pns)
            existing.matched_patents = list(set(existing_pns + new_pns))
            existing.matched_assignees = list(set((existing.matched_assignees or []) + matched_assignees))
            existing.updated_at = datetime.now(timezone.utc)
            await db.commit()
        return {"updated": True, "alert_id": str(existing.id)}
    else:
        alert = WatchlistAlert(
            rule_id=rule_id,
            rule_name=rule.name,
            alert_type=rule.rule_type,
            workspace_id=rule.workspace_id,
            matched_count=len(matched_patents),
            matched_patents=matched_numbers,
            matched_assignees=matched_assignees,
            navigation=navigation,
        )
        db.add(alert)
        await db.commit()
        await db.refresh(alert)
        return {"created": True, "alert_id": str(alert.id), "matched_count": len(matched_patents)}


async def check_watchlist_rules_batch(
    db: AsyncSession,
    patents: List[dict],
    workspace_id: str,
) -> List[dict]:
    """Check all patents against active watchlist rules; create/update consolidated alerts."""
    import uuid as _uuid
    try:
        ws_uuid = _uuid.UUID(workspace_id)
    except (ValueError, AttributeError):
        return []

    rules_result = await db.scalars(
        select(WatchlistRule).where(
            and_(WatchlistRule.workspace_id == ws_uuid, WatchlistRule.is_active == True)
        )
    )
    rules = list(rules_result)
    if not rules:
        return []

    # Pre-fetch taxonomy assignments for all patents
    patent_numbers = [p.get("patent_number") for p in patents if p.get("patent_number")]
    taxonomy_map: dict[str, list] = {}
    if patent_numbers:
        tax_rows = await db.scalars(
            select(PatentTaxonomy).where(PatentTaxonomy.patent_number.in_(patent_numbers))
        )
        for row in tax_rows:
            taxonomy_map.setdefault(row.patent_number, []).append(row.taxonomy_label or "")

    alerts_info = []
    for rule in rules:
        matched = []
        for patent in patents:
            hit = False
            rc = rule.rule_config or {}
            rt = rule.rule_type

            if rt == "competitor":
                name = (rc.get("competitor") or rc.get("competitor_name") or "").lower()
                if name and name in (patent.get("assignee") or "").lower():
                    hit = True
            elif rt == "legal_status":
                target = rc.get("legal_status", "").lower()
                if target and target in (patent.get("legal_status") or "").lower():
                    hit = True
            elif rt == "family_count":
                min_c = rc.get("min_family_members", 2)
                fc = patent.get("family_members_count") or patent.get("family_member_count") or 0
                if fc >= min_c:
                    hit = True
            elif rt == "taxonomy":
                target = rc.get("taxonomy_label", "")
                labels = taxonomy_map.get(patent.get("patent_number"), [])
                if target and any(target in l for l in labels):
                    hit = True
            elif rt == "combination":
                c1 = rc.get("condition1", {})
                c2 = rc.get("condition2", {})
                op = rc.get("operator", "AND")
                m1 = _check_single_condition(patent, c1, taxonomy_map)
                m2 = _check_single_condition(patent, c2, taxonomy_map)
                hit = (m1 and m2) if op == "AND" else (m1 or m2)

            if hit:
                matched.append(patent)

        if matched:
            info = await check_and_consolidate_alerts(db, matched, rule)
            alerts_info.append({"rule_name": rule.name, "matched_count": len(matched), **info})

    return alerts_info


async def check_taxonomy_watchlist_rules(
    db: AsyncSession,
    patent_number: str,
    taxonomy_label: str,
    workspace_id: str,
) -> List[dict]:
    """Check if a new taxonomy assignment triggers any watchlist rules."""
    import uuid as _uuid
    try:
        ws_uuid = _uuid.UUID(workspace_id)
    except (ValueError, AttributeError):
        return []

    rules_result = await db.scalars(
        select(WatchlistRule).where(
            and_(WatchlistRule.workspace_id == ws_uuid, WatchlistRule.is_active == True)
        )
    )

    alerts_created = []
    for rule in rules_result:
        rc = rule.rule_config or {}
        rt = rule.rule_type
        matched = False

        if rt == "taxonomy":
            target = rc.get("taxonomy_label", "").lower()
            if target and target in taxonomy_label.lower():
                matched = True
        elif rt == "combination":
            for cond in [rc.get("condition1", {}), rc.get("condition2", {})]:
                if cond.get("type") == "taxonomy":
                    target = (cond.get("value") or "").lower()
                    if target and target in taxonomy_label.lower():
                        matched = True

        if matched:
            patent_dict = {"patent_number": patent_number, "taxonomy_label": taxonomy_label}
            info = await check_and_consolidate_alerts(db, [patent_dict], rule)
            alerts_created.append(info)

    return alerts_created
