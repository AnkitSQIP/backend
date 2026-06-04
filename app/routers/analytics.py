import uuid
import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Form
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_, case, delete

from app.models import Patent, PatentTaxonomy, TaxonomyNode, DashboardView
from app.deps import get_db, get_current_user

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["analytics"])


# ── Helpers ──────────────────────────────────────────────────────────────────

def _ws_uuid(workspace_id: Optional[str]) -> Optional[uuid.UUID]:
    if not workspace_id:
        return None
    try:
        return uuid.UUID(workspace_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400, detail="Invalid workspace_id")


def _date_conditions(date_type: str, date_from: str = None, date_to: str = None, period: str = None):
    date_field = Patent.filing_date if date_type == "filing" else Patent.publication_date
    conds = []
    if period:
        days = {"week": 7, "month": 30, "quarter": 90, "year": 365}.get(period)
        if days:
            conds.append(date_field >= datetime.now(timezone.utc) - timedelta(days=days))
    elif date_from or date_to:
        if date_from:
            try:
                conds.append(date_field >= datetime.strptime(date_from, "%Y-%m-%d").replace(tzinfo=timezone.utc))
            except ValueError:
                pass
        if date_to:
            try:
                conds.append(date_field <= datetime.strptime(date_to, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=timezone.utc))
            except ValueError:
                pass
    return conds


def _assignee_conditions(assignees: Optional[str]):
    if not assignees:
        return []
    if "|||" in assignees:
        parts = [a.strip() for a in assignees.split("|||") if a.strip()]
    else:
        parts = [assignees.strip()] if assignees.strip() else []
    if not parts:
        return []
    return [or_(*[Patent.assignee.ilike(f"%{a}%") for a in parts])]


async def _get_ws_node_ids(db: AsyncSession, ws_uuid: Optional[uuid.UUID]) -> set:
    if not ws_uuid:
        return set()
    result = await db.scalars(select(TaxonomyNode.node_id).where(TaxonomyNode.workspace_id == ws_uuid))
    return set(result.all())


async def _get_pns_by_taxonomy_labels(db: AsyncSession, label_list: list, node_ids: set) -> Optional[list]:
    if not label_list:
        return None
    conds = [PatentTaxonomy.taxonomy_label.in_(label_list)]
    if node_ids:
        conds.append(PatentTaxonomy.taxonomy_node_id.in_(list(node_ids)))
    result = await db.scalars(select(PatentTaxonomy.patent_number).where(and_(*conds)))
    return list(set(result.all()))


def _patent_to_full_dict(p: Patent) -> dict:
    return {
        "patent_number": p.patent_number,
        "title": p.title,
        "abstract": p.abstract,
        "assignee": p.assignee,
        "inventor": p.inventor,
        "jurisdiction": p.jurisdiction,
        "filing_date": p.filing_date.isoformat() if p.filing_date else None,
        "publication_date": p.publication_date.isoformat() if p.publication_date else None,
        "grant_date": p.grant_date.isoformat() if p.grant_date else None,
        "priority_date": p.priority_date.isoformat() if p.priority_date else None,
        "legal_status": p.legal_status,
        "cpc_class": p.cpc_class,
        "ipc_class": p.ipc_class,
        "claims_count": p.claims_count,
        "independent_claims_count": p.independent_claims_count,
        "claims_text": p.claims_text,
        "first_claim": p.first_claim,
        "patent_family_id": p.patent_family_id,
        "family_members": p.family_members,
        "family_members_count": p.family_members_count,
        "backward_citation_count": p.backward_citation_count,
        "forward_citation_count": p.forward_citation_count,
        "patent_url": p.patent_url,
        "publication_country": p.publication_country,
        "num_claims": p.num_claims,
        "review_status": p.review_status,
        "workspace_id": str(p.workspace_id),
    }


# ── OVERVIEW ─────────────────────────────────────────────────────────────────

async def _monthly_trend(
    db: AsyncSession,
    ws_uuid: Optional[uuid.UUID],
    date_field,
    window_start: Optional[datetime],
) -> dict:
    """Single GROUP BY date_trunc('month') query. Returns {"YYYY-MM": count}.

    Replaces the old 12-iteration scalar loop — one round trip regardless of
    window size, which keeps the overview fast at 60k+ patents."""
    month_col = func.to_char(func.date_trunc("month", date_field), "YYYY-MM")
    stmt = (
        select(month_col.label("m"), func.count().label("c"))
        .where(date_field.isnot(None))
        .group_by(month_col)
    )
    if ws_uuid:
        stmt = stmt.where(Patent.workspace_id == ws_uuid)
    if window_start is not None:
        stmt = stmt.where(date_field >= window_start)
    result = await db.execute(stmt)
    return {r.m: r.c for r in result}


def _fill_month_buckets(counts: dict, months: int) -> list:
    """Build a continuous list of {month, count} for the last `months` months
    (gaps filled with 0). When months <= 0, return every present month sorted."""
    if months <= 0:
        return [{"month": k, "count": counts[k]} for k in sorted(counts)]
    now = datetime.now(timezone.utc)
    out = []
    for i in range(months - 1, -1, -1):
        target_month = now.month - i
        target_year = now.year
        while target_month <= 0:
            target_month += 12
            target_year -= 1
        label = f"{target_year:04d}-{target_month:02d}"
        out.append({"month": label, "count": counts.get(label, 0)})
    return out


@router.get("/analytics/overview")
async def get_analytics_overview(
    workspace_id: Optional[str] = None,
    months: int = 12,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Workspace overview KPIs + publication/filing trends.

    `months` controls the trend window (12/24/36). Pass 0 for the full history."""
    ws_uuid = _ws_uuid(workspace_id)

    def _base(stmt):
        if ws_uuid:
            return stmt.where(Patent.workspace_id == ws_uuid)
        return stmt

    total_patents = await db.scalar(_base(select(func.count()).select_from(Patent))) or 0

    thirty_days_ago = datetime.now(timezone.utc) - timedelta(days=30)
    stmt = select(func.count()).select_from(Patent).where(Patent.publication_date >= thirty_days_ago)
    if ws_uuid:
        stmt = stmt.where(Patent.workspace_id == ws_uuid)
    new_publications = await db.scalar(stmt) or 0

    stmt = select(func.count(Patent.assignee.distinct())).where(Patent.assignee.isnot(None))
    if ws_uuid:
        stmt = stmt.where(Patent.workspace_id == ws_uuid)
    active_assignees = await db.scalar(stmt) or 0

    stmt = (
        select(Patent.assignee, func.count().label("cnt"))
        .where(Patent.assignee.isnot(None))
        .group_by(Patent.assignee)
        .order_by(func.count().desc())
        .limit(10)
    )
    if ws_uuid:
        stmt = stmt.where(Patent.workspace_id == ws_uuid)
    top_result = await db.execute(stmt)
    top_assignees = [{"assignee": r.assignee, "count": r.cnt} for r in top_result]

    # Trend window start (None = full history for months <= 0)
    window_start = None
    if months and months > 0:
        now = datetime.now(timezone.utc)
        start_month = now.month - (months - 1)
        start_year = now.year
        while start_month <= 0:
            start_month += 12
            start_year -= 1
        window_start = datetime(start_year, start_month, 1, tzinfo=timezone.utc)

    pub_counts = await _monthly_trend(db, ws_uuid, Patent.publication_date, window_start)
    fil_counts = await _monthly_trend(db, ws_uuid, Patent.filing_date, window_start)
    publication_trends = _fill_month_buckets(pub_counts, months)
    filing_trends = _fill_month_buckets(fil_counts, months)

    return {
        "total_patents": total_patents,
        "new_publications_30d": new_publications,
        "active_assignees": active_assignees,
        "top_assignees": top_assignees,
        "publication_trends": publication_trends,
        "filing_trends": filing_trends,
    }


# ── PATENTS BY ASSIGNEE ───────────────────────────────────────────────────────

@router.get("/analytics/patents-by-assignee")
async def get_patents_by_assignee(
    assignee: str,
    workspace_id: Optional[str] = None,
    skip: int = 0,
    limit: int = 25,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _ws_uuid(workspace_id)
    stmt = select(Patent).where(Patent.assignee.ilike(f"%{assignee}%"))
    if ws_uuid:
        stmt = stmt.where(Patent.workspace_id == ws_uuid)
    total = await db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    result = await db.scalars(stmt.order_by(Patent.publication_date.desc()).offset(skip).limit(limit))
    patents = list(result.all())
    return {
        "patents": [
            {
                "id": str(p.id),
                "patent_number": p.patent_number,
                "title": p.title,
                "assignee": p.assignee,
                "publication_date": p.publication_date.isoformat() if p.publication_date else None,
                "grant_date": p.grant_date.isoformat() if p.grant_date else None,
                "legal_status": p.legal_status,
                "patent_url": p.patent_url,
            }
            for p in patents
        ],
        "total": total,
        "skip": skip,
        "limit": limit,
    }


# ── PATENTS BY TREND ──────────────────────────────────────────────────────────

@router.get("/analytics/patents-by-trend")
async def get_patents_by_trend(
    date_type: str,
    month: str,
    workspace_id: Optional[str] = None,
    skip: int = 0,
    limit: int = 25,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    try:
        year, month_num = month.split("-")
        month_start = datetime(int(year), int(month_num), 1, tzinfo=timezone.utc)
        if int(month_num) == 12:
            month_end = datetime(int(year) + 1, 1, 1, tzinfo=timezone.utc)
        else:
            month_end = datetime(int(year), int(month_num) + 1, 1, tzinfo=timezone.utc)
    except (ValueError, IndexError):
        raise HTTPException(status_code=400, detail="Invalid month format. Use YYYY-MM")

    date_field = Patent.filing_date if date_type == "filing" else Patent.publication_date
    ws_uuid = _ws_uuid(workspace_id)
    stmt = select(Patent).where(date_field >= month_start, date_field < month_end)
    if ws_uuid:
        stmt = stmt.where(Patent.workspace_id == ws_uuid)
    total = await db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    result = await db.scalars(stmt.order_by(date_field.desc()).offset(skip).limit(limit))
    patents = list(result.all())
    return {
        "patents": [
            {
                "id": str(p.id),
                "patent_number": p.patent_number,
                "title": p.title,
                "assignee": p.assignee,
                "filing_date": p.filing_date.isoformat() if p.filing_date else None,
                "publication_date": p.publication_date.isoformat() if p.publication_date else None,
                "legal_status": p.legal_status,
                "patent_url": p.patent_url,
            }
            for p in patents
        ],
        "total": total,
        "skip": skip,
        "limit": limit,
        "date_type": date_type,
        "month": month,
    }


# ── FILTER OPTIONS ────────────────────────────────────────────────────────────

@router.get("/analytics/filters/options")
async def get_filter_options(
    workspace_id: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _ws_uuid(workspace_id)

    stmt = select(Patent.assignee.distinct()).where(Patent.assignee.isnot(None))
    if ws_uuid:
        stmt = stmt.where(Patent.workspace_id == ws_uuid)
    assignees = sorted([r for r in (await db.scalars(stmt)).all() if r])

    stmt = select(Patent.legal_status.distinct()).where(Patent.legal_status.isnot(None))
    if ws_uuid:
        stmt = stmt.where(Patent.workspace_id == ws_uuid)
    db_statuses = [(r) for r in (await db.scalars(stmt)).all() if r]
    default_statuses = ["ACTIVE - APPLIED", "ACTIVE - GRANTED", "EXPIRED", "PENDING", "LAPSED"]
    legal_statuses = sorted(list(set(db_statuses + default_statuses)))

    taxonomy_labels = []
    if ws_uuid:
        result = await db.scalars(
            select(TaxonomyNode.label).where(TaxonomyNode.workspace_id == ws_uuid)
        )
        taxonomy_labels = sorted([r for r in result.all() if r])

    return {
        "assignees": assignees,
        "legal_statuses": legal_statuses,
        "taxonomy_labels": taxonomy_labels,
    }


# ── KEY INSIGHTS ──────────────────────────────────────────────────────────────

@router.get("/analytics/key-insights")
async def get_key_insights(
    period: str = "quarter",
    workspace_id: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _ws_uuid(workspace_id)
    period_days = {"month": 30, "quarter": 90, "year": 365}.get(period, 90)
    now = datetime.now(timezone.utc)
    current_period_start = now - timedelta(days=period_days)
    previous_period_start = current_period_start - timedelta(days=period_days)

    # Top 3 assignees
    stmt = (
        select(Patent.assignee, func.count().label("cnt"))
        .where(Patent.assignee.isnot(None))
        .group_by(Patent.assignee)
        .order_by(func.count().desc())
        .limit(3)
    )
    if ws_uuid:
        stmt = stmt.where(Patent.workspace_id == ws_uuid)
    top_3_assignees = [{"assignee": r.assignee, "patent_count": r.cnt} for r in (await db.execute(stmt))]

    # Workspace taxonomy node IDs
    node_ids = await _get_ws_node_ids(db, ws_uuid)

    # All workspace patent numbers + publication dates
    stmt = select(Patent.patent_number, Patent.publication_date, Patent.assignee)
    if ws_uuid:
        stmt = stmt.where(Patent.workspace_id == ws_uuid)
    all_patents = list((await db.execute(stmt)).all())
    patent_numbers = [p.patent_number for p in all_patents]

    # Top taxonomy labels
    taxonomy_labels = []
    if node_ids and patent_numbers:
        stmt = (
            select(PatentTaxonomy.taxonomy_node_id, func.count().label("cnt"))
            .where(
                PatentTaxonomy.patent_number.in_(patent_numbers),
                PatentTaxonomy.taxonomy_node_id.in_(list(node_ids)),
            )
            .group_by(PatentTaxonomy.taxonomy_node_id)
            .order_by(func.count().desc())
            .limit(5)
        )
        for row in (await db.execute(stmt)).all():
            node = await db.scalar(select(TaxonomyNode).where(TaxonomyNode.node_id == row.taxonomy_node_id))
            if node:
                taxonomy_labels.append({"label": node.label, "patent_count": row.cnt})

    # Highest growth assignee
    highest_growth_assignee = None
    def _make_aware(dt):
        if dt is None:
            return None
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

    current_pns = [p.patent_number for p in all_patents if _make_aware(p.publication_date) and _make_aware(p.publication_date) >= current_period_start]
    previous_pns = [p.patent_number for p in all_patents if _make_aware(p.publication_date) and previous_period_start <= _make_aware(p.publication_date) < current_period_start]

    current_counts: dict = {}
    previous_counts: dict = {}
    for p in all_patents:
        pub = _make_aware(p.publication_date)
        if p.assignee:
            if pub and pub >= current_period_start:
                current_counts[p.assignee] = current_counts.get(p.assignee, 0) + 1
            elif pub and previous_period_start <= pub < current_period_start:
                previous_counts[p.assignee] = previous_counts.get(p.assignee, 0) + 1

    max_growth = 0
    for assignee, cur in current_counts.items():
        prev = previous_counts.get(assignee, 0)
        growth = cur - prev
        if growth > max_growth:
            max_growth = growth
            highest_growth_assignee = {
                "assignee": assignee,
                "current_count": cur,
                "previous_count": prev,
                "growth": growth,
                "growth_percent": round(growth / prev * 100, 1) if prev > 0 else None,
            }

    # Highest growth taxonomy
    highest_growth_taxonomy = None
    if node_ids and patent_numbers:
        tax_result = await db.scalars(
            select(PatentTaxonomy).where(
                PatentTaxonomy.patent_number.in_(patent_numbers),
                PatentTaxonomy.taxonomy_node_id.in_(list(node_ids)),
            )
        )
        tax_assignments = list(tax_result.all())
        current_tax: dict = {}
        previous_tax: dict = {}
        pn_pub = {p.patent_number: _make_aware(p.publication_date) for p in all_patents}
        for ta in tax_assignments:
            pub = pn_pub.get(ta.patent_number)
            if pub:
                if pub >= current_period_start:
                    current_tax[ta.taxonomy_node_id] = current_tax.get(ta.taxonomy_node_id, 0) + 1
                elif previous_period_start <= pub < current_period_start:
                    previous_tax[ta.taxonomy_node_id] = previous_tax.get(ta.taxonomy_node_id, 0) + 1
        max_tax_growth = 0
        for nid, cur in current_tax.items():
            prev = previous_tax.get(nid, 0)
            growth = cur - prev
            if growth > max_tax_growth:
                max_tax_growth = growth
                node = await db.scalar(select(TaxonomyNode).where(TaxonomyNode.node_id == nid))
                if node:
                    highest_growth_taxonomy = {
                        "label": node.label,
                        "current_count": cur,
                        "previous_count": prev,
                        "growth": growth,
                        "growth_percent": round(growth / prev * 100, 1) if prev > 0 else None,
                    }

    # Newest labels
    newest_labels = []
    if ws_uuid:
        result = await db.scalars(
            select(TaxonomyNode)
            .where(TaxonomyNode.workspace_id == ws_uuid)
            .order_by(TaxonomyNode.created_at.desc())
            .limit(5)
        )
        for node in result.all():
            newest_labels.append({
                "label": node.label,
                "node_id": node.node_id,
                "created_at": node.created_at.isoformat() if node.created_at else None,
            })

    # Clustered label pairs
    clustered_label_pairs = []
    if node_ids and patent_numbers:
        tax_result = await db.scalars(
            select(PatentTaxonomy).where(
                PatentTaxonomy.patent_number.in_(patent_numbers),
                PatentTaxonomy.taxonomy_node_id.in_(list(node_ids)),
            )
        )
        patent_labels: dict = {}
        for ta in tax_result.all():
            if ta.taxonomy_label:
                patent_labels.setdefault(ta.patent_number, []).append(ta.taxonomy_label)
        pair_counts: dict = {}
        for labels in patent_labels.values():
            if len(labels) >= 2:
                unique_sorted = sorted(set(labels))
                for i in range(len(unique_sorted)):
                    for j in range(i + 1, len(unique_sorted)):
                        key = f"{unique_sorted[i]}|{unique_sorted[j]}"
                        pair_counts[key] = pair_counts.get(key, 0) + 1
        for key, count in sorted(pair_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
            l1, l2 = key.split("|")
            clustered_label_pairs.append({"label1": l1, "label2": l2, "co_occurrence_count": count})

    return {
        "period": period,
        "period_days": period_days,
        "insights": {
            "top_3_assignees": top_3_assignees,
            "top_taxonomy_labels": taxonomy_labels,
            "highest_growth_assignee": highest_growth_assignee,
            "highest_growth_taxonomy": highest_growth_taxonomy,
            "newest_labels": newest_labels,
            "clustered_label_pairs": clustered_label_pairs,
        },
    }


# ── COMPETITOR COMPARE ────────────────────────────────────────────────────────

@router.post("/analytics/competitor/compare")
async def compare_competitors(
    assignees: str = Form(...),
    workspace_id: Optional[str] = Form(None),
    date_type: str = Form("filing"),
    legal_statuses: Optional[str] = Form(None),
    taxonomy_labels: Optional[str] = Form(None),
    date_from: Optional[str] = Form(None),
    date_to: Optional[str] = Form(None),
    period: Optional[str] = Form(None),
    group_by: str = Form("month"),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _ws_uuid(workspace_id)
    if "|||" in assignees:
        assignee_list = [a.strip() for a in assignees.split("|||") if a.strip()]
    else:
        assignee_list = [assignees.strip()] if assignees.strip() else []
    if not assignee_list:
        return {"competitors": [], "time_series": [], "date_type": date_type}

    date_conds = _date_conditions(date_type, date_from, date_to, period)
    date_field = Patent.filing_date if date_type == "filing" else Patent.publication_date

    competitors_data = []
    for assignee in assignee_list:
        base_stmt = select(func.count()).select_from(Patent).where(Patent.assignee.ilike(f"%{assignee}%"))
        if ws_uuid:
            base_stmt = base_stmt.where(Patent.workspace_id == ws_uuid)
        total_patents = await db.scalar(base_stmt) or 0

        period_stmt = select(func.count()).select_from(Patent).where(Patent.assignee.ilike(f"%{assignee}%"))
        if ws_uuid:
            period_stmt = period_stmt.where(Patent.workspace_id == ws_uuid)
        for c in date_conds:
            period_stmt = period_stmt.where(c)
        period_patents = await db.scalar(period_stmt) or 0

        status_stmt = (
            select(Patent.legal_status, func.count().label("cnt"))
            .where(Patent.assignee.ilike(f"%{assignee}%"), Patent.legal_status.isnot(None))
            .group_by(Patent.legal_status)
        )
        if ws_uuid:
            status_stmt = status_stmt.where(Patent.workspace_id == ws_uuid)
        for c in date_conds:
            status_stmt = status_stmt.where(c)
        legal_breakdown = {r.legal_status: r.cnt for r in (await db.execute(status_stmt)).all()}

        competitors_data.append({
            "assignee": assignee,
            "total_patents": total_patents,
            "period_patents": period_patents,
            "legal_breakdown": legal_breakdown,
        })

    # Time series (last 12 months)
    now = datetime.now(timezone.utc)
    time_series = []
    for i in range(12):
        target_month = now.month - i
        target_year = now.year
        while target_month <= 0:
            target_month += 12
            target_year -= 1
        month_start = datetime(target_year, target_month, 1, tzinfo=timezone.utc)
        if target_month == 12:
            month_end = datetime(target_year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            month_end = datetime(target_year, target_month + 1, 1, tzinfo=timezone.utc)
        month_data = {"period": month_start.strftime("%Y-%m")}
        for assignee in assignee_list:
            stmt = select(func.count()).select_from(Patent).where(
                Patent.assignee.ilike(f"%{assignee}%"),
                date_field >= month_start,
                date_field < month_end,
            )
            if ws_uuid:
                stmt = stmt.where(Patent.workspace_id == ws_uuid)
            month_data[assignee] = await db.scalar(stmt) or 0
        time_series.append(month_data)
    time_series.reverse()

    return {
        "competitors": competitors_data,
        "time_series": time_series,
        "date_type": date_type,
        "date_range": {
            "start": (now - timedelta(days=365)).isoformat(),
            "end": now.isoformat(),
        },
        "filters_applied": {
            "legal_statuses": legal_statuses.split(",") if legal_statuses else [],
            "taxonomy_labels": taxonomy_labels.split(",") if taxonomy_labels else [],
        },
    }


# ── FILTERED PATENTS ──────────────────────────────────────────────────────────

@router.post("/analytics/patents/filtered")
async def get_filtered_patents(
    assignees: Optional[str] = Form(None),
    legal_statuses: Optional[str] = Form(None),
    taxonomy_labels: Optional[str] = Form(None),
    tagged_only: Optional[str] = Form(None),
    workspace_id: Optional[str] = Form(None),
    date_type: str = Form("filing"),
    date_from: Optional[str] = Form(None),
    date_to: Optional[str] = Form(None),
    period: Optional[str] = Form(None),
    skip: int = Form(0),
    limit: int = Form(25),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _ws_uuid(workspace_id)
    node_ids = await _get_ws_node_ids(db, ws_uuid)

    stmt = select(Patent)
    if ws_uuid:
        stmt = stmt.where(Patent.workspace_id == ws_uuid)

    for c in _assignee_conditions(assignees):
        stmt = stmt.where(c)

    if legal_statuses:
        status_list = [s.strip() for s in legal_statuses.split(",") if s.strip()]
        if status_list:
            stmt = stmt.where(Patent.legal_status.in_(status_list))

    if taxonomy_labels:
        label_list = [l.strip() for l in taxonomy_labels.split(",") if l.strip()]
        if label_list:
            pns = await _get_pns_by_taxonomy_labels(db, label_list, node_ids)
            if not pns:
                return {"patents": [], "total": 0}
            stmt = stmt.where(Patent.patent_number.in_(pns))
    elif tagged_only == "true":
        if node_ids:
            tagged_result = await db.scalars(
                select(PatentTaxonomy.patent_number).where(
                    PatentTaxonomy.taxonomy_node_id.in_(list(node_ids))
                )
            )
            tagged_pns = list(set(tagged_result.all()))
            if not tagged_pns:
                return {"patents": [], "total": 0}
            stmt = stmt.where(Patent.patent_number.in_(tagged_pns))
        else:
            return {"patents": [], "total": 0}

    for c in _date_conditions(date_type, date_from, date_to, period):
        stmt = stmt.where(c)

    total = await db.scalar(select(func.count()).select_from(stmt.subquery())) or 0
    result = await db.scalars(stmt.order_by(Patent.publication_date.desc()).offset(skip).limit(limit))
    patents = list(result.all())

    return {
        "patents": [
            {
                "id": str(p.id),
                "patent_number": p.patent_number,
                "title": p.title,
                "assignee": p.assignee,
                "publication_date": p.publication_date.isoformat() if p.publication_date else None,
                "legal_status": p.legal_status,
                "patent_url": p.patent_url,
            }
            for p in patents
        ],
        "total": total,
    }


# ── TECHNOLOGY LANDSCAPE ──────────────────────────────────────────────────────

@router.post("/analytics/technology/landscape")
async def get_technology_landscape(
    workspace_id: Optional[str] = Form(None),
    taxonomy_labels: Optional[str] = Form(None),
    assignees: Optional[str] = Form(None),
    legal_statuses: Optional[str] = Form(None),
    date_type: str = Form("filing"),
    date_from: Optional[str] = Form(None),
    date_to: Optional[str] = Form(None),
    period: Optional[str] = Form(None),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _ws_uuid(workspace_id)
    node_ids = await _get_ws_node_ids(db, ws_uuid)

    # All taxonomy nodes for workspace (static count)
    tax_nodes_result = await db.scalars(
        select(TaxonomyNode).where(TaxonomyNode.workspace_id == ws_uuid) if ws_uuid
        else select(TaxonomyNode)
    )
    taxonomy_nodes = list(tax_nodes_result.all())
    node_map = {n.node_id: n for n in taxonomy_nodes}

    # Build filtered patent query
    stmt = select(Patent.patent_number, Patent.assignee)
    if ws_uuid:
        stmt = stmt.where(Patent.workspace_id == ws_uuid)
    for c in _assignee_conditions(assignees):
        stmt = stmt.where(c)
    if legal_statuses:
        sl = [s.strip() for s in legal_statuses.split(",") if s.strip()]
        if sl:
            stmt = stmt.where(Patent.legal_status.in_(sl))
    for c in _date_conditions(date_type, date_from, date_to, period):
        stmt = stmt.where(c)

    ws_patents = list((await db.execute(stmt)).all())
    ws_pns = {p.patent_number for p in ws_patents}

    taxonomy_counts: dict = {}
    tagged_pns: set = set()

    if ws_pns and node_ids:
        tax_result = await db.scalars(
            select(PatentTaxonomy).where(
                PatentTaxonomy.patent_number.in_(list(ws_pns)),
                PatentTaxonomy.taxonomy_node_id.in_(list(node_ids)),
            )
        )
        for ta in tax_result.all():
            taxonomy_counts[ta.taxonomy_node_id] = taxonomy_counts.get(ta.taxonomy_node_id, 0) + 1
            tagged_pns.add(ta.patent_number)

    taxonomy_distribution = []
    for nid, count in taxonomy_counts.items():
        node = node_map.get(nid)
        if node:
            taxonomy_distribution.append({
                "node_id": nid,
                "label": node.label,
                "patent_count": count,
                "level": node.level,
            })
    taxonomy_distribution.sort(key=lambda x: x["patent_count"], reverse=True)

    # Assignee breakdown for top 5 taxonomies
    taxonomy_assignee_breakdown: dict = {}
    for tax in taxonomy_distribution[:5]:
        ta_result = await db.scalars(
            select(PatentTaxonomy.patent_number).where(
                PatentTaxonomy.taxonomy_node_id == tax["node_id"],
                PatentTaxonomy.patent_number.in_(list(ws_pns)),
            )
        )
        tax_pns = list(ta_result.all())
        if tax_pns:
            asg_stmt = (
                select(Patent.assignee, func.count().label("cnt"))
                .where(Patent.patent_number.in_(tax_pns), Patent.assignee.isnot(None))
                .group_by(Patent.assignee)
                .order_by(func.count().desc())
                .limit(5)
            )
            taxonomy_assignee_breakdown[tax["label"]] = [
                {"assignee": r.assignee, "count": r.cnt}
                for r in (await db.execute(asg_stmt)).all()
            ]

    return {
        "taxonomy_distribution": taxonomy_distribution[:20],
        "total_taxonomies": len(taxonomy_nodes),
        "total_tagged": len(tagged_pns),
        "total_patents_in_scope": len(ws_pns),
        "taxonomy_assignee_breakdown": taxonomy_assignee_breakdown,
        "filters_applied": {
            "assignees": assignees.split("|||") if assignees else [],
            "legal_statuses": legal_statuses.split(",") if legal_statuses else [],
            "date_type": date_type,
            "period": period,
        },
    }


# ── TECHNOLOGY BY ASSIGNEE ────────────────────────────────────────────────────

@router.post("/analytics/technology-by-assignee")
async def get_technology_by_assignee(
    assignees: str = Form(...),
    workspace_id: Optional[str] = Form(None),
    date_type: str = Form("filing"),
    date_from: Optional[str] = Form(None),
    date_to: Optional[str] = Form(None),
    period: Optional[str] = Form(None),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _ws_uuid(workspace_id)
    if "|||" in assignees:
        assignee_list = [a.strip() for a in assignees.split("|||") if a.strip()]
    else:
        assignee_list = [assignees.strip()] if assignees.strip() else []
    if not assignee_list:
        return {"assignees_data": [], "all_technologies": [], "message": "No assignees provided"}

    node_ids = await _get_ws_node_ids(db, ws_uuid)
    if not node_ids:
        return {"assignees_data": [], "all_technologies": [], "message": "No taxonomy nodes found"}

    tax_nodes_result = await db.scalars(
        select(TaxonomyNode).where(TaxonomyNode.node_id.in_(list(node_ids)))
    )
    node_map = {n.node_id: n.label for n in tax_nodes_result.all()}
    date_conds = _date_conditions(date_type, date_from, date_to, period)

    assignees_data = []
    all_technologies: set = set()

    for assignee in assignee_list:
        stmt = select(Patent.patent_number).where(Patent.assignee.ilike(f"%{assignee}%"))
        if ws_uuid:
            stmt = stmt.where(Patent.workspace_id == ws_uuid)
        for c in date_conds:
            stmt = stmt.where(c)
        pns = list((await db.scalars(stmt)).all())

        tech_counts: dict = {}
        if pns:
            ta_result = await db.scalars(
                select(PatentTaxonomy).where(
                    PatentTaxonomy.patent_number.in_(pns),
                    PatentTaxonomy.taxonomy_node_id.in_(list(node_ids)),
                )
            )
            for ta in ta_result.all():
                label = node_map.get(ta.taxonomy_node_id, ta.taxonomy_node_id)
                tech_counts[label] = tech_counts.get(label, 0) + 1
                all_technologies.add(label)

        assignees_data.append({
            "assignee": assignee,
            "total_patents": len(pns),
            "technology_counts": tech_counts,
        })

    # Sort technologies by total
    tech_totals: dict = {}
    for ad in assignees_data:
        for tech, cnt in ad["technology_counts"].items():
            tech_totals[tech] = tech_totals.get(tech, 0) + cnt
    sorted_techs = sorted(tech_totals.keys(), key=lambda x: tech_totals[x], reverse=True)[:15]

    return {
        "assignees_data": assignees_data,
        "all_technologies": sorted_techs,
        "filters_applied": {
            "date_type": date_type,
            "period": period,
            "date_from": date_from,
            "date_to": date_to,
        },
    }


# ── LEGAL STATUS ──────────────────────────────────────────────────────────────

@router.post("/analytics/legal-status")
async def get_legal_status_analytics(
    workspace_id: Optional[str] = Form(None),
    assignees: Optional[str] = Form(None),
    taxonomy_labels: Optional[str] = Form(None),
    legal_statuses: Optional[str] = Form(None),
    date_type: str = Form("filing"),
    date_from: Optional[str] = Form(None),
    date_to: Optional[str] = Form(None),
    period: Optional[str] = Form(None),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _ws_uuid(workspace_id)
    node_ids = await _get_ws_node_ids(db, ws_uuid)

    # Total patents (period-agnostic, no date/assignee filter)
    total_stmt = select(func.count()).select_from(Patent).where(Patent.legal_status.isnot(None))
    if ws_uuid:
        total_stmt = total_stmt.where(Patent.workspace_id == ws_uuid)
    total_patents = await db.scalar(total_stmt) or 0

    # Build filtered conditions list
    base_conds = [Patent.legal_status.isnot(None)]
    if ws_uuid:
        base_conds.append(Patent.workspace_id == ws_uuid)
    if legal_statuses:
        sl = [s.strip() for s in legal_statuses.split(",") if s.strip()]
        if sl:
            base_conds.append(or_(*[Patent.legal_status.ilike(f"%{s}%") for s in sl]))
    base_conds.extend(_assignee_conditions(assignees))
    base_conds.extend(_date_conditions(date_type, date_from, date_to, period))

    if taxonomy_labels:
        label_list = [l.strip() for l in taxonomy_labels.split(",") if l.strip()]
        if label_list:
            pns = await _get_pns_by_taxonomy_labels(db, label_list, node_ids)
            if not pns:
                return {
                    "status_distribution": [],
                    "total_patents": total_patents,
                    "period_patents": 0,
                    "filters_applied": {
                        "assignees": assignees.split("|||") if assignees else [],
                        "taxonomy_labels": label_list,
                        "date_type": date_type,
                        "period": period,
                    },
                }
            base_conds.append(Patent.patent_number.in_(pns))

    grp_stmt = (
        select(Patent.legal_status, func.count().label("cnt"))
        .where(and_(*base_conds))
        .group_by(Patent.legal_status)
        .order_by(func.count().desc())
    )
    dist_result = list((await db.execute(grp_stmt)).all())
    period_patents = sum(r.cnt for r in dist_result)

    return {
        "status_distribution": [{"status": r.legal_status, "count": r.cnt} for r in dist_result],
        "total_patents": total_patents,
        "period_patents": period_patents,
        "filters_applied": {
            "assignees": assignees.split("|||") if assignees else [],
            "taxonomy_labels": taxonomy_labels.split(",") if taxonomy_labels else [],
            "date_type": date_type,
            "period": period,
        },
    }


# ── CITATION ──────────────────────────────────────────────────────────────────

@router.get("/analytics/top-cited-patents")
async def get_top_cited_patents(
    mode: str = "top10",
    sort_by: str = "backward",
    workspace_id: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _ws_uuid(workspace_id)
    limit = {"top10": 10, "top20": 20, "top1percent": 100}.get(mode, 10)
    sort_field = Patent.forward_citation_count if sort_by == "forward" else Patent.backward_citation_count
    stmt = select(Patent).where(sort_field > 0)
    if ws_uuid:
        stmt = stmt.where(Patent.workspace_id == ws_uuid)
    result = await db.scalars(stmt.order_by(sort_field.desc()).limit(limit))
    patents = list(result.all())
    return {
        "patents": [
            {
                "patent_number": p.patent_number,
                "title": p.title,
                "assignee": p.assignee,
                "forward_citation_count": p.forward_citation_count or 0,
                "backward_citation_count": p.backward_citation_count or 0,
                "patent_url": p.patent_url,
            }
            for p in patents
        ],
        "mode": mode,
        "sort_by": sort_by,
    }


@router.post("/analytics/citation-analysis")
async def get_citation_analysis(
    mode: str = Form("top10"),
    workspace_id: Optional[str] = Form(None),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _ws_uuid(workspace_id)
    limit = {"top10": 10, "top20": 20, "top1percent": 100}.get(mode, 10)
    stmt = select(Patent).where(Patent.forward_citation_count > 0)
    if ws_uuid:
        stmt = stmt.where(Patent.workspace_id == ws_uuid)
    result = await db.scalars(stmt.order_by(Patent.forward_citation_count.desc()).limit(limit))
    patents = list(result.all())
    return {
        "patents": [
            {
                "patent_number": p.patent_number,
                "title": p.title,
                "assignee": p.assignee,
                "forward_citation_count": p.forward_citation_count or 0,
                "backward_citation_count": p.backward_citation_count or 0,
            }
            for p in patents
        ],
        "mode": mode,
    }


@router.get("/analytics/citation-stats")
async def get_citation_stats(
    workspace_id: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _ws_uuid(workspace_id)
    stmt = select(
        func.sum(func.coalesce(Patent.forward_citation_count, 0)).label("total_forward"),
        func.sum(func.coalesce(Patent.backward_citation_count, 0)).label("total_backward"),
        func.avg(Patent.forward_citation_count).label("avg_forward"),
        func.avg(Patent.backward_citation_count).label("avg_backward"),
        func.max(Patent.forward_citation_count).label("max_forward"),
        func.max(Patent.backward_citation_count).label("max_backward"),
        func.count(case((Patent.backward_citation_count > 0, 1))).label("with_citations"),
    )
    if ws_uuid:
        stmt = stmt.where(Patent.workspace_id == ws_uuid)
    row = (await db.execute(stmt)).one()
    return {
        "total_forward_citations": row.total_forward or 0,
        "total_backward_citations": row.total_backward or 0,
        "avg_forward_citations": round(float(row.avg_forward or 0), 2),
        "avg_backward_citations": round(float(row.avg_backward or 0), 2),
        "max_forward_citations": row.max_forward or 0,
        "max_backward_citations": row.max_backward or 0,
        "patents_with_citations": row.with_citations or 0,
    }


# ── FAMILY ANALYSIS ───────────────────────────────────────────────────────────

@router.get("/analytics/family-analysis")
@router.get("/analytics/family-patents")
async def get_family_analysis(
    min_family_members: int = 2,
    limit: int = 100,
    workspace_id: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _ws_uuid(workspace_id)
    stmt = select(Patent).where(Patent.family_members_count >= min_family_members)
    if ws_uuid:
        stmt = stmt.where(Patent.workspace_id == ws_uuid)
    result = await db.scalars(stmt.order_by(Patent.family_members_count.desc()).limit(limit))
    patents = list(result.all())
    return {
        "patents": [
            {
                "patent_number": p.patent_number,
                "title": p.title,
                "assignee": p.assignee,
                "family_members_count": p.family_members_count or 0,
                "patent_family_id": p.patent_family_id,
                "legal_status": p.legal_status,
                "patent_url": p.patent_url,
            }
            for p in patents
        ],
        "min_family_members": min_family_members,
    }


@router.get("/analytics/family-stats")
async def get_family_stats(
    workspace_id: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _ws_uuid(workspace_id)
    total_stmt = select(func.count()).select_from(Patent)
    if ws_uuid:
        total_stmt = total_stmt.where(Patent.workspace_id == ws_uuid)
    total_patents = await db.scalar(total_stmt) or 0

    stats_stmt = select(
        func.avg(Patent.family_members_count).label("avg"),
        func.max(Patent.family_members_count).label("max"),
        func.count(case((Patent.family_members_count > 1, 1))).label("with_family"),
    )
    if ws_uuid:
        stats_stmt = stats_stmt.where(Patent.workspace_id == ws_uuid)
    row = (await db.execute(stats_stmt)).one()

    dist_stmt = (
        select(Patent.family_members_count.label("fmc"), func.count().label("cnt"))
        .group_by(Patent.family_members_count)
        .order_by(Patent.family_members_count)
        .limit(20)
    )
    if ws_uuid:
        dist_stmt = dist_stmt.where(Patent.workspace_id == ws_uuid)
    distribution = [{"family_size": r.fmc or 0, "count": r.cnt} for r in (await db.execute(dist_stmt)).all()]

    return {
        "total_patents": total_patents,
        "avg_family_size": round(float(row.avg or 0), 2),
        "max_family_size": row.max or 0,
        "patents_with_family": row.with_family or 0,
        "distribution": distribution,
    }


# ── EXPORT ────────────────────────────────────────────────────────────────────

@router.get("/analytics/export/{workspace_id}")
async def export_workspace_data(
    workspace_id: str,
    assignees: Optional[str] = None,
    date_type: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _ws_uuid(workspace_id)
    node_ids = await _get_ws_node_ids(db, ws_uuid)

    stmt = select(Patent).where(Patent.workspace_id == ws_uuid)
    if assignees:
        stmt = stmt.where(Patent.assignee.ilike(f"%{assignees}%"))
    if date_type and (date_from or date_to):
        for c in _date_conditions(date_type, date_from, date_to):
            stmt = stmt.where(c)
    result = await db.scalars(stmt.order_by(Patent.publication_date.desc()))
    patents = list(result.all())
    patent_numbers = [p.patent_number for p in patents]

    # Workspace taxonomy labels
    tax_labels_result = await db.scalars(
        select(TaxonomyNode.label).where(TaxonomyNode.workspace_id == ws_uuid)
    )
    all_taxonomy_labels = sorted([l for l in tax_labels_result.all() if l])

    # Taxonomy assignments scoped to this workspace's nodes
    taxonomy_assignments: dict = {}
    if patent_numbers and node_ids:
        ta_result = await db.scalars(
            select(PatentTaxonomy).where(
                PatentTaxonomy.patent_number.in_(patent_numbers),
                PatentTaxonomy.taxonomy_node_id.in_(list(node_ids)),
            )
        )
        for ta in ta_result.all():
            taxonomy_assignments.setdefault(ta.patent_number, []).append({
                "node_id": ta.taxonomy_node_id,
                "label": ta.taxonomy_label,
            })

    patent_dicts = [_patent_to_full_dict(p) for p in patents]
    all_fields = sorted(list({k for d in patent_dicts for k in d.keys()}))

    return {
        "patents": patent_dicts,
        "taxonomy_assignments": taxonomy_assignments,
        "taxonomy_labels": all_taxonomy_labels,
        "all_taxonomy_labels": all_taxonomy_labels,
        "all_patent_fields": all_fields,
        "total": len(patents),
    }


@router.post("/analytics/export/filtered")
async def export_filtered_data(
    patent_numbers: Optional[str] = Form(None),
    workspace_id: Optional[str] = Form(None),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _ws_uuid(workspace_id)
    node_ids = await _get_ws_node_ids(db, ws_uuid)

    stmt = select(Patent)
    if patent_numbers:
        pn_list = [pn.strip() for pn in patent_numbers.split(",") if pn.strip()]
        stmt = stmt.where(Patent.patent_number.in_(pn_list))
    if ws_uuid:
        stmt = stmt.where(Patent.workspace_id == ws_uuid)
    result = await db.scalars(stmt)
    patents = list(result.all())
    pn_list = [p.patent_number for p in patents]

    all_taxonomy_labels = []
    if ws_uuid:
        tl_result = await db.scalars(
            select(TaxonomyNode.label).where(TaxonomyNode.workspace_id == ws_uuid)
        )
        all_taxonomy_labels = sorted([l for l in tl_result.all() if l])

    taxonomy_assignments: dict = {}
    if pn_list and node_ids:
        ta_result = await db.scalars(
            select(PatentTaxonomy).where(
                PatentTaxonomy.patent_number.in_(pn_list),
                PatentTaxonomy.taxonomy_node_id.in_(list(node_ids)),
            )
        )
        for ta in ta_result.all():
            taxonomy_assignments.setdefault(ta.patent_number, []).append({
                "node_id": ta.taxonomy_node_id,
                "label": ta.taxonomy_label,
            })

    patent_dicts = [_patent_to_full_dict(p) for p in patents]
    all_fields = sorted(list({k for d in patent_dicts for k in d.keys()}))

    return {
        "patents": patent_dicts,
        "taxonomy_assignments": taxonomy_assignments,
        "all_taxonomy_labels": all_taxonomy_labels,
        "all_patent_fields": all_fields,
        "total": len(patents),
    }


# ── GENERIC AGGREGATION (powers the custom chart builder + new dashboards) ─────
# All aggregation runs in Postgres and returns at most a few hundred grouped
# rows — raw patents (5..60k) never reach the browser.

# Dimensions that map directly to a Patent column.
_DIM_COLUMNS = {
    "assignee": Patent.assignee,
    "legal_status": Patent.legal_status,
    "jurisdiction": Patent.jurisdiction,
    "publication_country": Patent.publication_country,
    "cpc_class": Patent.cpc_class,
    "ipc_class": Patent.ipc_class,
    "review_status": Patent.review_status,
}

# Date dimensions -> (column, to_char format, date_trunc unit).
_DATE_DIMS = {
    "filing_year": (Patent.filing_date, "YYYY", "year"),
    "publication_year": (Patent.publication_date, "YYYY", "year"),
    "grant_year": (Patent.grant_date, "YYYY", "year"),
    "filing_month": (Patent.filing_date, "YYYY-MM", "month"),
    "publication_month": (Patent.publication_date, "YYYY-MM", "month"),
}


def _bucket_case(column, edges, labels):
    """Build a CASE expression bucketing a numeric column. `edges` are upper
    bounds (inclusive); `labels` has one more entry than `edges` for the
    overflow bucket. NULLs fall through to 'Unknown'."""
    whens = []
    prev = None
    for edge, label in zip(edges, labels):
        if prev is None:
            whens.append((column <= edge, label))
        else:
            whens.append((and_(column > prev, column <= edge), label))
        prev = edge
    expr = case(*whens, else_=labels[-1])
    return case((column.is_(None), "Unknown"), else_=expr)


_BUCKET_DIMS = {
    "family_size_bucket": lambda: _bucket_case(
        Patent.family_members_count, [1, 5, 10, 20], ["1", "2-5", "6-10", "11-20", "21+"]),
    "claims_bucket": lambda: _bucket_case(
        Patent.claims_count, [5, 10, 20, 40], ["1-5", "6-10", "11-20", "21-40", "41+"]),
    "forward_citation_bucket": lambda: _bucket_case(
        Patent.forward_citation_count, [0, 5, 20, 50], ["0", "1-5", "6-20", "21-50", "51+"]),
    "backward_citation_bucket": lambda: _bucket_case(
        Patent.backward_citation_count, [0, 5, 20, 50], ["0", "1-5", "6-20", "21-50", "51+"]),
}


async def _aggregate_conditions(db, ws_uuid, assignees, legal_statuses, taxonomy, date_type, date_from, date_to):
    conds = []
    if ws_uuid:
        conds.append(Patent.workspace_id == ws_uuid)
    conds += _assignee_conditions(assignees)
    if legal_statuses:
        statuses = [s.strip() for s in legal_statuses.split(",") if s.strip()]
        if statuses:
            conds.append(Patent.legal_status.in_(statuses))
    if date_type and (date_from or date_to):
        conds += _date_conditions(date_type, date_from, date_to)
    if taxonomy:
        labels = [t.strip() for t in taxonomy.split(",") if t.strip()]
        node_ids = await _get_ws_node_ids(db, ws_uuid)
        pns = await _get_pns_by_taxonomy_labels(db, labels, node_ids)
        if pns is not None:
            conds.append(Patent.patent_number.in_(pns if pns else ["__none__"]))
    return conds


@router.post("/analytics/aggregate")
async def aggregate(
    workspace_id: Optional[str] = Form(None),
    dimension: str = Form(...),
    measure: str = Form("count"),
    top_n: int = Form(20),
    assignees: Optional[str] = Form(None),
    legal_statuses: Optional[str] = Form(None),
    taxonomy: Optional[str] = Form(None),
    date_type: Optional[str] = Form(None),
    date_from: Optional[str] = Form(None),
    date_to: Optional[str] = Form(None),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Group patents by `dimension`, aggregate by `measure`. Returns
    [{label, value}] plus a total. Categorical dims are reduced to top_n with
    an 'Other' bucket; date dims return chronologically; bucket dims keep their
    natural bucket order."""
    ws_uuid = _ws_uuid(workspace_id)
    conds = await _aggregate_conditions(
        db, ws_uuid, assignees, legal_statuses, taxonomy, date_type, date_from, date_to)

    # Measure expression
    if measure == "avg_forward_citations":
        measure_expr = func.round(func.avg(Patent.forward_citation_count), 1)
    elif measure == "avg_backward_citations":
        measure_expr = func.round(func.avg(Patent.backward_citation_count), 1)
    elif measure == "sum_family_size":
        measure_expr = func.sum(Patent.family_members_count)
    else:
        measure_expr = func.count()

    is_temporal = dimension in _DATE_DIMS
    is_bucket = dimension in _BUCKET_DIMS

    if dimension == "taxonomy_label":
        node_ids = await _get_ws_node_ids(db, ws_uuid)
        stmt = (
            select(PatentTaxonomy.taxonomy_label.label("k"), func.count().label("v"))
            .join(Patent, Patent.patent_number == PatentTaxonomy.patent_number)
            .where(and_(*conds))
            .group_by(PatentTaxonomy.taxonomy_label)
        )
        if node_ids:
            stmt = stmt.where(PatentTaxonomy.taxonomy_node_id.in_(list(node_ids)))
    elif is_temporal:
        col, fmt, unit = _DATE_DIMS[dimension]
        key = func.to_char(func.date_trunc(unit, col), fmt)
        stmt = (
            select(key.label("k"), measure_expr.label("v"))
            .where(and_(col.isnot(None), *conds))
            .group_by(key)
        )
    elif is_bucket:
        key = _BUCKET_DIMS[dimension]()
        stmt = (
            select(key.label("k"), measure_expr.label("v"))
            .where(and_(*conds))
            .group_by(key)
        )
    elif dimension in _DIM_COLUMNS:
        col = _DIM_COLUMNS[dimension]
        stmt = (
            select(col.label("k"), measure_expr.label("v"))
            .where(and_(col.isnot(None), *conds))
            .group_by(col)
        )
    else:
        raise HTTPException(status_code=400, detail=f"Unknown dimension: {dimension}")

    result = await db.execute(stmt)
    rows = [
        {"label": (r.k if r.k is not None else "—"), "value": float(r.v or 0)}
        for r in result
    ]

    # Order + shape per dimension kind
    if is_temporal:
        rows.sort(key=lambda d: d["label"])
    elif is_bucket:
        order = {lab: i for i, lab in enumerate(
            ["Unknown", "0", "1", "1-5", "2-5", "6-10", "6-20", "11-20", "21-40",
             "21-50", "21+", "41+", "51+"])}
        rows.sort(key=lambda d: order.get(d["label"], 999))
    else:
        rows.sort(key=lambda d: d["value"], reverse=True)
        if top_n and len(rows) > top_n:
            head = rows[:top_n]
            tail = rows[top_n:]
            other_val = sum(d["value"] for d in tail)
            if other_val > 0:
                head.append({"label": f"Other ({len(tail)})", "value": other_val})
            rows = head

    # Count measures should come back as ints
    if measure == "count" or measure == "sum_family_size":
        for r in rows:
            r["value"] = int(r["value"])

    return {
        "dimension": dimension,
        "measure": measure,
        "data": rows,
        "total": sum(r["value"] for r in rows),
    }


# ── SAVED DASHBOARD VIEWS (per user + workspace) ──────────────────────────────

def _view_dict(v: DashboardView) -> dict:
    return {
        "id": str(v.id),
        "name": v.name,
        "config": v.config,
        "sort_order": v.sort_order,
        "created_at": v.created_at.isoformat() if v.created_at else None,
        "updated_at": v.updated_at.isoformat() if v.updated_at else None,
    }


def _current_user_uuid(current_user: dict) -> uuid.UUID:
    try:
        return uuid.UUID(current_user["user_id"])
    except (KeyError, ValueError, TypeError):
        raise HTTPException(status_code=401, detail="Invalid user token")


@router.get("/analytics/views")
async def list_dashboard_views(
    workspace_id: str,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _ws_uuid(workspace_id)
    uid = _current_user_uuid(current_user)
    result = await db.scalars(
        select(DashboardView)
        .where(DashboardView.workspace_id == ws_uuid, DashboardView.user_id == uid)
        .order_by(DashboardView.sort_order, DashboardView.created_at)
    )
    return {"views": [_view_dict(v) for v in result.all()]}


@router.post("/analytics/views")
async def create_dashboard_view(
    workspace_id: str = Form(...),
    name: str = Form(...),
    config: str = Form(...),
    sort_order: int = Form(0),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _ws_uuid(workspace_id)
    uid = _current_user_uuid(current_user)
    try:
        config_obj = json.loads(config)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="config must be valid JSON")
    view = DashboardView(
        workspace_id=ws_uuid, user_id=uid,
        name=name.strip() or "Untitled chart",
        config=config_obj, sort_order=sort_order,
    )
    db.add(view)
    await db.commit()
    await db.refresh(view)
    return _view_dict(view)


@router.put("/analytics/views/{view_id}")
async def update_dashboard_view(
    view_id: str,
    name: Optional[str] = Form(None),
    config: Optional[str] = Form(None),
    sort_order: Optional[int] = Form(None),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    uid = _current_user_uuid(current_user)
    try:
        vid = uuid.UUID(view_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid view id")
    view = await db.scalar(
        select(DashboardView).where(DashboardView.id == vid, DashboardView.user_id == uid)
    )
    if not view:
        raise HTTPException(status_code=404, detail="View not found")
    if name is not None:
        view.name = name.strip() or view.name
    if config is not None:
        try:
            view.config = json.loads(config)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="config must be valid JSON")
    if sort_order is not None:
        view.sort_order = sort_order
    await db.commit()
    await db.refresh(view)
    return _view_dict(view)


@router.delete("/analytics/views/{view_id}")
async def delete_dashboard_view(
    view_id: str,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    uid = _current_user_uuid(current_user)
    try:
        vid = uuid.UUID(view_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid view id")
    result = await db.execute(
        delete(DashboardView).where(DashboardView.id == vid, DashboardView.user_id == uid)
    )
    await db.commit()
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="View not found")
    return {"message": "View deleted", "id": view_id}
