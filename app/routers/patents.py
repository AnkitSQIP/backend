import uuid
import io
import logging
from datetime import datetime, timezone
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, update, func, or_, and_
import pandas as pd

from app.models import Patent, PatentTaxonomy, InvestigationQueueItem
from app.deps import get_db, get_current_user
from app.services.watchlist import check_watchlist_rules_batch
from app.database import AsyncSessionLocal


class BulkReopenBody(BaseModel):
    workspace_id: str
    patent_ids: list[str]

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["patents"])

COLUMN_MAPPING = {
    "patent_number": ["Record Number", "Patent Number", "Publication Number", "patent_number", "publication_number"],
    "title": ["Title", "Invention Title", "title"],
    "abstract": ["Abstract", "abstract"],
    "assignee": ["Current Assignee", "Assignee - Standardized", "Assignee", "assignee"],
    "inventor": ["Inventors", "Inventor", "inventor"],
    "filing_date": ["Filing/Application Date", "Filing Date", "Application Date", "filing_date"],
    "publication_date": ["Publication/Issue Date", "Publication Date", "publication_date"],
    "grant_date": ["Grant Date", "grant_date"],
    "legal_status": ["Legal Status Current", "Legal Status", "legal_status", "Status"],
    "cpc_class": ["CPC", "CPC Class", "cpc_class"],
    "claims_text": ["Claims", "claims_en", "claims_text"],
    "patent_url": ["Hyperlink", "hyperlink", "URL", "url", "patent_url", "Link", "link", "Patent URL", "PatSeer Link", "PatSeer URL"],
    "family_members_count": ["No. of Simple Family Members", "Simple Family Size", "Family Size", "family_members_count"],
    "family_members": ["Simple Family Members", "Family Members List", "family_members"],
    "patent_family_id": ["Simple Family ID", "Family ID", "family_id", "patent_family_id"],
    "forward_citation_count": ["No. of Forward Citing Families", "Forward Citations", "Cited By Count", "forward_citation_count"],
    "backward_citation_count": ["Backward Citation Count", "Backward Citations", "References Count", "backward_citation_count"],
    "publication_country": ["Publication Country", "Country", "publication_country"],
    "num_claims": ["Number Of Claims", "Claims Count", "num_claims"],
    "independent_claims_count": ["No. of Independent Claims", "Independent Claims Count", "independent_claims_count"],
    "first_claim": ["First Claim", "first_claim"],
}

# Max string lengths matching DB model constraints
_STRING_LIMITS = {
    "cpc_class": 200,
    "ipc_class": 200,
    "legal_status": 100,
    "publication_country": 50,
    "assignee": 500,
    "patent_number": 100,
}


def _find_col(df, names):
    for n in names:
        if n in df.columns:
            return n
    return None


def _parse_date(val):
    if val is None or (hasattr(val, "__class__") and val.__class__.__name__ == "float"):
        return None
    try:
        import pandas as _pd
        if _pd.isna(val):
            return None
    except Exception:
        pass
    try:
        if isinstance(val, str):
            for fmt in ["%Y-%m-%d", "%Y%m%d", "%d/%m/%Y", "%m/%d/%Y"]:
                try:
                    return datetime.strptime(val.strip(), fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
        return pd.to_datetime(val).to_pydatetime().replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _parse_ws_uuid(workspace_id: str) -> uuid.UUID:
    try:
        return uuid.UUID(workspace_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400, detail="Invalid workspace_id")


# ── UPLOAD ──────────────────────────────────────────────────────────────────

@router.post("/patents/upload")
async def upload_patents(
    file: UploadFile = File(...),
    workspace_id: str = Form(...),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _parse_ws_uuid(workspace_id)
    content = await file.read()

    # Extract hyperlinks from Excel
    hyperlinks_map = {}
    fname = file.filename or ""
    if fname.endswith(".xlsx") or fname.endswith(".xls"):
        try:
            import openpyxl
            wb = openpyxl.load_workbook(io.BytesIO(content), data_only=False)
            ws_sheet = wb.active
            for row_num in range(2, ws_sheet.max_row + 1):
                cell = ws_sheet.cell(row=row_num, column=1)
                if cell.hyperlink and cell.hyperlink.target and cell.value:
                    hyperlinks_map[str(cell.value).strip()] = cell.hyperlink.target
            logger.info(f"Extracted {len(hyperlinks_map)} hyperlinks")
        except Exception as e:
            logger.warning(f"Could not extract hyperlinks: {e}")
        df = pd.read_excel(io.BytesIO(content))
    else:
        df = pd.read_csv(io.BytesIO(content))

    logger.info(f"Upload columns: {list(df.columns)}")

    created_count = 0
    skipped_count = 0
    duplicate_patents = []
    uploaded_patents = []

    pn_col = _find_col(df, COLUMN_MAPPING["patent_number"])

    # Bulk duplicate check — ONE query for all patent numbers in workspace
    # Replaces N individual SELECT per row (was N DB round-trips for N rows)
    all_incoming_pnums = []
    for _, row in df.iterrows():
        if pn_col and not pd.isna(row[pn_col]):
            all_incoming_pnums.append(str(row[pn_col]).strip())
    existing_pnums: set[str] = set()
    if all_incoming_pnums:
        existing_result = await db.scalars(
            select(Patent.patent_number).where(
                Patent.workspace_id == ws_uuid,
                Patent.patent_number.in_(all_incoming_pnums),
            )
        )
        existing_pnums = set(existing_result.all())

    title_col = _find_col(df, COLUMN_MAPPING["title"])
    abstract_col = _find_col(df, COLUMN_MAPPING["abstract"])

    for _, row in df.iterrows():
        if not pn_col or pd.isna(row[pn_col]):
            continue
        patent_number = str(row[pn_col]).strip()

        # O(1) duplicate check via set — no DB call per row
        if patent_number in existing_pnums:
            skipped_count += 1
            if len(duplicate_patents) < 10:
                duplicate_patents.append(patent_number)
            continue
        title = str(row[title_col]) if title_col and not pd.isna(row.get(title_col)) else "No Title"
        abstract = str(row[abstract_col]) if abstract_col and not pd.isna(row.get(abstract_col)) else ""

        patent_data: dict = {
            "workspace_id": ws_uuid,
            "patent_number": patent_number,
            "title": title,
            "abstract": abstract,
            "combined_text": f"{title} {abstract}",
        }

        for field, cols in COLUMN_MAPPING.items():
            if field in ("patent_number", "title", "abstract"):
                continue
            col = _find_col(df, cols)
            if col is None:
                continue
            val = row.get(col)
            if val is None or (hasattr(val, "__class__") and val.__class__.__name__ == "float"):
                continue
            try:
                if pd.isna(val):
                    continue
            except Exception:
                pass
            if "date" in field:
                patent_data[field] = _parse_date(val)
            elif "count" in field or field in ("num_claims", "independent_claims_count"):
                try:
                    patent_data[field] = int(float(val))
                except Exception:
                    patent_data[field] = None
            else:
                sval = str(val).strip()
                limit = _STRING_LIMITS.get(field)
                patent_data[field] = sval[:limit] if limit else sval

        if patent_number in hyperlinks_map:
            patent_data["patent_url"] = hyperlinks_map[patent_number]

        db.add(Patent(**patent_data))
        created_count += 1
        uploaded_patents.append(patent_data)

        # Commit every 1000 rows — prevents one massive transaction for large uploads
        if created_count % 1000 == 0:
            await db.commit()
            logger.info(f"Upload: committed {created_count} patents so far")

    await db.commit()  # final commit for remaining rows

    # Background embedding generation (non-blocking — doesn't delay upload response)
    if uploaded_patents:
        import asyncio as _asyncio
        _asyncio.create_task(_generate_embeddings_bg(str(ws_uuid), db))

    # Watchlist batch check
    alerts_info = []
    if uploaded_patents:
        try:
            # Convert patent dicts: replace uuid workspace_id with str for watchlist service
            wl_patents = []
            for p in uploaded_patents:
                pd_copy = {k: v for k, v in p.items() if k != "workspace_id"}
                wl_patents.append(pd_copy)
            alerts_info = await check_watchlist_rules_batch(db, wl_patents, str(ws_uuid))
        except Exception as e:
            logger.warning(f"Watchlist batch check failed: {e}")

    response = {
        "message": "Upload successful",
        "created": created_count,
        "skipped_duplicates": skipped_count,
        "total_in_file": created_count + skipped_count,
        "alerts_generated": len(alerts_info),
    }
    if duplicate_patents:
        response["duplicate_examples"] = duplicate_patents
        if skipped_count > 10:
            response["duplicate_examples"].append(f"... and {skipped_count - 10} more")
    return response


# ── LIST / GET / SEARCH ──────────────────────────────────────────────────────

@router.get("/patents")
async def list_patents(
    workspace_id: Optional[str] = None,
    skip: int = 0,
    limit: int = 25,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Patent)
    if workspace_id:
        ws_uuid = _parse_ws_uuid(workspace_id)
        stmt = stmt.where(Patent.workspace_id == ws_uuid)
    count_stmt = select(func.count()).select_from(stmt.subquery())
    total = await db.scalar(count_stmt)
    result = await db.scalars(stmt.offset(skip).limit(limit))
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
            }
            for p in patents
        ],
        "total": total or 0,
        "skip": skip,
        "limit": limit,
    }


@router.get("/patents/{patent_number}")
async def get_patent_detail(
    patent_number: str,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    patent = await db.scalar(select(Patent).where(Patent.patent_number == patent_number))
    if not patent:
        raise HTTPException(status_code=404, detail="Patent not found")

    tax_result = await db.scalars(
        select(PatentTaxonomy).where(PatentTaxonomy.patent_number == patent_number)
    )
    taxonomy = [
        {
            "id": str(t.id),
            "patent_number": t.patent_number,
            "taxonomy_node_id": t.taxonomy_node_id,
            "taxonomy_label": t.taxonomy_label,
            "assigned_by": t.assigned_by,
            "assigned_at": t.assigned_at.isoformat() if t.assigned_at else None,
        }
        for t in tax_result.all()
    ]

    return {
        "id": str(patent.id),
        "patent_number": patent.patent_number,
        "title": patent.title,
        "abstract": patent.abstract,
        "assignee": patent.assignee,
        "inventor": patent.inventor,
        "jurisdiction": patent.jurisdiction,
        "filing_date": patent.filing_date.isoformat() if patent.filing_date else None,
        "publication_date": patent.publication_date.isoformat() if patent.publication_date else None,
        "grant_date": patent.grant_date.isoformat() if patent.grant_date else None,
        "priority_date": patent.priority_date.isoformat() if patent.priority_date else None,
        "legal_status": patent.legal_status,
        "cpc_class": patent.cpc_class,
        "ipc_class": patent.ipc_class,
        "claims_count": patent.claims_count,
        "independent_claims_count": patent.independent_claims_count,
        "claims_text": patent.claims_text,
        "first_claim": patent.first_claim,
        "patent_family_id": patent.patent_family_id,
        "family_members": patent.family_members,
        "family_members_count": patent.family_members_count,
        "backward_citation_count": patent.backward_citation_count,
        "forward_citation_count": patent.forward_citation_count,
        "patent_url": patent.patent_url,
        "publication_country": patent.publication_country,
        "num_claims": patent.num_claims,
        "review_status": patent.review_status,
        "workspace_id": str(patent.workspace_id),
        "taxonomy": taxonomy,
    }


@router.post("/patents/search/keyword")
async def keyword_search(
    query: str = Form(...),
    workspace_id: Optional[str] = Form(None),
    skip: int = Form(0),
    limit: int = Form(25),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    like = f"%{query}%"
    stmt = select(Patent).where(
        or_(
            Patent.title.ilike(like),
            Patent.abstract.ilike(like),
            Patent.claims_text.ilike(like),
            Patent.patent_number.ilike(like),
        )
    )
    if workspace_id:
        ws_uuid = _parse_ws_uuid(workspace_id)
        stmt = stmt.where(Patent.workspace_id == ws_uuid)

    count_stmt = select(func.count()).select_from(stmt.subquery())
    total = await db.scalar(count_stmt)
    result = await db.scalars(stmt.offset(skip).limit(limit))
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
        "total": total or 0,
        "query": query,
    }


# ── INVESTIGATION QUEUE ──────────────────────────────────────────────────────

@router.get("/investigation-queue")
async def list_investigation_queue(
    workspace_id: Optional[str] = None,
    status: str = "pending",
    limit: int = 50,
    skip: int = 0,
    slim: bool = False,
    scope_mode: bool = False,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Patent)
    ws_uuid = None
    if workspace_id:
        ws_uuid = _parse_ws_uuid(workspace_id)
        stmt = stmt.where(Patent.workspace_id == ws_uuid)

    if status == "pending":
        stmt = stmt.where(or_(Patent.review_status == "pending", Patent.review_status.is_(None)))
    elif status == "reviewed":
        stmt = stmt.where(Patent.review_status == "reviewed")

    # Apply scope filter when requested (pending tab only)
    if scope_mode and workspace_id and status == "pending":
        from app.services.scope import get_scope, get_scoped_patent_ids
        scope = await get_scope(workspace_id, db)
        scoped_ids = await get_scoped_patent_ids(workspace_id, scope, db)
        if scoped_ids is not None:
            import uuid as _uuid
            uuid_ids = [_uuid.UUID(i) for i in scoped_ids]
            if uuid_ids:
                stmt = stmt.where(Patent.id.in_(uuid_ids))
            else:
                # No matches — return empty result
                return {"queue": [], "total": 0}

    total = await db.scalar(select(func.count()).select_from(stmt.subquery()))
    # Sort by updated_at desc so recently-reviewed/reopened patents float to top
    order_col = Patent.updated_at if hasattr(Patent, "updated_at") else Patent.created_at
    result = await db.scalars(stmt.order_by(order_col.desc()).offset(skip).limit(limit))
    patents = list(result.all())

    # Batch-load taxonomy labels in ONE query (eliminates N per-patent API calls)
    from collections import defaultdict
    tax_map: dict[str, list[str]] = defaultdict(list)
    tax_id_map: dict[str, list[str]] = defaultdict(list)
    if patents:
        pnums = [p.patent_number for p in patents]
        tax_rows = list((await db.scalars(
            select(PatentTaxonomy).where(PatentTaxonomy.patent_number.in_(pnums))
        )).all())
        for t in tax_rows:
            tax_map[t.patent_number].append(t.taxonomy_label or "")
            tax_id_map[t.patent_number].append(t.taxonomy_node_id or "")

    def _item(p: Patent) -> dict:
        base = {
            "id": str(p.id),
            "patent_number": p.patent_number,
            "title": p.title,
            "assignee": p.assignee,
            "patent_url": p.patent_url,
            "review_status": p.review_status or "pending",
            "review_note": p.review_note,
            "reviewed_at": p.reviewed_at.isoformat() if p.reviewed_at else None,
            "workspace_id": str(p.workspace_id),
            "taxonomy_labels": tax_map[p.patent_number],
            "taxonomy_node_ids": tax_id_map[p.patent_number],
        }
        if not slim:
            base.update({
                "abstract": p.abstract,
                "publication_date": p.publication_date.isoformat() if p.publication_date else None,
                "filing_date": p.filing_date.isoformat() if p.filing_date else None,
                "legal_status": p.legal_status,
                "first_claim": p.first_claim,
                "cpc_class": p.cpc_class,
            })
        return base

    return {
        "queue": [_item(p) for p in patents],
        "total": total or 0,
    }


@router.post("/investigation-queue/{patent_id}/review")
async def mark_patent_reviewed(
    patent_id: str,
    note: Optional[str] = Form(None),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    try:
        pid = uuid.UUID(patent_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=404, detail="Patent not found")
    patent = await db.scalar(select(Patent).where(Patent.id == pid))
    if not patent:
        raise HTTPException(status_code=404, detail="Patent not found")
    patent.review_status = "reviewed"
    patent.reviewed_at = datetime.now(timezone.utc)
    if note and note.strip():
        patent.review_note = note.strip()
    await db.commit()
    return {"message": "Patent marked as reviewed"}


@router.post("/investigation-queue/{patent_id}/reopen")
async def reopen_patent_review(
    patent_id: str,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    try:
        pid = uuid.UUID(patent_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=404, detail="Patent not found")
    patent = await db.scalar(select(Patent).where(Patent.id == pid))
    if not patent:
        raise HTTPException(status_code=404, detail="Patent not found")
    patent.review_status = "pending"
    patent.reviewed_by = None
    patent.reviewed_at = None
    await db.commit()
    return {"message": "Patent reopened for review"}


@router.post("/investigation-queue/bulk-reopen")
async def bulk_reopen_patents(
    body: BulkReopenBody,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _parse_ws_uuid(body.workspace_id)
    if not body.patent_ids:
        # Reopen all reviewed in workspace
        await db.execute(
            update(Patent)
            .where(Patent.workspace_id == ws_uuid, Patent.review_status == "reviewed")
            .values(review_status="pending", reviewed_by=None, reviewed_at=None, updated_at=datetime.now(timezone.utc))
        )
    else:
        pids = []
        for pid in body.patent_ids:
            try:
                pids.append(uuid.UUID(pid))
            except (ValueError, AttributeError):
                pass
        if pids:
            CHUNK = 500
            for i in range(0, len(pids), CHUNK):
                chunk = pids[i:i + CHUNK]
                await db.execute(
                    update(Patent)
                    .where(Patent.id.in_(chunk))
                    .values(review_status="pending", reviewed_by=None, reviewed_at=None, updated_at=datetime.now(timezone.utc))
                )
    await db.commit()
    return {"message": "Patents reopened", "count": len(body.patent_ids)}


@router.post("/investigation-queue")
async def add_to_queue(
    patent_number: str = Form(...),
    workspace_id: str = Form(...),
    priority: str = Form("medium"),
    reason: Optional[str] = Form(None),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _parse_ws_uuid(workspace_id)
    item = InvestigationQueueItem(
        patent_number=patent_number,
        workspace_id=ws_uuid,
        assigned_to=uuid.UUID(current_user["user_id"]),
        priority=priority,
        reason=reason,
        status="pending",
    )
    db.add(item)
    await db.commit()
    await db.refresh(item)
    return {
        "message": "Added to queue",
        "item": {
            "id": str(item.id),
            "patent_number": item.patent_number,
            "priority": item.priority,
            "status": item.status,
        },
    }


@router.put("/investigation-queue/{item_id}/complete")
async def complete_queue_item(
    item_id: str,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    try:
        iid = uuid.UUID(item_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=404, detail="Item not found")
    item = await db.scalar(select(InvestigationQueueItem).where(InvestigationQueueItem.id == iid))
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    item.status = "completed"
    item.reviewed_at = datetime.now(timezone.utc)
    await db.commit()
    return {"message": "Item marked as complete"}


@router.put("/investigation-queue/{item_id}/reopen")
async def reopen_queue_item(
    item_id: str,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    try:
        iid = uuid.UUID(item_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=404, detail="Item not found")
    item = await db.scalar(select(InvestigationQueueItem).where(InvestigationQueueItem.id == iid))
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    item.status = "pending"
    item.reviewed_at = None
    await db.commit()
    return {"message": "Item reopened"}


async def _generate_embeddings_bg(workspace_id: str, _unused_db) -> None:
    """Background task: generate embeddings for patents missing them in this workspace."""
    from app.services.embed import get_embed_client
    embed_client = get_embed_client()
    if not embed_client:
        return
    try:
        import uuid as _uuid
        ws_uuid = _uuid.UUID(workspace_id)
        async with AsyncSessionLocal() as db:
            # Process in batches of 50 to avoid memory pressure
            BATCH = 50
            offset = 0
            while True:
                patents = list((await db.scalars(
                    select(Patent)
                    .where(Patent.workspace_id == ws_uuid, Patent.embedding.is_(None))
                    .limit(BATCH)
                    .offset(offset)
                )).all())
                if not patents:
                    break
                texts = [
                    f"{p.title or ''} {p.abstract or ''} {p.first_claim or ''}"[:2000]
                    for p in patents
                ]
                vectors = await embed_client.embed_batch(texts)
                for patent, vec in zip(patents, vectors):
                    if vec:
                        patent.embedding = vec
                await db.commit()
                offset += BATCH
        logger.info(f"Background embed: completed for workspace {workspace_id}")
    except Exception as e:
        logger.warning(f"Background embed failed: {e}")
