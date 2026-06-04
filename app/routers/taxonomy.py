import asyncio
import uuid
import logging
import json
import re
import io
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Form, Query, UploadFile, File
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, update, func
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError

from app.models import TaxonomyNode, PatentTaxonomy, Patent
from app.deps import get_db, get_current_user, require_role
from app.services.watchlist import check_taxonomy_watchlist_rules
from app.services.llm import get_llm
from app import auth as auth_module

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["taxonomy"])


def _parse_ws_uuid(workspace_id: str) -> uuid.UUID:
    try:
        return uuid.UUID(workspace_id)
    except (ValueError, AttributeError):
        raise HTTPException(status_code=400, detail="Invalid workspace_id")


def _safe_parse_id_array(content: str, finish_reason: str, patent_number: str = "") -> list[str]:
    """
    Safely extract tag IDs from LLM output.

    Handles three output formats:
      1. New evidence format: {"evidence": {"id": "quote"}, "tags": ["id1", "id2"]}
      2. Legacy features format: {"features": [...], "tags": ["id1", "id2"]}
      3. Bare JSON array fallback: ["id1", "id2"]

    Returns [] (never raises) on truncation, parse error, or missing data.
    Logs the evidence dict when present so each tag decision is traceable.
    """
    if finish_reason == "length":
        logger.warning("LLM response truncated (finish_reason=length) — discarding output to prevent partial/corrupt tags")
        return []
    try:
        # Attempt 1: structured object with "tags" key
        start = content.find('{')
        end = content.rfind('}')
        if start != -1 and end != -1 and end > start:
            try:
                parsed = json.loads(content[start:end + 1])
                if isinstance(parsed, dict) and "tags" in parsed:
                    tags = parsed["tags"]
                    if isinstance(tags, list):
                        ids = [t for t in tags if isinstance(t, str) and t.strip()]
                        # Log evidence dict for traceability (new format)
                        evidence = parsed.get("evidence", {})
                        if evidence and isinstance(evidence, dict):
                            label = f" [{patent_number}]" if patent_number else ""
                            logger.info(f"Classification evidence{label}: {json.dumps(evidence, ensure_ascii=False)}")
                        else:
                            logger.debug(f"Structured parse OK — tags={ids}")
                        return ids
            except (json.JSONDecodeError, ValueError):
                pass  # Fall through to bare-array attempt

        # Attempt 2: bare JSON array ["id1", "id2"]
        match = re.search(r'\[.*?\]', content, re.DOTALL)
        if not match:
            logger.warning(f"No JSON found in LLM output: {content[:120]!r}")
            return []
        parsed = json.loads(match.group())
        if not isinstance(parsed, list):
            logger.warning("LLM returned non-list JSON — discarding")
            return []
        ids = [item for item in parsed if isinstance(item, str) and item.strip()]
        return ids
    except (json.JSONDecodeError, ValueError) as exc:
        logger.warning(f"LLM JSON parse error ({exc}) — discarding output: {content[:120]!r}")
        return []


# ── SCALE CONTROLS ───────────────────────────────────────────────────────────
# Semaphore limits concurrent LLM calls (paid OpenRouter = no platform RPM cap)
MAX_CONCURRENT_LLM = 20
_CLASSIFY_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_LLM)
COMMIT_BATCH_SIZE = 50  # commit to DB every N patents in bulk jobs

# In-memory job registries (single-worker deployment — no cross-process sharing needed)
_BULK_JOBS: dict[str, dict] = {}
_ENHANCE_JOBS: dict[str, dict] = {}
_ENHANCE_SEMAPHORE = asyncio.Semaphore(10)  # cap concurrent enhance LLM calls

# Cancellation flags: job_id → True means "stop after current page"
_BULK_JOB_CANCELLED: dict[str, bool] = {}

# Credit-limit sentinel — returned by classify_one when OpenRouter 403 "Key limit exceeded"
_CREDIT_LIMIT = "CREDIT_LIMIT"

# SSE queues: job_id → asyncio.Queue — background task pushes events, SSE endpoint drains them
# One queue per active SSE connection; None if no client is listening.
_BULK_JOB_QUEUES: dict[str, asyncio.Queue] = {}


def _passes_domain_screen(
    patent_title: str,
    patent_abstract: str,
    patent_first_claim: str,
    root_nodes: list,
) -> bool:
    """
    Tier-1 keyword pre-screen (no LLM cost).

    Returns True if the patent's title+abstract+first_claim contains at least one
    keyword from any root category's [SCREEN KEYWORDS: ...] list.
    Returns True unconditionally when no root node has a SCREEN KEYWORDS block
    (conservative — only skip when we have explicit evidence of irrelevance).
    """
    text = (
        (patent_title or "") + " " +
        (patent_abstract or "")[:1000] + " " +
        (patent_first_claim or "")
    ).lower()

    has_any_keywords = False
    for root in root_nodes:
        if not root.description:
            return True  # No description → no keywords → always pass
        m = re.search(r'\[SCREEN KEYWORDS:\s*([^\]]+)\]', root.description, re.IGNORECASE)
        if not m:
            return True  # No keywords configured for this root → pass
        has_any_keywords = True
        keywords = [k.strip().lower() for k in m.group(1).split(',') if k.strip()]
        if any(kw and kw in text for kw in keywords):
            return True

    # Only return False when we had keyword lists and none matched
    return not has_any_keywords


# ── BULK CLASSIFY BACKGROUND TASK ────────────────────────────────────────────

async def _flush_bulk_chunk(
    db: AsyncSession,
    buffer: list[tuple],
    node_label_map: dict[str, str],
    user_email: str,
) -> None:
    """Write a chunk of classified patents to DB using bulk upsert — no N+1 queries."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)

    # Collect all tag rows to insert in one bulk statement
    tag_rows: list[dict] = []
    for patent, tag_ids in buffer:
        if tag_ids is None:
            continue  # LLM failure — leave as pending
        if tag_ids:
            for nid in tag_ids:
                tag_rows.append({
                    "patent_number": patent.patent_number,
                    "taxonomy_node_id": nid,
                    "taxonomy_label": node_label_map.get(nid, ""),
                    "assigned_by": f"ai_bulk:{user_email}",
                })
            patent.review_status = "reviewed"
            patent.reviewed_at = now

    # Single bulk INSERT ... ON CONFLICT DO NOTHING — replaces N×M individual queries
    if tag_rows:
        stmt = pg_insert(PatentTaxonomy).values(tag_rows).on_conflict_do_nothing()
        await db.execute(stmt)

    await db.commit()


async def _bulk_classify_task(
    job_id: str,
    workspace_id: uuid.UUID,
    include_claims: bool,
    user_email: str,
    scope_mode: bool = False,
) -> None:
    """Server-side background task: classify pending patents in workspace.
    When scope_mode=True, only classifies patents matching the saved workspace scope."""
    from app.database import AsyncSessionLocal

    _BULK_JOBS[job_id].update({
        "status": "running", "done": 0, "total": 0,
        "tagged": 0, "no_match": 0, "failed": 0,
    })

    # Page size for loading patents — keeps memory flat regardless of corpus size
    PATENT_PAGE_SIZE = 500

    try:
        # ── Phase 1: snapshot IDs + load taxonomy (short-lived session) ────────────
        async with AsyncSessionLocal() as db:
            # Snapshot pending patent IDs (lightweight — IDs only, not full objects)
            # Snapshot is taken once so page queries stay stable as status changes during processing
            id_result = await db.scalars(
                select(Patent.id).where(
                    Patent.workspace_id == workspace_id,
                    Patent.review_status == "pending",
                ).order_by(Patent.id)
            )
            all_pending_ids = list(id_result.all())

            # When scope_mode, restrict classification to scoped patent IDs only
            if scope_mode:
                from app.services.scope import get_scope, get_scoped_patent_ids
                scope = await get_scope(str(workspace_id), db)
                scoped_ids = await get_scoped_patent_ids(str(workspace_id), scope, db)
                if scoped_ids is not None:
                    all_pending_ids = [pid for pid in all_pending_ids if str(pid) in scoped_ids]
                    logger.info(f"Scope filter: {len(all_pending_ids)} of pending patents in scope")

            total = len(all_pending_ids)
            _BULK_JOBS[job_id]["total"] = total
            if total == 0:
                _BULK_JOBS[job_id]["status"] = "done"
                return

            # Load taxonomy and build tree once (shared across all patents)
            nodes_result = await db.scalars(
                select(TaxonomyNode).where(TaxonomyNode.workspace_id == workspace_id)
            )
            all_nodes = list(nodes_result.all())
            if not all_nodes:
                _BULK_JOBS[job_id].update({"status": "error", "error": "No taxonomy nodes configured"})
                return

            node_map = {n.node_id: n for n in all_nodes}
            roots = [n for n in all_nodes if not n.parent_id or n.parent_id not in node_map]
            children = [n for n in all_nodes if n.parent_id and n.parent_id in node_map]
            id_to_node = {n.node_id: n for n in all_nodes}
            node_label_map = {n.node_id: n.label for n in all_nodes}

            tree_lines: list[str] = []
            for n in roots + children:
                if n.parent_id and n.parent_id in node_map:
                    desc = (n.description or "")[:800]
                    desc_str = f" — {desc}" if desc else ""
                    parent_label = node_map[n.parent_id].label
                    tree_lines.append(f"  [TAG] {n.node_id}: {parent_label} > {n.label}{desc_str}")
                else:
                    desc = (n.description or "")[:400]
                    desc_str = f" — {desc}" if desc else ""
                    tree_lines.append(f"[ROOT TAG] {n.node_id}: {n.label}{desc_str}")

            taxonomy_tree = "\n".join(tree_lines)
            if len(taxonomy_tree) > 35000:
                taxonomy_tree = taxonomy_tree[:35000].rsplit("\n", 1)[0]

            roots_list = "\n".join(f"  • {n.label}" for n in roots)
            system_content = (
                "You are an expert patent classifier performing multi-label taxonomy classification.\n"
                "Output ONLY a JSON array of matched node IDs: [\"id1\",\"id2\"] or [] for no match.\n"
                "Rules:\n"
                "• Use IDs from both [ROOT TAG] and [TAG] entries — never invented IDs.\n"
                "• [ROOT TAG]: apply only when patent's primary subject clearly falls under this broad category — needs direct claim evidence.\n"
                "• [TAG]: apply only when exact claim language matches description or QUALIFYING PHRASES.\n"
                "• Check ALL claims (independent + dependent) before deciding.\n"
                "• Never tag from abstract, background, or prior art — claims only.\n"
                "• When uncertain: NO. Zero tags acceptable; wrong tags are not.\n"
                "• Output ONLY the JSON array — no text, no keys, no explanation."
            )
            llm = get_llm()
            _RETRY_BACKOFF = [2, 5, 10]
        # ── Phase 1 complete — taxonomy + ID snapshot loaded, session closed ──────
        # Phase 2: page-by-page processing with fresh short-lived DB sessions per page.
        # This prevents a single long-held DB connection from timing out on hour-long jobs.

        async def classify_one(patent: Patent) -> tuple:
            title = (patent.title or "").strip()
            abstract = (patent.abstract or "")[:800].strip()
            assignee = (patent.assignee or "").strip()
            cpc = (patent.cpc_class or "").strip()
            claims_raw = (patent.claims_text or patent.first_claim or "").strip() if include_claims else (patent.first_claim or "").strip()
            claims_section = claims_raw[:10000]

            # Tier 1: keyword pre-screen (no LLM cost)
            if not _passes_domain_screen(title, abstract, patent.first_claim or "", roots):
                logger.debug(f"Bulk pre-screen: {patent.patent_number} skipped")
                return patent, []

            prompt = (
                f"PATENT TAXONOMY CLASSIFICATION\n\n"
                f"Quality standard: Correct tagging > Complete tagging. "
                f"Assign ZERO tags rather than one wrong tag. "
                f"Only tag what you can directly quote from a claim.\n\n"
                f"━━━ PATENT ━━━\n"
                f"Title: {title}\n"
                f"{f'Assignee: {assignee}' if assignee else ''}\n"
                f"{f'CPC: {cpc}' if cpc else ''}\n"
                f"Abstract: {abstract}\n\n"
                f"{f'Claims:{chr(10)}{claims_section}' if claims_section else ''}\n\n"
                f"━━━ STEP 1: DOMAIN SCREEN ━━━\n"
                f"Does this patent primarily claim a device, method, or system in these domains?\n"
                f"{roots_list}\n"
                f"→ If NONE apply → return [] immediately.\n"
                f"→ If YES → continue to Step 2.\n\n"
                f"━━━ STEP 2: PER-TAG EVALUATION ━━━\n"
                f"Evaluate EVERY node in the taxonomy below — both [ROOT TAG] and [TAG]:\n"
                f"  a. Read its Description to understand what evidence qualifies.\n"
                f"  b. If Description contains 'QUALIFYING PHRASES:', search the patent claims for those\n"
                f"     exact phrases first — a verbatim or near-verbatim match is strong evidence.\n"
                f"  c. IMPORTANT: Check EVERY claim — independent AND dependent.\n"
                f"     Dependent claims contain material types, mechanisms, and functional details\n"
                f"     that are where secondary tags often live.\n"
                f"  d. Decide YES (you can quote a claim sentence) or NO (not found or ambiguous).\n\n"
                f"TAXONOMY:\n"
                f"[ROOT TAG] = broad category tag — apply ONLY when the patent's primary subject matter clearly falls under this category. Requires direct claim evidence, not just topical relevance.\n"
                f"[TAG] = specific child tag — apply when you can quote exact claim language matching the description.\n"
                f"Both [ROOT TAG] and [TAG] IDs may appear in your output.\n"
                f"{taxonomy_tree}\n\n"
                f"━━━ STEP 3: PRECISION FILTER ━━━\n"
                f"Remove a YES tag if ANY of these apply:\n"
                f"  ✗ Evidence is in background or prior art section — not in a claim\n"
                f"  ✗ Material tag: named material applies to wrong component\n"
                f"  ✗ Mechanism tag: behavior is implied, not an explicitly named mechanism element\n"
                f"  ✗ Delivery tag: primary independent claim is the implant, not the delivery system\n"
                f"  ✗ Still uncertain after checking all claims → default NO\n\n"
                f"━━━ OUTPUT FORMAT ━━━\n"
                f"Return ONLY a JSON array — no text before or after:\n"
                f'[\"node_id_1\", \"node_id_2\"]\n'
                f"Empty array [] if no tags pass. No keys, no evidence, no explanation."
            )

            last_exc = None
            async with _CLASSIFY_SEMAPHORE:
                for attempt in range(3):
                    try:
                        response = await llm.chat.completions.create(
                            model="deepseek/deepseek-v3.2",
                            messages=[
                                {"role": "system", "content": system_content},
                                {"role": "user", "content": prompt},
                            ],
                            max_tokens=300,
                            temperature=0.0,
                            timeout=45,
                        )
                        last_exc = None
                        break
                    except Exception as e:
                        err_str = str(e)
                        # 403 "Key limit exceeded" — no point retrying, signal caller immediately
                        if "Key limit exceeded" in err_str or ("403" in err_str and "limit" in err_str.lower()):
                            logger.error(f"OpenRouter credit/key limit hit: {e}")
                            return patent, _CREDIT_LIMIT
                        last_exc = e
                        logger.warning(f"Bulk LLM attempt {attempt+1}/3 for {patent.patent_number}: {e}")
                        if attempt < 2:
                            await asyncio.sleep(_RETRY_BACKOFF[attempt])

            if last_exc is not None:
                logger.error(f"Bulk classify: {patent.patent_number} failed after 3 attempts: {last_exc}")
                return patent, None

            choice = response.choices[0]
            finish_reason = getattr(choice, "finish_reason", "stop") or "stop"
            content = (choice.message.content or "").strip()
            raw_ids = _safe_parse_id_array(content, finish_reason, patent_number=patent.patent_number)
            tag_ids = [nid for nid in raw_ids if nid in id_to_node]
            return patent, tag_ids

        # Phase 2: page-by-page processing — fresh short-lived DB session per page.
        # 500 patents per page → 20 concurrent LLM calls via semaphore.
        # Memory stays flat: never more than 500 Patent objects in RAM at once.
        for page_start in range(0, total, PATENT_PAGE_SIZE):
            # Check cancellation before starting each new page
            if _BULK_JOB_CANCELLED.get(job_id):
                logger.info(f"Job {job_id}: cancelled by user after {_BULK_JOBS[job_id]['done']}/{total} patents")
                _BULK_JOBS[job_id]["status"] = "cancelled"
                sse_q = _BULK_JOB_QUEUES.pop(job_id, None)
                if sse_q:
                    sse_q.put_nowait({**_BULK_JOBS[job_id], "total": total})
                _BULK_JOB_CANCELLED.pop(job_id, None)
                return

            page_ids = all_pending_ids[page_start : page_start + PATENT_PAGE_SIZE]

            async with AsyncSessionLocal() as db:
                page_result = await db.scalars(
                    select(Patent).where(Patent.id.in_(page_ids))
                )
                page_patents = list(page_result.all())

                logger.info(
                    f"Job {job_id}: page {page_start // PATENT_PAGE_SIZE + 1}"
                    f" ({len(page_patents)} patents, {page_start}/{total} done)"
                )

                tasks = [asyncio.ensure_future(classify_one(p)) for p in page_patents]
                commit_buffer: list[tuple] = []

                credit_limit_hit = False
                for coro in asyncio.as_completed(tasks):
                    patent, tag_ids = await coro

                    # Credit limit — flush whatever was processed, then stop everything
                    if tag_ids is _CREDIT_LIMIT:
                        logger.error(f"Job {job_id}: OpenRouter credit limit hit — flushing {len(commit_buffer)} buffered patents and stopping")
                        if commit_buffer:
                            await _flush_bulk_chunk(db, commit_buffer, node_label_map, user_email)
                            commit_buffer.clear()
                        credit_limit_hit = True
                        break

                    commit_buffer.append((patent, tag_ids))

                    if tag_ids is None:
                        _BULK_JOBS[job_id]["failed"] += 1
                    elif tag_ids:
                        _BULK_JOBS[job_id]["tagged"] += 1
                    else:
                        _BULK_JOBS[job_id]["no_match"] += 1
                    _BULK_JOBS[job_id]["done"] += 1

                    # Push real-time progress to SSE client (if connected)
                    sse_q = _BULK_JOB_QUEUES.get(job_id)
                    if sse_q:
                        sse_q.put_nowait({
                            "done": _BULK_JOBS[job_id]["done"],
                            "total": total,
                            "tagged": _BULK_JOBS[job_id]["tagged"],
                            "no_match": _BULK_JOBS[job_id]["no_match"],
                            "failed": _BULK_JOBS[job_id]["failed"],
                            "status": "running",
                        })

                    if len(commit_buffer) >= COMMIT_BATCH_SIZE:
                        await _flush_bulk_chunk(db, commit_buffer, node_label_map, user_email)
                        commit_buffer.clear()
                        logger.info(
                            f"Job {job_id}: committed chunk — "
                            f"{_BULK_JOBS[job_id]['done']}/{total} done, "
                            f"{_BULK_JOBS[job_id]['tagged']} tagged"
                        )

                if credit_limit_hit:
                    _BULK_JOBS[job_id].update({
                        "status": "credit_limit_error",
                        "error": "OpenRouter credit/key limit exceeded. Please top up at openrouter.ai/settings/keys",
                    })
                    sse_q = _BULK_JOB_QUEUES.pop(job_id, None)
                    if sse_q:
                        sse_q.put_nowait({**_BULK_JOBS[job_id], "total": total})
                    _BULK_JOB_CANCELLED[job_id] = True  # prevent further pages
                    break  # exit page loop

                if commit_buffer:
                    await _flush_bulk_chunk(db, commit_buffer, node_label_map, user_email)
            # Page session closed — connection returned to pool before next page starts

            if _BULK_JOB_CANCELLED.get(job_id):
                break  # credit_limit or user cancel already set status above

        _BULK_JOBS[job_id]["status"] = "done"
        logger.info(
            f"Bulk classify job {job_id} complete: "
            f"{_BULK_JOBS[job_id]['tagged']} tagged, "
            f"{_BULK_JOBS[job_id]['no_match']} no-match, "
            f"{_BULK_JOBS[job_id]['failed']} failed"
        )
        sse_q = _BULK_JOB_QUEUES.pop(job_id, None)
        if sse_q:
            sse_q.put_nowait({**_BULK_JOBS[job_id], "total": _BULK_JOBS[job_id]["total"]})
    except Exception as exc:
        logger.error(f"Bulk classify job {job_id} crashed: {exc}", exc_info=True)
        _BULK_JOBS[job_id].update({"status": "error", "error": str(exc)})
        sse_q = _BULK_JOB_QUEUES.pop(job_id, None)
        if sse_q:
            sse_q.put_nowait({"status": "error", "error": str(exc)})


@router.post("/taxonomy/classify-workspace")
async def start_bulk_classify(
    workspace_id: str = Query(...),
    include_claims: bool = Query(True),
    scope_mode: bool = Query(False),
    current_user: dict = Depends(require_role(["ADMIN", "admin", "ANALYST", "analyst"])),
    db: AsyncSession = Depends(get_db),
):
    """Start a server-side background classification job for all pending patents.
    When scope_mode=true, only classifies patents matching the saved workspace scope."""
    ws_uuid = _parse_ws_uuid(workspace_id)
    job_id = str(uuid.uuid4())
    user_email = current_user.get("email", "system")
    _BULK_JOBS[job_id] = {"status": "queued", "done": 0, "total": 0, "tagged": 0, "no_match": 0, "failed": 0}
    asyncio.create_task(_bulk_classify_task(job_id, ws_uuid, include_claims, user_email, scope_mode=scope_mode))
    logger.info(f"Bulk classify job {job_id} started for workspace {workspace_id} by {user_email} scope_mode={scope_mode}")
    return {"job_id": job_id, "status": "queued"}


@router.post("/taxonomy/classify-workspace/{job_id}/cancel")
async def cancel_bulk_classify(
    job_id: str,
    current_user: dict = Depends(require_role(["ADMIN", "admin", "ANALYST", "analyst"])),
):
    """Signal the background job to stop after the current page finishes.
    Already-processed patents remain saved. No new LLM calls are started."""
    job = _BULK_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    if job.get("status") not in ("running", "queued"):
        return {"message": "Job already finished", "status": job.get("status")}
    _BULK_JOB_CANCELLED[job_id] = True
    return {"message": "Cancel signal sent — job will stop after current page", "job_id": job_id}


@router.get("/taxonomy/classify-workspace/{job_id}")
async def get_bulk_classify_status(
    job_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Poll the status of a bulk classification job (fallback for non-SSE clients)."""
    job = _BULK_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.get("/taxonomy/classify-workspace/{job_id}/stream")
async def stream_classify_progress(
    job_id: str,
    token: str = Query(...),  # EventSource cannot set Authorization header — use query param
):
    """SSE endpoint: streams real-time classification progress as each patent completes.

    Connect once after starting a job. Each 'data:' event is a JSON progress snapshot.
    A heartbeat comment (':\\n\\n') is sent every 25 seconds to keep the connection alive.
    The stream closes automatically when status becomes 'done' or 'error'.
    """
    # Validate token manually (cannot use Depends with EventSource)
    try:
        auth_module.decode_token(token)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    job = _BULK_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Register an asyncio.Queue for this connection.
    # The background task will push events via put_nowait().
    q: asyncio.Queue = asyncio.Queue(maxsize=0)  # unbounded
    _BULK_JOB_QUEUES[job_id] = q

    async def generate():
        # Send current snapshot immediately so client is never blind on connect
        current = dict(_BULK_JOBS.get(job_id, {}))
        yield f"data: {json.dumps(current)}\n\n"

        if current.get("status") in ("done", "error"):
            _BULK_JOB_QUEUES.pop(job_id, None)
            return

        while True:
            try:
                data = await asyncio.wait_for(q.get(), timeout=25)
                yield f"data: {json.dumps(data)}\n\n"
                if data.get("status") in ("done", "error"):
                    break
            except asyncio.TimeoutError:
                yield ": heartbeat\n\n"  # keeps ngrok/nginx from closing idle connection

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate",
            "Pragma": "no-cache",
            "X-Accel-Buffering": "no",    # disable nginx response buffering
            "Ngrok-Skip-Browser-Warning": "1",
            "Connection": "keep-alive",
            "Transfer-Encoding": "chunked",
        },
    )


# ── TAXONOMY IMPORT ───────────────────────────────────────────────────────────

def _build_conflict_response(node_id: str, label: str, existing_desc: str, incoming_desc: str) -> dict:
    return {
        "node_id": node_id,
        "label": label,
        "existing_description": existing_desc or "",
        "incoming_description": incoming_desc or "",
    }


@router.post("/taxonomy/import-from-workspace")
async def import_taxonomy_from_workspace(
    source_workspace_id: str = Form(...),
    target_workspace_id: str = Form(...),
    current_user: dict = Depends(require_role(["ADMIN", "admin", "ANALYST", "analyst"])),
    db: AsyncSession = Depends(get_db),
):
    """Copy taxonomy nodes from source → target workspace.
    Exact duplicates (same label + same description) are silently skipped.
    Conflicts (same label, different description) are returned for user resolution."""
    src_uuid = _parse_ws_uuid(source_workspace_id)
    tgt_uuid = _parse_ws_uuid(target_workspace_id)
    if src_uuid == tgt_uuid:
        raise HTTPException(status_code=400, detail="Source and target workspace must be different")

    src_nodes = list((await db.scalars(
        select(TaxonomyNode).where(TaxonomyNode.workspace_id == src_uuid)
    )).all())
    if not src_nodes:
        return {"created_roots": 0, "created_children": 0, "exact_duplicates": 0, "conflicts": []}

    # Existing nodes in target workspace — key by label (case-insensitive)
    tgt_nodes = list((await db.scalars(
        select(TaxonomyNode).where(TaxonomyNode.workspace_id == tgt_uuid)
    )).all())
    tgt_by_label = {n.label.strip().lower(): n for n in tgt_nodes}

    id_map: dict[str, str] = {}
    src_all_ids = {n.node_id for n in src_nodes}
    roots_src = [n for n in src_nodes if not n.parent_id or n.parent_id not in src_all_ids]
    children_src = [n for n in src_nodes if n not in roots_src]
    ordered = roots_src + children_src

    created_roots = created_children = exact_dupes = 0
    conflicts: list[dict] = []

    for node in ordered:
        lbl_key = node.label.strip().lower()
        existing = tgt_by_label.get(lbl_key)
        if existing:
            ex_desc = (existing.description or "").strip()
            in_desc = (node.description or "").strip()
            if ex_desc == in_desc:
                # Exact duplicate — skip silently, reuse existing id for child wiring
                id_map[node.node_id] = existing.node_id
                exact_dupes += 1
            else:
                # Conflict — same label, different description
                conflicts.append(_build_conflict_response(existing.node_id, node.label, ex_desc, in_desc))
                id_map[node.node_id] = existing.node_id
        else:
            new_id = str(uuid.uuid4())[:8]
            id_map[node.node_id] = new_id
            new_parent = id_map.get(node.parent_id) if node.parent_id else None
            db.add(TaxonomyNode(
                node_id=new_id,
                label=node.label,
                description=node.description,
                workspace_id=tgt_uuid,
                parent_id=new_parent,
                level=node.level,
            ))
            if node.level == 0:
                created_roots += 1
            else:
                created_children += 1

    await db.commit()
    return {
        "created_roots": created_roots,
        "created_children": created_children,
        "exact_duplicates": exact_dupes,
        "conflicts": conflicts,
    }


def _find_col_fuzzy(columns: list[str], keywords: list[str]) -> Optional[str]:
    """Find a column by keyword matching (case-insensitive partial match)."""
    for col in columns:
        col_lower = col.lower()
        if any(kw.lower() in col_lower for kw in keywords):
            return col
    return None


@router.post("/taxonomy/import-from-file")
async def import_taxonomy_from_file(
    file: UploadFile = File(...),
    workspace_id: str = Form(...),
    current_user: dict = Depends(require_role(["ADMIN", "admin", "ANALYST", "analyst"])),
    db: AsyncSession = Depends(get_db),
):
    """Import taxonomy from CSV/Excel file.

    Expected columns (detected by keyword, order doesn't matter):
    - Root category column: contains 'core', 'category', or 'root'
    - Child label column: contains 'feature', 'requirement', or 'name'
    - Description column: contains 'description' or 'desc'
    """
    import pandas as pd

    ws_uuid = _parse_ws_uuid(workspace_id)
    content = await file.read()
    fname = file.filename or ""

    try:
        if fname.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(content))
        else:
            df = pd.read_excel(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Could not parse file: {e}")

    cols = list(df.columns)
    root_col = _find_col_fuzzy(cols, ["core", "category", "root", "parent"])
    child_col = _find_col_fuzzy(cols, ["feature", "requirement", "child", "name", "label"])
    desc_col  = _find_col_fuzzy(cols, ["description", "desc", "detail"])

    if not root_col:
        raise HTTPException(status_code=400, detail=f"Could not find root category column. Columns found: {cols}")
    if not child_col:
        raise HTTPException(status_code=400, detail=f"Could not find child/feature column. Columns found: {cols}")

    # Build tree: root_label → [(child_label, description)]
    from collections import defaultdict
    root_children: dict[str, list[tuple[str, str]]] = defaultdict(list)

    for _, row in df.iterrows():
        root_val = row.get(root_col)
        child_val = row.get(child_col)
        desc_val  = row.get(desc_col) if desc_col else ""

        if root_val is None or (hasattr(root_val, '__class__') and root_val.__class__.__name__ == 'float'):
            continue
        try:
            import pandas as _pd
            if _pd.isna(root_val):
                continue
        except Exception:
            pass

        root_str  = str(root_val).strip()
        child_str = str(child_val).strip() if child_val is not None and str(child_val) != "nan" else ""
        desc_str  = str(desc_val).strip()  if desc_val  is not None and str(desc_val)  != "nan" else ""

        if root_str:
            if child_str:
                root_children[root_str].append((child_str, desc_str))
            else:
                # Row is just a root with no child — ensure root is created
                if root_str not in root_children:
                    root_children[root_str] = []

    if not root_children:
        raise HTTPException(status_code=400, detail="No data rows found in file")

    # Load existing nodes for duplicate/conflict detection
    existing_nodes = list((await db.scalars(
        select(TaxonomyNode).where(TaxonomyNode.workspace_id == ws_uuid)
    )).all())
    existing_by_label = {n.label.strip().lower(): n for n in existing_nodes}

    created_roots = created_children = exact_dupes = 0
    conflicts: list[dict] = []

    for root_label, children in root_children.items():
        root_key = root_label.strip().lower()
        existing_root = existing_by_label.get(root_key)

        if existing_root:
            # Root exists — check description conflict
            ex_desc = (existing_root.description or "").strip()
            # Root nodes from file have no description in this format, so no conflict on root
            root_id = existing_root.node_id
            exact_dupes += 1  # root itself is a duplicate
        else:
            root_id = str(uuid.uuid4())[:8]
            db.add(TaxonomyNode(
                node_id=root_id,
                label=root_label,
                workspace_id=ws_uuid,
                parent_id=None,
                level=0,
            ))
            created_roots += 1

        for child_label, child_desc in children:
            child_key = child_label.strip().lower()
            existing_child = existing_by_label.get(child_key)
            if existing_child:
                ex_desc = (existing_child.description or "").strip()
                in_desc = child_desc.strip()
                if ex_desc == in_desc:
                    exact_dupes += 1
                else:
                    conflicts.append(_build_conflict_response(existing_child.node_id, child_label, ex_desc, in_desc))
            else:
                child_id = str(uuid.uuid4())[:8]
                db.add(TaxonomyNode(
                    node_id=child_id,
                    label=child_label,
                    description=child_desc if child_desc else None,
                    workspace_id=ws_uuid,
                    parent_id=root_id,
                    level=1,
                ))
                created_children += 1

    await db.commit()
    logger.info(f"File import into {workspace_id}: {created_roots} roots, {created_children} children, {exact_dupes} dupes, {len(conflicts)} conflicts")
    return {
        "created_roots": created_roots,
        "created_children": created_children,
        "exact_duplicates": exact_dupes,
        "conflicts": conflicts,
    }


@router.post("/taxonomy/import-resolve-conflict")
async def resolve_import_conflict(
    node_id: str = Form(...),
    description: str = Form(...),
    workspace_id: str = Form(...),
    current_user: dict = Depends(require_role(["ADMIN", "admin", "ANALYST", "analyst"])),
    db: AsyncSession = Depends(get_db),
):
    """Replace existing node's description with incoming description (user chose Replace)."""
    ws_uuid = _parse_ws_uuid(workspace_id)
    node = await db.scalar(
        select(TaxonomyNode).where(TaxonomyNode.node_id == node_id, TaxonomyNode.workspace_id == ws_uuid)
    )
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    node.description = description
    await db.commit()
    return {"message": "Description updated", "node_id": node_id}


# ── TAXONOMY NODES ────────────────────────────────────────────────────────────

@router.get("/taxonomy")
async def list_taxonomy(
    parent_id: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(TaxonomyNode)
    if parent_id:
        stmt = stmt.where(TaxonomyNode.parent_id == parent_id)
    result = await db.scalars(stmt)
    nodes = list(result.all())
    return {
        "nodes": [
            {
                "node_id": n.node_id,
                "label": n.label,
                "parent_id": n.parent_id,
                "level": n.level,
                "workspace_id": str(n.workspace_id),
                "children": [],
            }
            for n in nodes
        ]
    }


@router.get("/taxonomy/tree")
async def get_taxonomy_tree(
    workspace_id: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(TaxonomyNode)
    if workspace_id:
        ws_uuid = _parse_ws_uuid(workspace_id)
        stmt = stmt.where(TaxonomyNode.workspace_id == ws_uuid)
    result = await db.scalars(stmt)
    nodes = list(result.all())

    node_map = {
        n.node_id: {
            "node_id": n.node_id,
            "label": n.label,
            "description": n.description,
            "parent_id": n.parent_id,
            "level": n.level,
            "workspace_id": str(n.workspace_id),
            "children": [],
        }
        for n in nodes
    }

    tree = []
    for node_id, node in node_map.items():
        pid = node.get("parent_id")
        if pid and pid in node_map:
            node_map[pid]["children"].append(node)
        else:
            tree.append(node)

    return {"tree": tree}


@router.post("/taxonomy")
async def create_taxonomy(
    node_id: str = Form(...),
    label: str = Form(...),
    workspace_id: str = Form(...),
    parent_id: Optional[str] = Form(None),
    level: int = Form(0),
    current_user: dict = Depends(require_role(["ADMIN", "admin", "ANALYST", "analyst"])),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _parse_ws_uuid(workspace_id)
    existing = await db.scalar(select(TaxonomyNode).where(TaxonomyNode.node_id == node_id))
    if existing:
        raise HTTPException(status_code=400, detail="Node ID already exists")
    node = TaxonomyNode(
        node_id=node_id,
        label=label,
        workspace_id=ws_uuid,
        parent_id=parent_id,
        level=level,
    )
    db.add(node)
    await db.commit()
    return {"message": "Node created", "node_id": node_id, "label": label}


@router.put("/taxonomy/{node_id}")
async def update_taxonomy_node(
    node_id: str,
    label: str = Form(...),
    workspace_id: str = Form(...),
    current_user: dict = Depends(require_role(["ADMIN", "admin", "ANALYST", "analyst"])),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _parse_ws_uuid(workspace_id)
    node = await db.scalar(
        select(TaxonomyNode).where(
            TaxonomyNode.node_id == node_id,
            TaxonomyNode.workspace_id == ws_uuid,
        )
    )
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    node.label = label
    await db.commit()
    return {"message": "Node updated", "node_id": node_id, "label": label}


@router.put("/taxonomy/{node_id}/description")
async def update_taxonomy_node_description(
    node_id: str,
    description: str = Form(""),
    workspace_id: str = Form(...),
    current_user: dict = Depends(require_role(["ADMIN", "admin", "ANALYST", "analyst"])),
    db: AsyncSession = Depends(get_db),
):
    ws_uuid = _parse_ws_uuid(workspace_id)
    node = await db.scalar(
        select(TaxonomyNode).where(
            TaxonomyNode.node_id == node_id,
            TaxonomyNode.workspace_id == ws_uuid,
        )
    )
    if not node:
        raise HTTPException(status_code=404, detail="Node not found")
    node.description = description.strip() or None
    await db.commit()
    return {"message": "Description updated", "node_id": node_id}


@router.delete("/taxonomy/{node_id}")
async def delete_taxonomy_node(
    node_id: str,
    workspace_id: Optional[str] = None,
    current_user: dict = Depends(require_role(["ADMIN", "admin", "ANALYST", "analyst"])),
    db: AsyncSession = Depends(get_db),
):
    # Delete children first
    child_stmt = delete(TaxonomyNode).where(TaxonomyNode.parent_id == node_id)
    if workspace_id:
        ws_uuid = _parse_ws_uuid(workspace_id)
        child_stmt = child_stmt.where(TaxonomyNode.workspace_id == ws_uuid)
    await db.execute(child_stmt)

    del_stmt = delete(TaxonomyNode).where(TaxonomyNode.node_id == node_id)
    if workspace_id:
        ws_uuid = _parse_ws_uuid(workspace_id)
        del_stmt = del_stmt.where(TaxonomyNode.workspace_id == ws_uuid)
    result = await db.execute(del_stmt)
    await db.commit()
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Node not found")
    return {"message": "Node deleted", "node_id": node_id}


# ── TAXONOMY ASSIGNMENTS ──────────────────────────────────────────────────────

@router.post("/taxonomy/assign/{patent_number}/{node_id}")
async def assign_taxonomy_to_patent(
    patent_number: str,
    node_id: str,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    existing = await db.scalar(
        select(PatentTaxonomy).where(
            PatentTaxonomy.patent_number == patent_number,
            PatentTaxonomy.taxonomy_node_id == node_id,
        )
    )
    if existing:
        return {"message": "Taxonomy already assigned", "assignment_id": str(existing.id)}

    tax_node = await db.scalar(select(TaxonomyNode).where(TaxonomyNode.node_id == node_id))
    taxonomy_label = tax_node.label if tax_node else ""
    workspace_id = str(tax_node.workspace_id) if tax_node else None

    assignment = PatentTaxonomy(
        patent_number=patent_number,
        taxonomy_node_id=node_id,
        taxonomy_label=taxonomy_label,
        assigned_by=current_user.get("email"),
    )
    db.add(assignment)
    await db.commit()
    await db.refresh(assignment)

    if taxonomy_label and workspace_id:
        try:
            await check_taxonomy_watchlist_rules(db, patent_number, taxonomy_label, workspace_id)
        except Exception as e:
            logger.warning(f"Watchlist check failed for taxonomy assignment: {e}")

    return {"message": "Taxonomy assigned", "assignment_id": str(assignment.id)}


@router.delete("/taxonomy/unassign/{patent_number}/{node_id}")
async def unassign_taxonomy_from_patent(
    patent_number: str,
    node_id: str,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        delete(PatentTaxonomy).where(
            PatentTaxonomy.patent_number == patent_number,
            PatentTaxonomy.taxonomy_node_id == node_id,
        )
    )
    await db.commit()
    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Assignment not found")
    return {"message": "Taxonomy unassigned", "deleted_count": result.rowcount}


@router.get("/patents/{patent_number}/taxonomy")
async def get_patent_taxonomy_assignments(
    patent_number: str,
    workspace_id: Optional[str] = None,
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    stmt = select(PatentTaxonomy).where(PatentTaxonomy.patent_number == patent_number)

    # Filter to workspace's taxonomy nodes if workspace_id provided
    if workspace_id:
        ws_uuid = _parse_ws_uuid(workspace_id)
        node_ids_result = await db.scalars(
            select(TaxonomyNode.node_id).where(TaxonomyNode.workspace_id == ws_uuid)
        )
        node_ids = list(node_ids_result.all())
        if node_ids:
            stmt = stmt.where(PatentTaxonomy.taxonomy_node_id.in_(node_ids))
        else:
            return {"assignments": []}

    result = await db.scalars(stmt)
    assignments = list(result.all())
    return {
        "assignments": [
            {
                "id": str(a.id),
                "patent_number": a.patent_number,
                "taxonomy_node_id": a.taxonomy_node_id,
                "taxonomy_label": a.taxonomy_label,
                "assigned_by": a.assigned_by,
                "assigned_at": a.assigned_at.isoformat() if a.assigned_at else None,
            }
            for a in assignments
        ]
    }


@router.post("/patents/{patent_number}/taxonomy")
async def assign_taxonomy_via_patent(
    patent_number: str,
    taxonomy_node_id: str = Form(...),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    existing = await db.scalar(
        select(PatentTaxonomy).where(
            PatentTaxonomy.patent_number == patent_number,
            PatentTaxonomy.taxonomy_node_id == taxonomy_node_id,
        )
    )
    if existing:
        return {
            "message": "Taxonomy already assigned",
            "assignment": {
                "id": str(existing.id),
                "patent_number": patent_number,
                "taxonomy_node_id": taxonomy_node_id,
                "taxonomy_label": existing.taxonomy_label,
            },
        }

    tax_node = await db.scalar(select(TaxonomyNode).where(TaxonomyNode.node_id == taxonomy_node_id))
    taxonomy_label = tax_node.label if tax_node else ""

    assignment = PatentTaxonomy(
        patent_number=patent_number,
        taxonomy_node_id=taxonomy_node_id,
        taxonomy_label=taxonomy_label,
        assigned_by=current_user.get("email"),
    )
    db.add(assignment)
    await db.commit()
    await db.refresh(assignment)

    return {
        "message": "Taxonomy assigned",
        "assignment": {
            "id": str(assignment.id),
            "patent_number": patent_number,
            "taxonomy_node_id": taxonomy_node_id,
            "taxonomy_label": taxonomy_label,
        },
    }


# ── AI DESCRIPTION ENHANCEMENT ───────────────────────────────────────────────

_ENHANCE_SYSTEM = (
    "You are a patent classification expert writing node descriptions for an AI-assisted taxonomy. "
    "Your descriptions instruct an AI classifier precisely when to apply or reject a tag. "
    "Descriptions must be domain-agnostic (work for any technology area), concrete, and rigorous. "
    "Follow the output format exactly as instructed — the classifier depends on it."
)


def _build_enhance_prompt(label: str, context: str, draft_line: str, is_root: bool = False) -> str:
    if is_root:
        return (
            f"Generate a description and screening keywords for this taxonomy root category.\n\n"
            f"Category: {label}\n"
            f"{context}"
            f"{draft_line}\n"
            f"Write 1-2 sentences describing what technology domain this category covers.\n"
            f"Then on a new line, add:\n"
            f"[SCREEN KEYWORDS: word1, word2, word3, ...]\n\n"
            f"The keywords are used for fast keyword pre-screening of patents BEFORE any LLM call. "
            f"Include 10-20 keywords or short phrases that would appear in patent titles or abstracts "
            f"for patents relevant to this category. Use inclusive vocabulary — false positives are "
            f"fine and filtered later; false negatives mean a patent is permanently skipped. "
            f"Include synonyms, abbreviations, anatomical terms, and technology-specific jargon.\n\n"
            f"Output format: description paragraph, then [SCREEN KEYWORDS: ...] on its own line. "
            f"No other text."
        )
    else:
        return (
            f"Write a classification description for this taxonomy leaf node.\n\n"
            f"Node: {label}\n"
            f"{context}"
            f"{draft_line}\n"
            f"This description instructs an AI classifier when to apply this tag to a patent "
            f"in ANY technology domain. Write 4-6 sentences covering:\n\n"
            f"1. QUALIFYING EVIDENCE — What explicit claim language must be present in at least one "
            f"claim (independent OR dependent)? Give 2-3 concrete example claim phrases.\n\n"
            f"2. COMPONENT/CONTEXT SCOPE:\n"
            f"   • MATERIAL tag: Name which specific device component the material must apply to.\n"
            f"   • MECHANISM tag: Require an explicitly named structural element in the claims — "
            f"implied behavior alone does not qualify.\n"
            f"   • DELIVERY/SYSTEM tag: Specify if the primary independent claim must be the "
            f"delivery component itself.\n"
            f"   • FUNCTIONAL tag: Outcome must be explicitly stated in a claim, not merely implied.\n\n"
            f"3. DISTINCTION — One sentence separating this tag from its nearest sibling tag.\n\n"
            f"4. FALSE POSITIVES — 2-3 concrete scenarios that must NOT trigger this tag.\n\n"
            f"Then on a separate line add:\n"
            f"QUALIFYING PHRASES: \"phrase1\" | \"phrase2\" | \"phrase3\" | \"phrase4\" | \"phrase5\" | \"phrase6\"\n\n"
            f"These are verbatim or near-verbatim claim language patterns that reliably trigger this tag. "
            f"Include vocabulary variants across different inventors, jurisdictions, and patent eras. "
            f"Include dependent claim patterns — secondary tags often live in dependent claims. "
            f"8-10 phrases preferred; more is better.\n\n"
            f"Output the paragraph first, then QUALIFYING PHRASES on its own line. No other formatting."
        )


@router.post("/taxonomy/enhance-description")
async def enhance_taxonomy_description(
    label: str = Form(...),
    description: str = Form(""),
    workspace_id: str = Form(...),
    parent_label: str = Form(""),
    is_root: bool = Form(False),
    current_user: dict = Depends(require_role(["ADMIN", "admin", "ANALYST", "analyst"])),
    db: AsyncSession = Depends(get_db),
):
    """Use AI to enhance/generate a taxonomy node description into a precise classifier guide."""
    _parse_ws_uuid(workspace_id)

    context = f"Parent category: {parent_label}\n" if parent_label else ""
    draft_line = f'Current description: "{description.strip()}"\n' if description.strip() else "No description yet.\n"

    prompt = _build_enhance_prompt(label, context, draft_line, is_root=is_root)

    try:
        llm = get_llm()
        response = await llm.chat.completions.create(
            model="deepseek/deepseek-v3.2",
            messages=[
                {"role": "system", "content": _ENHANCE_SYSTEM},
                {"role": "user", "content": prompt},
            ],
            max_tokens=700,
            temperature=0.2,
        )
    except Exception as e:
        logger.error(f"Description enhancement LLM call failed: {e}")
        raise HTTPException(status_code=503, detail=f"AI service unavailable: {str(e)}")

    enhanced = (response.choices[0].message.content or "").strip()
    if not enhanced:
        raise HTTPException(status_code=503, detail="AI returned empty response. Please try again.")

    return {"enhanced": enhanced}


async def _enhance_all_task(job_id: str, workspace_id: uuid.UUID) -> None:
    """Background task: enhance ALL taxonomy node descriptions in workspace."""
    from app.database import AsyncSessionLocal

    _ENHANCE_JOBS[job_id].update({"status": "running", "done": 0, "total": 0, "enhanced": 0, "failed": 0})
    try:
        async with AsyncSessionLocal() as db:
            all_nodes_result = await db.scalars(
                select(TaxonomyNode).where(TaxonomyNode.workspace_id == workspace_id)
            )
            all_nodes = list(all_nodes_result.all())
            total = len(all_nodes)
            _ENHANCE_JOBS[job_id]["total"] = total
            if not all_nodes:
                _ENHANCE_JOBS[job_id]["status"] = "done"
                return

            node_map = {n.node_id: n for n in all_nodes}
            llm = get_llm()

            async def _enhance_one(node: TaxonomyNode) -> tuple:
                node_is_root = (node.level == 0)
                parent_label = ""
                if node.parent_id and node.parent_id in node_map:
                    parent_label = node_map[node.parent_id].label
                context = f"Parent category: {parent_label}\n" if parent_label else ""
                existing = (node.description or "").strip()
                draft_line = f'Current description: "{existing}"\n' if existing else "No description yet.\n"
                prompt = _build_enhance_prompt(node.label, context, draft_line, is_root=node_is_root)
                async with _ENHANCE_SEMAPHORE:
                    try:
                        response = await llm.chat.completions.create(
                            model="deepseek/deepseek-v3.2",
                            messages=[
                                {"role": "system", "content": _ENHANCE_SYSTEM},
                                {"role": "user", "content": prompt},
                            ],
                            max_tokens=700,
                            temperature=0.2,
                        )
                        result = (response.choices[0].message.content or "").strip()
                        return node, result or None
                    except Exception as e:
                        logger.warning(f"Enhance job {job_id}: failed {node.node_id}: {e}")
                        return node, None

            tasks = [asyncio.ensure_future(_enhance_one(n)) for n in all_nodes]
            for coro in asyncio.as_completed(tasks):
                node, new_desc = await coro
                _ENHANCE_JOBS[job_id]["done"] += 1
                if new_desc:
                    node.description = new_desc
                    _ENHANCE_JOBS[job_id]["enhanced"] += 1
                else:
                    _ENHANCE_JOBS[job_id]["failed"] += 1

            await db.commit()

        _ENHANCE_JOBS[job_id]["status"] = "done"
        logger.info(
            f"Enhance job {job_id} done: "
            f"{_ENHANCE_JOBS[job_id]['enhanced']} enhanced, "
            f"{_ENHANCE_JOBS[job_id]['failed']} failed"
        )
    except Exception as exc:
        logger.error(f"Enhance job {job_id} crashed: {exc}", exc_info=True)
        _ENHANCE_JOBS[job_id].update({"status": "error", "error": str(exc)})


@router.post("/taxonomy/enhance-all-descriptions")
async def enhance_all_taxonomy_descriptions(
    workspace_id: str = Query(...),
    current_user: dict = Depends(require_role(["ADMIN", "admin", "ANALYST", "analyst"])),
    db: AsyncSession = Depends(get_db),
):
    """Start background re-enhancement of ALL taxonomy node descriptions. Returns job_id immediately."""
    ws_uuid = _parse_ws_uuid(workspace_id)
    job_id = str(uuid.uuid4())
    _ENHANCE_JOBS[job_id] = {"status": "queued", "done": 0, "total": 0, "enhanced": 0, "failed": 0}
    asyncio.create_task(_enhance_all_task(job_id, ws_uuid))
    logger.info(f"Enhance job {job_id} started for workspace {workspace_id}")
    return {"job_id": job_id, "status": "queued", "enhanced": 0, "failed": 0, "total": 0}


@router.get("/taxonomy/enhance-all-descriptions/{job_id}")
async def get_enhance_job_status(
    job_id: str,
    current_user: dict = Depends(get_current_user),
):
    """Poll status of an enhance-all job."""
    job = _ENHANCE_JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Enhance job not found")
    return job


# ── AI TAXONOMY GENERATION ────────────────────────────────────────────────────

@router.post("/taxonomy/generate")
async def generate_taxonomy_from_patents(
    workspace_id: str = Query(...),
    current_user: dict = Depends(require_role(["ADMIN", "admin", "ANALYST", "analyst"])),
    db: AsyncSession = Depends(get_db),
):
    """Auto-generate a 2-level taxonomy tree from the workspace's patents using AI."""
    ws_uuid = _parse_ws_uuid(workspace_id)

    patents_result = await db.execute(
        select(Patent.title, Patent.abstract)
        .where(Patent.workspace_id == ws_uuid)
        .limit(300)
    )
    rows = patents_result.all()
    if not rows:
        raise HTTPException(status_code=400, detail="No patents found in this workspace")

    # One line per patent: title — first 120 chars of abstract
    lines = []
    for row in rows:
        title = (row.title or "").strip()
        snippet = (row.abstract or "")[:120].strip().replace("\n", " ")
        if title:
            lines.append(f"{title}{(' — ' + snippet) if snippet else ''}")

    patents_text = "\n".join(lines[:200])

    prompt = (
        f"You are a patent intelligence analyst building a technology classification system.\n\n"
        f"Patent portfolio (title — abstract snippet):\n{patents_text}\n\n"
        f"Task: Generate a 2-level technology taxonomy that will be used to classify ALL patents in this portfolio.\n\n"
        f"Rules:\n"
        f"- Create 5–8 root categories covering the portfolio's core technology domains\n"
        f"- Create 2–5 subcategories under each root\n"
        f"- Labels must be technology-domain terms (e.g. 'Wireless Communication', 'Energy Storage') — "
        f"not product names, company names, or patent numbers\n"
        f"- Group patents by what the technology DOES and what DOMAIN it belongs to, not by surface keywords\n"
        f"- Patents use broad, obfuscated, or legal language to claim wider protection — "
        f"look past the wording to infer the underlying technology (e.g. 'electrochemical energy conversion' = battery/fuel cell)\n"
        f"- Each category must be semantically distinct with minimal overlap\n"
        f"- Categories should be broad enough to catch synonym/jargon variants but specific enough to be useful\n\n"
        f"Return JSON array only, no other text:\n"
        f"[{{\"id\":\"c1\",\"label\":\"Root Category\",\"parent_id\":null}},"
        f"{{\"id\":\"c1a\",\"label\":\"Subcategory\",\"parent_id\":\"c1\"}}]"
    )

    # ── Call LLM (infrastructure errors raise 503) ──────────────────────────────
    try:
        llm = get_llm()
        response = await llm.chat.completions.create(
            model="deepseek/deepseek-v3.2",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a patent taxonomy generator. Output ONLY a valid JSON array. "
                        "Never add explanations, markdown, or text outside the array. "
                        "Every element must have string fields 'id', 'label', and 'parent_id' (null or string). "
                        "Do not invent data — base every category solely on the provided patents."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            max_tokens=1200,
            temperature=0.15,
        )
    except Exception as e:
        logger.error(f"Taxonomy generation LLM call failed: {e}")
        raise HTTPException(status_code=503, detail=f"AI service unavailable: {str(e)}")

    # ── Parse output — bad output never raises, just yields empty list ─────────
    choice = response.choices[0]
    finish_reason = getattr(choice, "finish_reason", "stop") or "stop"
    content = (choice.message.content or "").strip()

    if finish_reason == "length":
        logger.warning("Taxonomy generation response truncated — output discarded to prevent corrupt tree")
        raise HTTPException(status_code=503, detail="AI response was cut off (token limit). Try again or reduce portfolio size.")

    try:
        match = re.search(r'\[.*\]', content, re.DOTALL)
        nodes_data: list = json.loads(match.group()) if match else []
        if not isinstance(nodes_data, list):
            raise ValueError("Expected a JSON array")
    except (json.JSONDecodeError, ValueError) as exc:
        logger.error(f"Taxonomy generation JSON parse failed ({exc}): {content[:200]!r}")
        raise HTTPException(status_code=503, detail="AI returned malformed output. Please try again.")

    # Namespace generated IDs per-workspace. taxonomy_nodes.node_id carries a
    # GLOBAL unique constraint, but the AI always emits generic ids (c1, c1a…)
    # that repeat across workspaces — bare ids collide and crash the commit with
    # a UniqueViolationError (surfaced to the browser as "Failed to fetch" because
    # the unhandled 500 escapes CORSMiddleware). Prefix is deterministic from the
    # workspace UUID so regeneration updates the same rows in place.
    ws_prefix = ws_uuid.hex[:8]
    def _ns(raw_id: str) -> str:
        return f"{ws_prefix}_{raw_id}"

    created = 0
    skipped = 0
    try:
        for node in nodes_data:
            # Strict validation — drop any node that isn't a clean {id, label} dict
            if not isinstance(node, dict):
                skipped += 1
                continue
            raw_id = str(node.get("id", "")).strip()
            label = str(node.get("label", "")).strip()
            raw_parent = node.get("parent_id") or None

            if not raw_id or not label:
                skipped += 1
                continue
            # parent_id must be a string or None — reject garbage types
            if raw_parent is not None and not isinstance(raw_parent, str):
                raw_parent = None

            node_id = _ns(raw_id)
            parent_id = _ns(raw_parent.strip()) if raw_parent else None

            existing = await db.scalar(
                select(TaxonomyNode).where(
                    TaxonomyNode.node_id == node_id,
                    TaxonomyNode.workspace_id == ws_uuid,
                )
            )
            if existing:
                existing.label = label
            else:
                db.add(TaxonomyNode(
                    node_id=node_id,
                    label=label,
                    workspace_id=ws_uuid,
                    parent_id=parent_id,
                    level=0 if parent_id is None else 1,
                ))
                created += 1

        await db.commit()
    except IntegrityError as exc:
        # Any residual node_id collision — fail cleanly so the error surfaces
        # through CORSMiddleware instead of as a raw "Failed to fetch".
        await db.rollback()
        logger.error(f"Taxonomy generation insert conflict: {exc}")
        raise HTTPException(
            status_code=409,
            detail="Taxonomy node ID conflict while generating. Please retry.",
        )

    if skipped:
        logger.warning(f"Taxonomy generation: skipped {skipped} malformed nodes from LLM output")
    return {"message": f"Generated {created} taxonomy nodes", "created": created, "total": len(nodes_data)}


# ── AI AUTO-TAG ───────────────────────────────────────────────────────────────

@router.post("/patents/{patent_number}/ai-suggest-taxonomy")
async def ai_suggest_taxonomy(
    patent_number: str,
    workspace_id: str = Query(...),
    include_claims: bool = Query(False),
    current_user: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
):
    """Use LLM to suggest taxonomy labels for a patent based on full patent context."""
    ws_uuid = _parse_ws_uuid(workspace_id)
    patent = await db.scalar(
        select(Patent).where(
            Patent.patent_number == patent_number,
            Patent.workspace_id == ws_uuid,
        )
    )
    if not patent:
        raise HTTPException(status_code=404, detail="Patent not found")

    nodes_result = await db.scalars(
        select(TaxonomyNode).where(TaxonomyNode.workspace_id == ws_uuid)
    )
    all_nodes = list(nodes_result.all())
    if not all_nodes:
        return {"suggestions": [], "message": "No taxonomy nodes found for this workspace"}

    # Build taxonomy tree lines: parent nodes first, then children indented
    node_map = {n.node_id: n for n in all_nodes}
    roots = [n for n in all_nodes if not n.parent_id or n.parent_id not in node_map]
    children = [n for n in all_nodes if n.parent_id and n.parent_id in node_map]
    ordered_nodes = roots + children

    tree_lines: list[str] = []
    for n in ordered_nodes:
        if n.parent_id and n.parent_id in node_map:
            desc = (n.description or "")[:800]
            desc_str = f" — {desc}" if desc else ""
            parent_label = node_map[n.parent_id].label
            tree_lines.append(f"  [TAG] {n.node_id}: {parent_label} > {n.label}{desc_str}")
        else:
            desc = (n.description or "")[:400]
            desc_str = f" — {desc}" if desc else ""
            tree_lines.append(f"[ROOT TAG] {n.node_id}: {n.label}{desc_str}")

    # DeepSeek V3.2 has 163k context — 35000 chars is safe
    MAX_TREE_CHARS = 35000
    taxonomy_tree = "\n".join(tree_lines)
    if len(taxonomy_tree) > MAX_TREE_CHARS:
        truncated = taxonomy_tree[:MAX_TREE_CHARS].rsplit("\n", 1)[0]
        dropped = taxonomy_tree[len(truncated):].count("\n")
        taxonomy_tree = truncated
        logger.warning(f"Taxonomy tree truncated for patent {patent_number}: ~{dropped} nodes dropped to stay within token budget")

    # Patent context fields
    title = (patent.title or "").strip()
    assignee = (patent.assignee or "").strip()
    abstract = (patent.abstract or "")[:800].strip()   # expanded: 400 → 800 chars
    cpc = (patent.cpc_class or "").strip()

    # Full claims text for accurate dependent-claim coverage; fallback to first_claim.
    claims_raw = (patent.claims_text or patent.first_claim or "").strip()
    claims_section = claims_raw[:10000]  # expanded: 8000 → 10000 chars

    # ── Tier-1 keyword pre-screen (no LLM cost) ────────────────────────────────
    # Skip LLM call entirely when the patent clearly doesn't belong to any root category's domain.
    if not _passes_domain_screen(title, abstract, patent.first_claim or "", roots):
        logger.info(f"Pre-screen: {patent_number} skipped — no domain keyword match in any root category")
        return {"suggestions": [], "no_match": True, "pre_screened": True}

    # ── Build classification prompt (top-down per-tag checklist) ───────────────
    roots_list = "\n".join(f"  • {n.label}" for n in roots)

    prompt = (
        f"PATENT TAXONOMY CLASSIFICATION\n\n"
        f"Quality standard: Correct tagging > Complete tagging. "
        f"Assign ZERO tags rather than one wrong tag. "
        f"Only tag what you can directly quote from a claim.\n\n"
        f"━━━ PATENT ━━━\n"
        f"Title: {title}\n"
        f"{f'Assignee: {assignee}' if assignee else ''}\n"
        f"{f'CPC: {cpc}' if cpc else ''}\n"
        f"Abstract: {abstract}\n\n"
        f"{f'Claims:{chr(10)}{claims_section}' if claims_section else ''}\n\n"
        f"━━━ STEP 1: DOMAIN SCREEN ━━━\n"
        f"Does this patent primarily claim a device, method, or system in these domains?\n"
        f"{roots_list}\n"
        f"→ If NONE apply → return {{\"evidence\":{{}},\"tags\":[]}} immediately.\n"
        f"→ If YES → continue to Step 2.\n\n"
        f"━━━ STEP 2: PER-TAG EVALUATION ━━━\n"
        f"Evaluate EVERY node in the taxonomy below — both [ROOT TAG] and [TAG]:\n"
        f"  a. For [ROOT TAG]: apply if the patent's primary subject clearly belongs to this broad category.\n"
        f"     Use the root description + its children's descriptions to understand the domain scope.\n"
        f"     Require at least one claim that directly addresses the root category's domain.\n"
        f"  b. For [TAG]: apply only when you can quote exact or near-verbatim claim language matching\n"
        f"     the description. If 'QUALIFYING PHRASES:' present, match those first.\n"
        f"  c. Check EVERY claim — independent AND dependent.\n"
        f"     Dependent claims contain material types, mechanisms, and functional details.\n"
        f"  d. Decide YES (can quote claim) or NO (not found or ambiguous).\n\n"
        f"TAXONOMY:\n"
        f"[ROOT TAG] = broad category — apply when patent primarily operates in this domain (needs claim evidence).\n"
        f"[TAG] = specific child tag — apply when exact claim language matches.\n"
        f"Both may appear in output. Never return invented IDs.\n"
        f"{taxonomy_tree}\n\n"
        f"━━━ STEP 3: PRECISION FILTER ━━━\n"
        f"Remove a YES tag if ANY of these apply:\n"
        f"  ✗ Evidence is in background or prior art section — not in a claim\n"
        f"  ✗ Material tag: named material applies to wrong component (e.g., spacer material ≠ gripper material)\n"
        f"  ✗ Mechanism tag: behavior is implied, not an explicitly named mechanism element\n"
        f"  ✗ Delivery tag: primary independent claim is the implant, not the delivery system\n"
        f"  ✗ Still uncertain after checking all claims → default NO\n\n"
        f"━━━ OUTPUT FORMAT ━━━\n"
        f"Return ONLY this JSON — no text before or after:\n"
        f'{{\"evidence\":{{\"node_id\":\"exact claim quote\"}},\"tags\":[\"node_id_1\"]}}\n'
        f"'evidence': one key per YES tag, value = the claim sentence that justifies it.\n"
        f"'tags': final node_ids after Step 3 filter. Both may be empty."
    )

    system_content = (
        "You are an expert patent classifier performing multi-label taxonomy classification.\n"
        "Output ONLY a valid JSON object: {\"evidence\":{...},\"tags\":[...]}.\n"
        "Rules:\n"
        "• Only use IDs from [TAG] entries — never [CATEGORY] IDs or invented IDs.\n"
        "• Every tag must be backed by a direct claim quote in the evidence dict.\n"
        "• Check ALL claims (independent + dependent) before deciding on each tag.\n"
        "• Material tags: the named material must apply to the correct device component.\n"
        "• Mechanism tags: require an explicitly named structural element, not implied behavior.\n"
        "• Delivery tags: primary independent claim must BE the delivery component.\n"
        "• When uncertain: NO is correct. Zero tags is acceptable; wrong tags are not.\n"
        "• No text outside the JSON object."
    )

    # ── Call LLM with retry + semaphore (handles scale: frontend sends batches of 5) ──
    _RETRY_BACKOFF = [2, 5, 10]
    llm = get_llm()
    last_exc: Exception | None = None

    async with _CLASSIFY_SEMAPHORE:
        for attempt in range(3):
            try:
                response = await llm.chat.completions.create(
                    model="deepseek/deepseek-v3.2",
                    messages=[
                        {"role": "system", "content": system_content},
                        {"role": "user", "content": prompt},
                    ],
                    max_tokens=1500,
                    temperature=0.0,
                )
                last_exc = None
                break  # success — exit retry loop
            except Exception as e:
                last_exc = e
                logger.warning(f"LLM classify attempt {attempt + 1}/3 failed for {patent_number}: {e}")
                if attempt < 2:
                    await asyncio.sleep(_RETRY_BACKOFF[attempt])

    if last_exc is not None:
        logger.error(f"AI taxonomy suggestion LLM call failed for {patent_number} after 3 attempts: {last_exc}")
        raise HTTPException(status_code=503, detail=f"AI service unavailable: {str(last_exc)}")

    # ── Parse output safely — bad output = no match, never raises ─────────────
    choice = response.choices[0]
    finish_reason = getattr(choice, "finish_reason", "stop") or "stop"
    content = (choice.message.content or "").strip()

    raw_ids = _safe_parse_id_array(content, finish_reason, patent_number=patent_number)

    id_to_label = {n.node_id: n.label for n in all_nodes}
    id_to_node = {n.node_id: n for n in all_nodes}
    # Allow both root and child nodes — filter only hallucinated IDs not in taxonomy
    suggestions = [
        {"node_id": nid, "label": id_to_label[nid]}
        for nid in raw_ids
        if nid in id_to_label
    ]

    if False:  # kept for diff visibility
        logger.warning(
            f"Placeholder"
        )
    hallucinated = [nid for nid in raw_ids if nid not in id_to_label]
    if hallucinated:
        logger.warning(
            f"AI returned {len(hallucinated)} hallucinated taxonomy ID(s) for {patent_number} — discarded: {hallucinated}"
        )

    return {"suggestions": suggestions, "no_match": len(suggestions) == 0}
