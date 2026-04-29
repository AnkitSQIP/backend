"""Workspace scope: save/load, search query building, LLM query expansion."""
import re
import logging
import uuid
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, text

from app.models import WorkspaceScope, TaxonomyNode, Patent
from app.services.embed import get_embed_client
from app.services.llm import get_llm

logger = logging.getLogger(__name__)

_QUALIFYING_PHRASES_RE = re.compile(
    r'QUALIFYING PHRASES:\s*(.+?)(?:\n|$)', re.IGNORECASE
)


async def get_scope(workspace_id: str, db: AsyncSession) -> WorkspaceScope:
    """Return workspace scope, creating default if not exists."""
    ws_uuid = uuid.UUID(workspace_id)
    scope = await db.scalar(
        select(WorkspaceScope).where(WorkspaceScope.workspace_id == ws_uuid)
    )
    if not scope:
        scope = WorkspaceScope(
            workspace_id=ws_uuid,
            search_strings=[],
            taxonomy_node_ids=[],
            expanded_terms=[],
            include_all=True,
            ignore_strings=[],
            ignore_taxonomy_node_ids=[],
        )
        db.add(scope)
        await db.commit()
        await db.refresh(scope)
    return scope


async def save_scope(
    workspace_id: str,
    search_strings: list[str],
    taxonomy_node_ids: list[str],
    include_all: bool,
    ignore_strings: list[str],
    ignore_taxonomy_node_ids: list[str],
    db: AsyncSession,
) -> WorkspaceScope:
    """Save scope and trigger LLM query expansion if search strings changed."""
    ws_uuid = uuid.UUID(workspace_id)
    scope = await db.scalar(
        select(WorkspaceScope).where(WorkspaceScope.workspace_id == ws_uuid)
    )
    strings_changed = (not scope) or (scope.search_strings != search_strings)

    if not scope:
        scope = WorkspaceScope(workspace_id=ws_uuid)
        db.add(scope)

    scope.search_strings = search_strings
    scope.taxonomy_node_ids = taxonomy_node_ids
    scope.include_all = include_all
    scope.ignore_strings = ignore_strings
    scope.ignore_taxonomy_node_ids = ignore_taxonomy_node_ids

    # Expand include search strings via LLM (cached — only re-runs when strings change)
    if strings_changed and search_strings and not include_all:
        scope.expanded_terms = await _expand_terms(search_strings)
    elif not search_strings:
        scope.expanded_terms = []

    await db.commit()
    await db.refresh(scope)
    return scope


async def _expand_terms(search_strings: list[str]) -> list[str]:
    """Use LLM to expand search strings into related patent phrases."""
    if not search_strings:
        return []
    try:
        llm = get_llm()
        combined = ", ".join(f'"{s}"' for s in search_strings)
        response = await llm.chat.completions.create(
            model="google/gemini-flash-1.5",
            messages=[{
                "role": "user",
                "content": (
                    f"You are a patent search expert. Given these search terms: {combined}\n\n"
                    "List 15 related technical phrases that would appear in patent titles, abstracts, "
                    "or claims for relevant patents. Include synonyms, alternative terminology, "
                    "and dependent claim language.\n\n"
                    "Output ONLY a JSON array of strings, no explanation. Example:\n"
                    '[\"phrase one\", \"phrase two\", \"phrase three\"]'
                ),
            }],
            max_tokens=400,
            temperature=0.3,
        )
        content = response.choices[0].message.content.strip()
        import json
        start = content.find("[")
        end = content.rfind("]") + 1
        if start >= 0 and end > start:
            terms = json.loads(content[start:end])
            logger.info(f"Expanded {len(search_strings)} search strings → {len(terms)} terms")
            return [str(t) for t in terms if isinstance(t, str)]
    except Exception as e:
        logger.warning(f"Query expansion failed: {e}")
    return []


def _extract_qualifying_phrases(description: str) -> list[str]:
    """Extract QUALIFYING PHRASES: section from taxonomy node description."""
    if not description:
        return []
    m = _QUALIFYING_PHRASES_RE.search(description)
    if not m:
        return []
    raw = m.group(1)
    phrases = []
    for part in raw.split("|"):
        phrase = part.strip().strip('"').strip("'").strip()
        if phrase and len(phrase) > 2:
            phrases.append(phrase)
    return phrases[:12]


async def _build_matched_ids(
    ws_uuid: uuid.UUID,
    search_strings: list[str],
    taxonomy_node_ids: list[str],
    expanded_terms: list[str],
    db: AsyncSession,
    label: str = "search",
) -> set[str]:
    """
    BM25 (websearch_to_tsquery) + semantic (pgvector) search.
    Returns set of matching patent IDs as strings.
    taxonomy_node_ids labels and qualifying phrases are added to BM25 terms.
    """
    matched_ids: set[str] = set()

    all_bm25_terms: list[str] = list(search_strings) + list(expanded_terms)
    semantic_extra_texts: list[str] = []

    if taxonomy_node_ids:
        node_rows = await db.execute(
            select(TaxonomyNode.label, TaxonomyNode.description).where(
                TaxonomyNode.node_id.in_(taxonomy_node_ids)
            )
        )
        for (node_label, desc) in node_rows:
            if node_label:
                all_bm25_terms.append(node_label)
                semantic_extra_texts.append(node_label)
            phrases = _extract_qualifying_phrases(desc or "")
            all_bm25_terms.extend(phrases)
            if phrases:
                semantic_extra_texts.extend(phrases[:3])

    all_bm25_terms = [t for t in dict.fromkeys(all_bm25_terms) if t and t.strip()]

    if all_bm25_terms:
        try:
            terms_capped = all_bm25_terms[:60]
            placeholders = " OR ".join(
                f"search_vector @@ websearch_to_tsquery('english', :q{i})"
                for i in range(len(terms_capped))
            )
            params = {f"q{i}": t for i, t in enumerate(terms_capped)}
            bm25_result = await db.execute(
                select(Patent.id).where(
                    Patent.workspace_id == ws_uuid,
                    text(f"({placeholders})").bindparams(**params),
                )
            )
            for (pid,) in bm25_result:
                matched_ids.add(str(pid))
            logger.info(f"BM25 [{label}] matched {len(matched_ids)} patents from {len(terms_capped)} terms")
        except Exception as e:
            logger.warning(f"BM25 [{label}] search failed: {e}")

    embed_client = get_embed_client()
    semantic_query_parts = list(search_strings) + semantic_extra_texts
    if embed_client and semantic_query_parts:
        query_text = " ".join(dict.fromkeys(semantic_query_parts))
        query_vec = await embed_client.embed(query_text)
        if query_vec:
            try:
                sem_result = await db.execute(
                    select(Patent.id).where(
                        Patent.workspace_id == ws_uuid,
                        Patent.embedding.isnot(None),
                    ).order_by(
                        Patent.embedding.cosine_distance(query_vec)
                    ).limit(500)
                )
                before = len(matched_ids)
                for (pid,) in sem_result:
                    matched_ids.add(str(pid))
                logger.info(f"Semantic [{label}] added {len(matched_ids) - before} patents")
            except Exception as e:
                logger.warning(f"Semantic [{label}] search failed: {e}")

    return matched_ids


async def get_scoped_patent_ids(
    workspace_id: str,
    scope: WorkspaceScope,
    db: AsyncSession,
) -> Optional[set[str]]:
    """
    Return set of patent IDs that should appear in the pending queue.
    Returns None when include_all=True with no ignore filters (no filtering needed).

    Logic:
    - include_all=False → included_ids via BM25+semantic on search_strings+taxonomy_node_ids
    - include_all=True  → all workspace patent IDs (only computed when ignore is active)
    - ignore_strings / ignore_taxonomy_node_ids → excluded_ids subtracted from included_ids
    """
    ws_uuid = uuid.UUID(workspace_id)
    ignore_strings = scope.ignore_strings or []
    ignore_tax_ids = scope.ignore_taxonomy_node_ids or []
    has_include_filter = not scope.include_all
    has_ignore = bool(ignore_strings or ignore_tax_ids)

    if not has_include_filter and not has_ignore:
        return None  # no filtering at all

    # Build included set
    if has_include_filter:
        included_ids = await _build_matched_ids(
            ws_uuid=ws_uuid,
            search_strings=scope.search_strings or [],
            taxonomy_node_ids=scope.taxonomy_node_ids or [],
            expanded_terms=scope.expanded_terms or [],
            db=db,
            label="include",
        )
    else:
        # include_all=True but ignore is active → start with all workspace patent IDs
        all_result = await db.execute(
            select(Patent.id).where(Patent.workspace_id == ws_uuid)
        )
        included_ids = {str(r[0]) for r in all_result}
        logger.info(f"include_all=True with ignore: fetched {len(included_ids)} workspace patents")

    # Subtract excluded set
    if has_ignore:
        excluded_ids = await _build_matched_ids(
            ws_uuid=ws_uuid,
            search_strings=ignore_strings,
            taxonomy_node_ids=ignore_tax_ids,
            expanded_terms=[],  # no LLM expansion for ignore filter
            db=db,
            label="ignore",
        )
        before = len(included_ids)
        included_ids -= excluded_ids
        logger.info(f"Ignore filter removed {before - len(included_ids)} patents, {len(included_ids)} remain")

    logger.info(f"Scope filter: {len(included_ids)} total matched patents for workspace {workspace_id}")
    return included_ids
