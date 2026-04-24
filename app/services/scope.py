"""Workspace scope: save/load, search query building, LLM query expansion."""
import re
import logging
import uuid
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, distinct, text
from sqlalchemy.dialects.postgresql import array

from app.models import WorkspaceScope, TaxonomyNode, PatentTaxonomy, Patent
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

    # Expand search strings via LLM (cached — only re-runs when strings change)
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
        # Parse JSON array
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
    # Phrases are pipe-separated, may be quoted
    phrases = []
    for part in raw.split("|"):
        phrase = part.strip().strip('"').strip("'").strip()
        if phrase and len(phrase) > 2:
            phrases.append(phrase)
    return phrases[:12]  # cap at 12 to keep tsquery manageable


def _build_tsquery(terms: list[str]) -> Optional[str]:
    """Convert list of phrase strings into a PostgreSQL tsquery."""
    if not terms:
        return None
    parts = []
    for term in terms:
        # Each multi-word phrase becomes phrase query with <-> operator
        words = [w for w in re.sub(r"[^\w\s]", " ", term).split() if len(w) > 1]
        if not words:
            continue
        if len(words) == 1:
            parts.append(f"{words[0]}:*")
        else:
            parts.append(" <-> ".join(words))
    if not parts:
        return None
    # OR across all phrase parts
    return " | ".join(f"({p})" for p in parts[:50])  # cap at 50 parts


async def get_scoped_patent_ids(
    workspace_id: str,
    scope: WorkspaceScope,
    db: AsyncSession,
) -> Optional[set[str]]:
    """
    Return set of patent IDs matching the current scope.
    Returns None when include_all=True (caller should not filter).
    """
    if scope.include_all:
        return None

    ws_uuid = uuid.UUID(workspace_id)
    matched_ids: set[str] = set()

    # Collect all search terms: user strings + LLM expanded + taxonomy qualifying phrases
    all_bm25_terms = list(scope.search_strings) + list(scope.expanded_terms)

    # Add qualifying phrases from selected taxonomy nodes
    if scope.taxonomy_node_ids:
        node_rows = await db.execute(
            select(TaxonomyNode.description).where(
                TaxonomyNode.node_id.in_(scope.taxonomy_node_ids)
            )
        )
        for (desc,) in node_rows:
            all_bm25_terms.extend(_extract_qualifying_phrases(desc or ""))

    # 1. BM25 full-text search
    tsq = _build_tsquery(all_bm25_terms)
    if tsq:
        try:
            bm25_result = await db.execute(
                select(Patent.id).where(
                    Patent.workspace_id == ws_uuid,
                    text("search_vector @@ to_tsquery('english', :q)").bindparams(q=tsq),
                )
            )
            for (pid,) in bm25_result:
                matched_ids.add(str(pid))
            logger.info(f"BM25 matched {len(matched_ids)} patents")
        except Exception as e:
            logger.warning(f"BM25 search failed: {e}")

    # 2. Semantic search via pgvector (only if embed service available + embeddings exist)
    embed_client = get_embed_client()
    if embed_client and scope.search_strings:
        query_text = " ".join(scope.search_strings)
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
                logger.info(f"Semantic search added {len(matched_ids) - before} patents")
            except Exception as e:
                logger.warning(f"Semantic search failed: {e}")

    logger.info(f"Scope filter: {len(matched_ids)} total matched patents for workspace {workspace_id}")
    return matched_ids
