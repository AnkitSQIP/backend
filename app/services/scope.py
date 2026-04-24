"""Workspace scope: save/load, search query building, LLM query expansion."""
import re
import logging
import uuid
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, distinct, text, or_
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

    # Collect all BM25 search terms: user strings + LLM expanded + taxonomy qualifying phrases
    # Each term is searched independently via websearch_to_tsquery — OR across all terms.
    # websearch_to_tsquery handles natural language, commas (OR), +signs (AND), semicolons, etc.
    all_bm25_terms: list[str] = list(scope.search_strings) + list(scope.expanded_terms)

    # Add taxonomy node labels and qualifying phrases from selected nodes (and their parents)
    semantic_extra_texts: list[str] = []
    if scope.taxonomy_node_ids:
        node_rows = await db.execute(
            select(TaxonomyNode.label, TaxonomyNode.description).where(
                TaxonomyNode.node_id.in_(scope.taxonomy_node_ids)
            )
        )
        for (label, desc) in node_rows:
            if label:
                all_bm25_terms.append(label)
                semantic_extra_texts.append(label)
            phrases = _extract_qualifying_phrases(desc or "")
            all_bm25_terms.extend(phrases)
            if phrases:
                semantic_extra_texts.extend(phrases[:3])

    # Deduplicate and drop empty terms
    all_bm25_terms = [t for t in dict.fromkeys(all_bm25_terms) if t and t.strip()]

    # 1. BM25 full-text search — one websearch_to_tsquery call per term, OR'd together
    # websearch_to_tsquery is safe with any input (unlike to_tsquery which requires valid syntax)
    if all_bm25_terms:
        try:
            # Build: search_vector @@ websearch_to_tsquery('english', :q0) OR ...
            # Cap at 60 terms to keep query size reasonable
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
            logger.info(f"BM25 matched {len(matched_ids)} patents from {len(terms_capped)} terms")
        except Exception as e:
            logger.warning(f"BM25 search failed: {e}")

    # 2. Semantic search via pgvector (only if embed service available + embeddings exist)
    # Searches user strings + taxonomy term texts — OR logic (union with BM25 results)
    embed_client = get_embed_client()
    semantic_query_parts = list(scope.search_strings) + semantic_extra_texts
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
                logger.info(f"Semantic search added {len(matched_ids) - before} patents")
            except Exception as e:
                logger.warning(f"Semantic search failed: {e}")

    logger.info(f"Scope filter: {len(matched_ids)} total matched patents for workspace {workspace_id}")
    return matched_ids
