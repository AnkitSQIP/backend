# IPWatch — All LLM Prompts Reference

All prompts used in the system. Exact text as implemented in `app/routers/taxonomy.py`.

---

## 1. Description Enhancement — Root Category Node

**Endpoint**: `POST /api/taxonomy/enhance-all-descriptions` (nodes with `level == 0`)  
**Model**: `deepseek/deepseek-v3.2` · `max_tokens: 700` · `temperature: 0.2`

**System prompt** (`_ENHANCE_SYSTEM`):
```
You are a taxonomy expert who writes precise, actionable descriptions for patent classification systems.
Your descriptions must be clear, specific, and domain-agnostic — they should work equally well
for medical devices, software, chemicals, or any other technology area.
Output only the description text with no preamble or commentary.
```

**User prompt**:
```
Generate a description and screening keywords for this taxonomy root category.

Category: {label}
{context (e.g. "Parent category: X\n" or "")}
{draft_line (e.g. 'Current description: "..."' or "No description yet.")}

Write 1-2 sentences describing what technology domain this category covers.
Then on a new line, add:
[SCREEN KEYWORDS: word1, word2, word3, ...]

The keywords are used for fast keyword pre-screening of patents BEFORE any LLM call.
Include 10-20 keywords or short phrases that would appear in patent titles or abstracts
for patents relevant to this category. Use inclusive vocabulary — false positives are
fine and filtered later; false negatives mean a patent is permanently skipped.
Include synonyms, abbreviations, anatomical terms, and technology-specific jargon.

Output format: description paragraph, then [SCREEN KEYWORDS: ...] on its own line.
No other text.
```

**Output written to DB**: full text including `[SCREEN KEYWORDS: ...]` line.  
Keywords are parsed at classification time by `_passes_domain_screen()` for Tier-1 pre-screening.

---

## 2. Description Enhancement — Child/Leaf Node

**Endpoint**: `POST /api/taxonomy/enhance-all-descriptions` (nodes with `level > 0`)  
**Model**: `deepseek/deepseek-v3.2` · `max_tokens: 700` · `temperature: 0.2`

**System prompt**: same as root (see above)

**User prompt**:
```
Write a classification description for this taxonomy leaf node.

Node: {label}
{context}
{draft_line}

This description instructs an AI classifier when to apply this tag to a patent
in ANY technology domain. Write 4-6 sentences covering:

1. QUALIFYING EVIDENCE — What explicit claim language must be present in at least one
   claim (independent OR dependent)? Give 2-3 concrete example claim phrases.

2. COMPONENT/CONTEXT SCOPE:
   • MATERIAL tag: Name which specific device component the material must apply to.
   • MECHANISM tag: Require an explicitly named structural element in the claims —
     implied behavior alone does not qualify.
   • DELIVERY/SYSTEM tag: Specify if the primary independent claim must be the
     delivery component itself.
   • FUNCTIONAL tag: Outcome must be explicitly stated in a claim, not merely implied.

3. DISTINCTION — One sentence separating this tag from its nearest sibling tag.

4. FALSE POSITIVES — 2-3 concrete scenarios that must NOT trigger this tag.

Then on a separate line add:
QUALIFYING PHRASES: "phrase1" | "phrase2" | "phrase3" | "phrase4" | "phrase5" | "phrase6"

These are verbatim or near-verbatim claim language patterns that reliably trigger this tag.
Include vocabulary variants across different inventors, jurisdictions, and patent eras.
Include dependent claim patterns — secondary tags often live in dependent claims.
8-10 phrases preferred; more is better.

Output the paragraph first, then QUALIFYING PHRASES on its own line. No other formatting.
```

**Output written to DB**: full text including `QUALIFYING PHRASES:` line.  
The classifier reads this directly and uses phrases as verbatim search targets.

---

## 3. Bulk Workspace Classification (Automate Tags)

**Endpoint**: `POST /api/taxonomy/classify-workspace`  
**Model**: `deepseek/deepseek-v3.2` · `max_tokens: 300` · `temperature: 0.0`  
**Concurrency**: Semaphore(20) · Retry: 3 attempts with [2s, 5s, 10s] backoff  
**DB writes**: Bulk `INSERT ... ON CONFLICT DO NOTHING` — no N×M individual queries  
**Tier 1**: Keyword pre-screen via `[SCREEN KEYWORDS]` in root descriptions (no LLM cost)

> **Output format**: bare JSON array `["id1","id2"]` — no evidence dict.
> Evidence is omitted for speed (reduces output from ~600 tokens → ~30 tokens per patent).
> Model reasoning is unchanged; only the output format is compact.

**System prompt**:
```
You are an expert patent classifier performing multi-label taxonomy classification.
Output ONLY a JSON array of matched node IDs: ["id1","id2"] or [] for no match.
Rules:
• Use IDs from both [ROOT TAG] and [TAG] entries — never invented IDs.
• [ROOT TAG]: apply only when patent's primary subject clearly falls under this broad
  category — needs direct claim evidence.
• [TAG]: apply only when exact claim language matches description or QUALIFYING PHRASES.
• Check ALL claims (independent + dependent) before deciding.
• Never tag from abstract, background, or prior art — claims only.
• When uncertain: NO. Zero tags acceptable; wrong tags are not.
• Output ONLY the JSON array — no text, no keys, no explanation.
```

**User prompt**:
```
PATENT TAXONOMY CLASSIFICATION

Quality standard: Correct tagging > Complete tagging. Assign ZERO tags rather than one
wrong tag. Only tag what you can directly quote from a claim.

━━━ PATENT ━━━
Title: {title}
Assignee: {assignee}  [omitted if empty]
CPC: {cpc}            [omitted if empty]
Abstract: {abstract}  [first 800 chars]

Claims:
{claims_text}         [first 10,000 chars of claims_text or first_claim]

━━━ STEP 1: DOMAIN SCREEN ━━━
Does this patent primarily claim a device, method, or system in these domains?
  • {root_category_1}
  • {root_category_2}
  ...
→ If NONE apply → return [] immediately.
→ If YES → continue to Step 2.

━━━ STEP 2: PER-TAG EVALUATION ━━━
Evaluate EVERY node in the taxonomy below — both [ROOT TAG] and [TAG]:
  a. For [ROOT TAG]: apply if the patent's primary subject clearly belongs to this broad
     category. Use the root description + its children's descriptions to understand domain
     scope. Require at least one claim that directly addresses the root category's domain.
  b. For [TAG]: apply only when you can quote exact or near-verbatim claim language matching
     the description. If 'QUALIFYING PHRASES:' present, match those first.
  c. Check EVERY claim — independent AND dependent.
     Dependent claims contain material types, mechanisms, and functional details.
  d. Decide YES (can quote claim) or NO (not found or ambiguous).

TAXONOMY:
[ROOT TAG] = broad category — apply when patent primarily operates in this domain (needs claim evidence).
[TAG] = specific child tag — apply when exact claim language matches.
Both may appear in output. Never return invented IDs.
{taxonomy_tree}  [capped at 35,000 chars]

━━━ STEP 3: PRECISION FILTER ━━━
Remove a YES tag if ANY of these apply:
  ✗ Evidence is in background or prior art section — not in a claim
  ✗ Material tag: named material applies to wrong component
  ✗ Mechanism tag: behavior is implied, not an explicitly named mechanism element
  ✗ Delivery tag: primary independent claim is the implant, not the delivery system
  ✗ Still uncertain after checking all claims → default NO

━━━ OUTPUT FORMAT ━━━
Return ONLY a JSON array — no text before or after:
["node_id_1", "node_id_2"]
Empty array [] if no tags pass. No keys, no evidence, no explanation.
```

---

## 4. Single-Patent AI Suggest Tags

**Endpoint**: `POST /api/patents/{patent_number}/ai-suggest-taxonomy`  
**Model**: `deepseek/deepseek-v3.2` · `max_tokens: 1500` · `temperature: 0.0`  
**Option**: `include_claims=true` sends full claims text (default: first claim only)

> **Output format**: full evidence dict `{"evidence":{...},"tags":[...]}` — kept for
> traceability and debugging on single-patent calls.

**System prompt**:
```
You are an expert patent classifier performing multi-label taxonomy classification.
Output ONLY a valid JSON object: {"evidence":{...},"tags":[...]}.
Rules:
• Use IDs from both [ROOT TAG] and [TAG] entries — never invented IDs.
• [ROOT TAG]: apply only when patent's primary subject clearly falls under this broad
  category — needs direct claim evidence.
• [TAG]: apply only when exact claim language matches description or QUALIFYING PHRASES.
• Every tag must have a direct claim quote in the evidence dict.
• Check ALL claims (independent + dependent) before deciding.
• Never tag from abstract, background, or prior art — claims only.
• When uncertain: NO. Zero tags acceptable; wrong tags are not.
• No text outside the JSON object.
```

**User prompt**: same structure as Bulk Classification (Steps 1–3), with output format:
```
━━━ OUTPUT FORMAT ━━━
Return ONLY this JSON — no text before or after:
{"evidence":{"node_id":"exact claim quote"},"tags":["node_id_1"]}
'evidence': one key per YES tag, value = the claim sentence that justifies it.
'tags': final node_ids after Step 3 filter. Both may be empty.
```

With `include_claims=false`: only `first_claim` sent in claims section.  
With `include_claims=true`: full `claims_text` (up to 10,000 chars) sent.

---

## Taxonomy Tree Format in Prompts

Root nodes (both bulk and single-patent):
```
[ROOT TAG] {node_id}: {label} — {description[:400]}
```

Child nodes:
```
  [TAG] {node_id}: {parent_label} > {label} — {description[:800]}
```

Descriptions written by enhancement contain:
- **Root**: 1-2 sentence domain description + `[SCREEN KEYWORDS: ...]`
- **Child**: 4-6 sentence classification guide + `QUALIFYING PHRASES: "..." | "..." | ...`

The classifier reads `QUALIFYING PHRASES` as verbatim search targets — highest confidence signal.  
The pre-screener reads `[SCREEN KEYWORDS]` to skip out-of-domain patents without any LLM call.

---

## Tagging Rules — Root vs Child

| Node type | Tag colour (UI) | When to apply |
|---|---|---|
| `[ROOT TAG]` (level 0) | Yellow | Patent's primary domain clearly matches this category — requires claim evidence |
| `[TAG]` (level > 0) | Green | Exact or near-verbatim claim language matches description/QUALIFYING PHRASES |

Both root and child tags are stored in `PatentTaxonomy` table. Root tags provide broad
classification; child tags provide specific feature classification. Both are filterable
and exportable in the Investigation Queue UI.

---

## Key Constants

| Parameter | Value | Location |
|---|---|---|
| Max concurrent LLM calls (classify) | 20 | `MAX_CONCURRENT_LLM` |
| Max concurrent enhance calls | 10 | `_ENHANCE_SEMAPHORE` |
| DB commit batch size | 50 patents | `COMMIT_BATCH_SIZE` |
| Max taxonomy tree chars | 35,000 | `MAX_TREE_CHARS` |
| Max abstract chars (classify) | 800 | inline in classify |
| Max claims chars (classify) | 10,000 | inline in classify |
| Root desc chars in prompt | 400 | inline in tree builder |
| Child desc chars in prompt | 800 | inline in tree builder |
| Bulk classify max_tokens | 300 | LLM call (array output, compact) |
| Single-patent max_tokens | 1,500 | LLM call (evidence dict output) |
| Enhance max_tokens | 700 | LLM call |
| Enhance temperature | 0.2 | LLM call |
| Classify temperature | 0.0 | LLM call |

## DB Flush Strategy

`_flush_bulk_chunk()` uses a single bulk `INSERT ... ON CONFLICT DO NOTHING` statement
for all tag assignments in a chunk. This replaces the previous N×M individual
`SELECT` + `INSERT` queries (was 220+ round-trips for 44 patents with ~5 tags each,
adding 11-22 seconds of Neon DB latency).
