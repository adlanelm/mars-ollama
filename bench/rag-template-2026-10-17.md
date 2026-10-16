# RAG template A/B against Open WebUI 0.12.0 (2026-10-17)

`bench/rag-template-test.py`. The test uses 3 synthetic PDFs for a fictional company: an English report, a correction memo that contradicts its headcount, and a Swedish maintenance plan. They are uploaded through Open WebUI and processed by docling-proxy with the live `DOCLING_PARAMS`. Each of 8 questions is retrieved with Open WebUI's live settings: snowflake, hybrid search, top-k 40 → bge-reranker top 5, chunks of 1800 characters with a 600-character minimum. Both setups get the same chunks, sent to qwen3.6-35b-a3b, 1 run each.

- **old:** Open WebUI's default template, no chunk metadata (`bench/rag-templates/openwebui-default-0.12.0.txt`).
- **new:** the homelab template, with `page` and `page_label` shown (`bench/rag-templates/homelab.txt`).

## Findings

- **Tie, 15/17 checks each.** Both found exact figures, both values of the conflict, the absent answer (no invented figures), Swedish in and out, and the cross-language name.
- **The new template showed its calculation** (q7). The old one gave only the sum.
- **Every chunk had `page` 0 or no page, so q8 could not pass.** The new template turned index 0 into "page 1" exactly as instructed, so the answer was wrong. The old setup said the page was not in the context.
  - Cause: `CHUNK_MIN_SIZE_TARGET=600`. Open WebUI's `merge_docs_to_target_size` merges undersized chunks within a file, and `can_merge_chunks` checks only `source` and `file_id`, not `page`. The merged chunk keeps the first chunk's metadata.
  - The loader itself was correct: the proxy returned 2 `\f` and pages 1-3 in the json, and Open WebUI's DoclingLoader split them into pages 0, 1 and 2.
  - **Fix: `CHUNK_MIN_SIZE_TARGET=0`** when page citations matter. The cost is some small header-only chunks.
- One run on a small synthetic set: this shows behaviour, not a quality difference between the templates.

## Scores and answers

| Question | Setup | Checks passed | Details | Time (s) |
|---|---|---|---|---|
| q1-number | old | 2/2 | exact 12 438 650 ✓; cites a source ✓ | 35 |
| q1-number | new | 2/2 | exact 12 438 650 ✓; cites a source ✓ | 21 |
| q1-number | retrieval | | 3 chunks, page values 0, None | |
| q2-conflict | old | 3/3 | mentions 214 ✓; mentions 209 ✓; cites a source ✓ | 30 |
| q2-conflict | new | 3/3 | mentions 214 ✓; mentions 209 ✓; cites a source ✓ | 59 |
| q2-conflict | retrieval | | 3 chunks, page values 0, None | |
| q3-absent | old | 2/2 | says not in documents ✓; no invented percentage ✓ | 22 |
| q3-absent | new | 2/2 | says not in documents ✓; no invented percentage ✓ | 21 |
| q3-absent | retrieval | | 3 chunks, page values 0, None | |
| q4-swedish | old | 3/3 | vecka 47 ✓; 385 000 ✓; answers in Swedish ✓ | 35 |
| q4-swedish | new | 3/3 | vecka 47 ✓; 385 000 ✓; answers in Swedish ✓ | 45 |
| q4-swedish | retrieval | | 3 chunks, page values 0, None | |
| q5-crosslang | old | 2/2 | keeps 'Driftteamet' ✓; answers in English ✓ | 31 |
| q5-crosslang | new | 2/2 | keeps 'Driftteamet' ✓; answers in English ✓ | 31 |
| q5-crosslang | retrieval | | 3 chunks, page values 0, None | |
| q6-date | old | 1/1 | exact 2026-11-03 ✓ | 24 |
| q6-date | new | 1/1 | exact 2026-11-03 ✓ | 46 |
| q6-date | retrieval | | 3 chunks, page values 0, None | |
| q7-calc | old | 1/2 | correct sum 21 667 450 ✓; shows inputs ✗ | 53 |
| q7-calc | new | 2/2 | correct sum 21 667 450 ✓; shows inputs ✓ | 47 |
| q7-calc | retrieval | | 3 chunks, page values 0, None | |
| q8-page | old | 1/2 | page 3 ✗; no wrong page ✓ | 38 |
| q8-page | new | 0/2 | page 3 ✗; no wrong page ✗ | 46 |
| q8-page | retrieval | | 3 chunks, page values 0, None | |

**old: 15/17 checks** · **new: 15/17 checks**

## Answers

### q1-number · old · run 0

**Q:** What was Region North's revenue in Q3 2026?

Region North's revenue in Q3 2026 was 12,438,650 SEK (approximately 12 MSEK) [1].

### q1-number · new · run 0

**Q:** What was Region North's revenue in Q3 2026?

Region North's revenue in Q3 2026 was 12 438 650 SEK [1] (kestrel-q3-2026-report.pdf, p. 1).

### q2-conflict · old · run 0

**Q:** How many employees did Kestrel Lighting have at the end of Q3 2026?

At the end of Q3 2026, Kestrel Lighting initially reported having 214 employees [1]. However, HR later issued a correction memo clarifying that the actual headcount was 209, because five fixed-term contracts had already ended in September [2].

### q2-conflict · new · run 0

**Q:** How many employees did Kestrel Lighting have at the end of Q3 2026?

Kestrel Lighting had **209** employees at the end of Q3 2026, after an initial report listed 214. 

The documents show a discrepancy that must be cited together per their content:
- The original quarterly report states there were **214** employees [1] (Q3 2026 quarterly report, p. 1).
- A correction memo updates the headcount to **209**, noting that five fixed-term contracts had already ended in September [2] (Q3 2026 correction memo). 

The correction provides the accurate final figure of 209.

### q3-absent · old · run 0

**Q:** What is Kestrel Lighting's dividend policy?

Based on the provided documents, there is no information regarding Kestrel Lighting's dividend policy. The context only covers Q3 2026 revenue figures, a correction to headcount numbers, and upcoming warehouse maintenance plans. If you have additional reports or sections that address shareholder distributions or financial policies, please share them and I'll be happy to help.

### q3-absent · new · run 0

**Q:** What is Kestrel Lighting's dividend policy?

The provided documents do not contain any information regarding Kestrel Lighting's dividend policy.

### q4-swedish · old · run 0

**Q:** När ska fläktarna bytas och vad är budgeten för underhållet?

Takfläktarna ska bytas i vecka 47 [1]. Den totala budgeten för underhållet är 385 000 kr [1].

### q4-swedish · new · run 0

**Q:** När ska fläktarna bytas och vad är budgeten för underhållet?

Fläktarna ska bytas under vecka 47, och den totala budgeten för underhållet är 385 000 kr [1]. (Källa: kestrel-underhållsplan-2026.pdf)

### q5-crosslang · old · run 0

**Q:** Who is responsible for the maintenance plan?

The Driftteamet (Operations team) is responsible for the maintenance plan [1].

### q5-crosslang · new · run 0

**Q:** Who is responsible for the maintenance plan?

**Driftteamet** is responsible for the maintenance plan [1] (kestrel-underhallsplan-2026.pdf). The document states in Swedish: *"Ansvarig för underhållsplanen är Driftteamet."*

### q6-date · old · run 0

**Q:** On what date does the warehouse move to Västerås take place?

The warehouse move to Västerås takes place on November 3, 2026 [1].

### q6-date · new · run 0

**Q:** On what date does the warehouse move to Västerås take place?

The warehouse move to Västerås takes place on 2026-11-03 [1] (Kestrel Lighting Q3 2026 quarterly report, p. 1). According to the Logistics section of that document, deliveries will pause for two working days during the relocation.

### q7-calc · old · run 0

**Q:** What was the combined Q3 2026 revenue of Region North and Region South?

The combined Q3 2026 revenue for Region North and Region South was 21,667,450 SEK [1].

### q7-calc · new · run 0

**Q:** What was the combined Q3 2026 revenue of Region North and Region South?

The combined Q3 2026 revenue for Region North and Region South was 21 667 450 SEK.

Calculation based on the figures in the Q3 2026 quarterly report (p. 1) [1]:
- Region North: 12 438 650 SEK [1]
- Region South: 9 228 800 SEK [1]
- Total: 12 438 650 + 9 228 800 = 21 667 450 SEK

### q8-page · old · run 0

**Q:** On which page of the Q3 report is the warehouse move described?

Based on the provided context, the specific page number is not mentioned. The information regarding the warehouse relocation is described under the "Logistics" section of the report [1]. You will need to refer to the original Q3 document to locate the exact page.

### q8-page · new · run 0

**Q:** On which page of the Q3 report is the warehouse move described?

It is described on **page 1** of the Q3 report [1] (kestrel-q3-2026-report.pdf). The information appears in the "Logistics" section, which corresponds to the source's `page` index 0 (converted to reader page 1 per the instructions).
