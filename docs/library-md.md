# library-md: Calibre library to Markdown for RAG

Status: **script written, tested on 5 books (2026-10-30), full run not started; figure captions added and tested on a few books**. Script: [`bin/library-md`](../bin/library-md).

See also: [clients.md](clients.md), [gpu-switching.md](gpu-switching.md).

## What it is

Converts the Calibre library `~/MEGA/mainlib` (526 books: 287 PDF, 246 EPUB, 26 CHM, 12 MOBI, 4 FB2, 2 AZW3, 1 MD; 268 books are PDF-only, 13 of them scanned without any text layer) into a Markdown copy for local RAG (Open WebUI + Qdrant later; Open WebUI on jupiter.internal is not used yet). The Calibre library is only read. Output goes to `~/MEGA/library-md`, next to the Calibre library, so MEGA syncs it (438 MB for 497 books). State, the Markdown cache and logs (4 GB, rewritten constantly) stay outside MEGA in `~/.cache/library-md` (`--work`, called WORK below).

## Output format

| Item | Choice | Why |
|---|---|---|
| One file per book | UTF-8 Markdown | Open WebUI loads `.md` as plain text (no docling round trip) and splits on Markdown headers, which suits chapter headings |
| Front matter | YAML, scalars written as JSON | JSON scalars are valid YAML; title, authors, year, category, topics, `depends_on`, status, summary, Calibre id and tags, conversion method, `figures_described`, `figures_described_by` |
| Outdated banner | `> **Outdated** (year, judged date): reason` after the front matter, then the why: newer edition in the library, what the book relies on, evidence from the text, what changed since, age | The reader of a retrieved chunk sees it and why |
| Filename | `Title - Surname (Year) (OUTDATED).md`, ASCII only (letters, digits, space, `. _ ( ) -`), Cyrillic transliterated (`Создание сильных брендов` -> `Sozdanie silnykh brendov`), `C++`/`C#` -> `Cpp`/`CSharp`, `&` -> `and`, a colon -> ` - `; titles over 70 chars lose the subtitle, then are cut at a word; a name clash gets ` id<calibre id>` | Every retrieved chunk's source name carries `(OUTDATED)`; names survive Open WebUI knowledge upload, URLs, shells and Windows |
| Title source | the Calibre title and first author; the LLM's title/author only replaces a junk Calibre value. Front matter keeps the original (e.g. Cyrillic) title | Fix titles in Calibre, then `place` |
| Folders | one per top-level category, same ASCII rule (`Data and AI`) | Browsing, and upload by folder |
| `catalog.csv`, `README.md` | one row / one line per book, with status, reason, what changed and newer edition | A catalog that can itself be indexed |
| `replacements.md` | per outdated book: replacement (or what to use instead), why it fits, alternatives, sources, confidence | Written by the `replace` stage, see [Replacements](#replacements) |
| Figures | `> **Figure:** description` line at the picture's position | Described by granite-vision in the `caption` stage, see [Figures and captions](#figures-and-captions); decorative images and books in skipped categories have none |

Layout: `OUT/<Category>/<name>.md`, `OUT/catalog.csv`, `OUT/README.md` (also lists books that failed to convert), `OUT/replacements.md`; cache and state in `WORK`.

## Usage

```bash
bin/library-md                                   # everything: convert (+ caption worker), classify, replace, place, caption, place
bin/library-md convert --limit 20                # one stage (convert|classify|replace|caption|place), at most 20 pending books
bin/library-md status                            # counts per stage and the failures
bin/library-md skip --ids 883                    # never convert these books (--retry-failed leaves them alone)
bin/library-md classify --reclassify && bin/library-md place
bin/library-md --ids 823,924                     # only these Calibre ids
```

Long runs, detached, with a log:

```bash
setsid nohup bin/library-md >> ~/.cache/library-md/run.log 2>&1 < /dev/null &
tail -f ~/.cache/library-md/run.log      # live, one line per book
pkill -TERM -xf 'python3 bin/library-md'           # stop; rerun the start line to resume
```

A detached `&` run starts with SIGINT ignored, so the script treats SIGTERM like Ctrl-C.

Options: `--lib`, `--out` (default `~/MEGA/library-md`), `--work` (default `~/.cache/library-md`), `--ids`, `--limit`, `--retry-failed`, `--reclassify`, `--model` (default `qwen3.6-35b-a3b`), `--thinking`, `--epub-engine pandoc|docling`, `--min-text` (200), `--pdf-timeout`, `--no-gpu-wait`, `--caption-model` (default `granite-vision-4.1`), `--caption-skip` (comma-separated categories, default `Fiction & Literature`), `--figure-min-area` (40000 px), `--docling`, `--llm`, `--searxng` (default `https://jupiter.internal:8888`), `--search-delay` (8 s).

## Conversion routes

| Source | Route |
|---|---|
| EPUB | pandoc (`gfm`, no raw HTML); docling as fallback |
| AZW3, MOBI, FB2, CHM | `ebook-convert` to EPUB, then pandoc |
| MD, TXT | copied |
| PDF with a text layer | docling-proxy async API |
| Scanned PDF | docling-proxy with OCR (easyocr, `force_ocr`), language from Calibre: eng to en, rus to ru, deu to de, and so on |

- Format preference when a book has several: EPUB, AZW3, AZW, MOBI, FB2, CHM, MD, TXT, PDF.
- Why pandoc first: docling 2.138 does read EPUB, but in the Kubernetes Best Practices EPUB it joined each multi-line YAML code block into one line of inline code. Pandoc kept fenced blocks. `--epub-engine docling` prefers docling.
- Calibre's own txt/markdown output was rejected: it mangled code (bold-wrapped keys) and flattened all headings to `#`.
- Scan detection: `pdftotext` on 20 pages from the middle of the book (covers and front matter are often images). Below 200 non-space chars per page means OCR. A text PDF that yields under 50 chars per page is retried with OCR.
- Cleanup: image references replaced by figure markers (see below) or removed, formulas become `[formula]`, links into the book's own files are unwrapped. A result under 1000 characters counts as a failure.

## Timings

Measured 2026-10-30 on 5 test books.

| Step | Time |
|---|---|
| EPUB | about 1-3 s |
| 21-page text PDF | 15 s |
| Large text PDF (split into 50-page chunks) | about 11 s per chunk, e.g. 1556 pages in about 6 min. Before the pypdf link fix in docling-proxy (2026-10-30) cutting each chunk took 50-130 s |
| Figure caption, granite-vision-4.1 | about 1.7-2 s per image (147 figures in 219 s, 32 in 66 s) |
| Scanned PDF | about 4-6 s per page (20 pages 85 s, 58 pages 345 s); a 400-page scan is 30-40 min |
| Classification, qwen3.6-35b-a3b, thinking off | about 6 s per book, about 1 h for the library |

`--thinking` was about 10x slower and did **not** improve the outdated call in testing.

## Figures and captions

Pictures matter in technical books (architecture diagrams, dashboards), so `convert` keeps them and a separate `caption` stage describes them.

- Kept: data URIs from docling (`image_export_mode=embedded`) and pandoc `--extract-media` files. The cache holds `<id>.md` with a `<!-- figure:HASH -->` marker per kept picture and the image files in `CACHE/<id>/<hash>.<ext>`.
- Dropped: below 64 px a side or `--figure-min-area` (40000 px area), non-raster (SVG, WebP), and any image repeated 3+ times in a book (icons, logos).
- Each picture is described once per book, at its first position; later repeats of the same image are omitted.
- Model: granite-vision-4.1, temperature 0. The prompt has the book title, about 500 characters before and about 300 after the picture, and asks for 2-4 sentences: kind of image, title and labels, key values and relations.
- granite-vision ignores instructions to skip the "The image is ..." opener and to answer DECORATIVE for covers. The script strips the opener and maps captions starting with "book cover", "cover" or "logo" to DECORATIVE (dropped from the output).
- Output: `> **Figure:** Architecture diagram. It depicts a Kubernetes application setup with an Ingress HTTP Balancer ...` or `> **Figure:** Screenshot of a Grafana dashboard ... titled 'Kubernetes / USE Method / Cluster' ...`. Front matter `figures_described` is the count, `figures_described_by` the model.
- Scale (measured 2026-10-30): about 19k candidate images in EPUBs; `pdfimages` counts about 57k image objects in PDF-only books, which overcounts (masks, tiles, scanned pages). At 1.7-2 s each, captioning is on the order of a day or more. `--caption-skip` skips categories (default "Fiction & Literature"; books with no kept figures are skipped too).
- `caption` needs the book converted (version 2); it does not need classification. `--caption-skip` filters only books already classified.
- `all` order: convert with a caption worker thread alongside (describes figures of already-converted books, polls every 30 s for newly converted ones) -> when convert ends the worker stops after its current figure and unloads granite -> classify -> place (text output first) -> caption (leftovers, including books whose now-known category is skipped) -> place. Each book is re-placed when its captions are done (only once classified), so output gets figures progressively.
- Measured 2026-10-30: with docling converting at the same time 32 captions took 75 s instead of 57 s alone; docling chunk times were unchanged.
- Restart cost: PDFs converted by the older version that contain images are marked once (`figures = -1`), so later starts don't re-scan them with `pdfimages` (the first scan of about 200 PDFs took about 10 min).
- Why a separate stage and not docling's picture description: per-image hash dedupe and resumability, a context-aware prompt (title and surrounding text), the opener and DECORATIVE fixes, skippable categories, and control over which images are sent.

## Classification

The local LLM (llama-swap) answers with JSON-schema constrained output: category (enum), subcategory, topics, title and author (only when the Calibre metadata is junk), year, edition, `depends_on`, status `current|outdated|timeless`, `status_reason`, `status_evidence` (up to 5 concrete signals from the text: versions, APIs, products), `what_changed` (for outdated books: what replaced them and what to use today), summary. Books classified before these two fields existed (2026-11-01) lack them until `classify --reclassify`; their banner shows the rest.

Inputs: Calibre metadata including the owner's `lib:*` tags, up to 80 headings, about 40 lines mentioning versions or APIs sampled across the whole book, and the first 7000 characters.

Lesson: without the version lines and the rule "depends_on first; fast-moving tech older than about 4 years is outdated unless the evidence says otherwise", the model called Kubernetes Best Practices (2019, `extensions/v1beta1`) current. With them it says outdated, with a concrete reason.

Example banner:

```markdown
> **Outdated** (2012, judged 2026-11-01): The practical case studies rely on obsolete versions (Linux 3.2 and Windows 7) ...
>
> - Newer edition in this library: Operating System Concepts - Abraham Silberschatz (2018, 10th edition), Calibre #839
> - Relies on: Linux 3.2 kernel; Windows 7; POSIX Pthreads API
> - Age: published 2012, 14 years before this check
```

### Newer edition in the library

`place` (no LLM) groups classified books by first-author surname and title (lowercased, without edition words, brackets and punctuation; a subtitle only splits a group when two subtitled books disagree) and marks every older edition outdated, whatever the LLM said, with "A newer edition is in this library: ..." as the reason. Newer means a higher edition number, or, when the older book's edition is unknown, at least a 2nd edition published 2+ years later. Years alone don't count: Calibre often has the reprint or ebook date, and two copies of one edition a year apart are common. Fiction is left out. Front matter `newer_edition_in_library` names the newer book. On 2026-11-01 it found 4 (Principles of Banking, Physics for Game Developers, Operating System Concepts, The Art of Scalability); the LLM had called only Operating System Concepts outdated (the other three timeless or current).

Status is the LLM's judgment, limited by its knowledge cutoff. Spot-check `catalog.csv`. Categories are the `CATEGORIES` dict at the top of the script: edit it, run `classify --reclassify`, then `place`.

## Replacements

`replace` researches a replacement for every book whose status (after the newer-edition check) is outdated, with the classify model (qwen3.6 through llama-swap) and the homelab searxng only; nothing goes to a cloud model. Per book:

1. The LLM writes 2-4 queries from the classification (title, author, edition, depends_on, what changed): the latest edition, current books on the subject, one in the book's language if it is not English.
2. searxng returns up to 8 results per query (title, URL, ~300-char snippet); pirate links (`.pdf`/`.epub` files, GitHub blobs, libgen and similar) are dropped.
3. The LLM names up to 4 candidates and writes queries that confirm their edition, year and publisher; a second search round runs.
4. With the numbered results and the owner's books in the same category (id, title, year, status), it returns the replacement or `no_replacement` (docs or a course when no current book exists), `already_in_library`, `why_fit`, alternatives with `why_not`, `verdict_disputed` and confidence. It cites result numbers, which the script turns into URLs, so a source is always a real search result; an `already_in_library` id outside the library is dropped.

Results live in the `replacements` table of `state.db`, keyed by a hash of the book's classification: a reclassify that changes a book redoes it, books that stop being outdated drop out of the document. Queries are spaced by `--search-delay` (8 s). When searxng is unreachable or all its engines report themselves unavailable, the stage stops and leaves the rest pending instead of letting the model answer from memory; rerun later. The homelab searxng has three web engines (brave, google cse, duckduckgo); on 2026-11-01 a run without the delay got all three suspended after ~10 books (too many requests / access denied), and 58 answers made without results were thrown away. A pick without a confirming result is capped at medium confidence. `--retry-failed` retries books whose LLM step failed. `OUT/replacements.md` is rewritten at the end of every `replace` run: overview table per category, then details. The model only sees snippets, so editions and years can be off; confidence says how well the results back the pick. About 15-45 s per book.

## Calibre metadata fixes (2026-11-01)

- Titles and authors in Calibre were audited by subagents (book text first, web search for ISBNs and Russian books) and 309 books fixed with `calibredb set_metadata` (Calibre renames the files itself): junk and file-name titles, publisher prefixes, "Last, First" and "Unknown" authors, Latin transliterations and mojibake of Russian titles back to Cyrillic, a few language fields; then `author_sort` synced. Backup: `WORK/calibre-metadata.before-titles-2026-11-01.db`; old and new values: `WORK/calibre-titles-2026-11-01.json`. Six low-confidence proposals were left alone (747, 783, 881, 883, 1038, 1164).
- Duplicates: 19 copies of the same edition moved to Calibre's trash (emptied 2026-11-01, so only the DB backups below remain); kept EPUB over PDF, then the copy with ISBN or year, then one with a file. Different editions stay (the older one is marked outdated by the edition check). Backup `WORK/calibre-metadata.before-dedupe-2026-11-01.db`, list with reasons `WORK/calibre-dedupe-2026-11-01.tsv`. `place` removed their output files. Then the 4 older editions with a newer one in the library (806, 723, 997, 845; `WORK/calibre-old-editions-2026-11-01.tsv`, backup `calibre-metadata.before-editions-2026-11-01.db`); 682 kept as the only edition with a file. Then the 5 entries without any file (571, 593, 747, 748, 865; `WORK/calibre-fileless-removed-2026-11-01.tsv`, backup `calibre-metadata.before-fileless-2026-11-01.db`) were removed and the missing PDF formats of 581, 613, 907, 908 dropped (their EPUBs stay). The library now has 498 books and `check_library` is clean.
- `OUT/replacements.md`: a first version was researched by cloud subagents with web search; it was replaced the same day by the `replace` stage (local LLM and searxng only).

## GPU rule

docling and llama-swap may run at the same time (granite-vision-4.1 is light; user decision 2026-10-30). One 16 GB GPU, see [gpu-switching.md](gpu-switching.md).

- `convert` (docling) waits only while a ComfyUI container runs. `--no-gpu-wait` overrides. It does not wait for loaded llama-swap models.
- `classify` (qwen3.6) is not mixed with granite: both are in llama-swap's chat group and would swap on every request. So the caption worker stops when convert ends, and classify runs alone.
- `classify` and the caption stage wait for any docling task the script started (for example a timed-out PDF still running), and unload their model when done (`POST /api/models/unload/<model>`, aux models stay).
- A lock file (`WORK/lock`) allows one run at a time.

## Resumability

State is in `WORK/state.db`: per book the source signature (`format:size:mtime`), conversion status, method and `conv_version`, docling task id, kept figure count, classification JSON, caption status and output path. Table `captions` holds one caption per image hash (model, error), so identical images, also across books, are described once.

| Event | Effect on rerun |
|---|---|
| Ctrl-C | Rerun resumes; an in-flight docling task id is reused |
| Source file changed | Reconverted and reclassified |
| Calibre metadata changed | Only re-placed |
| Caption stage interrupted | Rerun skips hashes already in `captions`; `--retry-failed` redoes failed captions |
| Book converted before figures existed (`conv_version` 1; 2 means figures kept) | Reconverted after the never-converted books; a PDF without any images (`pdfimages -list`) only gets a version bump; one with images is marked `figures = -1` once and skipped by the scan on later starts |
| Book deleted in Calibre | Its output file is removed |
| Conversion or classification failed | Skipped until `--retry-failed` |

`status` prints counts and the failures. `OUT` holds only the books, `catalog.csv`, `README.md` and `replacements.md`, so it can be uploaded to Open WebUI as is.

## Work folder history

- 2026-11-04: the work folder (then `~/llm/library-md/.library-md`) was deleted by mistake, together with the old output copy around it. The state was rebuilt from the placed files in `OUT`: front matter became `cls_json` and the conversion fields, `> **Figure:**` lines became figure markers with matching `captions` rows, and the source signature was taken from the current Calibre file. Re-rendering matched 500 of 503 books apart from the date stamps, so `place` does not rewrite them. Lost for good: the source images (books captioned later need `convert` again first), the Calibre metadata backups, and the replacement research (redone with `replace`). The folder then moved to `~/.cache/library-md`.

## Known limits

- OCR typos from easyocr.
- Formulas become `[formula]`; figures are described by a small vision model (can miss details or misread text), and tiny, vector (SVG), WebP and repeated images are dropped.
- Classification quality, especially the outdated call, depends on the model's knowledge cutoff.
- Failed books (image-only, unreadable) are listed under "Not converted" in `OUT/README.md`.
