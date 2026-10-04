# Smart Gazette — project context for Claude Code

## Who I am and how to work with me

Final-year CS student; Smart Gazette is my capstone. Act as a senior technical
mentor: explain the reasoning behind every change, use analogies, go in small
steps. **Always name the exact file and method for every edit.** Measure before
changing code. One change → one test → one commit.

The repo lives in a OneDrive-synced folder. If a git command fails with an
`index.lock` error, stop and tell me — I'll pause OneDrive.

## What the system does

Java 17 / Spring Boot 3.1.5 app that turns Kenya Gazette PDFs into structured
records and readable articles. PostgreSQL (`ddl-auto=update`), Thymeleaf,
Groq (`openai/gpt-oss-20b`, `openai/gpt-oss-120b`) and Gemini via REST.

Pipeline:

```
PDF → text extraction → cleaning → notice segmentation → keyword pre-filter
    → category → template extraction (or AI fallback) → article generation → DB
```

## Current task: make the extractor and classifier as good as possible

The corpus lives **outside the repo**, one folder per year:

```
C:\Users\User\OneDrive\Documents\Projects claude\Smart Gazette\Testing\Material\Atricle raw material\M5\
    2022\  2023\  2024\  2025\  2026\
```

Rules for the corpus:

- Read-only. Never move, rename or edit these PDFs.
- The path contains spaces, and "Atricle" is spelled that way on purpose.
  Always quote the path.
- Write all generated output inside the repo, in `raw/`, `cleaned/` and
  `reports/`. Those folders are gitignored.
- Use the year folders. Notice numbers restart each year (lesson 14), so check
  ascending order within each year, and report results per year as well as in
  total.

Use the corpus to strengthen the
extraction layer and the keyword classifier, which everything downstream
depends on. **Measure first; do not change code until I've seen the numbers.**

### Tools already built (in `tools/`)

| File | Purpose |
|---|---|
| `inspect.js` | Runs pdf-inspector `extractText` on a PDF (Node) |
| `gazette_clean.py` | Header stripping, fragment joining, notice-boundary lock |
| `probate_template.py` | Rule-based probate extraction |
| `land_template.py` | Rule-based land/title extraction |
| `corrigenda_template.py` | Rule-based corrigenda extraction |
| `names.py` | Shared proper-noun cleaning (aliases, capacities, ID numbers) |
| `generate.py` | Template article generation, grouped land digests, county normalisation |
| `pipeline.py` | End to end: classify document → clean → segment → route → extract → generate |
| `category_census.py` | Coverage per schema category |
| `keyword_probability.py` | Scores each pre-filter keyword against ground truth |
| `audit.py` | 6 check classes; run after every template or cleaner change (checks 3–5 currently skip — lesson 32) |
| `proper_nouns.py` | **Placeholder** pass-through; the original was never committed. Name-quality numbers are provisional |
| `ocr_check.py` | Tesseract witness: OCR per page (cached), three-way notice check, page agreement, scan detection → `reports/ocr/<year>/` |
| `election_probe.py` | Election notices for a year: pipeline + scan OCR text, categorisation, clusters → `reports/corpus/<year>/election_notices.json` |
| `corpus_report.py` | Year report: categories, Java pre-filter port, template failures, schema fill, keywords, Misc clusters, structure, tables (incl. multi-page), visibility, samples → `reports/corpus/<year>/` |

Also (measurement): `extractor_eval.py` (Tesseract vs pipeline vs pdftotext),
`inspect_positions.js` + `positions_eval.py` (prototype positions extractor),
`vocab_repair.py` (prototype word repair), `residual_failures.py`,
`misc_review.py`.

Cross-year results live in `reports/FINDINGS.md`; the extractor / table /
category analysis and the fix plan in `reports/EXTRACTOR_REPORT.md` — read both
before starting corpus work.

Run order for one PDF (fix 1, Oct 2026 — positions-based extraction, lesson 35):

```
node tools/inspect_positions.js "<gazette.pdf>" > raw/rawNNN.txt
python tools/gazette_clean.py raw/rawNNN.txt cleaned/cleanedNNN.txt
python tools/pipeline.py raw/rawNNN.txt
```

`tools/inspect.js` (`extractText`) is kept as the legacy extractor for
comparison. Java: point `extraction.inspector.script` in
`application.properties` at `tools/inspect_positions.js`.

Note: PowerShell's `>` writes UTF-16 (with a BOM). `pipeline.py`,
`keyword_probability.py` and `audit.py` try UTF-16 first, which silently
garbles UTF-8 input — feed them UTF-16 files, or use `corpus_report.py`, which
decodes UTF-16 only when a BOM is present (lesson 32).

### Java side

- `PdfInspectorService` — calls `inspect.js` via `ProcessBuilder`
- `GazetteTextCleaner` — Java port of `gazette_clean.py`, including the boundary lock
- `GeographyService` — canonical county normalisation from `reference/geography.json`
- Flag: `extraction.engine=legacy|inspector` in `application.properties`
- `keywordPreFilter` in `GazetteService` — must stay in sync with `keyword_probability.py`

## Tesseract: independent OCR witness (measurement only)

pdf-inspector and `pdftotext` both read the PDF's embedded **text layer**, so
they can share the same mistakes. Tesseract reads the **rendered page image**,
so it's an independent check. Tesseract is an OCR engine, not an AI model. It
runs locally, needs no API and has no quota. It makes its own errors
(`l`/`1`, `O`/`0`, small caps), so **a disagreement is a flag to investigate,
not a verdict**.

Setup: Tesseract on PATH (UB Mannheim Windows build), `pdftoppm` from poppler,
and `pip install pytesseract`. Verify all three first. Then time one gazette
and tell me the seconds per page before OCR-ing anything large.

Uses:

1. **Three-way notice check.** Compare notice numbers across pdf-inspector,
   `pdftotext` and Tesseract. Two-against-one tells us which extractor is
   wrong.
2. **Field check.** For every templated `parcel_id`, cause number and notice
   number, confirm it appears in the Tesseract text of the same page. Use
   fuzzy matching and tolerate OCR character confusions. A value that isn't
   on the page is a likely extraction bug.
3. **Broken text layer.** Pages where the text layer and Tesseract disagree
   heavily (garbled glyph mapping, missing text, text hidden under images).
4. **Scanned / alternate-layout gazettes.** Tesseract is the only way to read
   these. Report how usable its output is (lesson 15).
5. **Table inventory.** Use `tesseract ... tsv` word bounding boxes to find
   table pages and estimate their columns, header rows and row counts. This is
   to understand what's there before we design a table lane. Don't build the
   lane yet.

OCR a sample: every scanned or alternate-layout gazette, every page flagged as
a table, and about 10 born-digital gazettes of different issue types. OCR the
whole corpus only if the per-page time makes that reasonable.

You may write new measurement scripts in `tools/` (e.g. `tools/ocr_check.py`,
`tools/table_inventory.py`). Don't change the pipeline, the templates, the
cleaner or any Java code until I've seen the numbers.

## Categories

Current (each has a schema in `src/main/resources/schemas/field/<name>.json`):

```
Appointments, Legislation, Tenders, Land_Property, Court_Legal,
Public_Service_HR, Licensing, Company_Registrations, Miscellaneous,
Corrigenda, Change_of_Name, County_Government
```

`getSchemaForCategory` builds the path as `category.toLowerCase() + ".json"`, so a
new category only needs its schema file and pre-filter entry.

**You may propose new categories.** If the corpus shows a notice type that is
common, structurally distinct, and currently landing in Miscellaneous or being
misfiled, propose it with: its share of notices, 5+ example notice numbers,
distinguishing phrases, and a draft schema. Don't create it until I agree.
Candidates already seen: **Elections** (IEBC + political parties + petitions;
schema drafted in `reports/FINDINGS.md`), water tariffs, Admission of
Advocates lists, political party notices, EIA / environmental notices,
mining licences, county assembly sittings, disposal of uncollected goods,
unclaimed financial assets, Kenya Revenue Authority overstayed goods.

## Current measured state (Oct 2026: 2022–2026, born-digital, after fixes 1–4)

Measured with `tools/fix_eval.py`:

- Notice detection: 0–1 missed and 0–1 extra per year vs the pdftotext/Tesseract truth
- Probate: **81–88% of notices, 93–97% of cause blocks** templated
- Land: **92–94%** of notices templated
- Corrigenda: 0–25% (the rest are NLC schedule corrections → table lane, fix 7)
- Words intact 98%, numbers intact 96–98%; `petitioner_relationship` filled 70–78%;
  `advocate_firm` filled 99.8% of records that name an advocate (only ~4% do)
- Java pre-filter: 85–89% coverage at 99.4–100% precision
- Scans: still invisible to the pipeline (scan lane = fix 5, RapidOCR)
- Earlier baseline (20 gazettes, before this work): land 90.6%, probate 88.1%, corrigenda 86.2% — measured on a hand-picked sample with the old extractor; not comparable

How we got here: `docs/JOURNAL.md`. Plan and status: `reports/PLAN.md`.

## Lessons already learned — check the corpus for all of these

1. **pdf-inspector markdown mode is unusable** — its table heuristic reads the
   two-column layout as a table. Only `extractText` works.
2. **Two-column interleaving** is what PDFBox produced; pdf-inspector fixes it.
3. **Fragmentation**: small caps split per letter (`G / AZETTE`), words cut
   mid-token (`Nairob` + `i`). Markers must be canonicalised BEFORE joining.
4. **Running headers mid-notice**: `THE KENYA GAZETTE`, datelines, page numbers,
   timestamps (`2:40 PM`), doubled page numbers (`4114 4 14`).
5. **Years eaten as page numbers** — a bare-number regex deleted "2025". Page
   stripping must be context-aware.
6. **Cross-references look like headers.** "Gazette Notice No. 719 of 2026 is
   revoked" inside notice 1341. Fixed with: case-sensitive header matching (real
   headers are small caps → uppercase) + a longest-increasing-subsequence pass.
   A single-pass "must exceed previous" rule discarded ~900 notices when an issue
   opened with a CORRIGENDA section.
7. **Split notice numbers**: `NO. 1900` + `3` = 19003.
8. **Source typos**: `GAZETE` (one T) in the published PDF.
9. **Masthead + first notice on the same page**: discarding the first segment
   loses the first notice (13497 went missing this way).
10. **Contents index** with dot leaders at the front of main issues.
11. **Corrigenda must not be split on `CAUSE NO.`** — they quote cause numbers
    inside correction text (63 fragments from 8 real corrigenda).
12. **Keyword traps**: `successioncause` (0% precision) and
    `lettersofadministration` (47.6%) steal land notices, because land title
    notices cite the succession that transferred the title. Generic single words
    (`nomination`, `appoints`, `tender`, `permit`, `licence`) steal election and
    legislation notices. Multi-word phrases from an Act's formula are safe.
13. **Glued words** (`THE LANDREGISTRATION ACT`) — match on whitespace-squashed text.
14. **Notice numbers restart each calendar year** (one volume = one year) and
    increment across every issue in that year.
15. **Alternate layout**: scanned probate compilations (e.g. Vol. CXXVII No. 186,
    producer "PFUPDF Engine", 1 font) have no `GAZETTE NOTICE NO.` headers —
    notices are introduced by bare margin numbers. Detected, not yet handled.
16. **Tables** (exchequer statements, unclaimed assets, political party lists)
    are flagged but have no dedicated lane.
17. `notice_number` must be stored as bare digits.

Lessons 18–38 come from the 2022–2026 corpus runs. Evidence, counts and
example notices are in `reports/FINDINGS.md`.

18. **Scanned library copies with a hidden OCR layer** (National Council for
    Law Reporting stamps; OmniPage, PaperStream, Samsung scanners). pdf-inspector
    ignores ToUnicode for non-embedded CID fonts: its text is shifted by 29 code
    points and loses every digit and space, so the pipeline sees 0 notices.
    pdftotext reads the same layer (well for OmniPage, e.g. 2024 No 77).
    Invisible notices: 2,133 in 2022 (12 scans, over half the year), 254 in
    2023, 1,078 in 2024, 0 in 2026. Detect scans by image size relative to the
    page (`pdfimages -list`): 150 dpi scans exist (2024 No 95).
19. **Letter-spaced, mixed-case headers on scan layers** (`G a z et te N o t i
    ce No.`, 2024 No 95, 203 headers) are rejected by the uppercase header rule.
20. **One character per line** from pdf-inspector (2024 No 148, 312k lines, no
    line ending in `. ; :`): `clean()` Stage 2 never flushes its buffer and
    re-splits it every line, which is quadratic (hours). Guard before cleaning.
21. **Header not followed by a line break + capital** — title on the same line
    (2026 No 90, `GAZETTE NOTICE NO. 7653 THE PUBLIC HOLIDAYS ACT`) or a NUL
    `\x00` after the number (2023 No 26, 1150). The `@@HDR@@` lookahead fails
    and the notice is lost or merged.
22. **Dateline rule eats content** — `P_DATE` deletes any date-shaped line,
    including dates of death and `Dated the …` (~484 in 2023), then treats the
    next bare numbers as page numbers, deleting split years
    (`CAUSE NO. E640 OF By`).
23. **Split numbers**: years `20 21` / `202 2`, ordinals `7 thJanuary`. Date
    regexes needing `\d{4}` fail. Largest named cause of probate failures.
24. **Drop-cap fragments attach to the wrong word**: `a dministration`,
    `a dvocates`, `application shaving`, `ORDERSS PECIALS ITTINGO FTHE`.
25. **Curly punctuation is never normalised** (`’ “ ” –`). Stage 0 only fixes
    the PowerShell mojibake. `RE_RELATION` and corrigenda `RE_AMEND` expect
    ASCII: `petitioner_relationship` fills ~1%, corrigenda extract 0%.
26. **Templates need the same whitespace tolerance as the categoriser**:
    `knownas`, `titleNo.`, `known a s`, `sit uate`, `REGISTRAT ION` break
    `RE_LR` / `RE_LR2` / `RE_ACT` (212 land notices in 2024).
27. **The CORRIGENDA section has no notice number**, so its `CAUSE NO. … amend
    the … printed as` lines glue onto the previous probate notice and fail as
    fake cause blocks.
28. **Categoriser order and generic words misfile**: land transmission notices
    citing a cause go to Court_Legal; water tariffs split Tenders/Licensing;
    a Companies Act incorporation schedule became Tenders. County_Government
    has a schema but no pre-filter key or signal.
29. **Notice numbering can jump inside an issue** (2026 No 137: 12299 → 13000 …
    13012, continued by the next issue). Never treat a large jump as
    impossible. Uppercase cross-references exist when "OF 2016" falls onto the
    next line (2026 No 22) — the LIS lock catches them.
30. **Order**: inside an issue pdf-inspector's order is reliable (the lock lost
    0 real notices in three years). Folders list alphabetically (No 1, No 10,
    No 105, No 11…): sort by integer issue number before any cross-issue logic.
    `gazetteNumber` / `noticeNumber` are `String` in `Gazette.java`, so
    `ORDER BY gazetteNumber` is lexicographic.
31. **Tables inside notices and across pages** (2023 No 26 notice 1305: tariff
    tables + an annex table spanning p29–31 with its caption repeated on each
    page). 31–55 multi-page tables a year; column counts change between pages;
    the next notice often starts mid-page under the table. Segmentation keeps
    rows with their notice, but cell content is scrambled (16–59% of rows
    unrecognisable). On two-column pages the owning header can be in the other
    column: assign table ownership by content, not by the nearest header above.
    The pipeline's table flag catches ~5% of table pages.
32. **Tool traps**: decoding UTF-16 first silently garbles UTF-8 files (0
    notices reported); `audit.py` checks 3–5 skip silently (paths
    `/mnt/user-data`, `tools/fresh`, `tools/schemas` don't exist) so its
    `ERRORS: 0` is hollow; `tools/proper_nouns.py` is a stub; save extractor
    output with `newline=''` on Windows.
33. **Duplicate source files**: 2022 has `No 38.pdf` and `No 38 (1).pdf`,
    byte-identical (browser re-download). The scraper de-duplicates by
    (gazette number, date), but manual uploads rely on header extraction, which
    returns empty volume/number/date — so a re-upload would store every notice
    twice. De-duplicate by file hash or by (volume, issue) read from the masthead.
    Related: whole notices can be one giant table (2022 No 43: an 82-page Land
    Registration conversion list, notice 2528).
34. **Election notices have no home** (2022 election year; 2026 pre-election).
    IEBC notices (returning officers, tallying centres, voter registration,
    petitions) are filed as Legislation via `electionsact`; party notices go
    to Miscellaneous; identical IEBC "delete and insert" corrections split
    between Corrigenda and Legislation. `keyword_probability.py`'s TRUTH lists
    THE ELECTIONS ACT as Legislation, so the ground truth itself is wrong for
    them. The declaration of elected persons (2022 No 169, 9949–9951) is in a
    scan. Traps: the IEBC's full name and "Registrar of Political Parties"
    also appear in National Treasury exchequer tables (73% / 66% precision).
    Safe keys and a draft `Elections` schema are in `reports/FINDINGS.md`.
35. **The fragmentation comes from `extractText`, not the PDFs.**
    pdf-inspector's `extractTextWithPositions` returns whole lines with correct
    words plus x/y/font. Rebuilding pages from it (`tools/inspect_positions.js`:
    baseline grouping, join by real gap, left column then right, full-width
    lines as band breaks) lifts probate from 47–59% to 70–81% of notices
    (blocks 90–93%) and numbers intact from 88–92% to 96–97% with no template
    changes. Known issue: ordering around full-width lines loses 3–7 notices a
    year and cuts wide tables. `classifyPdf` detects plain scans for free but
    calls hidden-OCR-layer scans TextBased. Details: `reports/EXTRACTOR_REPORT.md`.
36. **pdftotext: perfect words, unreliable order.** Best extractor in 2022–23,
    but in 2024–26 its reading order breaks (probate 22–24%, 700–900 category
    changes a year). Use it as a per-gazette dictionary (`tools/vocab_repair.py`
    re-cuts `a dministration`, `20 21`, `knownas`, `ORDERSS PECIALS`), for
    OmniPage scan layers, and `-layout` as the born-digital table reference —
    never as the main text.
37. **Tesseract**: 99.6% of words, 94–98% of numbers vs the text layer; finds
    every notice in every scan when split on its own headers (the pdf-inspector
    cleaner merges OCR notices); templates 39–54% on scans. Best table-row
    source (48–79% rows complete vs 28–48% today) except exchequer tables
    (OCR digit errors). 2.5–3.9 s/page — the scan lane, not the main extractor.
38. **OCR is a witness, not a replacement, for born-digital text.** Field check
    (`tools/field_check.py`): 99.5–100% of the fix-1 values are printed on their
    page per Tesseract; the few flags include real template bugs (parcel IDs
    truncated at a bracket or over-captured into the location). PaddleOCR's
    native CPU build crashes on this Windows machine (oneDNN); RapidOCR (same
    PP-OCR models via ONNX) reads numbers far better than Tesseract (96–99% vs
    75–82% on tables) at ~3x the time — the scan-lane choice. Our own pages are
    free, exactly labelled OCR training data (image + positions boxes + text).
39. **A category nobody routes to is invisible.** County_Government had a
    schema but no rule: 558 county-headed notices (2022–26) went to
    Appointments/Misc or cost an AI triage call. Fix 6: case-sensitive heading
    rule (`category_census.COUNTY_HEAD` = Java `CategoryRules`), checked before
    body keywords; planning notices stay Land. AI triage 2,821 → 2,232.
    Step 2 added Elections, Uncollected_Goods, Environment, Utility_Tariffs
    the same way (`category_census.HEADING_CATEGORIES` = Java
    `CategoryRules.headingCategory`, identical on 21,250): AI triage → 1,574,
    Misc 956 → 322. Corrigenda still wins (an election correction is Corrigenda).
    Step 3 dropped 0%-precise keys (Java `retirement`; Python bare PROMOTION /
    RETIREMENT and `amend the`) and moved `electionsact` to Elections.
    Public_Service_HR now gets 0 notices: none exist in 2022–26.
41. **A word inside a longer name is not that word.** "Retirement Benefits
    Authority", "Export Promotion Agency" fired HR keys; "amend the" fired in
    draft-regulation bodies. Read every notice a key fires on before keeping it.
42. **Scan lane (fix 5).** Scan = >= half the pages carry a page-sized image
    (pdfimages, printed size). Tesseract -> `scan_lane.fix_headers` + `ocr_fix`
    (= Java `OcrTextFixer`, identical on 1,272 pages) -> normal cleaner. 97.9%
    of 3,796 scan notices found (was ~0), probate blocks 86.6%, land 79.5%.
    The OCR word corrector changed real names until narrowed (seen-in-corpus
    words are real; capitalised -> only very common targets): judge a
    corrector by reading its corrections. Notices carry `extraction_source`.
43. **Never score a reader with its own output.** "97.9% found" counted
    Tesseract's own headers; consecutive notice numbering (slots first..last
    number of an issue) is the independent referee: 94.2%.
44. **Look at how a component fails before adding another.** RapidOCR on gap
    pages reached 97.5%, but Tesseract had read the "missing" headers garbled
    ("GAZETTR NOTICE NO", "NOTICENO"): fuzzy headers (edit distance <= 3, no
    digits before the number) give 96.5% with one engine. Decision: Tesseract
    only; RapidOCR tools are measurement aids. Land parcel ids: comma before
    the next clause optional (land on scans 76.9% -> 85.0%).
45. **Check the referee too.** pdftotext -layout misaligns some tables by a
    row (2024 No 89 p28); the page image (Tesseract word boxes) is the table
    referee now, and it has its own quirk (joins side-by-side tables).
    `tools/table_eval.py --ref tesseract`.
46. **Table lane (fix 7).** `inspect_positions.js` writes " | " between cells
    (>= 2 wide gaps, or aligned runs of >= 3; justified prose excluded;
    columns learnt per page column); the cleaner keeps rows on their own line
    and drops a header repeated as the first row of a page. Rows kept with
    cells 0.3-12% -> 40-82%; 0 category changes. A library function's name is
    a hypothesis: detectVectorGridInRegion invents grids on prose pages.
47. **Tables as data (fix 8).** `table_extract.tables()` = Java
    `TableExtractor` (identical on 21,250): runs of cell rows, wrapped cells
    joined, continued pieces merged. Headers from the document's own signals:
    typeface (bold = separate font; extractor writes "--- | ---"), then a
    learnt header vocabulary (`table_header_words.txt`). Never "digit-free
    first row = header". "Tables (N)" tab on the notice page.
48. **Table rows -> records (fix 9).** `table_records` maps header words to
    roles (parcel_id, owner, area_ha, old_lr/new_parcel, name, position),
    repairs print defects, validates (parcel_ok); `land_table_template` reads
    acquisition schedules and their Deletion/Corrigenda/Addendum corrigenda
    (corrigenda 5.3% -> 28.0%, 2,148 parcel records). Refused rows are
    counted (rows_not_read), never guessed.
49. **Templates are in Java now** (`service.templates`, = tools/ templates,
    identical on 16,860 notices). Regex port needs UNICODE_CHARACTER_CLASS +
    UNIX_LINES, underscore-free group names, exact strip/title. GazetteService
    uses a template result instead of the AI extraction call (single, whole
    multi-cause notice mapped by index, batch fields replaced). JUnit
    `NoticeTemplatesTest` (CI-safe) found two real gaps: "probate of written
    will to the estate of" (285 blocks) and corrigenda amends_notice.
    Any template change: Python + Java + parity + test.
    Score routing with `tools/category_eval.py` (heading = referee).
40. **Cited laws are reference data, resolved by key.** 24% of county notices
    cite the Constitution (Art. 179, 184, 183, 235 first; Art. 88 IEBC overall).
    `reference/constitution.json` + `county_governments_act.json` (built from
    Kenya Law by `tools/build_law_reference.py`); `tools/law_refs.py` = Java
    `LawReferenceService` (identical on 21,250 notices) feeds the "Laws Cited"
    tab. Add laws in order of `python tools/law_refs.py` "add next".
50. **A new issue is the honest test** (`tools/issue_report.py`, No 166/175).
51. **Change-of-name template** (`change_of_name_template` = Java
    `ChangeOfNameTemplate`, identical on 1,550): 98.0% of deed polls, built
    spec-first (`docs/specs/`, `docs/PROCESS.md`). The two printed former
    names are an internal witness: disagree -> refuse. Held-out No 166 still
    found wordings the 5-year corpus lacked ("re-adopted", "in lieu thereof
    adopted", "Folio."): always run the held-out check before calling a
    template done. AI extraction calls left: No 166 112 -> 60, No 175 89 -> 63.
52. **Appointments template** (`appointments_template` = Java
    `AppointmentsTemplate`, identical on 641): one record per person, 77.5%
    of notices / 866 people. One doubtful name refuses the whole list; an
    unlabelled "Chairperson and Members" is not guessed. A review error became
    a rule (name words "to/be/as/is") that caught 4 more. Held-out No 175 had a
    new verb ("makes the following appointment"). AI extraction calls left:
    No 166 -> 45, No 175 -> 52. Printed dates no longer make published_date "today".
53. **Ruled tables (fix 7b)** from the printed ruling lines: `tools/pdf_rules.js`
    (poppler `pdftocairo -svg`; detectVectorGridInRegion is text-derived and
    unusable) -> lattices -> rows/cells in `inspect_positions.js` (shared by
    Python and Java). 1,890 pages (26%): rows at table width 96.8k -> 110.0k,
    templates +39/-0, held-out No 166 54% -> 98%. Checks: `ruled_eval.py`
    (pages without a lattice byte-identical), `ruled_chars.py` (characters of
    changed pages identical - 0 of 1,890 differ), `ruled_downstream.py`,
    `ruled_test.js` (in audit). Empty cell = "a | | c"; label row = "text |"
    (cleaner + TableExtractor accept it; TableRecords skips 1-cell labels).
    poppler is now needed by every extraction; Java timeout 300 s.
54. **Scan tables** (`tools/scan_tables.py` = Java `ScanTableReader`, wired into
    `ScanLaneService`, which now asks Tesseract for tsv): ruling lines from the
    page image + word boxes -> rows/cells; unruled tables from aligned word
    groups (cell-sized only); no " | " outside tables ("| shall" -> "I shall",
    "| 2057" -> "|2057"). Letters/digits identical on 1,167 pages, fake cells
    4,169 -> 0, notices 3,842 -> 3,846. Where rules were lost: one row per
    printed line, never a guessed join. Scorecard `scan_tables_eval.py`;
    `scan_lane.py --engine tables`. Parity: page text 1,167/1,167, rules 40/40.
55. **Pages without a text layer** (`tools/mixed_pages.py` = Java
    `ScanLaneService.fillTextlessPages`, called by GazetteService): the
    extractor keeps every PDF page (classifyPdf page count; trailing textless
    pages were dropped - 2025 No 163 was 8 of 115 pages); a page with < 5
    words is OCR'd via the scan-lane page path and spliced in; source
    `text-layer+ocr`. In the corpus they are 199 maps + 2 blank pages (map
    titles only). `mixed_pages_check.py` proves an image page of text is read.
    Extractor stage complete.
56. **Figures** (`tools/figures.py` = Java `FigureClassifier` + `FigureService`,
    identical on 790): poppler SVG gives every image placement; the extractor
    writes `[[FIGURE:p.k]]` at its place (in its table cell), so the marker
    travels into its notice. Every image kept (a stamp can authenticate) and
    classified (map 205, table_symbol 202, chart 13, table_image 5, ...);
    cleaned copy for faded scans, original always kept. `NoticeFigure` +
    "Figures (N)" tab + images in place in the original text. Each review
    error became a rule (KAA "Concession" fees made a chart a map). Templates
    +5/-0 (a ruled header below a label row is now marked).
57. **A library's promise is a hypothesis; find the first number that
    differs.** Figure text: PP-OCRv6 (RapidOCR's models) in Java,
    `service.PpOcr` on ONNX Runtime (map titles 45 -> 124 of 205 vs
    Tesseract). The RapidOCR Java wrapper dropped every space; ONNX Runtime
    1.21+ will not start on JDK 17.0.10's VC++ runtime (pinned 1.20.0); its
    Java metadata read breaks 4-byte characters, so the model's character
    list (540 entries short, the space last) is a UTF-8 file
    (`tools/fetch_ocr_models.py`) checked against the model's output size.
    Models in `models/ppocr` (gitignored). The app must point
    `extraction.inspector.script` at `tools/inspect_positions.js`.
58. **Law library** (`tools/law_pdf.py` + `build_law_reference.py pdfs raw/law`;
    `law_refs.laws_for` = Java `LawReferenceService.lawsFor`, parity 21,250):
    42 laws as reference data + catalog `reference/laws.json` (names = data).
    7.6% of notices cite a section, ~46% name an Act only in the heading, so
    the kind of notice decides the section (`reference/implied.json`, each rule
    checked against the section's words; LRA s.33(3)/(5), s.31(1): 8,175 of
    8,636). Law links 1,682 -> 11,297 notices. Articles: fixed sentence quoting
    stored text + AI quotes checked word for word. Kenya Law forbids bulk
    downloads: the user saves the PDFs. A Java string `"\s"` (one backslash)
    instead of `"\\s"` compiles silently to a space - audit check 7.

## What I want from the corpus run

Report, without changing code:

1. **Per-gazette table**: producer / born-digital or scanned, page count, notice
   count, ascending-lock result (kept / demoted), category distribution,
   template coverage.
2. **Independent check**: compare notice counts against `pdftotext` for every
   born-digital gazette. List every mismatch and diagnose it.
3. **Failure log**: every notice the templates rejected, with category and the
   field that failed. Group by cause, largest first.
4. **Keyword report**: re-run `keyword_probability.py` on the full corpus. Flag
   any keyword whose precision dropped below 95%, and propose new keys that score
   ≥95% for categories with weak coverage.
5. **Unknown notice types**: cluster the Miscellaneous notices by their heading
   (Act + subject line) and list the clusters by size.
6. **New scenarios**: anything in the layout or text that none of the 17 lessons
   above covers — new header forms, new running-header shapes, new fragmentation,
   new cross-reference patterns, new layouts. For each: example notice number,
   gazette, and what it breaks.
7. **Samples**: 3 before/after `content` examples per issue type (main, special,
   county, scanned) so I can eyeball quality.
8. **Tesseract cross-check**: results of the three-way notice check and the
   field check (see the Tesseract section). List every value that isn't on its
   page, grouped by cause. Include broken-text-layer pages and a usability
   verdict for the scanned gazettes.
9. **Table inventory**: which gazettes contain tables, which pages, what kind
   (exchequer, unclaimed assets, party lists, other), estimated columns and
   rows, and how the current cleaner mangles each kind.

Then recommend the next three changes, ranked by how many notices each fixes.

## Rules for any change you later make

- Run `python tools/audit.py` before and after; it must end `ERRORS: 0`.
- Re-run the pipeline over the whole corpus and show before/after numbers —
  a fix that helps one gazette and breaks another is not a fix.
- Keep the Python tools and the Java ports in sync; say which Java file needs
  the matching change.
- Never store an unusable key: a template returns `None` rather than a fragment.
- Commit only after a verified improvement, with the numbers in the message.

## Open items (not the current task, but don't break them)

- `parseSafeJson` "Unterminated string" — truncated AI output
- Header extraction returns empty volume / number / date
- Significance scores inflated → IFTTT tweets firing on routine notices
- Checkpointing missing in `processTextSegment` and `processBatchedGroup`
- Planned: alternate-layout handler,
  Tess4J OCR lane, table lane, frontend branches for new categories
- Possible later: TypeSafe Jev for triage and significance scoring (typed
  decisions only — it cannot extract text)
