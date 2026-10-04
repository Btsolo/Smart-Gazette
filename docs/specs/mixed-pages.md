# Spec: page-level OCR for pages without a text layer

Status: **implemented** (4 Oct 2026, full page-level OCR chosen by the project owner; results in section 6) · Layer 2/3 (extraction / scan lane) · Roadmap step 4

## 1. Requirement

A born-digital issue can contain pages that are only an image (no text
layer). Read every such page with the scan lane's OCR and insert its text at
its place, so nothing printed is invisible. Mark the gazette as read partly
from images.

## 2. Analysis (264 born-digital / mixed issues 2022–2026, 7,518 pages, + held-out No 166 / 175)

| finding | result |
|---|---|
| pages whose extracted text has < 5 words | **201**, in 4 issues |
| what they are | **199 scanned maps** (registry index maps of land-registration conversion notices: 2022 No 43 ×46, 2025 No 163 ×107, 2025 No 190 ×46), **2 blank** end pages (2023 No 99) |
| Tesseract on them | 0 words on all 201 (maps at this contrast give nothing) |
| 2025 No 73 (labelled "mixed") | its image pages are maps too (petroleum blocks); their titles are already in the text layer |
| held-out No 166 / 175 | no such page |
| extractor bug found | trailing pages without text are **dropped**: 2025 No 163 comes out as 8 pages of 115, so page numbers after a map section are lost |

So on today's corpus the OCR recovers **no notice text**. The point is
coverage of future issues that contain a scanned text page; the project
owner chose the full OCR over a quality-gated one, knowing this.

## 3. Design

1. **Every PDF page is kept** (`inspect_positions.js`). The page count comes
   from pdf-inspector's `classifyPdf`; pages without text are empty
   placeholders, so page numbers line up.
2. **Textless page = fewer than 5 words** of ≥ 3 letters in its extracted
   text (the same test as the analysis).
3. **Each textless page is OCR'd** through the scan lane's own per-page path:
   - Tesseract word boxes and the page image;
   - `ScanTableReader` (tables, no fake cells);
   - `OcrTextFixer` (header and word repair).

   Its text replaces the empty page.
4. **Marking.** If any page was OCR'd, the gazette's extraction source is
   `text-layer+ocr`, so every notice records that part of the issue was read
   from images.
5. The rest of the pipeline (cleaner, segmentation, templates) is unchanged.

Python: `tools/mixed_pages.py` (lab, uses the OCR cache). Java:
`ScanLaneService.fillTextlessPages`, called by `GazetteService` after the
pdf-inspector extraction.

## 4. Rules and fail-safe

- Only pages with no usable text are touched; every other page is
  byte-identical.
- OCR text gets the same repair as the scan lane, so it cannot fake a cell
  marker. A header it fakes would have to pass the ascending lock.
- If OCR fails, the page stays empty, as today.

## 5. Acceptance criteria

1. Born-digital issues without textless pages: byte-identical extraction.
2. The 4 issues: every PDF page present; textless pages OCR'd; notices,
   categories and templates unchanged (no notice text exists on them).
3. A synthetic check: an image page with printed text is read and inserted at
   its place (JUnit + `audit.py`).
4. Python = Java on the page selection and splicing; `audit.py` ERRORS: 0.
5. Time: OCR only for textless pages (about 3 s each at 300 dpi).

## 6. Results (4 Oct 2026)

| criterion | result |
|---|---|
| 1. other issues | byte-identical (pages are only added where a PDF had trailing pages without text) |
| 2. the textless pages | every PDF page present (2025 No 163: 115 of 115, was 8). All 201 pages OCR'd: 12 gave text, 233 words in all, mostly **map titles** ("NAIROBI BLOCK 194 (RUNDA EVERGREEN) Sheet 1 of 3") with some OCR noise. Notices, categories and templates unchanged |
| 3. image page with printed text | `tools/mixed_pages_check.py`: a PDF page that is only an image of a notice comes out as a readable notice (header, name, parcel number). Run by `audit.py` |
| 4. tests | `audit.py` ERRORS: 0 (2 page-OCR cases); JUnit `ScanLanePagesTest` (splice, failed OCR); 26 tests pass |
| 5. time | lab ~2-4 s per textless page; Java OCRs the pages in parallel (scan-lane workers) |
