# Spec: figures — capture every image, classify it, show the meaningful ones

Status: **implemented** (Stage A + B, 4 Oct 2026) · was approved for implementation (4 Oct 2026: every image captured then classified, nothing ignored — stamps can authenticate; local cleanup with the original kept) · Phase 4c

## 1. Requirement

Capture every image a gazette prints, find which notice it belongs to,
classify what it is, make faded / brown-paper scans clearer without
inventing detail, and show the meaningful ones with their article.
Nothing is discarded. Stamps, seals and logos are kept and labelled,
because they can carry authentication.

## 2. Analysis (2022–2026)

Embedded images (`pdfimages -list`) and placements (`pdftocairo -svg`):

| kind (by size / place / context) | where | count |
|---|---|---|
| page scans | 21 scanned issues | ~1,190 pages |
| registry index / block maps (brown paper, 0.2–1.1 megapixels) | land-registration conversion notices (2022 No 43, 2025 No 163, No 190) | 199 pages |
| location maps beside coordinate tables | coal / petroleum concessions (2025 No 73, 2026 No 75) | a few |
| charts, infographics | KRA and other annual reports (2023 No 154, 2025 No 43, No 83) | ~15 |
| prescribed images | Tobacco Control Act health warnings (2025 No 31) | 10 |
| party symbols in table cells | candidate / party lists (2025 No 222 and others) | ~250 |
| coat of arms | every cover | 1 per issue |
| logos, library stamps, small marks | notices, covers | ~300 |

The SVG of a page defines each image once (`<image id=… width height>`) and
places it with `<use href=#id transform=matrix(…)>`. So every placement has
an exact position on the page. Masks are placed with a
`color-to-alpha` filter and are skipped.

Local cleanup (tested on 2025 No 163 / No 190):
- dividing by a blurred copy of the page removes the brown tint, stains and
  uneven light;
- a contrast stretch and an unsharp mask make the lines and labels clear;
- small scans are upscaled 2×.

It never adds detail. A 417-pixel map stays limited by its pixels.
AI upscaling was declined: it can invent digits on legal maps.

## 3. Design

**Stage A — capture and classify** (extractor + lab, then Java):

1. **Placements.** `pdf_rules.js` also returns each page's image placements
   (bbox in page points, image id, size in pixels; masks skipped).
2. **Notice link.** `inspect_positions.js` writes a marker `[[FIGURE p.k]]`
   (page p, k-th figure) at the figure's place in the reading order:
   - inside a ruled table, in the cell the figure sits in (a party symbol
     stays with its row);
   - otherwise as its own line at the figure's height, in its page column.

   The marker travels with the text into its notice. A figure list is
   written beside the text (`--figures <file.json>`).
3. **Crop.** Each figure is cut from the page rendered at 200 dpi
   (`pdftoppm -x -y -W -H`): what the reader sees, overlays included.
4. **Read.** Each crop is OCR'd: map titles, chart labels and stamp text
   become the figure's text.
5. **Classify** (rules, in this order):

   | kind | rule |
   |---|---|
   | `page_scan` | covers ≥ 85% of the page |
   | `coat_of_arms` | cover page, top centre, the cover's image |
   | `table_symbol` | inside a table cell |
   | `map` | OCR / notice words: map, registry index, block, scale, sheet, coordinates; or a large figure on a page without text |
   | `chart` | the notice cites "Figure n" / chart, or OCR shows axis numbers and labels |
   | `prescribed_image` | the notice says the images / pictorial warnings are set out |
   | `stamp_seal` | OCR stamp words (received, library, council, registrar, certified, seal) or a small colour mark on a cover / signature area |
   | `logo` | small, at a notice heading, or the same image placed in several notices |
   | `signature` | small, wide, just above / beside a signatory line |
   | `photo` | medium / large continuous-tone image |
   | `mark` | anything smaller than 0.25 sq in |
   | `other` | none of the above, kept for review |
6. **Cleanup.** For scanned kinds (maps, page scans, stamps, photos) there
   are two files: the original and a cleaned copy.

**Stage B — store and show** (Java + site):

- A `NoticeFigure` record per figure: gazette, notice, page, kind, bbox,
  original / cleaned file, OCR text, size.
- Files go in the figures directory, served by the app.
- A "Figures (N)" tab on the notice page: meaningful kinds first, with a
  cleaned ↔ original toggle; stamps, logos and seals listed under "Other
  images".
- In "Original notice text" the marker becomes the image itself.

## 4. Rules and fail-safe

- Every placement is captured and classified; none is dropped.
- The original image is always kept. A cleaned image is labelled as such and
  never replaces the original.
- A marker never changes the notice text around it; the cleaner and templates
  ignore it (the regression run checks templates, categories and notices).

## 5. Acceptance criteria

1. **Coverage:** every image placement in the corpus is captured (count =
   placements in the SVGs, masks excluded).
2. **Notice link:** a review of 30 figures shows each linked to the right
   notice (and table symbols to the right row).
3. **Classification:** a review of 60 figures, all kinds. Accuracy is
   reported per kind; every `other` is listed.
4. **Cleanup:** before / after review of 10 scanned figures; no detail
   invented (cleaned = a monotone function of the original pixels, no
   generative step).
5. **Nothing else changes:** notices, categories and templates are the same;
   pages without figures are byte-identical; `audit.py` ERRORS: 0.
6. **Python = Java** for markers and classification; JUnit cases.
7. **Stage B:** figures visible on the notice page in the running app.

## 6. As built and measured (4 Oct 2026)

**Capture (Python lab = Java):** 790 image placements in 262 born-digital
issues, every one captured and classified:

| kind | count | kind | count |
|---|---|---|---|
| coat_of_arms | 263 | chart | 13 |
| map | 205 | prescribed_image | 9 |
| table_symbol | 202 | logo | 7 |
| mark | 84 | table_image | 5 |
| photo | 1 | form | 1 |

527 are linked to a notice (the rest are cover crests). 143 faded / brown
scans have a cleaned copy; 1 map scanned upside down (2025 No 163 p61) has
its cleaned copy turned the right way up.

**Rules added after review** (each a misreading seen in the corpus):
- `form`: a prescribed form printed as an image (2026 No 75, EPRA Form 3);
- a page-sized figure in a map notice is a map before the chart / table tests
  (block maps are full of parcel numbers);
- `table_image` needs comma-grouped amounts ("142,390") — a one-row strip of
  a KRA statement has two; a county map's block numbers 1–24 have none;
- a report that cites "Figure n" makes its images charts, before the
  notice-level map test ("KAA Concession Fees" made a chart a map);
- chart axis words: financial year, billion, actual, target;
- "concession" counts only as concession area / block / map.

**OCR engine for figures: PP-OCRv6** (the models RapidOCR 3.9 ships), run in
Java on ONNX Runtime by `service.PpOcr` — a port of RapidOCR's pipeline
(detection DB post-processing, crop, CTC recognition; no angle classifier).
`tools/figure_ocr_eval.py` on the 241 figures that carry text:

| figures | Tesseract | RapidOCR (Python) | PpOcr (Java) |
|---|---|---|---|
| maps whose title matches the notice (of 205) | 45 | 129 | 124 |
| map words in the corpus vocabulary | 37.6% | 53.3% | 50.5% |
| table-image amounts well-formed | 22/22 | 22/22 | 22/22 |
| prescribed-image real words | 21 | 137 | 139 |
| time, all maps | — | 3,236 s | 1,557 s |

Tesseract misread amounts on a KRA statement (`9,224,208` for `3,224,208`).
Page text stays on Tesseract. Classification on the PP-OCR text: 0 kinds
change. Java wrapper libraries tried first: `rapidocr` (mymonstercat) drops
every space; ONNX Runtime 1.21+ will not start on JDK 17.0.10's Visual C++
runtime (pinned 1.20.0); ONNX Runtime's Java metadata read breaks 4-byte
characters, so the character list is a file (`tools/fetch_ocr_models.py`).

**Acceptance:**
1. Coverage: 790 / 790 placements captured.
2. Notice link: 30 sampled figures (all kinds) — 30 right.
3. Classification: the same 30 — 29 right, 1 debatable (a party emblem
   outside a table filed as `logo`, 2025 No 1); kind-by-kind reviews found
   and fixed 7 misreadings (rules above).
4. Cleanup: no generative step; Java and Python copies match visually. The
   contrast stretch also darkens old stains on brown paper (2022 No 43) —
   the original is always one click away.
5. Nothing else changes: notices 20,980 = 20,980, 0 category changes,
   templates 16,955 → 16,960 (+5 / −0, including 2025 No 164 notice 10358
   after the header-below-a-label fix); `audit.py` ERRORS: 0.
6. Python = Java: classification and notice link identical on 790 / 790;
   JUnit `FigureClassifierTest` (13), `FigureServiceTest` (5), `PpOcrTest` (4).
7. Stage B in the running app: 2025 No 73 → 10 figures (55 s), 2022 No 43 →
   47 figures with 46 maps on the notice page, "Figures (N)" tab, cleaned ↔
   original toggle, images in place in "Original notice text".

**Settings:** `figures.enabled` (true), `figures.dir` (figures),
`figures.dpi` (200), `figures.ocr.models-dir` (models/ppocr).
