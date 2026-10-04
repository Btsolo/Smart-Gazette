# Spec: ruled tables (fix 7b)

Status: **implemented** (4 Oct 2026; poppler in every extraction approved; results in section 8)

## 1. Requirement

Rebuild **ruled** tables — tables printed with ruling lines — as one text line
per table row with its cells, including rows whose cells wrap onto several
lines, so the cleaner, `TableExtractor`, the templates and the "Tables" tab
see whole rows. Today such rows are broken: the first line keeps some cells,
the wrapped lines become orphan lines ("Siaya/Sumba/3291 | SC 9" …
"Doe | Roe, | 0.0485"), and some ruled tables are not seen as tables
at all.

## 2. Analysis (sample: 30 table pages + 30 table-free pages per year, 2022–2026)

**Where the rows come from.** pdf-inspector's `detectVectorGridInRegion` was
tried first and rejected:
- it finds a grid only for a whole page (every crop returns nothing);
- its column walls are not the printed rules (17/119/222/369/578 against the
  printed 58/157/244/472/538 on 2024 No 203 p48);
- its row walls sit on text baselines, and on prose it makes one "row" per
  text line.

It is derived from the text, so it cannot tell a table from prose
(lesson 46 again).

The **printed ruling lines** can. They are read from the page's vector
drawing commands with poppler's `pdftocairo -svg` (poppler is already a
dependency of the scan lane): `tools/pdf_rules.js`. Prose has none. On p48
they give the 5 true column walls, and a taller row around a two-line owner
cell.

| measure | table pages | table-free pages (control) |
|---|---|---|
| pages with a ruled lattice (≥ 3 walls, ≥ 3 rules) | **62 of 150 (41%)**, every year | 5 of 150 |
| text lines in the lattices → rows | 2,050 → 1,543: **507 wrapped lines** belong to the row above | 67 → 33 |
| text items running across a wall | 3.0% | 17% |

**What the crossings are.** They are not false lattices:
- some PDF writers put two cells in one text item ("13/12/2022
  2022JKAHI263" across the wall at x 211, 2024 No 116), which today's
  extractor keeps as one cell;
- the control pages' lattices are ruled tables today's extractor does not see
  at all. In an audit table (2025 No 43 p53) a text item runs across columns
  ("on the external Issue / Observations from Auditor"), so the table is
  printed as interleaved prose.

The walls are what reveal both.

**Why p48 breaks today.** There are two faults:
1. The page is all table, so the area column (x 495) is learnt as the
   "right page column" and the full-width table is split between the columns.
2. Wrapped cells have no row.

The lattice fixes both.

**Not affected:** tables without rules (whitespace tables, e.g. 2024 No 203
p42, p46) keep today's handling.

**Cost:** `pdftocairo -svg` takes about 6 s for a 76-page issue (the
extractor itself about 1.4 s). This is negligible next to the AI calls of an
upload.

## 3. Design (in `tools/inspect_positions.js` — the one extractor Python and Java both run)

1. **Rules.** `pdf_rules.js` reads the ruling lines of every page:
   - one `pdftocairo -svg` run per block of ≤ 50 pages;
   - collinear pieces are joined into walls and rules (printers draw a wall
     one cell at a time).
   - If poppler is missing or fails, there are no rules, and the output is
     exactly today's.
2. **Lattices.** ≥ 3 vertical walls (≥ 6 pt tall) with overlapping spans
   (≥ 2 columns), plus ≥ 3 horizontal rules between the outer walls (≥ 2
   rows), and text inside.
   - A one-cell frame (a boxed notice) is not a lattice.
   - The rule printed between the two page columns is not a wall: it is
     long, near the page centre, and no rule crosses it.
   - A lattice whose rows mostly hold a single cell of text is not a table.
3. **Cells.** Every text item whose centre lies inside a lattice belongs to
   it and leaves the normal layout.
   - An item running across a wall *that exists at its height* is split at
     the space nearest the wall.
   - An item with no space near the wall stays whole in the cell of its
     centre (counted as unsplit).
4. **Rows.** Every rule that fully crosses at least one column bounds rows
   (an underline never does).
   - Each column keeps its own rules, so a cell merged downwards belongs to
     the row where it starts, and the sub-rows beside it stay rows.
   - A cell's text is its baselines top to bottom (a raised "rd" belongs to
     its "3"), each read left to right.
   - A missing internal wall at that height (a merged cell) gives one cell.
   - Cells are joined by `" | "`; an empty cell is written `"a | | c"`.
   - A row merged across the whole table (a date, venue or section label) is
     written `"label |"`, so it stays in its table.
   - Rows without text are dropped.
5. **Header.** As in fix 8: the first rows set entirely in a font other than
   the table's body font are the header and get the `--- | ---` line.
6. **Placement.**
   - A lattice crossing the page gutter is full-width (it cuts a band, like
     today's spanning lines).
   - Otherwise its rows go into their page column at the lattice's height.
   - The rest of the page is laid out exactly as today.
7. **Pages with no lattice are untouched**: byte-identical output.

Downstream, in Python and Java:
- `TableExtractor` splits cells on a bar with whitespace (or the line start)
  before it and whitespace (or the line end) after it, so empty cells keep
  their column.
- The cleaner and `TableExtractor` accept a row ending `" |"`.
- `TableRecords` does not take a one-cell label of ≥ 4 words for a record.
- `PdfInspectorService` timeout default: 120 s → 300 s.
- poppler-utils must be on the server (already in the deploy plan for the
  scan lane).

## 4. Rules and fail-safe

- Text is never dropped. Every item inside a lattice lands in exactly one
  cell; the word count of the page is unchanged (checked per page in the lab).
- The lattice comes only from drawn lines, never from text gaps. No rules
  means no change.
- Scanned pages are unaffected (scan lane).

## 5. Acceptance criteria

1. **No words lost:** bag-of-words recall against pdftotext over the corpus
   is not lower than today, in every year.
2. **Rows rebuilt:** on lattice tables, orphan lines fall and the share of
   rows matching their table's width rises (`table_extract` scorecard).
   2024 No 203 p48 and 2024 No 116 p51 come out row-correct.
3. **Visual review** of 30 rebuilt tables against the page images: no
   wrongly joined rows, no cells in the wrong column, no lost text.
4. **Prose unchanged:** pages without a lattice are byte-identical. Notices
   found, categories, probate blocks, land and the other template rates are
   not worse in any year. `audit.py` ERRORS: 0.
5. **Held-out:** No 166 and No 175 pass the same checks
   (`issue_report.py`).
6. **Time:** median extraction time per issue grows by ≤ 10 s.

The existing table referee (`table_eval.py`) counts *printed lines* as rows,
so a correctly joined two-line row can score as "not kept". It is reported,
but criteria 2–3 decide.

## 6. Test plan

- `audit.py` cases on a small ruled-table fixture;
- the lab scorecard `tools/ruled_probe.js` (lattices, joins, splits, words
  in = words out);
- a whole-corpus re-extraction with before/after numbers (`fix_eval.py`,
  `table_eval.py`, `category_eval.py`);
- held-out No 166 / 175.

## 8. Results (4 Oct 2026, 260 born-digital gazettes 2022–2026 + held-out No 166 / 175)

| criterion | result |
|---|---|
| 1. no text lost | **0 of 1,890 changed pages** differ in their characters (ignoring spaces, bars and `---`): text only moves into rows and cells and word breaks change. Most word changes are repairs ("3 rd" → "3rd", "Jo hn" → "John", "22Apr" → "22 Apr") |
| 2. rows rebuilt | 1,890 pages (26%) hold ruled tables: 199,815 lines → 121,056 rows. Rows at their table's width 96,771 → **110,049**. 2024 No 203 p48 and 2024 No 116 p51 row-correct |
| 3. visual review | 30 pages against their images. **Three real errors were found and fixed**: the character-width split estimate, cells merged downwards, and a page-gutter rule taken as a wall. A one-row table's header and date-label rows were found through the template losses. Three oddities come from the PDFs' own text order (shared by the old extractor) |
| 4. prose unchanged | other pages byte-identical; notices 20,977 → 20,978 (one notice lost by the old extractor is found, 2026 No 22 #1663); 2 category changes, both company-dissolution lists (one now correct); notices read by templates **+39, 0 lost**; `audit.py` ERRORS: 0 |
| 5. held-out | No 166 / 175: characters identical, notices and categories unchanged, templates 217 → 219 / 179 → 179, rows at width **54% → 98%** / 88% → 90% |
| 6. time | median 1.6 s per gazette; slowest 125 s (6 in parallel), so the Java timeout was raised to 300 s |
| parity | `TableExtractor` 20,978 / 20,978, templates all identical, cleaner 89 / 89 gazettes (Python = Java on the new extraction); JUnit 21 pass |

Also found: an acquisition schedule grouped by inquiry date (2025 No 93 #6418) now reads 111 parcels with their areas, against 43 without areas before.

Known limits:
- words the print itself breaks in narrow columns ("Mornin g Compos er") stay broken;
- the PDF's own text order is kept ("Stc 54 Pkgs 15 of Pallets").
