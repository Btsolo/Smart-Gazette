# Spec: tables in scanned gazettes

Status: **implemented** (4 Oct 2026; results in section 7) · Layer 3 (OCR / scan lane) · Roadmap step 5 (table lane)

## 1. Requirement

Give scanned tables the same shape as born-digital ones: one text line per
table row, cells separated by `" | "`, wrapped cells joined to their row where
the print shows it. The scan lane must also stop inventing cells: today a
column rule read as `|` becomes a cell marker.

## 2. Analysis (21 scanned issues, 1,167 pages, 2022–2025; Tesseract word boxes at 300 dpi)

| measure | result |
|---|---|
| pages with tables (≥ 5 lines whose words form ≥ 3 separated groups) | **224 (19%)**, ~11,000 table lines |
| largest | 2024 No 148 (70 pages, company register), 2022 No 238 (38), No 058 (18), No 231 (17), No 206 (15), No 169 (10, election results) |
| table pages with ruling lines visible in the image | 116 (52%) |
| crude line-finder on prose pages (control) | 7 of 79 "lattices" (page frames, column rules): the image test needs validation |
| lines with `" | "` in today's scan text | **4,169**: 2,811 on table pages, 142 in prose |
| "tables" `table_extract` finds in scan notices today | 51 notices, 3,054 rows, all split at rule artifacts |

What Tesseract gives a ruled table today:

```
1319] PVT-6LULKE2X __| King Solomon's Garden Limited Private Limited 2/9/2024
```

- Rows mostly come out as lines (good).
- The column rules turn into characters (`]`, `__|`, `|`, `}`, `;`).
- The cells run together.
- Wrapped cells drift onto their own lines.

In prose, a stray `|` is mostly a misread "I" ("from the date hereof, | shall
issue") or a speck.

## 3. Design (scan lane: `tools/scan_lane.py`, Java `ScanLaneService`)

Inputs per page: Tesseract's word boxes (TSV, already cached; Java switches
from `txt` to `tsv` output) and the page image (already rendered for OCR).

1. **Table regions** from the word boxes: runs of ≥ 3 lines whose words form
   ≥ 3 groups separated by wide gaps, aligned across lines (the born-digital
   rule of fix 7, on word boxes).
2. **Ruled tables.** Inside a region, the image's ruling lines:
   - dark rows / columns, tolerant of a slight skew;
   - ≥ 3 vertical and ≥ 3 horizontal lines, crossing each other.

   Rows come from the horizontal rules, cells from the vertical rules
   (merged sideways as in fix 7b); a word belongs to the cell of its centre.
   - Characters standing on a rule (`| ] [ } { _`) are the rule itself and
     are dropped.
   - A word read across a wall with the rule inside it is cut at the bar.
   - Lines between two rules join their row.
   - **Where rules were lost** (a band holding several entries in the key
     column), each Tesseract line is its own row, with cells from the walls.
     Nothing is joined: which lines form a row is not printed there.
3. **Unruled tables.** Runs of ≥ 3 lines whose words form ≥ 3 groups that
   line up with the next line's groups (shared starts). Every group must be
   cell-sized (≤ 34% of the page width), so prose beside a table is not a
   row. Each line becomes a row; rows are not joined.
4. **Outside tables no cells.** A standalone `|`:
   - before a lowercase word becomes "I" ("| shall issue" → "I shall
     issue");
   - before a number it is kept, glued to the number ("NO. | 2057" →
     "NO. |2057"), so the existing number repair still reads 12057;
   - otherwise it is dropped.
5. Then the normal scan-lane steps (header repair, OCR word repair, cleaner).

## 4. Rules and fail-safe

- Words are never dropped; only the rule characters are (`| ] [ } { _` standing
  alone or attached at a wall).
- No validated table means the page text is today's, except for the stray-bar
  rule (step 4).
- A ruled lattice must look like a table: words inside it, and most rows with
  ≥ 2 filled cells. A page frame or a column rule is not a table.

## 5. Acceptance criteria

1. **No text lost:** on every scan page, the letters and digits after equal
   those before, except the "I"s of step 4.
2. **No fake cells:** no `" | "` outside a detected table.
3. **Rows rebuilt:** on table pages, rows with cells and rows at their table's
   width rise. The big registers (2024 No 148, 2022 No 238) are row-correct
   on review.
4. **Visual review** of 20 scanned tables against their images: no wrong
   rows, no cells in the wrong column.
5. **Nothing else worse:** notices found (consecutive-numbering referee),
   categories and template rates on scans are not lower. `audit.py`
   ERRORS: 0.
6. **Python = Java** on all scan pages (scan-lane parity), with JUnit cases.
7. **Time:** ≤ +0.5 s per scanned page.

## 6. Test plan

- `audit.py` cases on synthetic word boxes / images;
- the lab scorecard (scan pages before / after, letters, fake cells, rows);
- `scan_lane.py` evaluation (notices against slots, probate, land);
- review against images;
- parity of `ScanLaneService` with the Python lane.

## 7. Results (4 Oct 2026, 21 scanned issues, 1,167 pages)

| criterion | result |
|---|---|
| 1. no text lost | **1,167 / 1,167 pages**: letters and digits identical (beyond 58 stray "|" read as "I") |
| 2. no fake cells | **0** lines with cells outside a table (were 4,169) |
| 3. rows rebuilt | 220 pages with tables: 185 ruled tables, 7,253 ruled rows (wrapped cells joined) and 2,745 unruled rows. The company register (2024 No 148) and the customs registers (2022 No 238) are row-correct on review |
| 4. review | 23 table samples and 4 page images. **Two real errors found and fixed**: prose beside a table read as rows, and rows joined where rules were lost (2022 No 169) |
| 5. nothing worse | notices **3,842 → 3,846** of 3,982 printed (none lost after the "NO. \| 2057" fix); probate blocks +6, land +2; `audit.py` ERRORS: 0 |
| 6. Python = Java | page assembly 1,167 / 1,167 pages; rule finding 40 / 40 images; JUnit 3 cases (24 tests in total pass) |
| 7. time | rules median 0.16 s, max 0.89 s per page (Java reuses the OCR image; no extra render) |

Known limits:
- rule artifacts inside **unruled** tables ("[8/1/2022") stay, because no wall says they are rules;
- rows broken where a scan lost its rules stay one line per row;
- side-by-side tables are read straight across, as for born-digital pages.
