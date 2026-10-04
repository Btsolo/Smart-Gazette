# How Smart Gazette is built

A small, explicit development process, so every change is planned, measured,
tested and documented the same way. The order of work comes from the roadmap
(private: `reports/ROADMAP.md`); the detailed plan is `reports/PLAN.md`; the
reasons behind each change are in `docs/JOURNAL.md`.

## 1. The system, layer by layer

Each layer has one job and a clear input and output. A change belongs to one
layer; if it touches two, it is two changes.

| # | Layer | Input -> output | Python (lab) | Java (production) |
|---|---|---|---|---|
| 1 | Lane choice | PDF -> born-digital / scan | `ocr_check.py` | `ScanLaneService.isScanned` |
| 2 | Text extraction | PDF -> page text (cells marked " \| ") | `inspect_positions.js` | `PdfInspectorService` (runs the same JS) |
| 2s | OCR (scans) | page images -> repaired text | `scan_lane.py` | `ScanLaneService`, `OcrTextFixer` |
| 3 | Cleaning | page text -> one notice text | `gazette_clean.py`, `vocab_repair.py` | `GazetteTextCleaner`, `VocabRepair` |
| 4 | Issue header | cover text -> volume, number, date | `gazette_header.py` | `GazetteHeaderParser` |
| 5 | Segmentation | text -> notices (GAZETTE NOTICE NO. boundary, ascending lock) | `gazette_clean.apply_ascending_lock` | `GazetteService.segmentTextByNotices` |
| 6 | Categorisation | notice -> category (heading rules, then keys, then AI triage) | `category_census.py` | `CategoryRules`, `keywordPreFilter` |
| 7 | Field extraction | notice -> fields (schema) | `*_template.py`, `table_extract.py`, `table_records.py` | `service.templates.*`, `TableExtractor`; AI only when no template reads it |
| 8 | Article generation | fields -> title, summary, article (AI writes prose) | - | `generateNarrativeContent` |
| 9 | Storage | records -> PostgreSQL | - | `Gazette`, `GazetteRepository` |
| 10 | Presentation | records -> pages (tables, laws cited, OCR banner) | - | controllers + Thymeleaf |

Rule of thumb from the roadmap: **AI extracts facts only where a template
cannot; Java (templates, rules) does everything deterministic.**

## 2. The life of a change

Every feature or fix goes through these steps, in order. Small fixes may make
a step a single line, but no step is skipped.

1. **Requirement** - what and why, in one paragraph, linked to a roadmap/plan
   item. Measurable **acceptance criteria** (numbers, not "works well").
2. **Analysis** - measure the current state on the corpus first: how many
   notices, which variants, how they fail today. This is the baseline.
3. **Spec** - `docs/specs/<feature>.md`: inputs, outputs (the schema fields),
   the rules, where it sits in the layers above, how it fails safe, the test
   plan. Written *before* the code. Example names only (John Doe), never real
   people.
4. **Implementation in the lab** - Python in `tools/`, measured against the
   acceptance criteria on the corpus; every output reviewed by reading a
   sample, not only by counting.
5. **Production port** - Java, rule for rule; a **parity check** against
   Python on the corpus (only when ported code changes), and **JUnit tests**
   from real notice shapes (they run in CI without the private corpus).
6. **Verification** - `tools/audit.py` (ERRORS: 0), `mvn test`, the corpus
   scorecards, and a **held-out check** on issues the system never saw
   (`tools/issue_report.py`). No other category or layer may get worse.
7. **Review** - read what changed: a sample of new records, the notices that
   newly fail, the diff. Say what is not done.
8. **Documentation** - journal section (what / why / how / numbers), a
   lesson in `CLAUDE.md` when something general was learnt, the plan and the
   roadmap status updated.
9. **Commit** - one change per commit, message with the measured numbers.
   Code and docs go to the code repo; reports and test data only to the
   private `reports/` repo; nothing is pushed without the owner's OK.

## 3. Definition of done

- [ ] acceptance criteria met, with the numbers in the commit message
- [ ] Python and Java agree (when both exist); JUnit + audit pass
- [ ] held-out issues checked; no regression in other categories
- [ ] spec, journal, plan/roadmap status updated
- [ ] no real personal data in tracked files

## 4. Branches and releases (planned with the owner)

- `main` = what is deployed by CI/CD; `develop` = integration; one feature
  branch per change, merged into `develop` by squash (old commits with real
  names never leave the laptop).
- CI runs `mvn test` and the Python `audit.py` regression cases; the corpus
  checks stay local (the corpus is private).
