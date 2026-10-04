# Spec: change-of-name template

Status: **implemented** (3 Oct 2026; results in section 8) · Layer 7 (field extraction) · Roadmap step 3

## 1. Requirement

Read deed-poll change-of-name notices into fields **without an AI extraction
call**, complete enough to fill one row of the planned collective
change-of-name table (one article = introduction + table; see
`reports/PLAN.md`, Phase 6). Change of name is the largest category with no
template: 1,550 notices in 2022–2026, 52 of the 112 AI extractions left in
the held-out issue No 166.

## 2. Analysis (2022–2026, 1,550 notices)

| element | share | example (placeholder names) |
|---|---|---|
| deed poll dated … | 99.2% | "by a deed poll dated 11th September, 2026" |
| registered in the Registry of Documents at <place> | 99.2% | "at Nairobi" |
| Presentation No. / Volume / Folio | 99.4–99.5% | "Presentation No. 463, in Volume D1, Folio 362/3386" |
| File No. | 96.1% | "File No. MMXXVI" |
| filed by advocates ("by our client") | 94.6% | signed "EXAMPLE & COMPANY, Advocates for John Doe" (96.0%) |
| self-filed ("by me") | 2.6% | signed by the person |
| on behalf of a minor | 8.6% | "(1) John Doe and (2) Jane Doe (Guardians) … on behalf of Baby Doe (Minor)" |
| formerly known as X | 99.6% | |
| … renounced … former name X | 94.6% | the same X again - a built-in check |
| assumed and adopted the name Y | 98.9% | |
| address (P.O. Box) | 76.2% | |

Not deed polls (must be refused → AI path): a bank renamed by board
resolution (Central Bank of Kenya Act), a trade association's change of name
(Labour Relations Act), corrections of a year of birth.

## 3. Output

The existing schema fields (`schemas/field/change_of_name.json`), plus the
fields the collective table needs (**proposed schema additions, marked +**):

| field | rule |
|---|---|
| `former_name` | from "formerly known as X" (else "former name X") |
| `assumed_name` | from "assumed and adopted the name (of) Y" |
| `aliases` | further "alias" names of the person, if any |
| `person_address` | "of P.O. Box …, <town>" of the person (or guardians) |
| `citizenship` | "in the Republic of Kenya" → "Kenya" |
| `deed_poll_date` | "deed poll dated <date>" as printed |
| `registration_date` | only if printed (rare) |
| + `registry` | "<place>" of the Registry of Documents |
| + `presentation_number`, `volume`, `folio`, `file_number` | as printed |
| + `filed_by` | `advocates` / `self` |
| + `advocate_firm` (existing) | the signature line before ", Advocates for" |
| + `applicants` | the people who made the deed poll (the person, or parents / guardians) |
| + `on_behalf_of_minor` | true when the change is for a minor |
| `notice_id` | the notice number (from the notice's own first line) |

## 4. Rules and fail-safe

- Only notices with the deed-poll formula ("deed poll" + "assumed and
  adopted", "assumed and re-adopted" or "in lieu thereof adopted"); anything
  else returns null → AI path, as today.
- `deed_poll_date` is optional: a misprinted date ("16th September August,
  2026") leaves it empty; the notice is still read.
- `registry` is one capitalised word, matched case-sensitively ("at Nairobi
  as Presentation" must not give "Nairobi as").
- "by our client(s)" and "by my client(s)" → `advocates`; "by me" / "by us" →
  `self`. `advocate_firm` is taken only from a ", Advocates for" signature;
  a signature without "for" leaves it empty (not guessed).
- **Internal witness:** when both "formerly known as X" and "former name X"
  are printed, X must be the same (after whitespace/case normalisation),
  otherwise refuse. A name is the record's key, and a wrong one is worse than
  an AI call.
- Names go through the shared name cleaner (`names.py` / `Names`).
- A name over 80 characters, or one containing "formerly" / "assumed", is a
  mis-parse → refuse.

## 5. Acceptance criteria

1. ≥ 95% of the deed-poll notices (2022–2026) read by the template.
2. Review of 50 random records: no wrong former / assumed name.
3. Non-deed-poll notices (bank, association, year-of-birth correction) are refused.
4. Held-out No 166 and No 175: ≥ 95% of their change-of-name notices read.
5. Python and Java identical on all change-of-name notices; JUnit cases pass
   (advocates, self-filed, minor with guardians, refused bank notice).
6. No change in any other category's template rates.

## 6. Where it plugs in

`category_census.template_for('Change_of_Name')` → `NoticeTemplates.extract`
case `Change_of_Name` → `GazetteService.templateExtract` (no change there).

## 7. Test plan

`tools/audit.py` cases; `NoticeTemplatesTest` cases; corpus scorecard in
`tools/change_of_name_template.py` (`__main__`); held-out via
`tools/issue_report.py`.

## 8. Results (3 Oct 2026)

| criterion | result |
|---|---|
| 1. corpus read | **1,505 of 1,536 deed polls (98.0%)**; not read: 25 former names disagree (genuine misprints), 6 other wording |
| 2. 50-record review | 50/50 former and assumed names correct (after the registry and "Dr." fixes) |
| 3. non-deed polls | refused (audit + JUnit) |
| 4. held-out | No 175: 26/26. No 166: 49/52 at first - "assumed and re-adopted", "in lieu thereof adopted" and "Folio." were not in the corpus; after widening the rule 52/52 (so No 166 is no longer an unseen issue for this template) |
| 5. parity / JUnit | Python = Java on all 1,550 change-of-name notices; `NoticeTemplatesTest` 14 tests pass |
| 6. other categories | unchanged (parity on 16,860 probate / land / corrigenda notices identical; template counts unchanged) |

Field fill on the records read: names, filed_by, volume, folio 100%; registry,
file number, applicants, deed date 99%; advocate firm 96%; presentation 96%;
address 76%; minor 9%; aliases 3%.
