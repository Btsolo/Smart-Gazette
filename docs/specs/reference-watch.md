# Spec: reference watch — notice when reference data may be out of date

Status: **built** (5 Oct 2026; supplements fetched, flags to the log) · Phase 4b (future-proofing)

## 1. Requirement

The system holds reference data it did not write: the laws (46 so far), the
counties / constituencies / wards, and the rules built on them (which section
a kind of notice is issued under). The world changes: Acts are amended,
repealed or replaced, wards are created or renamed. The system should notice
this **from what it reads** and either update itself (where that is safe) or
ask the owner — through the log, and later the review queue — exactly what to
do. Not only for laws: for every kind of reference data.

## 2. Analysis

**How law changes reach the Gazette.** New Acts, amendment Acts, Bills and
Legal Notices are published as **Kenya Gazette Supplements** — separate PDFs
(e.g. "Kenya Gazette Supplement No. 168 (Acts No. 22)", the County Allocation
of Revenue Act 2026 in the library). The ordinary notices the system
processes only mention them in passing. Keyword hits in notices
("commencement", "Revised Edition", "Regulations, 2012") are mostly noise
(2022-2026: "commencement" 31 notices, nearly all "commencement of work";
"Revised Edition" 30, a boilerplate line).

**Two signals in notices are precise** (2022-2026, 21,250 notices):

| signal | found | meaning |
|---|---|---|
| a cited section is not in our copy | 7 sections, 21 citations (Water Act s.158 x9; Elections Act ss.1A, 1D, 5A x8; County Governments Act s.2D; Universities Act s.2A; EAC CMA s.248) | our copy may be older than the amendment that added it - or the notice cites another (older) law |
| a notice cites "<Law> (Amendment) Act, YEAR" for a law we hold | 25 citations (Urban Areas and Cities 2019 x21, County Governments 2020 x3, EMCA 2015) - all **already included** in our copies | if YEAR is after our copy's "text as at" date, our copy predates the amendment |

**Other reference data.** Geography: a county notice naming a ward or
constituency that `geography.json` does not know is the same kind of signal.
Statistical data (the word vocabulary, table header words) is learnt from the
corpus itself and can be refreshed automatically (Phase 4b).

## 3. Design

**1. Watchers** (`tools/reference_watch.py` = Java `ReferenceWatch`, run on
each processed gazette and over the corpus by `audit.py`):

| watcher | raises a flag when | suggested action |
|---|---|---|
| `law.missing_section` | a notice cites section N of a law we hold and our copy has no section N | check the current version on Kenya Law; if it has section N, save its PDF to `raw/law/` |
| `law.amended_after_copy` | a notice cites "<Law> (Amendment) Act, YEAR" or the "Statute Law (Miscellaneous Amendments) Act, YEAR" naming a law we hold, and YEAR is after our copy's date | save the current version |
| `law.amending_supplement` | a processed Gazette Supplement says "AN ACT of Parliament to amend the <Law> Act" | the strongest signal: save the current version once Kenya Law publishes it |
| `law.not_held` | an Act we do not hold is cited N times (default 5) | add it (the "add next" list) |
| `law.old_copy` | our copy's date is more than 3 years old | yearly reminder to check |
| `geography.unknown_place` | a county notice names a ward / constituency not in `geography.json` | check with the IEBC list |

**2. Flags, not silent changes.** A flag is a record (`ReferenceFlag`):
data set, key (e.g. `water_act`), watcher, detail (e.g. `s.158`), evidence
(gazette, notice number, the sentence), first / last seen, count, status
(OPEN / DONE / DISMISSED). The same flag seen again only updates its count.

**3. Telling the owner.** Today: one WARN line per new flag in the log,
searchable as `REFERENCE-WATCH`, and a daily summary line of open flags.
Later (roadmap): the review queue (stage 3) and the health report (stage 6)
list open flags; the admin page once Spring Security is in place.

**4. What the system does by itself, and what it asks.**
- **Laws and the rules built on them (implied sections): never changed
  silently** - legal text must come from the publisher. The flag says
  exactly what to download. After the owner saves the PDF, one command
  rebuilds the library, and the watchers **close their own flags**: a
  `missing_section` flag closes when the section now exists, an
  `amended_after_copy` flag when the copy's date is now after the amendment.
- **Geography: asks** (the IEBC list is the authority).
- **Statistical data** (vocabulary, header words): refreshed automatically
  by the learning step (Phase 4b), checked by `audit.py`.

**5. The library keeps its history.** A rebuilt law keeps the old file's
"retrieved" date unless the text changed, and the catalog records each law's
version date, so a rebuild only shows real changes in git.

## 4. Rules and fail-safe

- A watcher only raises flags; it never edits reference data or notices.
- A flag carries its evidence (the notice and sentence), so the owner can
  judge it without searching.
- `audit.py` section 8 "reference freshness" lists open flags over the
  corpus (warnings, not errors).

## 5. Acceptance criteria

1. On the corpus: the 21 missing-section citations give 7 flags; 0
   `amended_after_copy` flags (all cited amendments are included).
2. A synthetic notice citing "the Water (Amendment) Act, 2027" raises
   `law.amended_after_copy` for `water_act`; rebuilding with a copy dated
   2027 closes it.
3. Python = Java for the watchers; JUnit cases.
4. In the running app: processing a gazette with such a citation logs
   `REFERENCE-WATCH` once, and stores one flag.

## 6. Questions for you

1. **Gazette Supplements:** they carry the actual Acts and amendments. Should
   the nightly scraper also fetch new supplements (a few more requests a week
   to Kenya Law; covered by the permission you are asking for), or rely on
   notices + your checks?
2. **Where flags reach you before the review queue exists:** the log only,
   or also a short email when a new flag appears?

## 7. As built (5 Oct 2026)

Answers: supplements are fetched (the scraper sends whatever new PDFs Kenya
Law lists; a supplement is recognised by its masthead and has no notice
headers); flags go to the log for now.

- `tools/reference_watch.py` = Java `ReferenceWatch`, parity 21,250 / 21,250
  notices. Corpus: 4 flags - Water Act s.158 (x9), EAC Customs Management
  Act s.248 (x2, our scan lacks it), Elections Act s.1A (x2), Universities
  Act s.2A (x1). 0 amendment Acts newer than our copies. law.not_held: 17
  Acts cited >= 5 times (Public Audit Act 10, ...). law.old_copy (5 years):
  Constitution 2010, Disposal of Uncollected Goods 1987, EAC CMA 2008.
- Found on the way: subsections with a capital letter ("(1A)", "(2D)") were
  read as section numbers - fixed in law_refs.py and LawReferenceService.
- `ReferenceFlag` table; `REFERENCE-WATCH` WARN line per new flag; daily
  review 07:00 (`reference-watch.cron`) closes answered flags and logs a
  summary; a dismissed flag stays dismissed.
- Gazette Supplements: recognised before any AI call, watched, not
  processed as notices (tested in the app with the County Allocation of
  Revenue Act 2026 supplement).
- A no-change rebuild of the library leaves the law files as they were.
- audit.py section 8: the watchers' self-test + copies due for a check.
- Not built yet: `geography.unknown_place` (needs ward names read from
  county notices) and the scraper check that Kenya Law's listing includes
  supplements (to confirm on the live site).
