# Spec: law library — the laws notices cite, stored, linked and quoted

Status: **draft for your review** (4 Oct 2026) · Phase 4d

## 1. Requirement

Notices are issued under laws: the Constitution, Acts, regulations. Keep
those laws in the system as reference data, like the counties already are,
instead of searching for them when needed. Link each notice to the law it
relies on, and let the article quote and explain the relevant part when it
helps the reader ("aside from talking about just the notice, it can include
that too").

## 2. Analysis (2022–2026, 20,980 notices)

**What we have:** the Constitution and the County Governments Act
(`src/main/resources/reference/`, built from Kenya Law by
`tools/build_law_reference.py`); `tools/law_refs.py` = Java
`LawReferenceService` finds "section 45 of the County Governments Act" /
"Article 179 of the Constitution" and feeds the "Laws Cited" tab.

**How notices cite laws** — two very different ways:

| | notices | example |
|---|---|---|
| a section / article is cited | 1,587 (7.6%) | "section 36 of the Universities Act" |
| the Act is named in the heading only | 9,786 (46.6%) | "THE LAND REGISTRATION ACT (No. 3 of 2012)" |
| no law named | ~9,600 | probate cause lists, tenders |

- 480 distinct Acts are named; the top 10 cover most notices: Land
  Registration Act 8,643 · Uncollected Goods 316 · County Governments 286 ·
  Land Act 152 · Physical and Land Use Planning 141 · Environmental
  Management and Co-ordination 141 · Insolvency 113 · Companies 103 ·
  Universities 91 · Water 83.
- Most-cited sections we do not hold: Universities Act s.36 (74), Companies
  Act s.897 (51), Mining Act s.34 (50), Political Parties Act s.20 (44),
  Land Act s.112 (41), Competition Act s.46 (38), State Corporations Act s.6
  (36), Land Act s.162 (28), Water Act s.66 (26).
- **The heading-only case is the big one** — 8,614 of the 9,786 are Land
  Registration Act notices. They do not say which section, but the *kind* of
  notice does: a lost-title replacement notice is always issued under the
  same section. Our templates already know the kind (land, change of name,
  appointments...). So most of the value comes from mapping notice kind →
  section, not from parsing citations.

**Things that can go wrong:**
- Laws change. A 2022 notice relied on the Act as it stood in 2022; Kenya
  Law shows the current version. Each law file records its version date, and
  the page says which version is shown.
- Acts are renamed or repealed (the Physical Planning Act → the Physical and
  Land Use Planning Act, 2019). Old names must still resolve.
- Names are printed inconsistently: glued words, "Co-ordination" /
  "Coordination", with or without "(Cap. 300)" / "(No. 3 of 2012)".
- An AI writing about law can invent a provision. Any quote must come from
  the stored text, word for word, and be checked.

## 3. Design

**1. Law files (reference data, in git, reviewable).** One JSON per law in
`src/main/resources/reference/laws/<key>.json`, the schema the builder
already writes (sections keyed by number, each with its text and clauses,
schedules), plus a catalog `laws/index.json`:

```
{ "key": "land_registration_act", "title": "Land Registration Act",
  "citation": "No. 3 of 2012", "cap": null,
  "names": ["Land\\s*Registration\\s*Act"],           # how notices print it
  "former_names": [], "status": "in force",
  "version_date": "2024-…", "source_url": "https://new.kenyalaw.org/…" }
```

The names come from the catalog, not from code, so adding a law (or an old
name) is a data change — the "learn from every gazette" principle (Phase 4b).

**2. Resolver** (`law_refs.py` = Java `LawReferenceService`, extended):
- explicit citations, as today, for every law in the catalog;
- **Act-level link** when the heading names an Act and no section is cited
  (shown as the Act, with its long title and a link to the source);
- **implied section** from the notice kind: `laws/implied.json` maps a
  template result to a section, e.g. *land / lost title replacement → Land
  Registration Act s.N*. Every mapping is checked by reading the section's
  text before it is added, and is shown as "the section this kind of notice
  is issued under", never as if the notice cited it.

**3. The article.** The generation step receives a short "LAW CONTEXT": at
most two provisions (the explicit or implied ones), exact text, trimmed to
about 1,500 characters. Rules for the model: it *may* add one short passage
explaining the law; any quotation must be copied from LAW CONTEXT inside
quotation marks and name its section; no interpretation beyond plain
meaning. **A checker** then finds every quoted passage in the article and
confirms it appears word for word in the stored text; if not, the quote is
removed (or the article regenerated). Template-written articles get one
fixed sentence ("This notice is issued under section N of the … Act, which
provides that "…".") filled from the stored text — no AI involved.

**4. On the page.** "Laws Cited" shows explicit citations, the Act-level
link and implied sections (labelled), each with the version date and the
source link.

**5. Building the library.** `tools/build_law_reference.py` in batch mode
from a list in citation order (`python tools/law_refs.py` "add next"). Each
source page is downloaded once into `raw/law/` (gitignored) — with your
approval for the downloads — and rebuilt when Kenya Law publishes a newer
version. A health number tracks "citations resolved" per issue, and unknown
Act names are listed for the next batch.

## 4. Rules and fail-safe

- Law text is never written or paraphrased by AI into the library; it is the
  published text, whitespace tidied.
- A citation of a section we do not hold is shown as "not in Smart Gazette
  yet" (found = false), never guessed.
- A quote that is not word for word in the stored text never reaches the
  article.
- Repealed laws stay in the catalog (old notices cite them), marked repealed.

## 5. Acceptance criteria

1. Coverage: notices with at least one resolved law link, before → after
   (today: explicit citations resolved for 398 notices).
2. Implied sections: every mapping reviewed against the Act's text; a review
   of 30 notices shows the right section.
3. Quotes: the checker finds 0 quotes that are not in the stored text on a
   corpus run; 20 generated articles read for accuracy.
4. Python = Java for the resolver on all 20,980 notices; JUnit cases.
5. Held-out check on No 166 / No 175 (new wordings, new Acts).

## 6. Order of work

1. Catalog + the top 10 Acts by notices (above) + the Acts behind the most
   cited sections (Universities, Mining, Political Parties, Competition,
   State Corporations, Water, Elections).
2. Implied sections for the templated kinds (land first: 8,614 notices).
3. LAW CONTEXT + quote checker in article generation.
4. Next batches from the "add next" list.

## 7. Questions for you

1. **Downloads:** each Act is one page from Kenya Law (new.kenyalaw.org).
   Approve fetching the first batch (~17 pages)? Kenya Law's terms of use
   should be checked before bulk download.
2. **First batch size:** top ~17 (recommended), or top 40?
3. **AI quotes or template sentence only?** Recommended: both — the template
   sentence always; AI explanation allowed only with the quote checker.
4. **Old versions:** show the current version with its date (recommended),
   or keep point-in-time versions so a 2022 notice shows the 2022 text?
