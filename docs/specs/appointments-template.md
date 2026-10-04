# Spec: appointments template

Status: **implemented** (4 Oct 2026; one record per person and the schema additions approved; results in section 8)

## 1. Requirement

Read appointment notices into fields **without an AI extraction call**, one
record per appointed person, complete enough to fill one row of the planned
collective appointments table (one article = introduction + table, `reports/PLAN.md`
Phase 6) and to feed the planned person / body links (IEBC-style "who sits
where"). Appointments is the next category with no template: 641 notices in
2022–2026 (250 in 2025 alone).

## 2. Analysis (2022–2026, 641 notices routed to appointments)

One formula carries most notices:

> THE <ACT> (Cap. n) <BODY> APPOINTMENT · IN EXERCISE of the powers conferred
> by <provision> of the <Act>, [I, <name>, President …,] <authority>
> (re-)appoint(s)- <names> to be / as <role> of <body>, for a period of
> <term>, with effect from <date>. · Dated the <date>. · <SIGNATORY>, <title>.

| shape | notices | handling |
|---|---|---|
| the formula (prototype reads it) | 523 (82%) | **template** |
| tables / schedules (county committees, commissioners for oaths, 70-name inspector lists) | 42 (7%) | refuse → AI / table lane later |
| other shapes: WHEREAS taskforces, "it is notified … has been appointed", customs areas and transit sheds (places, not people), court guardianship (Mental Health Act - misfiled) | 67 (10%) | refuse → AI path |
| verb found, role not found | 9 | refuse |

Inside the 523 formula notices:

| element | share |
|---|---|
| term printed ("for a period of three (3) years") | 92% |
| effective date printed | 95% |
| re-appointment | 19% |
| revokes an earlier appointment ("... vide Gazette Notice No. 450 of 2024 is revoked", "*G.N. 401/2025") | 18% |
| several persons in one notice | 27% |
| a provision per person ("Under paragraph (d)- X") | 15% |
| mixed roles ("Chairperson and Members", "X-Chairperson; Members: ...") | 5% |
| honorifics ("(DR.)", "Eng.", "Amb.", "(RTD.) GEN.") | 29% |
| signed by a Cabinet Secretary / the President | 69% / 22% |

Print defects to tolerate (lesson 26): glued words ("DOE**to** be",
"ROE**as** a", "Health**appoints**-", "Rott**to** be"), "appoints:" /
"appoints--", page-footer residue inside long name lists ("83848384217").

## 3. Output - one record per person

Existing schema fields (`schemas/field/appointments.json`), plus **proposed
additions (+)** for the collective table and the links:

| field | rule |
|---|---|
| `person_name` | one name from the list, honorific moved out |
| + `honorific` | "Dr.", "Prof.", "Eng.", "Amb.", "Rtd. Gen." ... as printed |
| `position` | "Member", "Non-Executive Chairperson", "Chancellor" ... (role without the body) |
| `agency` | the body: "Council of the Machakos University", "Board of Directors of the Kenya Rural Roads Authority" |
| `appointment_type` | `appointment` / `re-appointment` / `revocation` |
| `appointing_authority` | "Cabinet Secretary for Education", "President", "Judicial Service Commission" |
| `term_length` | "three (3) years" |
| `effective_date` | as printed |
| `revokes_gn_number` | the earlier notice revoked, "450/2024" |
| + `act` | the Act on the heading line ("The Universities Act") |
| + `legal_provision` | "section 36 (1) (d)" (+ the per-person "Under paragraph (d)" when printed) |
| + `signatory` | "JANE ROE" (as printed) |
| + `date_signed` | "28th November, 2025" |
| `notice_id` | from the notice's own first line |

## 4. Rules and fail-safe

- Only the formula: an (re-)appoint / revoke-the-appointment verb, a name
  list, then "to be" / "as" + a role. Anything else → null → AI path.
- **"as read with"** is a legal phrase, never a role.
- A role about a place ("customs area", "transit shed", "for the purposes of") → refuse.
- **Names are the key.** Each name goes through the shared cleaner and must
  look like a personal name (2–6 words, no verb, no "Board"/"Act"/"Council",
  ≤ 60 characters), otherwise the whole notice is refused - never a partial list.
- Mixed roles: read only when every name is labelled ("Chairperson:" /
  "-Chairperson" / "Members:"); otherwise refuse.
- Tables / schedules and name lists over 40 names → refuse (table lane later).
- A name word that is a verb or particle ("to", "be", "as", "is", "serve")
  means the list was split badly ("John Doe be Roe", "Is is Doe"):
  refuse. "to serve as" is a role form, not part of the name.
- Verb forms read: (re-)appoint(s), "revokes the appointment of", "makes the
  following appointment" (seen first in held-out No 175).

## 5. Acceptance criteria

1. ≥ 90% of the formula notices read (≥ 75% of all 641).
2. Review of 50 random notices: no wrong person name, no person missing from a list.
3. Out-of-scope shapes (tables, taskforces, customs places, guardianship) refused.
4. Held-out No 166 / No 175: ≥ 90% of their formula appointment notices read.
5. Python = Java on all appointment notices; JUnit cases (single, list, re-appointment
   with revocation, per-person paragraphs, glued words, refusals).
6. No change in other categories' template rates.

## 6. Where it plugs in

`category_census.template_for('appointments')` → `NoticeTemplates.extract`
case `Appointments` (returns a list, one record per person; `GazetteService`
already stores lists) → `GazetteService.templateExtract`.

## 7. Test plan

`tools/audit.py` cases; `NoticeTemplatesTest`; scorecard in
`tools/appointments_template.py` (`__main__`); held-out via the cached
No 166 / 175 text.

## 8. Results (4 Oct 2026)

| criterion | result |
|---|---|
| 1. corpus read | **497 of 641 (77.5%)**, 866 people; ~93% of the formula notices (not read: 67 other shapes, 42 tables, 18 names check, 6 places, 5 mixed roles, 5 no role, 1 list over 40) |
| 2. review | two samples of 50 notices: the first found one wrong name ("X to serve") - fixed, and the new rule then refused 4 more badly split names; the second: 50/50 notices, every name right, nobody missing from a list |
| 3. out of scope refused | tables, customs places, taskforces, guardianship (audit + JUnit) |
| 4. held-out | No 166 15/15; No 175 10/11 at first ("makes the following appointment-" not in the corpus), 11/11 after (so No 175 is no longer unseen for this template); its 4 refusals are correct (2 transit sheds, a schedule, an exploratory committee) |
| 5. parity / JUnit | Python = Java on all 641 appointment notices; `NoticeTemplatesTest` 18 pass |
| 6. other categories | unchanged (parity identical on 18,410 other notices) |

AI extraction calls left on the held-out issues: No 166 **60 → 45**, No 175 **63 → 52**.

Field fill on the people read: name, position, type, authority 100%; provision 98%;
signatory / date signed 97%; body 97%; act 96%; effective date 95%; term 92%;
honorific 21%; revoked notice 15%.

Known limits (refused or left empty, never guessed):
- an unlabelled "Chairperson and Members" list (who chairs is not printed);
- a surname the shared cleaner reads as a verb ("... Were" → refused);
- the person whose appointment is revoked ("The appointment of X … is revoked") is
  not a record yet - only the revoked notice number is kept;
- dates stay as printed: `GazetteService` now falls back to the issue's date (not
  today) when a printed date is not ISO.
