package com.smartgazette.smartgazette.service.templates;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression cases for the rule-based templates, from real notice shapes
 * (names replaced). They run without the corpus, so a CI build checks them.
 * The full parity check against tools/ runs on the private corpus.
 */
class NoticeTemplatesTest {

    private static final String PROBATE = "GAZETTE NOTICE NO. 8060\n"
            + "IN THE HIGH COURT OF KENYA AT NYERI\nPROBATE AND ADMINISTRATION\n"
            + "TAKE NOTICE that applications having been made in this court in:\n"
            + "CAUSE NO. E8 OF 2024\n"
            + "By (1) John Doe and (2) Jane Doe, both of P.O. Box 815-10101, Karatina in Kenya, "
            + "the deceased's son and daughter, respectively, for a grant of letters of administration intestate "
            + "to the estate of Peter Doe, who died at Karatina on 8th August, 2023.\n"
            + "CAUSE NO. E9 OF 2024\n"
            + "By Mary Roe, the deceased's widow, through Messrs. A. B.\nExample & Co., advocates, "
            + "for a grant of probate of written will to the estate of Paul Roe, who died on 3rd January, 2022.\n"
            + "The Court will proceed to issue the same unless cause be shown to the contrary and appearance in this "
            + "respect entered within thirty (30) days from the date of publication of this notice.\n";

    @Test
    @SuppressWarnings("unchecked")
    void probateNoticeGivesOneRecordPerCause() {
        Object r = NoticeTemplates.extract("Court_Legal", PROBATE);
        assertInstanceOf(List.class, r);
        List<Map<String, Object>> recs = (List<Map<String, Object>>) r;
        assertEquals(2, recs.size());
        Map<String, Object> a = recs.get(0);
        assertEquals("E8 OF 2024", a.get("case_reference"));
        assertEquals("Peter Doe", a.get("deceased_name"));
        assertEquals(List.of("John Doe", "Jane Doe"), a.get("petitioner_names"));
        assertEquals("8th August, 2023", a.get("date_of_death"));
        assertEquals("Karatina", a.get("place_of_death"));
        assertEquals("son and daughter", a.get("petitioner_relationship"));
        assertEquals("Grant Of Letters Of Administration Intestate", a.get("notice_subtype"));
        assertEquals("thirty (30) days from the date of publication", a.get("filing_deadline"));
        assertTrue(String.valueOf(a.get("court_name")).startsWith("HIGH COURT OF KENYA AT NYERI"));
        Map<String, Object> b = recs.get(1);
        assertEquals("A. B. Example & Co", b.get("advocate_firm"));
        assertEquals("grant of probate", b.get("action_type"));
    }

    @Test
    void probateIsAllOrNothing() {
        String broken = PROBATE.replace("who died on 3rd January, 2022.", "");
        assertNull(NoticeTemplates.extract("Court_Legal", broken.replace("Paul Roe,", "")));
    }

    @Test
    void formerlyCauseNumberDoesNotSplitTheBlock() {
        List<String> blocks = ProbateTemplate.splitCauses("CAUSE NO. E5 OF 2025 (Formerly CAUSE NO. 285 OF 2024) By X");
        assertEquals(1, blocks.size());
        assertTrue(blocks.get(0).contains("(Formerly CAUSE NO. 285 OF 2024)"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void landTitleNotice() {
        String n = "GAZETTE NOTICE NO. 1\nTHE LAND REGISTRATION ACT\n(No. 3 of 2012)\nISSUE OF A NEW LAND TITLE DEED\n"
                + "WHEREAS John Doe, of P.O. Box 5, Kisumu in the Republic of Kenya, is registered as proprietor of all "
                + "that piece of land containing 2.78 hectares or thereabout, situate in the district of Kisumu, "
                + "registered under title No. Kisumu/Karateng/1874, and whereas sufficient evidence has been adduced "
                + "to show that the land title deed issued thereof has been lost, notice is given that after the "
                + "expiration of sixty (60) days from the date hereof, I shall issue a new land title deed.";
        Map<String, Object> r = (Map<String, Object>) NoticeTemplates.extract("Land_Property", n);
        assertNotNull(r);
        assertEquals("Kisumu/Karateng/1874", r.get("parcel_id"));
        assertEquals(List.of("John Doe"), r.get("parties"));
        assertEquals("Kisumu", r.get("county"));
        assertEquals("sixty (60) days", r.get("objection_period"));
        assertEquals(Boolean.FALSE, r.get("is_ownership_change"));
    }

    @Test
    void landParcelWithoutCommaBeforeTheNextClause() {
        String n = "GAZETTE NOTICE NO. 1\nTHE LAND REGISTRATION ACT\nISSUE OF A NEW LAND TITLE DEED\n"
                + "WHEREAS John Doe, of P.O. Box 5, Kisumu, is registered as proprietor of all that piece of land "
                + "registered under ttle No Kisumu/Karateng/1874 and whereas the land title deed has been lost";
        @SuppressWarnings("unchecked")
        Map<String, Object> r = (Map<String, Object>) NoticeTemplates.extract("Land_Property", n);
        assertNotNull(r);
        assertEquals("Kisumu/Karateng/1874", r.get("parcel_id"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void acquisitionCorrigendumTables() {
        String n = "GAZETTE NOTICE NO. 1\nTHE LAND ACT (No. 6 of 2012) CONSTRUCTION OF MERU-MIKINDURI-MAUA ROAD PROJECT "
                + "DELETION, CORRIGENDA AND ADDENDUM IN PURSUANCE of the Land Act and further to Gazette Notice Nos. "
                + "5621 of 2009 and 5785 of 2023, the National Land Commission on behalf of Kenya Rural Roads "
                + "Authority (KeRRA) gives notice.\n"
                + "Deletion\nParcel Number | Registered Owner | Area Acq. (Ha)\n--- | --- | ---\n"
                + "Akachiu/Auki/334 | Charles Doe | 0.0113\nAkachiu/Auki/288 | Sebastian Doe | 0.0192\n"
                + "Addendum\nParcel Number | Registered Owner | Area Acq. (Ha)\n--- | --- | ---\n"
                + "Akachiu/Auki/632 | 0.0786\nAkachiu/Auki/ 25 | John Doe | 0.1084\n"
                + "Akachiu/Auki/40 Jane Doe 0.0017 | TBD | 0.0050\n";
        Map<String, Object> r = (Map<String, Object>) NoticeTemplates.extract("Corrigenda", n);
        assertNotNull(r);
        assertEquals("Correction of a compulsory acquisition schedule", r.get("notice_subtype"));
        assertEquals("Kenya Rural Roads Authority", r.get("acquiring_body"));
        assertEquals(List.of("5621 of 2009", "5785 of 2023"), r.get("related_notices"));
        List<Map<String, Object>> secs = (List<Map<String, Object>>) r.get("sections");
        assertEquals("Deletion", secs.get(0).get("section"));
        assertEquals("Addendum", secs.get(1).get("section"));
        List<Map<String, String>> add = (List<Map<String, String>>) secs.get(1).get("parcels");
        assertEquals(Map.of("parcel_id", "Akachiu/Auki/632", "owner", "", "area_ha", "0.0786"), add.get(0));
        assertEquals("Akachiu/Auki/25", add.get(1).get("parcel_id"));
        assertEquals(1, r.get("rows_not_read"));
    }

    @Test
    @SuppressWarnings("unchecked")
    void acquisitionScheduleGroupedByInquiryDate() {
        // ruled schedule (fix 7b): date / venue rows merged across the table are labels, not records
        String n = "GAZETTE NOTICE NO. 6\nTHE LAND ACT (No. 6 of 2012) CONSTRUCTION OF EXAMPLE ROAD PROJECT INQUIRY "
                + "the National Land Commission on behalf of Kenya National Highways Authority, (KeNHA) gives notice.\n"
                + "SCHEDULE\nParcel No. | Registered Owner (s) | Area to Acquire (Ha.)\n--- | --- | ---\n"
                + "Tuesday, 1st July, 2025 at Example Chief's Office from 9.00 a.m. to 4.00 p.m. |\n"
                + "Example/Block I/1 | Jane Doe | 0.10\n"
                + "Wednesday, 2nd July, 2025 at Example Chief's Office from 9.00 a.m. to 4.00 p.m. |\n"
                + "Example/Block I/2 | TBD | 0.20\n";
        Map<String, Object> r = (Map<String, Object>) NoticeTemplates.extract("Land_Property", n);
        assertNotNull(r);
        assertEquals(2, r.get("parcel_count"));
        assertEquals(0, r.get("rows_not_read"));
    }

    @Test
    void probateCorrigendum() {
        String n = "GAZETTE NOTICE NO. 2\nCORRIGENDUM\nIN Gazette Notice No. 5121 of 2022,\n"
                + "CAUSE NO. E7 of 2020, amend the deceased's name printed as \"Harrison Doe\" to read \"Harrison Roe\".";
        @SuppressWarnings("unchecked")
        Map<String, Object> r = (Map<String, Object>) NoticeTemplates.extract("Corrigenda", n);
        assertNotNull(r);
        assertEquals("E7 of 2020", r.get("cause_reference"));
        assertEquals("5121", r.get("amends_notice"));
    }

    private static final String DEED_POLL = "GAZETTE NOTICE NO. 1\nCHANGE OF NAME NOTICE is given that by a deed poll "
            + "dated 11th September, 2026, duly executed and registered in the Registry of Documents at Nairobi as "
            + "Presentation No. 463, in Volume D1, Folio 362/3386, File No.\nMMXXVI, by our client, John Doe, of P.O. Box "
            + "111-10206, Thika in the Republic of Kenya, formerly known as John Roe, formally and absolutely renounced "
            + "and abandoned the use of his former name John Roe and in lieu thereof assumed and adopted the name John "
            + "Doe, for all purposes and authorizes and requests all persons at all times to designate, describe and "
            + "address him by his assumed name John Doe only.\nEXAMPLE & COMPANY, Advocates for John Doe.";

    @SuppressWarnings("unchecked")
    private static Map<String, Object> changeOfName(String n) {
        return (Map<String, Object>) NoticeTemplates.extract("Change_of_Name", n);
    }

    @Test
    void changeOfNameDeedPoll() {
        Map<String, Object> r = changeOfName(DEED_POLL);
        assertNotNull(r);
        assertEquals("John Roe", r.get("former_name"));
        assertEquals("John Doe", r.get("assumed_name"));
        assertEquals("Nairobi", r.get("registry"));
        assertEquals("463", r.get("presentation_number"));
        assertEquals("D1", r.get("volume"));
        assertEquals("362/3386", r.get("folio"));
        assertEquals("MMXXVI", r.get("file_number"));
        assertEquals("advocates", r.get("filed_by"));
        assertEquals("EXAMPLE & COMPANY", r.get("advocate_firm"));
        assertEquals(List.of("John Doe"), r.get("applicants"));
        assertEquals("11th September, 2026", r.get("deed_poll_date"));
        assertEquals("Kenya", r.get("citizenship"));
        assertEquals(Boolean.FALSE, r.get("on_behalf_of_minor"));
        assertEquals("1", r.get("notice_id"));
    }

    @Test
    void changeOfNameSelfFiledHasNoFirm() {
        Map<String, Object> r = changeOfName(DEED_POLL.replace("by our client,", "by me,")
                .replace("EXAMPLE & COMPANY, Advocates for John Doe.", "JOHN DOE."));
        assertNotNull(r);
        assertEquals("self", r.get("filed_by"));
        assertNull(r.get("advocate_firm"));
    }

    @Test
    void changeOfNameMinorWithGuardians() {
        Map<String, Object> r = changeOfName(DEED_POLL
                .replace("by our client, John Doe, of", "by our clients, (1) Jane Doe and (2) Jim Doe (Guardians), both of")
                .replace("Kenya, formerly known", "Kenya, on behalf of Baby Doe (minor), formerly known"));
        assertNotNull(r);
        assertEquals(Boolean.TRUE, r.get("on_behalf_of_minor"));
        assertEquals(List.of("Jane Doe", "Jim Doe"), r.get("applicants"));
    }

    @Test
    void changeOfNameKeepsTitlesAndToleratesMisprintedDates() {
        assertEquals("Dr. John Doe", changeOfName(DEED_POLL.replace("adopted the name John Doe", "adopted the name Dr. John Doe"))
                .get("assumed_name"));
        Map<String, Object> r = changeOfName(DEED_POLL.replace("dated 11th September, 2026", "dated 16th September August, 2026"));
        assertNotNull(r);
        assertNull(r.get("deed_poll_date"));
    }

    @Test
    void changeOfNameWordingVariants() {
        // both seen in held-out No 166
        Map<String, Object> r = changeOfName(DEED_POLL.replace("assumed and adopted the name", "in lieu thereof adopted the name")
                .replace("Folio 362", "Folio. 362"));
        assertNotNull(r);
        assertEquals("John Doe", r.get("assumed_name"));
        assertEquals("362/3386", r.get("folio"));
        r = changeOfName(DEED_POLL.replace("assumed and adopted the name", "assumed and re-adopted the name"));
        assertNotNull(r);
        assertEquals("John Doe", r.get("assumed_name"));
    }

    @Test
    void changeOfNameRefusals() {
        // the two printed former names disagree -> the AI path decides
        assertNull(changeOfName(DEED_POLL.replace("his former name John Roe", "his former name Peter Poe")));
        // a company renaming is not a deed poll
        assertNull(changeOfName("GAZETTE NOTICE NO. 2\nTHE CENTRAL BANK OF KENYA ACT (Cap. 491) CHANGE OF NAME IT IS "
                + "notified that Example Microfinance Bank Limited has changed its name to Sample Microfinance Bank Limited."));
    }

    private static final String APPOINTMENT = "GAZETTE NOTICE NO. 3\nTHE UNIVERSITIES ACT (Cap. 210) EXAMPLE UNIVERSITY "
            + "APPOINTMENT IN EXERCISE of the powers conferred by section 36 (1) (a) of the Universities Act, the Cabinet "
            + "Secretary for Information, Communications and the Digital Economy appoints- JOHN DOE (DR.) to be the "
            + "Non-Executive Chairperson of the Council of Example University, for a period of three (3) years, with effect "
            + "from the 28th November, 2025. The appointment* of Jim Roe is revoked.\nDated the 27th November, 2025.\n"
            + "JANE ROE, Cabinet Secretary for Information, Communications and the Digital Economy.\n*G.N. 401/2025\n";

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> appointments(String n) {
        return (List<Map<String, Object>>) NoticeTemplates.extract("Appointments", n);
    }

    @Test
    void appointmentOnePerson() {
        List<Map<String, Object>> r = appointments(APPOINTMENT);
        assertNotNull(r);
        assertEquals(1, r.size());
        Map<String, Object> a = r.get(0);
        assertEquals("JOHN DOE", a.get("person_name"));
        assertEquals("DR", a.get("honorific"));
        assertEquals("Non-Executive Chairperson", a.get("position"));
        assertEquals("Council of Example University", a.get("agency"));
        assertEquals("appointment", a.get("appointment_type"));
        // the title is kept whole: a later ", the" is inside it
        assertEquals("Cabinet Secretary for Information, Communications and the Digital Economy", a.get("appointing_authority"));
        assertEquals("three (3) years", a.get("term_length"));
        assertEquals("28th November, 2025", a.get("effective_date"));
        assertEquals("401/2025", a.get("revokes_gn_number"));
        assertEquals("Universities Act", a.get("act"));
        assertEquals("section 36 (1) (a)", a.get("legal_provision"));
        assertEquals("JANE ROE", a.get("signatory"));
        assertEquals("27th November, 2025", a.get("date_signed"));
        assertEquals("3", a.get("notice_id"));
    }

    @Test
    void appointmentListGivesOneRecordPerPerson() {
        List<Map<String, Object>> r = appointments(APPOINTMENT.replace("JOHN DOE (DR.) to be the Non-Executive Chairperson",
                "Under paragraph (a)- John Doe - Chairperson, Under paragraph (d)- Jane Doe, Jim Doe (Prof.), "
                        + "to be the Chairperson and Members"));
        assertNotNull(r);
        assertEquals(3, r.size());
        assertEquals(List.of("John Doe", "Jane Doe", "Jim Doe"), r.stream().map(x -> x.get("person_name")).toList());
        assertEquals(List.of("Chairperson", "Member", "Member"), r.stream().map(x -> x.get("position")).toList());
        assertEquals("section 36 (1) (a); paragraph (d)", r.get(2).get("legal_provision"));
        assertEquals("Prof", r.get(2).get("honorific"));
    }

    @Test
    void appointmentGluedWordsAndReappointment() {
        List<Map<String, Object>> r = appointments(APPOINTMENT.replace("appoints- JOHN DOE (DR.) to be", "re-appoints- JOHN DOEto be"));
        assertNotNull(r);
        assertEquals("JOHN DOE", r.get(0).get("person_name"));
        assertEquals("re-appointment", r.get(0).get("appointment_type"));
        assertEquals("JOHN DOE", appointments(APPOINTMENT.replace("JOHN DOE (DR.) to be", "JOHN DOE to serve as"))
                .get(0).get("person_name"));
        // seen first in held-out No 175
        Map<String, Object> cj = appointments(APPOINTMENT.replace(
                "the Cabinet Secretary for Information, Communications and the Digital Economy appoints- JOHN DOE (DR.) to be",
                "the Chief Justice makes the following appointment- HON. JOHN DOE as")).get(0);
        assertEquals("JOHN DOE", cj.get("person_name"));
        assertEquals("HON", cj.get("honorific"));
        assertEquals("Chief Justice", cj.get("appointing_authority"));
    }

    @Test
    void appointmentRefusals() {
        // one fragmented name refuses the whole list - never a partial list
        assertNull(appointments(APPOINTMENT.replace("JOHN DOE (DR.)", "John Doe, Jane Roe to")));
        // who is the chair is not printed -> not guessed
        assertNull(appointments(APPOINTMENT.replace("JOHN DOE (DR.) to be the Non-Executive Chairperson",
                "Jane Doe, Jim Doe, to be the Chairperson and Members")));
        // tables are for the table lane
        assertNull(appointments("GAZETTE NOTICE NO. 4\nAPPOINTMENT OF COMMISSIONERS FOR OATHS\nS/No. | Name\n1. | John Doe\n"));
    }

    @Test
    void otherCategoriesHaveNoTemplate() {
        assertNull(NoticeTemplates.extract("Tenders", PROBATE));
    }
}
