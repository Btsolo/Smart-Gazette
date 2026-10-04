package com.smartgazette.smartgazette.service;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The law library (reference/laws.json + 42 laws): citations resolve to stored text. CI-safe. */
class LawReferenceServiceTest {

    private static final LawReferenceService SVC = new LawReferenceService();

    @BeforeAll
    static void load() throws Exception {
        Method m = LawReferenceService.class.getDeclaredMethod("load");     // @PostConstruct outside Spring
        m.setAccessible(true);
        m.invoke(SVC);
    }

    @Test
    void sectionOfANewActResolves() {
        List<LawReferenceService.LawRef> r = SVC.refs("pursuant to section 33 (5) of the Land Registration Act, 2012");
        assertEquals(1, r.size());
        assertEquals("land_registration_act", r.get(0).law());
        assertTrue(r.get(0).found());
        assertEquals("Lost or destroyed certificates and registers", r.get(0).title());
    }

    @Test
    void theLongerTitleWins() {
        // "Land Act" must not swallow "Land Registration Act", nor the reverse
        assertEquals("land_act", SVC.refs("under section 107 of the Land Act").get(0).law());
        assertEquals("land_registration_act", SVC.refs("under section 33 of the LandRegistration Act").get(0).law());
    }

    @Test
    void spellingVariantsOfAName() {
        assertEquals("environmental_management_and_co_ordination_act",
                SVC.refs("section 58 of the ENVIRONMENTAL MANAGEMENT AND COORDINATION ACT").get(0).law());
    }

    private static final String LOST_TITLE = "GAZETTE NOTICE NO. 1\nTHE LAND REGISTRATION ACT (No. 3 of 2012)\n"
            + "ISSUE OF A NEW LAND TITLE DEED\nWHEREAS John Doe is registered as proprietor of all that piece of land "
            + "known as Example/Block 1/2, and whereas sufficient evidence has been adduced to show that the land title "
            + "deed issued thereof has been lost, notice is given that after the expiration of sixty (60) days from the "
            + "date hereof, I shall issue a new land title deed provided that no objection has been received.";

    @Test
    void kindOfNoticeGivesItsSection() {
        List<LawReferenceService.LawRef> r = SVC.lawsFor(LOST_TITLE);
        assertEquals(1, r.size());
        assertEquals("implied", r.get(0).kind());
        assertEquals("section 33(3)", r.get(0).label());
        assertTrue(r.get(0).text().contains("sixty days"));
    }

    @Test
    void dispensingWithTheTitleIsSection31() {
        String n = LOST_TITLE.replace("ISSUE OF A NEW LAND TITLE DEED", "REGISTRATION OF INSTRUMENT")
                .replace("I shall issue a new land title deed", "I intend to dispense with the production of the said land title deed");
        assertEquals("section 31(1)", SVC.lawsFor(n).get(0).label());
    }

    @Test
    void anActNamedInTheHeadingIsLinked() {
        List<LawReferenceService.LawRef> r = SVC.lawsFor("GAZETTE NOTICE NO. 2\nTHE WATER ACT\nNOTICE is given of new tariffs.");
        assertEquals("act", r.get(0).kind());
        assertEquals("water_act", r.get(0).law());
    }

    @Test
    void articleEndsWithTheLawQuotedFromStoredText() {
        String a = SVC.withLaw("The Land Registrar will issue a new title deed after sixty days.", LOST_TITLE);
        assertTrue(a.contains("This notice is issued under section 33(3) of the Land Registration Act"), a);
        assertTrue(a.contains("“If the Registrar is satisfied"), a);
    }

    @Test
    void anInventedQuoteIsRemovedAGenuineOneKept() {
        List<String> sources = List.of("the Registrar may issue a replacement certificate of title upon the expiry of sixty days");
        String checked = LawReferenceService.checkQuotes(
                "The law says \"the Registrar may issue a replacement certificate of title\". "
                        + "It also says \"owners must pay a penalty of one million shillings\". Objections are welcome.", sources);
        assertTrue(checked.contains("may issue a replacement certificate"), checked);
        assertTrue(!checked.contains("one million"), checked);
        assertTrue(checked.contains("Objections are welcome."), checked);
    }

    @Test
    void aSectionWeDoNotHoldIsNotGuessed() {
        LawReferenceService.LawRef r = SVC.refs("section 999 of the Water Act, 2016").get(0);
        assertEquals("water_act", r.law());
        assertTrue(!r.found());
    }
}
