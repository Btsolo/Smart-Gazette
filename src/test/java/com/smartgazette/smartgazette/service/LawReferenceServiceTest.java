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

    @Test
    void aSectionWeDoNotHoldIsNotGuessed() {
        LawReferenceService.LawRef r = SVC.refs("section 999 of the Water Act, 2016").get(0);
        assertEquals("water_act", r.law());
        assertTrue(!r.found());
    }
}
