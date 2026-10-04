package com.smartgazette.smartgazette.service;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Reference watch (docs/specs/reference-watch.md): signs that a law we hold has changed. CI-safe, no database. */
class ReferenceWatchTest {

    private static final LawReferenceService LAWS = new LawReferenceService();
    private static ReferenceWatch watch;

    @BeforeAll
    static void load() throws Exception {
        Method m = LawReferenceService.class.getDeclaredMethod("load");
        m.setAccessible(true);
        m.invoke(LAWS);
        watch = new ReferenceWatch(LAWS, null);
    }

    @Test
    void anAmendmentActNewerThanOurCopy() {
        List<ReferenceWatch.Flag> f = watch.flags("GAZETTE NOTICE NO. 9\nTHE WATER ACT\n"
                + "IN EXERCISE of the powers conferred by section 72 of the Water Act, as amended by the Water (Amendment) Act, 2099, the Board gives notice.");
        assertEquals(1, f.size(), f.toString());
        assertEquals("law.amended_after_copy", f.get(0).watcher());
        assertEquals("water_act", f.get(0).key());
        assertTrue(f.get(0).action().contains("raw/law/"), f.get(0).action());
    }

    @Test
    void anAmendmentAlreadyInOurCopyRaisesNothing() {
        assertTrue(watch.flags("pursuant to the Urban Areas and Cities (Amendment) Act, 2019, the Governor appoints.").isEmpty());
    }

    @Test
    void aCitedSectionOurCopyDoesNotHave() {
        List<ReferenceWatch.Flag> f = watch.flags("pursuant to section 999 of the Land Act, 2012");
        assertEquals("law.missing_section", f.get(0).watcher());
        assertEquals("s.999", f.get(0).detail());
        assertFalse(watch.isClosed("land_act", "law.missing_section", "s.999"));
        assertTrue(watch.isClosed("land_act", "law.missing_section", "s.107"));     // after a rebuild that has it
    }

    @Test
    void aSupplementThatAmendsALawWeHold() {
        List<ReferenceWatch.Flag> f = watch.flags("KENYA GAZETTE SUPPLEMENT\nACTS, 2099\nTHE LAND (AMENDMENT) ACT, 2099\n"
                + "AN ACT of Parliament to amend the Land Act and for connected purposes");
        assertTrue(f.stream().anyMatch(x -> x.watcher().equals("law.amending_supplement") && x.key().equals("land_act")), f.toString());
    }

    @Test
    void supplementsAreRecognised() {
        assertTrue(GazetteService.isGazetteSupplement("SPECIAL ISSUE\nKenya Gazette Supplement No. 168 (Acts No. 22)\nREPUBLIC OF KENYA"));
        assertFalse(GazetteService.isGazetteSupplement("THE KENYA GAZETTE\nGAZETTE NOTICE NO. 1\nTHE LAND REGISTRATION ACT"));
    }

    @Test
    void oldCopiesAreReminded() {
        List<ReferenceWatch.Flag> f = watch.oldCopies(LocalDate.of(2026, 10, 5));
        assertTrue(f.stream().anyMatch(x -> x.key().equals("disposal_of_uncollected_goods_act")), f.toString());
        assertFalse(f.stream().anyMatch(x -> x.key().equals("land_act")));          // text as at 2025
    }
}
