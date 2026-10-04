package com.smartgazette.smartgazette.service;

import com.smartgazette.smartgazette.service.FigureClassifier.Box;
import com.smartgazette.smartgazette.service.FigureClassifier.Context;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Figure kinds (= tools/figures.classify); cases from the corpus review. CI-safe: no OCR, no files. */
class FigureClassifierTest {

    private static final Context NOTICE = new Context("10357", false, "[[FIGURE:2.1]]");

    /** a box of w x h inches at (x, y) points on page p */
    private static Box box(int p, double x, double y, double wIn, double hIn) {
        return new Box(p, x, y, x + wIn * 72, y + hIn * 72);
    }

    @Test
    void markerTakesTheNoticeAboveItAndItsTableRow() {
        Map<String, Context> m = FigureClassifier.noticeOfMarkers(
                "[[FIGURE:1.1]]\nGAZETTE NOTICE NO. 15731\nTHE ELECTIONS ACT\nJohn Doe | [[FIGURE:4.2]] | Party\n[[FIGURE:4.3]]");
        assertNull(m.get("1.1").notice());
        assertEquals("15731", m.get("4.2").notice());
        assertTrue(m.get("4.2").inTable());
        assertFalse(m.get("4.3").inTable());
    }

    @Test
    void coverCrestIsTheCoatOfArms() {
        assertEquals("coat_of_arms", FigureClassifier.classify(box(1, 220, 60, 2.9, 2.6), Context.NONE, "ENVA CAA TZE", 1, null));
    }

    @Test
    void symbolInATableCell() {
        Context row = new Context("15731", true, "John Doe | [[FIGURE:4.2]] | Party");
        assertEquals("table_symbol", FigureClassifier.classify(box(4, 300, 400, 0.4, 0.4), row, "", 12, "GAZETTE NOTICE NO. 15731"));
    }

    @Test
    void pageSizedBlockMapFullOfNumbersIsAMap() {
        // 2025 No 163: upside-down scan, OCR mostly parcel numbers and a "%"
        assertEquals("map", FigureClassifier.classify(box(61, 30, 40, 7.3, 8.0), NOTICE,
                "4% 1234 5678 9012 3456 sae am ins", 1, "THE LAND REGISTRATION ACT ... the registry index map ..."));
    }

    @Test
    void chartWithNumbersAndLabels() {
        assertEquals("chart", FigureClassifier.classify(box(12, 60, 100, 5.8, 2.5), NOTICE,
                "3,500 40.0% 3,000 21.7% 13.8% 20.0% Revenue growth", 1, "ANNUAL REPORT"));
    }

    @Test
    void thinStripOfATableImage() {
        // 2023 No 154 p37: a 6.6 x 0.6 in row of financial statements
        assertEquals("table_image", FigureClassifier.classify(box(37, 40, 300, 6.6, 0.6), NOTICE,
                "Kshs Agency Income 25,911,449 20,361,521 Bonus 3,224,208 195,566 29,278,047 Capital Grants", 2, "KENYA REVENUE AUTHORITY"));
    }

    @Test
    void prescribedFormImage() {
        // 2026 No 75: EPRA Form 3 certificate, in a notice that also speaks of coal blocks
        assertEquals("form", FigureClassifier.classify(box(37, 40, 300, 2.7, 2.8), NOTICE,
                "FORM3. The Energy Act, 2019 COAL DRIVER CERTIFICATE", 1, "coal blocks map"));
    }

    @Test
    void smallLocationMapInAConcessionNotice() {
        assertEquals("map", FigureClassifier.classify(box(6, 40, 300, 2.7, 1.8), NOTICE,
                "15 L11 14 L12", 1, "the petroleum blocks set out in the First Schedule"));
    }

    @Test
    void healthWarningImages() {
        assertEquals("prescribed_image", FigureClassifier.classify(box(5, 40, 300, 2.4, 2.9), NOTICE,
                "WARNING!", 8, "the pictorial health warning images set out in the Schedule"));
    }

    @Test
    void tinyMarkAndRepeatedLogo() {
        assertEquals("mark", FigureClassifier.classify(box(30, 40, 300, 0.1, 0.1), NOTICE, "", 29, "x"));
        assertEquals("logo", FigureClassifier.classify(box(36, 40, 300, 0.9, 0.4), NOTICE, "Regulatory Authority", 4, "x"));
    }

    @Test
    void stampTextOnASmallImage() {
        assertEquals("stamp_seal", FigureClassifier.classify(box(1, 400, 500, 1.5, 1.0), NOTICE,
                "NATIONAL COUNCIL FOR LAW REPORTING LIBRARY RECEIVED", 1, "x"));
    }

    @Test
    void reportChartIsNotAMapBecauseItsTablesSayConcession() {
        // 2023 No 154 notice 8883: "KAA Concession Fees" in a statement, "Figure 3" cited
        String report = "KAA Concession Fees | 2,971 | | 2,898 ... as shown in Figure 3 below";
        assertEquals("chart", FigureClassifier.classify(box(14, 40, 300, 6.2, 2.8), NOTICE,
                "2,500 2,000 1,500 2015/16 2016/17 Financial Year Actual Target", 1, report));
        assertEquals("table_image", FigureClassifier.classify(box(39, 40, 300, 5.6, 0.8), NOTICE,
                "14,241 15,306 68 Liabilities Payables Net Foreign currency liability", 1, report));
    }

    @Test
    void scannedKindsGetACleanedCopy() {
        assertTrue(FigureClassifier.scanned("map"));
        assertFalse(FigureClassifier.scanned("chart"));
    }
}
