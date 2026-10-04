package com.smartgazette.smartgazette.service;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Page-level OCR splicing (= tools/mixed_pages.fill). CI-safe: OCR is a stub. */
class ScanLanePagesTest {

    private static final String RAW =
            "GAZETTE NOTICE NO. 1 THE LAND ACT notice text here\n\f\n\fsecond page with enough words here now";

    @Test
    void textlessPageIsReplacedAtItsPlace() {
        assertEquals(List.of(2), ScanLaneService.textlessPages(RAW));
        ScanLaneService.Filled f = ScanLaneService.fill(RAW, p -> "OCR text of page " + p);
        assertEquals(List.of(2), f.pages());
        assertEquals("GAZETTE NOTICE NO. 1 THE LAND ACT notice text here\n\fOCR text of page 2\n\fsecond page with enough words here now",
                f.text());
    }

    @Test
    void failedOcrLeavesThePageAsItWas() {
        ScanLaneService.Filled f = ScanLaneService.fill(RAW, p -> null);
        assertEquals(List.of(), f.pages());
        assertEquals(RAW, f.text());
    }
}
