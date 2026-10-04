package com.smartgazette.smartgazette.service;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Which gazettes the nightly scrape takes from Kenya Law's listing. CI-safe: a synthetic page, no network. */
class GazetteScrapingServiceTest {

    // newest first, as the listing shows them: a special issue and the regular issue on the same day
    private static final Document LISTING = Jsoup.parse("""
            <table>
            <tr><td class="cell-title"><a href="/akn/ke/officialGazette/2026-10-02/176">Kenya Gazette Vol. CXXVIII-No. 176</a></td><td>2 October 2026</td></tr>
            <tr><td class="cell-title"><a href="/akn/ke/officialGazette/2026-10-02/175">Kenya Gazette Vol. CXXVIII-No. 175</a></td><td>2 October 2026</td></tr>
            <tr><td class="cell-title"><a href="/akn/ke/officialGazette/2026-09-25/170">Kenya Gazette Vol. CXXVIII-No. 170</a></td><td>25 September 2026</td></tr>
            </table>""", "https://new.kenyalaw.org/gazettes/2026");

    @Test
    void everyUnprocessedGazetteIsTakenOldestFirst() {
        List<GazetteScrapingService.Listed> fresh = GazetteScrapingService.newGazettes(LISTING, 5,
                g -> g.number().endsWith("170"));                    // 170 was processed last week
        assertEquals(List.of("Vol. CXXVIII-No. 175", "Vol. CXXVIII-No. 176"), fresh.stream().map(GazetteScrapingService.Listed::number).toList());
        assertEquals(LocalDate.of(2026, 10, 2), fresh.get(0).date());
        assertEquals("https://new.kenyalaw.org/akn/ke/officialGazette/2026-10-02/175", fresh.get(0).detailsUrl());
        assertTrue(fresh.get(0).destination().getPath().endsWith("Kenya_Gazette_Vol__CXXVIII_No__175_Dated_2026-10-02.pdf"),
                fresh.get(0).destination().getPath());
    }

    @Test
    void onlyTheNewestFewAreChecked() {
        assertEquals(1, GazetteScrapingService.newGazettes(LISTING, 1, g -> false).size());
        assertTrue(GazetteScrapingService.newGazettes(LISTING, 5, g -> true).isEmpty());
    }
}
