package com.smartgazette.smartgazette.service;

import com.smartgazette.smartgazette.model.Gazette;
import com.smartgazette.smartgazette.repository.GazetteRepository;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Collections;
import java.util.ArrayList;
import org.jsoup.select.Elements;

@Service
@EnableScheduling
public class GazetteScrapingService {

    private static final Logger log = LoggerFactory.getLogger(GazetteScrapingService.class);

    private static final String KENYA_LAW_GAZETTE_URL = "https://new.kenyalaw.org/gazettes/";

    private final GazetteService gazetteService;
    private final GazetteRepository gazetteRepository;

    @Autowired
    public GazetteScrapingService(GazetteService gazetteService, GazetteRepository gazetteRepository) {
        this.gazetteService = gazetteService;
        this.gazetteRepository = gazetteRepository;
    }

    /** Who is asking: Kenya Law's terms ask for responsible use, so the scraper
     *  names itself instead of posing as a browser. */
    @org.springframework.beans.factory.annotation.Value("${scraper.user-agent:SmartGazette/1.0 (+https://github.com/Btsolo/Smart-Gazette)}")
    private String userAgent;

    /** The daily scrape (on by default; a staging copy can switch it off). */
    @org.springframework.beans.factory.annotation.Value("${scraper.enabled:true}")
    private boolean scraperEnabled;

    /** How many of the newest gazettes on the listing are checked per run. */
    @org.springframework.beans.factory.annotation.Value("${scraper.max-per-run:5}")
    private int maxPerRun;

    /** Pause between two requests to Kenya Law (its robots.txt asks for 5 s). */
    @org.springframework.beans.factory.annotation.Value("${scraper.pause-seconds:5}")
    private int pauseSeconds;

    /**
     * Every evening at 22:00 Nairobi time by default: a notice published during
     * the day (a special issue for a holiday, say) is on the site the same
     * night instead of the next morning. Gazettes come out on any day, so the
     * run is daily. scraper.cron / scraper.zone change it.
     */
    @Scheduled(cron = "${scraper.cron:0 0 22 * * *}", zone = "${scraper.zone:Africa/Nairobi}")
    public void scheduledScrape() {
        if (!scraperEnabled) {
            log.info("Scheduled gazette scrape is switched off (scraper.enabled=false).");
            return;
        }
        scrapeForNewGazettes();
    }

    /** A gazette on the listing page. */
    record Listed(String number, LocalDate date, String detailsUrl, File destination) {}

    /** The gazettes to process from a listing page: of the newest {@code max}
     *  links, those {@code done} says are not processed yet, oldest first. */
    static List<Listed> newGazettes(Document doc, int max, java.util.function.Predicate<Listed> done) {
        Elements links = doc.select("td.cell-title a[href^='/akn/ke/officialGazette/']");
        List<Listed> fresh = new ArrayList<>();
        for (Element link : links.subList(0, Math.min(max, links.size()))) {
            String number = link.text().replace("Kenya Gazette ", "").trim();
            LocalDate date = findDateInTableRow(link);
            Listed g = new Listed(number, date, link.attr("abs:href"), destinationFile(number, date));
            if (!done.test(g)) fresh.add(g);
        }
        Collections.reverse(fresh);                     // oldest first: notices keep their order
        return fresh;
    }

    /**
     * Checks the newest gazettes on Kenya Law's listing and processes every one
     * not processed yet, oldest first, one at a time. "Processed" = a notice
     * points at its saved PDF, so a gazette whose processing failed is tried
     * again on the next run.
     */
    public void scrapeForNewGazettes() {
        log.info("--- STARTING GAZETTE SCRAPE ---");
        String listingUrl = KENYA_LAW_GAZETTE_URL + LocalDate.now().getYear();
        try {
            Document doc = fetch(listingUrl, null);
            if (doc == null) {
                log.error("Could not read the gazette listing {}. Scrape failed.", listingUrl);
                return;
            }
            Elements links = doc.select("td.cell-title a[href^='/akn/ke/officialGazette/']");
            if (links.isEmpty()) {
                log.warn("No gazette links on {} - the page layout may have changed.", listingUrl);
                return;
            }
            List<Listed> fresh = newGazettes(doc, maxPerRun, g ->
                    gazetteRepository.existsByOriginalPdfPath(g.destination().getAbsolutePath())
                    || (g.date() != null && gazetteRepository.findFirstByGazetteNumberAndGazetteDate(g.number(), g.date()).isPresent()));
            if (fresh.isEmpty()) {
                log.info("--- SCRAPE FINISHED: no new gazettes among the newest {} ---", Math.min(maxPerRun, links.size()));
                return;
            }
            log.info("{} new gazette(s) to process: {}", fresh.size(), fresh.stream().map(Listed::number).toList());
            for (Listed g : fresh) {
                if (!download(g, listingUrl)) continue;
                waitUntilIdle();                        // one gazette at a time
                gazetteService.processAndSavePdf(g.destination(), g.destination().getAbsolutePath());
            }
            log.info("--- SCRAPE FINISHED: {} gazette(s) sent for processing ---", fresh.size());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("Unexpected error during scraping: {}", e.getMessage(), e);
        }
    }

    /** Details page -> PDF link -> PDF saved to permanent storage. */
    private boolean download(Listed g, String listingUrl) throws InterruptedException {
        Document details = fetch(g.detailsUrl(), listingUrl);
        Element pdfLink = details == null ? null : details.select("a:contains(Download PDF)").first();
        if (pdfLink == null) {
            log.warn("No 'Download PDF' link for gazette {} ({}).", g.number(), g.detailsUrl());
            return false;
        }
        String pdfUrl = pdfLink.attr("abs:href");
        for (int attempt = 1; attempt <= 3; attempt++) {
            pause();
            try {
                Connection.Response pdf = Jsoup.connect(pdfUrl)
                        .userAgent(userAgent)
                        .referrer(g.detailsUrl())
                        .ignoreContentType(true)
                        .followRedirects(true)
                        .timeout(120000)
                        .maxBodySize(0)
                        .execute();
                String type = pdf.contentType();
                if (type == null || !type.contains("application/pdf")) {
                    log.error("Gazette {}: the download is not a PDF ({}).", g.number(), type);
                    return false;
                }
                g.destination().getParentFile().mkdirs();
                try (FileOutputStream out = new FileOutputStream(g.destination())) {
                    out.write(pdf.bodyAsBytes());
                }
                log.info("Saved gazette {} ({} bytes) to {}", g.number(), pdf.bodyAsBytes().length, g.destination().getAbsolutePath());
                return true;
            } catch (IOException e) {
                log.warn("Gazette {}: PDF download attempt {}/3 failed: {}", g.number(), attempt, e.getMessage());
                Thread.sleep(attempt * 10_000L);
            }
        }
        return false;
    }

    /** A page from Kenya Law, after the polite pause; 3 attempts with growing waits. */
    private Document fetch(String url, String referrer) throws InterruptedException {
        for (int attempt = 1; attempt <= 3; attempt++) {
            pause();
            try {
                Connection c = Jsoup.connect(url)
                        .userAgent(userAgent)
                        .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                        .followRedirects(true)
                        .timeout(60000);
                if (referrer != null) c.referrer(referrer);
                return c.get();
            } catch (IOException e) {
                log.warn("Fetching {} failed (attempt {}/3): {}", url, attempt, e.getMessage());
                Thread.sleep(attempt * 10_000L);
            }
        }
        return null;
    }

    private void pause() throws InterruptedException {
        Thread.sleep(Math.max(0, pauseSeconds) * 1000L);
    }

    /** Processing runs one PDF at a time; a gazette sent while another is being
     *  processed would be refused, so wait (at most 3 hours). */
    private void waitUntilIdle() throws InterruptedException {
        long until = System.currentTimeMillis() + 3 * 60 * 60 * 1000L;
        while (gazetteService.isBusy() && System.currentTimeMillis() < until) Thread.sleep(30_000L);
    }

    /** Kenya_Gazette_Vol_CXXVII_No_225_Dated_2025-11-07.pdf in storage/gazettes/ */
    static File destinationFile(String number, LocalDate date) {
        String safeNumber = number.replaceAll("[^a-zA-Z0-9]", "_");
        String safeDate = date != null ? date.toString() : "Unknown_Date";
        return new File("storage/gazettes/", "Kenya_Gazette_" + safeNumber + "_Dated_" + safeDate + ".pdf");
    }

    static LocalDate findDateInTableRow(Element link) {
        try {
            Element row = link.closest("tr");
            if (row == null) return null;
            Element dateCell = row.select("td").last();
            String dateText = dateCell.text();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("d MMMM yyyy", Locale.ENGLISH);
            return LocalDate.parse(dateText, formatter);
        } catch (Exception e) {
            return null;
        }
    }

    private String extractIdentifierFromUrl(String url) {
        try {
            String[] parts = url.split("/");
            String fileName = parts[parts.length - 1];
            return fileName.replace(".pdf", "");
        } catch (Exception e) {
            return url;
        }
    }

    public void runScraperManually() {
        log.info("--- 👨‍💻 MANUAL SCRAPE TRIGGERED ---");
        new Thread(this::scrapeForNewGazettes).start();
    }
}