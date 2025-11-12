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

    /**
     * This is the final, robust 2-STEP scraping method.
     * 1. Scrapes listings page to find the latest gazette's "details page".
     * 2. Scrapes the "details page" to find the real "Download PDF" link.
     */
    @Scheduled(cron = "0 0 5 * * MON-FRI", zone = "Africa/Nairobi")
    public void scrapeForNewGazettes() {
        log.info("--- 🤖 STARTING SCHEDULED GAZETTE SCRAPE ---");

        int maxRetries = 3;
        int retryDelay = 5000; // 5 seconds

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                log.info("Scrape attempt {}/{}", attempt, maxRetries);

                String currentYear = String.valueOf(LocalDate.now().getYear());
                String scrapeUrl = KENYA_LAW_GAZETTE_URL + currentYear;
                log.info("Scraping URL: {}", scrapeUrl);

                // --- STEP 1: Get the Main Listings Page ---
                Document doc = Jsoup.connect(scrapeUrl)
                        .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
                        .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8")
                        .referrer("https://www.google.com/")
                        .followRedirects(true)
                        .timeout(60000) // 60 seconds
                        .get();

                // Find the first link in the table (the one to the details page)
                Element latestGazetteLink = doc.select("td.cell-title a[href^='/akn/ke/officialGazette/']").first();

                if (latestGazetteLink == null) {
                    log.warn("Could not find any gazette links. HTML structure may have changed.");
                    if (attempt < maxRetries) {
                        log.info("Retrying in {} seconds...", retryDelay / 1000);
                        Thread.sleep(retryDelay);
                        continue;
                    } else {
                        log.error("All retry attempts failed to find link. Giving up.");
                        return;
                    }
                }

                // --- Extract Info (This is all working) ---
                String detailsPageUrl = latestGazetteLink.attr("abs:href");
                String linkText = latestGazetteLink.text();
                String gazetteNumber = linkText.replace("Kenya Gazette ", "").trim();
                LocalDate gazetteDate = findDateInTableRow(latestGazetteLink);

                log.info("Found latest gazette: Number='{}', Date='{}', URL='{}'", gazetteNumber, gazetteDate, detailsPageUrl);

                // --- Check for Duplicates (This is all working) ---
                if (gazetteDate != null) {
                    Optional<Gazette> existing = gazetteRepository.findFirstByGazetteNumberAndGazetteDate(gazetteNumber, gazetteDate);
                    if (existing.isPresent()) {
                        log.info("Gazette ({}, {}) already processed. Skipping.", gazetteNumber, gazetteDate);
                        log.info("--- 🤖 SCHEDULED SCRAPE FINISHED (SKIPPED) ---");
                        return;
                    }
                }

                // --- STEP 2: Go to the Details Page to find the PDF link ---
                log.info("New gazette found! Navigating to details page: {}", detailsPageUrl);
                Document detailsDoc = Jsoup.connect(detailsPageUrl)
                        .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                        .referrer(scrapeUrl) // Act like we clicked the link
                        .timeout(60000)
                        .get();

                // --- THIS IS THE FINAL FIX (based on your HTML source) ---
                // We are looking for the link with the text "Download PDF"
                Element pdfLink = detailsDoc.select("a:contains(Download PDF)").first();

                if (pdfLink == null) {
                    log.warn("Could not find a 'Download PDF' link on the details page: {}", detailsPageUrl);
                    if (attempt < maxRetries) {
                        log.info("Retrying in {} seconds...", retryDelay / 1000);
                        Thread.sleep(retryDelay);
                        continue;
                    }
                    log.error("All retry attempts failed to find PDF link. Giving up.");
                    return;
                }

                // The link should be .../eng@2025-11-07/source
                String pdfUrl = pdfLink.attr("abs:href");
                log.info("Found download URL: {}", pdfUrl);

                // --- STEP 3: Download the PDF ---
                log.info("Downloading PDF...");
                Connection.Response pdfResponse = Jsoup.connect(pdfUrl)
                        .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                        .referrer(detailsPageUrl) // Act like we clicked the download button
                        .ignoreContentType(true)
                        .followRedirects(true)
                        .timeout(120000) // 2 minutes for large PDFs
                        .maxBodySize(0)
                        .execute();

                // Check if the downloaded file is actually a PDF
                String contentType = pdfResponse.contentType();
                if (contentType == null || !contentType.contains("application/pdf")) {
                    log.error("Downloaded file is NOT a PDF! Content-Type: {}. Halting.", contentType);
                    log.info("--- 🤖 SCHEDULED SCRAPE FINISHED (WRONG FILE TYPE) ---");
                    return;
                }

                byte[] pdfBytes = pdfResponse.bodyAsBytes();
                log.info("PDF downloaded successfully ({} bytes)", pdfBytes.length);

                // --- STEP 4: Save to temp file and send to AI pipeline ---
                Path tempPath = Files.createTempFile("scraped-gazette-", ".pdf");
                File tempFile = tempPath.toFile();

                try (FileOutputStream out = new FileOutputStream(tempFile)) {
                    out.write(pdfBytes);
                }

                log.info("Saved to temp file: {}", tempFile.getPath());
                gazetteService.processAndSavePdf(tempFile);

                log.info("--- 🤖 SCHEDULED SCRAPE FINISHED (SUCCESS) ---");
                return; // Success - exit retry loop

            } catch (IOException e) {
                log.error("Scrape attempt {}/{} failed: {}", attempt, maxRetries, e.getMessage());

                if (attempt < maxRetries) {
                    log.info("Waiting {} seconds before retry...", retryDelay / 1000);
                    try {
                        Thread.sleep(retryDelay);
                        retryDelay *= 2; // Exponential backoff (5s, 10s, 20s)
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                } else {
                    log.error("All {} retry attempts exhausted. Scraping failed.", maxRetries, e);
                }
            } catch (Exception e) {
                log.error("Unexpected error during scraping: {}", e.getMessage(), e);
                return;
            }
        }

        log.info("--- 🤖 SCHEDULED SCRAPE FINISHED (FAILED) ---");
    }

    /**
     * Helper method to find the date in the same table row as the link
     */
    private LocalDate findDateInTableRow(Element link) {
        try {
            Element row = link.closest("tr");
            if (row == null) {
                log.warn("Could not find parent <tr> for link. Cannot parse date.");
                return null;
            }

            // Find the last 'td' (table data) element in that row
            Element dateCell = row.select("td").last();
            String dateText = dateCell.text(); // e.g., "7 November 2025"

            // Define the format of the date
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("d MMMM yyyy", Locale.ENGLISH);

            // Parse the date
            return LocalDate.parse(dateText, formatter);

        } catch (Exception e) {
            log.warn("Could not parse date from table row (Text: '{}'). Error: {}", (link.closest("tr") != null ? link.closest("tr").text() : "N/A"), e.getMessage());
            return null; // Return null if parsing fails
        }
    }

    /**
     * This is a helper method to allow you to manually test the scraper.
     */
    public void runScraperManually() {
        log.info("--- 👨‍💻 MANUAL SCRAPE TRIGGERED ---");
        // We run this in a new thread so it doesn't block the UI
        new Thread(this::scrapeForNewGazettes).start();
    }
}