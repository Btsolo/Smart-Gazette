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

    @Scheduled(cron = "0 0 5 * * MON-FRI", zone = "Africa/Nairobi")
    public void scrapeForNewGazettes() {
        log.info("--- 🤖 STARTING SCHEDULED GAZETTE SCRAPE ---");

        int maxRetries = 3;
        int retryDelay = 5000;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                log.info("Scrape attempt {}/{}", attempt, maxRetries);

                String currentYear = String.valueOf(LocalDate.now().getYear());
                String scrapeUrl = KENYA_LAW_GAZETTE_URL + currentYear;
                log.info("Scraping URL: {}", scrapeUrl);

                // Step 1: Get Listings Page
                Document doc = Jsoup.connect(scrapeUrl)
                        .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                        .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
                        .referrer("https://www.google.com/")
                        .followRedirects(true)
                        .timeout(60000)
                        .get();

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

                String detailsPageUrl = latestGazetteLink.attr("abs:href");
                String linkText = latestGazetteLink.text();
                String gazetteNumber = linkText.replace("Kenya Gazette ", "").trim();
                LocalDate gazetteDate = findDateInTableRow(latestGazetteLink);

                log.info("Found latest gazette: Number='{}', Date='{}', URL='{}'", gazetteNumber, gazetteDate, detailsPageUrl);

                // Check duplicates
                if (gazetteDate != null) {
                    Optional<Gazette> existing = gazetteRepository.findFirstByGazetteNumberAndGazetteDate(gazetteNumber, gazetteDate);
                    if (existing.isPresent()) {
                        log.info("Gazette ({}, {}) already processed. Skipping.", gazetteNumber, gazetteDate);
                        log.info("--- 🤖 SCHEDULED SCRAPE FINISHED (SKIPPED) ---");
                        return;
                    }
                }

                // Step 2: Get Details Page
                log.info("New gazette found! Navigating to details page: {}", detailsPageUrl);
                Document detailsDoc = Jsoup.connect(detailsPageUrl)
                        .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                        .referrer(scrapeUrl)
                        .timeout(60000)
                        .get();

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

                String pdfUrl = pdfLink.attr("abs:href");
                log.info("Found download URL: {}", pdfUrl);

                // Step 3: Download PDF
                log.info("Downloading PDF...");
                Connection.Response pdfResponse = Jsoup.connect(pdfUrl)
                        .userAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
                        .referrer(detailsPageUrl)
                        .ignoreContentType(true)
                        .followRedirects(true)
                        .timeout(120000)
                        .maxBodySize(0)
                        .execute();

                String contentType = pdfResponse.contentType();
                if (contentType == null || !contentType.contains("application/pdf")) {
                    log.error("Downloaded file is NOT a PDF! Content-Type: {}. Halting.", contentType);
                    log.info("--- 🤖 SCHEDULED SCRAPE FINISHED (WRONG FILE TYPE) ---");
                    return;
                }

                byte[] pdfBytes = pdfResponse.bodyAsBytes();
                log.info("PDF downloaded successfully ({} bytes)", pdfBytes.length);

                // --- CRITICAL FIX: Save directly to PERMANENT storage ---
                File storageDir = new File("storage/gazettes/");
                if (!storageDir.exists()) {
                    storageDir.mkdirs();
                }

                // Create a clean filename
                String safeGazetteNumber = gazetteNumber.replaceAll("[^a-zA-Z0-9.-]", "_");
                String fileName = safeGazetteNumber + "_" + (gazetteDate != null ? gazetteDate : "unknown_date") + ".pdf";
                File destinationFile = new File(storageDir, fileName);

                try (FileOutputStream out = new FileOutputStream(destinationFile)) {
                    out.write(pdfBytes);
                }

                String finalPdfPath = destinationFile.getAbsolutePath();
                log.info("Saved PDF to permanent storage: {}", finalPdfPath);

                // Pass the PERMANENT file to the service
                gazetteService.processAndSavePdf(destinationFile, finalPdfPath);

                log.info("--- 🤖 SCHEDULED SCRAPE FINISHED (SUCCESS) ---");
                return;

            } catch (IOException e) {
                log.error("Scrape attempt {}/{} failed: {}", attempt, maxRetries, e.getMessage());

                if (attempt < maxRetries) {
                    log.info("Waiting {} seconds before retry...", retryDelay / 1000);
                    try {
                        Thread.sleep(retryDelay);
                        retryDelay *= 2;
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

    private LocalDate findDateInTableRow(Element link) {
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