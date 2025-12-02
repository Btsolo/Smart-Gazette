package com.smartgazette.smartgazette.controller;

import com.smartgazette.smartgazette.model.Gazette;
import com.smartgazette.smartgazette.service.ExcelExportService;
import com.smartgazette.smartgazette.service.GazetteScrapingService;
import com.smartgazette.smartgazette.service.GazetteService;
import com.smartgazette.smartgazette.service.IftttWebhookService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MaxUploadSizeExceededException;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;


import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import com.smartgazette.smartgazette.model.ProcessingStatus;
import java.util.stream.Collectors;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Comparator;
import java.net.MalformedURLException;

@Controller
public class GazetteController {

    private static final Logger log = LoggerFactory.getLogger(GazetteController.class);

    private final GazetteService gazetteService;
    private final IftttWebhookService iftttWebhookService;
    private final ExcelExportService excelExportService;
    private final GazetteScrapingService scrapingService;

    public GazetteController(GazetteService gazetteService, IftttWebhookService iftttWebhookService, ExcelExportService excelExportService, GazetteScrapingService scrapingService) {
        this.gazetteService = gazetteService;
        this.iftttWebhookService = iftttWebhookService;
        this.excelExportService = excelExportService;
        this.scrapingService = scrapingService;
    }

    // --- Public Page Display Methods ---

    @GetMapping("/")
    public String home(Model model,
                       @RequestParam(name = "page", defaultValue = "1") int pageNum,
                       @RequestParam(name = "filter", defaultValue = "latest") String filter) {
        int pageSize = 20;

        // Pass the filter to the service
        Page<Gazette> page = gazetteService.listSuccessfulGazettesPaginated(pageNum, pageSize, filter);

        model.addAttribute("gazettes", page.getContent());
        model.addAttribute("currentPage", pageNum);
        model.addAttribute("totalPages", page.getTotalPages());
        model.addAttribute("totalItems", page.getTotalElements());

        // Pass the current filter back to the view so we can highlight the button
        model.addAttribute("currentFilter", filter);

        return "home";
    }

    @GetMapping("/gazette/{id}")
    public String viewGazetteDetail(@PathVariable Long id, Model model) {
        // --- FIX: Calling the missing method now that it's added to the service ---
        Gazette g = gazetteService.incrementViewCount(id);

        if (g == null) return "redirect:/";
        model.addAttribute("gazette", g);
        return "gazette-detail";
    }

    @GetMapping("/categories")
    public String showCategoriesPage(Model model) {

        Map<String, String> categories = new LinkedHashMap<>();
        categories.put("Appointments", "Public service appointments and board changes.");
        categories.put("Legislation", "New acts, bills, and legislative supplements.");
        categories.put("Tenders", "Procurement notices and invitations to tender.");
        categories.put("Land_Property", "Land acquisition, disposal, and title deed notices.");
        categories.put("Court_Legal", "Probate, administration, and other court notices.");
        categories.put("Public_Service_HR", "Promotions, transfers, and HR notices.");
        categories.put("Licensing", "Applications and grants for various licenses.");
        categories.put("Company_Registrations", "Company incorporation and dissolution notices.");
        categories.put("Miscellaneous", "Other public notices and general information.");

        model.addAttribute("categories", categories);
        return "categories";
    }

    @GetMapping("/about")
    public String showAboutPage() {
        return "about";
    }

    // --- Admin Page Display Methods ---

    @GetMapping("/login")
    public String showLoginPage() {
        return "login";
    }

    @GetMapping("/admin")
    public String showAdminRoot() {
        // This is the new root for admin, as requested
        return "redirect:/login";
    }

    @GetMapping("/admin/dashboard")
    public String showAdminMetrics(Model model) {
        List<Gazette> allGazettes = gazetteService.getAllGazettes();
        List<Gazette> successList = allGazettes.stream().filter(g -> g.getStatus() == ProcessingStatus.SUCCESS).toList();
        List<Gazette> failedList = allGazettes.stream().filter(g -> g.getStatus() == ProcessingStatus.FAILED).toList();

        // --- 1. KPI CARDS (From your Figma design) ---
        long totalArticles = allGazettes.size();
        long failedArticles = failedList.size(); // This is your "API Errors"
        long successfulArticles = successList.size();
        double successRate = (totalArticles > 0) ? ((double) successfulArticles / totalArticles) * 100.0 : 0.0;

        // --- 2. DEEPER ENGAGEMENT METRICS (Your Request) ---
        long totalViews = successList.stream().mapToLong(Gazette::getViewCount).sum();
        long totalThumbsUp = successList.stream().mapToLong(Gazette::getThumbsUp).sum();
        long totalThumbsDown = successList.stream().mapToLong(Gazette::getThumbsDown).sum();
        long totalEngagement = totalViews + totalThumbsUp + totalThumbsDown; // Simple engagement metric

        // --- 3. PASS ALL DATA TO THE MODEL ---

        // Processing Cluster
        model.addAttribute("totalArticles", totalArticles);
        model.addAttribute("successfulArticles", successfulArticles);
        model.addAttribute("failedArticles", failedArticles);
        model.addAttribute("successRate", successRate);

        // Engagement Cluster
        model.addAttribute("totalEngagement", totalEngagement);
        model.addAttribute("totalViews", totalViews);
        model.addAttribute("totalThumbsUp", totalThumbsUp);
        model.addAttribute("totalThumbsDown", totalThumbsDown);

        // --- 4. CATEGORY PIE CHART (Real Data) ---
        Map<String, Long> categoryCounts = successList.stream()
                .filter(g -> g.getCategory() != null)
                .collect(Collectors.groupingBy(Gazette::getCategory, Collectors.counting()));

        model.addAttribute("categoryLabels", categoryCounts.keySet());
        model.addAttribute("categoryData", categoryCounts.values());

        // --- 5. "AI INSIGHTS" & ERROR BREAKDOWN (Live Data) ---
        Map<String, Long> errorReasonCounts = failedList.stream()
                .map(g -> {
                    String summary = g.getSummary();
                    if (summary == null) return "Unknown Reason";
                    if (summary.startsWith("The AI failed during processing. Reason: ")) {
                        return summary.replace("The AI failed during processing. Reason: ", "");
                    }
                    if (summary.startsWith("AI failed to generate summary.")) {
                        return "Generation Failed";
                    }
                    return "Unknown Failure";
                })
                .collect(Collectors.groupingBy(reason -> reason, Collectors.counting()));

        model.addAttribute("errorReasonCounts", errorReasonCounts);

        List<Gazette> topArticles = successList.stream()
                .filter(g -> g.getThumbsUp() > 0)
                .sorted(Comparator.comparing(Gazette::getThumbsUp).reversed())
                .limit(3) // Top 3
                .toList();
        model.addAttribute("topArticles", topArticles);

        // --- 6. PLACEHOLDERS (For charts we'll implement later) ---
        // This is for your "Processing over time" stacked bar chart
        model.addAttribute("processingDayLabels", List.of("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"));
        model.addAttribute("processingSuccessData", List.of(20, 30, 45, 50, 23, 34, 40));
        model.addAttribute("processingFailData", List.of(2, 1, 0, 3, 1, 0, 2));

        // This is for your "Website Traffic" line chart
        model.addAttribute("trafficLabels", List.of("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"));
        model.addAttribute("websiteTrafficData", List.of(150, 220, 200, 310, 350, 300, 400));
        model.addAttribute("socialTrafficData", List.of(50, 60, 55, 80, 100, 90, 120)); // Placeholder for "X Traffic"

        return "admin-dashboard";
    }

    @GetMapping("/admin/content")
    public String showAdminContent(Model model,
                                   @RequestParam(name = "filter", required = false) String filter,
                                   @RequestParam(name = "batch", required = false) String batchNumber) {
        List<Gazette> gazettes;

        if (batchNumber != null && !batchNumber.isEmpty()) {
            // Drill-down: Show only notices for this batch
            gazettes = gazetteService.getGazettesByBatch(batchNumber);
            model.addAttribute("currentBatch", batchNumber); // For breadcrumb
            model.addAttribute("currentFilter", "batch");
        } else {
            // Normal filtering
            String activeFilter = (filter != null) ? filter : "latest";
            gazettes = gazetteService.getAllGazettes(activeFilter);
            model.addAttribute("currentFilter", activeFilter);
        }

        model.addAttribute("gazettes", gazettes);
        model.addAttribute("activePage", "content");
        return "admin-content";
    }

    // NEW: "Hacker" Log Viewer (Step 1: Directory)
    @GetMapping("/admin/logs")
    public String showAdminLogs(Model model) {
        // This query will group notices by their gazette publication (Date + Number)
        // We will implement this properly later. For now, here is placeholder data.
        Map<String, String> gazetteFiles = new LinkedHashMap<>();
        gazetteFiles.put("CIV117S (8/21/2024)", "500 notices");
        gazetteFiles.put("CIV158 (8/03/2024)", "200 notices");
        gazetteFiles.put("CIII57S (8/03/2024)", "1 notice");
        gazetteFiles.put("CIII17S (8/03/2024)", "300 notices");

        model.addAttribute("gazetteFiles", gazetteFiles);
        return "admin-logs"; // Renders admin-logs.html
    }

    // NEW: "Hacker" Log Viewer (Step 2: Log Console)
    @GetMapping("/admin/logs/{gazetteFileId}")
    public String showAdminLogDetail(@PathVariable String gazetteFileId, Model model) {
        // Placeholder data. Later this will fetch real logs.
        model.addAttribute("gazetteFileId", gazetteFileId);
        String logData = "2025-11-06T05:24:16.S03+03:00 [task-5] INFO: Received OOK generation response: {\"title\":\"Heads Up, Kilifi...\"}\n" +
                "2025-11-06T05:24:16.S03+03:00 [task-5] INFO: Saving SUCCESS article: 'Heads Up, Kilifi!' (Cat: 'Land_Property', ...)\n" +
                "2025-11-06T05:24:19.S03+03:00 [task-5] DEBUG: Pausing for 3 seconds...\n" +
                "2025-11-06T05:24:22.S03+03:00 [task-5] INFO: -----> Processing Notice 2/222...\n" +
                "2025-11-06T05:24:25.S03+03:00 [task-5] WARN: Triage returned an unexpected value: 'Land'. Defaulting to Miscellaneous.";
        model.addAttribute("logData", logData);
        return "admin-log-detail"; // Renders admin-log-detail.html
    }


    @GetMapping("/admin/settings")
    public String showAdminSettings(Model model) {
        return "admin-settings"; // Renders admin-settings.html
    }

    @GetMapping("/add")
    public String showAddForm() {
        return "add";
    }

    @GetMapping("/edit/{id}")
    public String showEditForm(@PathVariable Long id, Model model) {
        Gazette gazette = gazetteService.getGazetteById(id);
        model.addAttribute("gazette", gazette);
        return "edit-gazette";
    }

    // --- Admin Action Methods ---

    @PostMapping("/add")
    public String handlePdfUpload(@RequestParam("pdfFile") MultipartFile pdfFile, RedirectAttributes redirectAttributes) {
        if (pdfFile == null || pdfFile.isEmpty()) {
            redirectAttributes.addFlashAttribute("error", "Please select a PDF to upload.");
            return "redirect:/admin/content";
        }
        File tempFile = null;

        try {
            Path tempPath = Files.createTempFile("sg-upload-", ".pdf");
            tempFile = tempPath.toFile();
            String tempFilePath = tempPath.toString();

            pdfFile.transferTo(tempFile);

            // Pass the file to service. Note: Service will NOT delete it anymore.
            gazetteService.processAndSavePdf(tempFile, tempFilePath);

            // --- NEW: We must schedule deletion or delete immediately if synchronous (but process is async) ---
            // Since processAndSavePdf is @Async, we cannot delete the file immediately here.
            // Ideally, the Service should handle "isTemp" flag, but for now, we rely on OS temp cleanup
            // OR we can't delete it yet because the async thread needs it.
            // *Correction*: For this specific upload flow, we will leave the temp file.
            // The OS usually cleans /tmp, or we accept this trade-off to fix the scraper bug.

            redirectAttributes.addFlashAttribute("message", "File uploaded! Processing has started...");
        } catch (IOException e) {
            log.error("Failed to save uploaded file: {}", e.getMessage(), e);
            redirectAttributes.addFlashAttribute("error", "Failed to upload file.");
            if (tempFile != null) {
                tempFile.delete();
            }
        }
        return "redirect:/admin/content";
    }

    @PostMapping("/update")
    public String updateGazette(@ModelAttribute Gazette formGazette, RedirectAttributes redirectAttributes) {
        Gazette existingGazette = gazetteService.getGazetteById(formGazette.getId());
        if (existingGazette != null) {
            existingGazette.setTitle(formGazette.getTitle());
            existingGazette.setSummary(formGazette.getSummary());
            existingGazette.setArticle(formGazette.getArticle());
            existingGazette.setXSummary(formGazette.getXSummary());
            existingGazette.setActionableInfo(formGazette.getActionableInfo());
            existingGazette.setLink(formGazette.getLink());

            gazetteService.saveGazette(existingGazette);
            redirectAttributes.addFlashAttribute("message", "Gazette entry #" + existingGazette.getId() + " updated successfully.");
        } else {
            redirectAttributes.addFlashAttribute("error", "Could not find notice to update.");
        }
        return "redirect:/admin/content";
    }


    @GetMapping("/delete/{id}")
    public String deleteGazette(@PathVariable Long id, RedirectAttributes redirectAttributes) {
        gazetteService.deleteGazette(id);
        redirectAttributes.addFlashAttribute("message", "Gazette entry #" + id + " has been deleted.");
        return "redirect:/admin/content";
    }

    @GetMapping("/ifttt-post/{id}")
    public String postToIfttt(@PathVariable Long id, RedirectAttributes redirectAttributes) {
        Gazette gazette = gazetteService.getGazetteById(id);
        if (gazette != null) {
            iftttWebhookService.postTweet(gazette.getXSummary());
            redirectAttributes.addFlashAttribute("message", "Post for entry #" + id + " sent to IFTTT.");
        }
        return "redirect:/admin/content";
    }

    @GetMapping("/admin/export/excel")
    public ResponseEntity<InputStreamResource> exportToExcel() {
        List<Gazette> gazettes = gazetteService.getAllGazettes();
        ByteArrayInputStream in = excelExportService.generateExcelReport(gazettes);
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Disposition", "attachment; filename=gazettes.xlsx");

        return ResponseEntity
                .ok()
                .headers(headers)
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .body(new InputStreamResource(in));
    }

    @GetMapping("/admin/retry-failed")
    public String retryFailedNotices(RedirectAttributes redirectAttributes) {
        log.info("Manual retry trigger received.");
        gazetteService.retryFailedNotices();
        redirectAttributes.addFlashAttribute("message", "Retry process for FAILED notices has been started in the background.");
        return "redirect:/admin/content";
    }

    @GetMapping("/admin/stop-processing")
    public String stopProcessing(RedirectAttributes redirectAttributes) {
        String message = gazetteService.requestStopProcessing();
        redirectAttributes.addFlashAttribute("message", message);
        return "redirect:/admin/content";
    }

    // --- NEW ENDPOINT FOR PDF DOWNLOAD ---
    @GetMapping("/gazette/pdf/{id}")
    @ResponseBody
    public ResponseEntity<Resource> servePdf(@PathVariable Long id) {
        Gazette gazette = gazetteService.getGazetteById(id);
        if (gazette == null || gazette.getOriginalPdfPath() == null) {
            log.warn("PDF download failed: Notice #{} has no linked PDF path.", id);
            return ResponseEntity.notFound().build();
        }

        try {
            // Using File object to ensure absolute path handling is robust across OS
            File file = new File(gazette.getOriginalPdfPath());
            if (!file.exists()) {
                log.error("PDF file not found on disk: {}", gazette.getOriginalPdfPath());
                return ResponseEntity.notFound().build();
            }

            Resource resource = new UrlResource(file.toURI());

            if (resource.exists() || resource.isReadable()) {
                return ResponseEntity.ok()
                        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_PDF_VALUE)
                        // Content-Disposition inline allows browser to open it instead of forcing download
                        .header(HttpHeaders.CONTENT_DISPOSITION, "inline; filename=\"" + file.getName() + "\"")
                        .body(resource);
            } else {
                return ResponseEntity.notFound().build();
            }
        } catch (MalformedURLException e) {
            log.error("Malformed URL for PDF path: {}", gazette.getOriginalPdfPath(), e);
            return ResponseEntity.badRequest().build();
        }
    }

    static {
        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", "20");
    }

    // --- (METRIC COLLECTION) ---

    @PostMapping("/gazette/{id}/thumbsup")
    @ResponseBody
    public ResponseEntity<Void> handleThumbsUp(@PathVariable Long id) {
        gazetteService.addThumbUp(id);
        return ResponseEntity.ok().build();
    }

    @PostMapping("/gazette/{id}/thumbsdown")
    @ResponseBody
    public ResponseEntity<Void> handleThumbsDown(@PathVariable Long id) {
        gazetteService.addThumbDown(id);
        return ResponseEntity.ok().build();
    }

    // --- Error Handling ---
    @ExceptionHandler(MaxUploadSizeExceededException.class)
    public String handleMaxSize(MaxUploadSizeExceededException ex, RedirectAttributes redirectAttributes) {
        log.warn("Upload rejected: file too large");
        redirectAttributes.addFlashAttribute("error", "File is too large.");
        return "redirect:/admin/content";
    }

    // --- NEW ENDPOINT FOR MANUAL SCRAPING ---
    @GetMapping("/admin/run-scraper")
    public String runScraperManually(RedirectAttributes redirectAttributes) {
        log.info("Manual scrape trigger received from admin.");
        scrapingService.runScraperManually();
        redirectAttributes.addFlashAttribute("message", "Scraper job started in the background. Refresh in a few minutes.");
        return "redirect:/admin/content";
    }

    // --- NEW ENDPOINT FOR CATEGORY PAGE ---
    @GetMapping("/category/{categoryName}")
    public String showCategoryPage(@PathVariable String categoryName,
                                   @RequestParam(name = "page", defaultValue = "1") int pageNum,
                                   @RequestParam(name = "filter", defaultValue = "latest") String filter,
                                   Model model) {
        int pageSize = 20;

        // Ensure we are passing the page object correctly based on the filter
        Page<Gazette> page;
        if ("popular".equals(filter)) {
            page = gazetteService.listSuccessfulGazettesByCategory(categoryName, pageNum, pageSize, "popular"); // You might need to expose the specific repo method in service if not already dynamically handled
        } else if ("significant".equals(filter)) {
            page = gazetteService.listSuccessfulGazettesByCategory(categoryName, pageNum, pageSize, "significant");
        } else {
            page = gazetteService.listSuccessfulGazettesByCategory(categoryName, pageNum, pageSize, "latest");
        }

        model.addAttribute("gazettes", page.getContent());
        model.addAttribute("currentPage", pageNum);
        model.addAttribute("totalPages", page.getTotalPages());
        model.addAttribute("totalItems", page.getTotalElements());
        model.addAttribute("categoryName", categoryName.replace("_", " "));
        model.addAttribute("currentFilter", filter);

        return "category-view";
    }

    // --- NEW ENDPOINT FOR BATCH MANAGEMENT PAGE ---
    @GetMapping("/admin/batch")
    public String showAdminBatch(Model model) {
        model.addAttribute("batches", gazetteService.getGazetteBatches());
        return "admin-batch"; // New page we will create
    }

    // --- NEW ENDPOINT FOR DELETING A BATCH ---
    @PostMapping("/admin/batch/delete")
    public String deleteGazetteBatch(@RequestParam("path") String path, RedirectAttributes redirectAttributes) {
        try {
            gazetteService.deleteGazetteBatch(path);
            redirectAttributes.addFlashAttribute("message", "Successfully deleted gazette batch and all its notices.");
        } catch (Exception e) {
            log.error("Failed to delete batch: {}", e.getMessage(), e);
            redirectAttributes.addFlashAttribute("error", "Failed to delete batch.");
        }
        return "redirect:/admin/batch";
    }

    // --- NEW ENDPOINT FOR DELETING MULTIPLE NOTICES ---
    @PostMapping("/admin/content/delete-bulk")
    public String deleteSelectedNotices(@RequestParam("selectedIds") List<Long> selectedIds, RedirectAttributes redirectAttributes) {
        if (selectedIds == null || selectedIds.isEmpty()) {
            redirectAttributes.addFlashAttribute("error", "No notices were selected for deletion.");
            return "redirect:/admin/content";
        }
        gazetteService.deleteGazetteInBulk(selectedIds);
        redirectAttributes.addFlashAttribute("message", selectedIds.size() + " notices deleted successfully.");
        return "redirect:/admin/content";
    }

    // --- MOCK LOGIN LOGIC ---
    @PostMapping("/login")
    public String processLogin(@RequestParam("username") String username,
                               @RequestParam("password") String password,
                               RedirectAttributes redirectAttributes) {
        // Simple hardcoded check for the demo
        if ("admin".equals(username) && "password".equals(password)) {
            return "redirect:/admin/dashboard";
        } else {
            redirectAttributes.addAttribute("error", "true");
            return "redirect:/login";
        }
    }

    // ---BATCH EXPORT ---
    @PostMapping("/admin/batch/export")
    public ResponseEntity<InputStreamResource> exportBatch(@RequestParam("path") String path) {
        ByteArrayInputStream in = gazetteService.exportBatchToExcel(path);

        // Create a clean filename from the path
        String filename = "batch_export.xlsx";
        try {
            File f = new File(path);
            filename = "Export_" + f.getName().replace(".pdf", "") + ".xlsx";
        } catch (Exception e) {}

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Disposition", "attachment; filename=" + filename);

        return ResponseEntity
                .ok()
                .headers(headers)
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .body(new InputStreamResource(in));
    }
}