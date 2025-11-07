package com.smartgazette.smartgazette.controller;

import com.smartgazette.smartgazette.model.Gazette;
import com.smartgazette.smartgazette.service.ExcelExportService;
import com.smartgazette.smartgazette.service.GazetteService;
import com.smartgazette.smartgazette.service.IftttWebhookService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MaxUploadSizeExceededException;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.support.RedirectAttributes;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import com.smartgazette.smartgazette.model.ProcessingStatus;
import java.util.stream.Collectors;
import java.util.Map;
import java.util.LinkedHashMap;

@Controller
public class GazetteController {

    private static final Logger log = LoggerFactory.getLogger(GazetteController.class);

    private final GazetteService gazetteService;
    private final IftttWebhookService iftttWebhookService;
    private final ExcelExportService excelExportService;

    public GazetteController(GazetteService gazetteService, IftttWebhookService iftttWebhookService, ExcelExportService excelExportService) {
        this.gazetteService = gazetteService;
        this.iftttWebhookService = iftttWebhookService;
        this.excelExportService = excelExportService;
    }

    // --- Public Page Display Methods ---

    @GetMapping("/")
    public String home(Model model) {
        List<Gazette> successfulGazettes = gazetteService.getAllGazettes()
                .stream()
                .filter(g -> g.getStatus() == ProcessingStatus.SUCCESS)
                .toList();
        model.addAttribute("gazettes", successfulGazettes);
        return "home";
    }

    @GetMapping("/gazette/{id}")
    public String viewGazetteDetail(@PathVariable Long id, Model model) {
        Gazette g = gazetteService.getGazetteById(id);
        if (g == null) return "redirect:/";
        model.addAttribute("gazette", g);
        return "gazette-detail";
    }

    @GetMapping("/categories")
    public String showCategoriesPage(Model model) {
        // This is based on your new Figma design
        Map<String, String> categories = new LinkedHashMap<>();
        categories.put("Legislation", "Info about legislation related notices");
        categories.put("Land_Property", "Info about Land Property related notices");
        categories.put("Appointments", "Info about Appointments related notices");
        categories.put("Company_Registration", "Info about Company Registration related notices");
        categories.put("Court_Legal", "Info about Court Legal related notices");
        categories.put("Tenders", "Info about Tenders related notices");

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
        long total = allGazettes.size();
        long successful = allGazettes.stream().filter(g -> g.getStatus() == ProcessingStatus.SUCCESS).count();
        long failed = total - successful;
        double rate = (total > 0) ? ((double) successful / total) * 100.0 : 0.0;

        // Metrics for KPI cards (from image_66c020.png)
        model.addAttribute("totalArticles", total);
        model.addAttribute("successfulArticles", successful);
        model.addAttribute("failedArticles", failed);
        model.addAttribute("successRate", rate);

        // Data for "Processing over time" chart (placeholder data for now)
        model.addAttribute("chartLabels", List.of("01", "03", "05", "07", "09", "11", "13", "15", "17", "19", "21", "23", "25", "27", "29"));
        model.addAttribute("chartData", List.of(5, 7, 15, 20, 18, 7, 7, 8, 11, 2, 9, 0, 8, 11, 3));

        // Data for "Website Traffic" chart (placeholder)
        model.addAttribute("trafficLabels", List.of("02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"));
        model.addAttribute("websiteTrafficData", List.of(20, 25, 22, 30, 45, 40, 50, 60, 55, 70, 75));
        model.addAttribute("xTrafficData", List.of(10, 15, 12, 20, 35, 30, 40, 50, 45, 60, 65));

        // Data for Category Pie Chart (from image_66c038.png)
        Map<String, Long> categoryCounts = allGazettes.stream()
                .filter(g -> g.getCategory() != null)
                .collect(Collectors.groupingBy(Gazette::getCategory, Collectors.counting()));

        model.addAttribute("categoryLabels", categoryCounts.keySet());
        model.addAttribute("categoryData", categoryCounts.values());

        return "admin-dashboard"; // Renders admin-dashboard.html
    }

    @GetMapping("/admin/content")
    public String showAdminContent(Model model) {
        model.addAttribute("gazettes", gazetteService.getAllGazettes());
        return "admin-content"; // Renders admin-content.html
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
            pdfFile.transferTo(tempFile);
            gazetteService.processAndSavePdf(tempFile);
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
            existingGazette.setContent(formGazette.getContent());
            existingGazette.setLink(formGazette.getLink());
            gazetteService.saveGazette(existingGazette);
            redirectAttributes.addFlashAttribute("message", "Gazette entry #" + existingGazette.getId() + " updated successfully.");
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
        return "redirect:/admin/dashboard";
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
}