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

    // --- Page Display Methods ---

    @GetMapping("/")
    public String home(Model model) {
        model.addAttribute("gazettes", gazetteService.getAllGazettes());
        return "home";
    }

    @GetMapping("/gazette/{id}")
    public String viewGazetteDetail(@PathVariable Long id, Model model) {
        Gazette g = gazetteService.getGazetteById(id);
        if (g == null) return "redirect:/";
        model.addAttribute("gazette", g);
        return "gazette-detail";
    }

    @GetMapping("/admin")
    public String showAdminDashboard(Model model) {
        model.addAttribute("gazettes", gazetteService.getAllGazettes());
        return "admin";
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

    // --- Action Methods ---

    @PostMapping("/add")
    public String handlePdfUpload(@RequestParam("pdfFile") MultipartFile pdfFile, RedirectAttributes redirectAttributes) {
        if (pdfFile == null || pdfFile.isEmpty()) {
            redirectAttributes.addFlashAttribute("error", "Please select a PDF to upload.");
            return "redirect:/admin";
        }
        File tempFile = null;
        try {
            Path tempPath = Files.createTempFile("sg-upload-", ".pdf");
            tempFile = tempPath.toFile();
            pdfFile.transferTo(tempFile);
            gazetteService.processAndSavePdf(tempFile);
            redirectAttributes.addFlashAttribute("message", "File uploaded! Processing has started in the background. New articles will appear below within a few minutes.");
        } catch (IOException e) {
            log.error("Failed to save uploaded file: {}", e.getMessage(), e);
            redirectAttributes.addFlashAttribute("error", "Failed to upload file.");
            if (tempFile != null) {
                tempFile.delete();
            }
        }
        return "redirect:/admin";
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
        return "redirect:/admin";
    }

    @GetMapping("/delete/{id}")
    public String deleteGazette(@PathVariable Long id, RedirectAttributes redirectAttributes) {
        gazetteService.deleteGazette(id);
        redirectAttributes.addFlashAttribute("message", "Gazette entry #" + id + " has been deleted.");
        return "redirect:/admin";
    }

    @GetMapping("/ifttt-post/{id}")
    public String postToIfttt(@PathVariable Long id, RedirectAttributes redirectAttributes) {
        Gazette gazette = gazetteService.getGazetteById(id);
        if (gazette != null) {
            iftttWebhookService.postTweet(gazette.getXSummary());
            redirectAttributes.addFlashAttribute("message", "Post for entry #" + id + " sent to IFTTT.");
        }
        return "redirect:/admin";
    }

    @GetMapping("/admin/export/excel")
    public ResponseEntity<InputStreamResource> exportToExcel() {
        List<Gazette> gazettes = gazetteService.getAllGazettes();
        ByteArrayInputStream in = excelExportService.generateExcelReport(gazettes);
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Disposition", "attachment; filename=gazettes.xlsx");
        return ResponseEntity.ok().headers(headers).contentType(MediaType.APPLICATION_OCTET_STREAM).body(new InputStreamResource(in));
    }

    // --- Error Handling ---
    @ExceptionHandler(MaxUploadSizeExceededException.class)
    public String handleMaxSize(MaxUploadSizeExceededException ex, RedirectAttributes redirectAttributes) {
        log.warn("Upload rejected: file too large");
        redirectAttributes.addFlashAttribute("error", "File is too large.");
        return "redirect:/admin";
    }
}