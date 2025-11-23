package com.smartgazette.smartgazette.service;

import com.google.cloud.vertexai.VertexAI;
import com.google.cloud.vertexai.api.Blob;
import com.google.cloud.vertexai.api.Content;
import com.google.cloud.vertexai.api.GenerateContentResponse;
import com.google.cloud.vertexai.api.GenerationConfig;
import com.google.cloud.vertexai.api.Part;
import com.google.cloud.vertexai.generativeai.GenerativeModel;
import com.google.cloud.vertexai.generativeai.ResponseHandler;
import com.google.protobuf.ByteString;

import com.smartgazette.smartgazette.model.Gazette;
import com.smartgazette.smartgazette.model.ProcessingStatus;
import com.smartgazette.smartgazette.repository.GazetteRepository;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import com.smartgazette.smartgazette.model.GazetteBatchDTO; // <-- Added for Batch Delete

@Service
public class GazetteService {

    private static final Logger log = LoggerFactory.getLogger(GazetteService.class);
    private final GazetteRepository gazetteRepository;
    private final String PDF_STORAGE_PATH = "storage/gazettes/";

    private final AtomicBoolean isProcessing = new AtomicBoolean(false);
    private final AtomicBoolean stopProcessing = new AtomicBoolean(false);

    private final VertexAI vertexAI;
    private final GenerativeModel geminiProModel;
    private final GenerativeModel geminiFlashModel;
    private final IftttWebhookService iftttWebhookService;
    private final ExcelExportService excelExportService;

    @Value("${gemini.model.pro:gemini-2.5-pro}")
    private String geminiProModelName;

    @Value("${gemini.model.flash:gemini-2.5-flash}")
    private String geminiFlashModelName;

    public GazetteService(GazetteRepository gazetteRepository,
                          IftttWebhookService iftttWebhookService,
                          ExcelExportService excelExportService, // <-- ADD PARAM
                          @Value("${gcp.project.id}") String projectId,
                          @Value("${gcp.location}") String location) {
        this.gazetteRepository = gazetteRepository;
        this.iftttWebhookService = iftttWebhookService;
        this.excelExportService = excelExportService;

        log.info("Initializing Vertex AI SDK for project '{}' in location '{}'", projectId, location);
        this.vertexAI = new VertexAI(projectId, location);

        GenerationConfig textGenConfig = GenerationConfig.newBuilder()
                .setTemperature(0.2f)
                .setMaxOutputTokens(4096)
                .setTopP(0.95f)
                .build();

        GenerationConfig visionGenConfig = GenerationConfig.newBuilder()
                .setMaxOutputTokens(8192)
                .setTemperature(0.1f)
                .build();

        this.geminiProModel = new GenerativeModel.Builder()
                .setModelName("gemini-2.5-pro")
                .setVertexAi(this.vertexAI)
                .setGenerationConfig(textGenConfig)
                .build();

        this.geminiFlashModel = new GenerativeModel.Builder()
                .setModelName("gemini-2.5-flash")
                .setVertexAi(this.vertexAI)
                .setGenerationConfig(visionGenConfig)
                .build();

        log.info("✅ Vertex AI SDK initialization complete!");
    }

    // --- Core Public Methods ---
    public List<Gazette> getAllGazettes() {
        return gazetteRepository.findAllWithCorrectSorting();
    }
    public Gazette getGazetteById(Long id) { return gazetteRepository.findById(id).orElse(null); }
    public void deleteGazette(Long id) { gazetteRepository.deleteById(id); }
    public Gazette saveGazette(Gazette gazette) { return gazetteRepository.save(gazette); }
    public String requestStopProcessing() {
        if (isProcessing.get()) {
            log.warn("ADMIN REQUEST: Stop processing signal received. Will stop on next notice.");
            stopProcessing.set(true);
            return "Stop signal sent. Processing will halt on the next notice.";
        } else {
            return "No processing job is currently running.";
        }
    }
    public Page<Gazette> listSuccessfulGazettesPaginated(int pageNum, int pageSize) {
        Pageable pageable = PageRequest.of(pageNum - 1, pageSize);
        return gazetteRepository.findAllSuccessfulWithCorrectSorting(pageable);
    }
    public Page<Gazette> listSuccessfulGazettesByCategory(String category, int pageNum, int pageSize) {
        Pageable pageable = PageRequest.of(pageNum - 1, pageSize);
        return gazetteRepository.findAllSuccessfulByCategory(category, pageable);
    }

    // --- NEW BATCH MANAGEMENT METHODS ---
    public List<GazetteBatchDTO> getGazetteBatches() {
        return gazetteRepository.findGazetteBatches();
    }

    public void deleteGazetteBatch(String originalPdfPath) {
        // 1. Delete the PDF file from storage
        try {
            Path pdfPath = Paths.get(originalPdfPath);
            Files.deleteIfExists(pdfPath);
            log.info("Deleted PDF file: {}", originalPdfPath);
        } catch (IOException e) {
            log.error("Failed to delete PDF file: {}. Error: {}", originalPdfPath, e.getMessage());
        }

        // 2. Delete all database entries associated with that file
        gazetteRepository.deleteAllByOriginalPdfPath(originalPdfPath);
        log.info("Deleted all database notices for path: {}", originalPdfPath);
    }
    // --- END BATCH MANAGEMENT METHODS ---


    @Async
    public void processAndSavePdf(File file, String originalPdfPath) {
        if (!isProcessing.compareAndSet(false, true)) {
            log.warn("Cannot start PDF processing. Another job (like a retry) is already in progress.");
            return;
        }
        stopProcessing.set(false);

        JSONObject overallGazetteDetails = null;
        String highQualityFullText = null;

        try (PDDocument document = PDDocument.load(file)) {
            log.info(">>>> Starting async PDF processing for file: {}", file.getName());

            // --- [CALL 0] High-Fidelity Hybrid OCR Extraction (Phase 2.6) ---
            try {
                highQualityFullText = extractHighFidelityTextFromPdf(document);
                if (highQualityFullText == null || highQualityFullText.isBlank()) {
                    log.warn("Hybrid Vision OCR failed. Falling back to full PDFTextStripper for file: {}", file.getName());
                    highQualityFullText = new PDFTextStripper().getText(document);
                } else {
                    log.info("Successfully extracted hybrid text (Vision P1 + Stripper P2+).");
                }
            } catch (Exception e) {
                log.error("Critical error during Hybrid OCR step. Falling back to PDFTextStripper.", e);
                highQualityFullText = new PDFTextStripper().getText(document);
            }
            // --- END OF CALL 0 ---

            if (highQualityFullText != null && !highQualityFullText.isBlank()) {
                overallGazetteDetails = extractGazetteHeaderDetails(highQualityFullText);
            }

            List<String> notices = segmentTextByNotices(highQualityFullText);
            log.info("PDF segmented into {} potential notices.", notices.size());

            if (notices.isEmpty() && highQualityFullText != null && !highQualityFullText.isBlank()) {
                log.warn("Segmentation found 0 notices. Assuming a single-notice document.");
                notices.add(highQualityFullText);
                log.info("Processing document as 1 single notice.");
            }

            for (int i = 0; i < notices.size(); i++) {
                String noticeText = notices.get(i);
                int sourceOrder = i + 1;
                log.info("-----> Processing Notice {}/{}...", sourceOrder, notices.size());

                if (stopProcessing.get()) {
                    log.warn("Processing manually stopped by admin at notice #{}", sourceOrder);
                    break;
                }
                Gazette gazette = null;
                try {
                    // --- FIX: Pass originalPdfPath to the single notice processor ---
                    gazette = processSingleNotice(noticeText, sourceOrder, overallGazetteDetails, originalPdfPath);

                    if (gazette != null) {
                        log.info("Saving {} article: '{}' (Cat: '{}', Num: {}, GazDate: {})",
                                gazette.getStatus(), gazette.getTitle(), gazette.getCategory(), gazette.getNoticeNumber(), gazette.getGazetteDate());
                        gazetteRepository.save(gazette);
                    }
                } catch (Exception e) {
                    log.error("Error processing or checking notice #{}. Creating a fallback.", sourceOrder, e);
                    // --- FIX: Pass the 5th argument: originalPdfPath ---
                    gazetteRepository.save(createFallbackGazette(noticeText, sourceOrder, overallGazetteDetails, "Unhandled pipeline error", originalPdfPath));
                }

                try {
                    log.debug("Pausing for 500ms to respect rate limits...");
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Rate limit pause interrupted.");
                }
            }
            log.info("<<<< Successfully finished processing PDF file: {}", file.getName());
        } catch (Exception e) {
            log.error("Critical error during PDF processing pipeline for file: {}", file.getName(), e);
        } finally {
            // --- Scraper handles final file cleanup ---
            try {
                Files.deleteIfExists(file.toPath()); // Deletes the temporary upload file
            } catch (IOException ignored) {}

            isProcessing.set(false);
            stopProcessing.set(false);
            log.info("Processing lock released.");
        }
    }


    private String extractHighFidelityTextFromPdf(PDDocument document) throws IOException, InterruptedException {
        log.info("Starting Vision OCR for FIRST PAGE ONLY...");
        PDFRenderer pdfRenderer = new PDFRenderer(document);
        List<Part> partsList = new ArrayList<>();

        partsList.add(Part.newBuilder().setText("""
                You are a high-fidelity Optical Character Recognition (OCR) service.
                Extract all text from the following page image, perfectly preserving all original line breaks, spacing, and formatting.
                Return ONLY the extracted text, with no other commentary.
                """).build());

        if (document.getNumberOfPages() > 0) {
            BufferedImage bim = pdfRenderer.renderImageWithDPI(0, 300);
            byte[] imageBytes;
            try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                ImageIO.write(bim, "jpeg", baos);
                imageBytes = baos.toByteArray();
            }

            partsList.add(Part.newBuilder()
                    .setInlineData(
                            Blob.newBuilder()
                                    .setMimeType("image/jpeg")
                                    .setData(ByteString.copyFrom(imageBytes))
                                    .build()
                    )
                    .build());
        } else {
            log.warn("PDF has 0 pages. Cannot perform Vision OCR.");
            return null;
        }

        String firstPageCleanText = generateWithRetry(geminiFlashModel, partsList);

        if (firstPageCleanText != null) {
            StringBuilder fullCleanText = new StringBuilder(firstPageCleanText).append("\n\n");
            log.info("Successfully extracted high-fidelity text for Page 1.");

            if (document.getNumberOfPages() > 1) {
                log.info("Using fast PDFTextStripper for pages 2 through {}.", document.getNumberOfPages());
                PDFTextStripper stripper = new PDFTextStripper();
                stripper.setStartPage(2);
                stripper.setEndPage(document.getNumberOfPages());
                String restOfDocumentText = stripper.getText(document);
                fullCleanText.append(restOfDocumentText);
            }
            return fullCleanText.toString();
        } else {
            log.error("Vision OCR call failed for Page 1.");
            return null;
        }
    }

    private List<String> segmentTextByNotices(String fullText) {
        List<String> notices = new ArrayList<>();
        final Pattern pattern = Pattern.compile("(?m)^GAZETTE NOTICE NO\\.\\s*\\d+", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(fullText);
        int lastEnd = 0;
        while (matcher.find()) {
            if (matcher.start() > lastEnd) {
                String notice = fullText.substring(lastEnd, matcher.start()).trim();
                if (!notice.isEmpty()) {
                    notices.add(notice);
                }
            }
            lastEnd = matcher.start();
        }
        if (lastEnd < fullText.length()) {
            String lastNotice = fullText.substring(lastEnd).trim();
            if (!lastNotice.isEmpty()) {
                notices.add(lastNotice);
            }
        }

        if (!notices.isEmpty() && !pattern.matcher(notices.get(0)).find()) {
            log.info("Removing potential header text from segmentation.");
            notices.remove(0);
        }
        return notices;
    }

    private JSONObject extractGazetteHeaderDetails(String headerText) {
        log.info("Attempting to extract Gazette header details from clean text...");
        String prompt = """
        Analyze the following text from the beginning of a Kenya Gazette PDF.
        Extract the Volume (e.g., "Vol. CXXVII"), the Issue Number (e.g., "No. 36"), and the Publication Date (e.g., "21st February, 2025").
        Return ONLY a valid JSON object with three keys: "gazetteVolume", "gazetteNumber", and "gazetteDate" (in YYYY-MM-DD format).
        If a value is not found, return an empty string "".

        TEXT:
        %s

        OUTPUT JSON:
        {
          "gazetteVolume": "...",
          "gazetteNumber": "...",
          "gazetteDate": "YYYY-MM-DD"
        }
        """.formatted(headerText.substring(0, Math.min(headerText.length(), 2000)));

        String jsonResponse = generateWithRetry(geminiFlashModel, prompt);
        JSONObject headerDetails = parseSafeJson(jsonResponse);

        if (headerDetails == null) {
            log.error("Failed to extract Gazette header details from text.");
            return null;
        }
        log.info("Successfully extracted Gazette header details: {}", headerDetails.toString());
        return headerDetails;
    }

    // --- NEW: processSingleNotice with correct signature ---
    private Gazette processSingleNotice(String noticeText, int sourceOrder, JSONObject overallGazetteDetails, String originalPdfPath) {

        // --- STEP 1: AI Triage ---
        String category = triageNoticeCategory(noticeText);

        if (category == null) {
            log.warn("Triage failed for notice segment {}. Creating fallback.", sourceOrder);
            // --- FIX: Pass all required arguments ---
            return createFallbackGazette(noticeText, sourceOrder, overallGazetteDetails, "Triage failed", originalPdfPath);
        }
        log.info("Triage complete for notice segment {}. Category: {}", sourceOrder, category);

        // --- STEP 2: AI Extraction ---
        String schemaPath = "/schemas/field/" + category.toLowerCase() + ".json";
        String schemaContent = loadSchemaFile(schemaPath);
        if (schemaContent.isEmpty()) {
            log.error("Schema file not found for category '{}' (Notice {}). Searched two paths. Creating fallback.", category, sourceOrder);
            // --- FIX: Pass all required arguments ---
            return createFallbackGazette(noticeText, sourceOrder, overallGazetteDetails, "Schema file not found", originalPdfPath);
        }

        String extractionPrompt = """
        Extract structured data from the text below according to the provided JSON schema.
        CRITICAL RULES:
        1. Return ONLY valid JSON with an "items" array or object as the root key.
        2. Each item in the array must be a complete JSON object.
        3. Ensure proper JSON syntax: use commas between items, proper brackets, and quotes.
        4. If multiple items exist, each must be a separate object in the "items" array.
        5. Never break JSON syntax - validate before returning.

        SCHEMA:
        %s

        TEXT TO EXTRACT:
        %s
        
        Return format (if one item): { "items": { ... } }
        Return format (if multiple items): { "items": [ { ... }, { ... } ] }
        """.formatted(schemaContent, noticeText);

        String jsonResponse = generateWithRetry(geminiProModel, extractionPrompt);
        JSONObject extractedDataWrapper = parseSafeJson(jsonResponse);

        if (extractedDataWrapper == null || !extractedDataWrapper.has("items")) {
            log.error("Extraction failed for notice segment {}. AI did not return a valid 'items' wrapper.", sourceOrder);
            // --- FIX: Pass all required arguments ---
            return createFallbackGazette(noticeText, sourceOrder, overallGazetteDetails, "Extraction failed: no 'items' wrapper", originalPdfPath);
        }
        Object extractedData = extractedDataWrapper.get("items");

        boolean isNull = extractedData == null || extractedData == JSONObject.NULL;
        boolean isEmptyObject = (extractedData instanceof JSONObject) && ((JSONObject) extractedData).isEmpty();

        if (isNull || isEmptyObject) {
            log.error("Extraction failed for notice segment {}. AI returned 'items' as null or an empty object.", sourceOrder);
            // --- FIX: Pass all required arguments ---
            return createFallbackGazette(noticeText, sourceOrder, overallGazetteDetails, "Extraction failed: 'items' was null or empty", originalPdfPath);
        }

        log.info("Extraction complete for notice segment {}.", sourceOrder);

        // --- STEP 3: AI Generation ---
        JSONObject generatedContent = generateNarrativeContent(extractedData, category);

        if (generatedContent == null) {
            log.error("Generation step failed for notice segment {}. Saving with extracted data only.", sourceOrder);
        } else {
            log.info("Generation complete for notice segment {}.", sourceOrder);
        }

        // --- FIX: Pass originalPdfPath to final creator method ---
        return createGazetteFromJson(extractedData, generatedContent, noticeText, category, sourceOrder, overallGazetteDetails, originalPdfPath);
    }

    private String triageNoticeCategory(String noticeText) {
        String triagePrompt = """
        Classify the following gazette notice text into ONE of the following categories:
        Appointments
        Legislation
        Tenders (for 'Invitation to Tender', 'procurement', 'bids', 'disposal of assets')
        Land_Property (for 'Issue of Land Title', 'land acquisition', 'EIA', 'provisional certificate', 'replacement title', 'replacement of lost', 'Certificate of Lease', 'lost title deed')
        Court_Legal (for 'Insolvency', 'probate', 'cause list', 'dissolution of marriage')
        Public_Service_HR
        Licensing
        Company_Registrations (for 'incorporation', 'dissolution of company')
        Miscellaneous

        Your response MUST be ONLY one of the words listed above. Do not include any other text, explanation, or punctuation.

        TEXT:
        %s
        """.formatted(noticeText.substring(0, Math.min(noticeText.length(), 4000)));

        String category = generateWithRetry(geminiFlashModel, triagePrompt);

        if (category != null) {
            String cleanedCategory = category.replaceAll("[^a-zA-Z_]", "").trim();
            List<String> validCategories = List.of(
                    "Appointments", "Legislation", "Tenders", "Land_Property", "Court_Legal",
                    "Public_Service_HR", "Licensing", "Company_Registrations", "Miscellaneous"
            );
            if (validCategories.contains(cleanedCategory)) {
                return cleanedCategory;
            } else {
                log.warn("Triage returned an unexpected value: '{}'. Defaulting to Miscellaneous.", category);
                return "Miscellaneous";
            }
        }
        log.warn("Triage returned null. Defaulting to Miscellaneous.");
        return "Miscellaneous";
    }

    private JSONObject generateNarrativeContent(Object extractedData, String category) {
        String generationPrompt = """
        You are an expert editorial assistant for Smart Gazette. Your goal is to simplify government notices for Kenyan youth.
        Based ONLY on the structured JSON data provided below, generate a JSON object containing five fields: "title", "summary", "article", "xSummary", and "actionableInfo".

        **Your Instructions:**
        1.  **Grouping Logic:**
            - If the category is `Land_Property` or `Tenders` and the input data contains multiple similar items (look for arrays), create a single "digest" article. Title like "Land Transfer Notices" or "New Tenders". Article uses markdown lists. Otherwise, create a normal article.
        
        2.  **Actionable Info Logic (Tiered Approach):**
            - **Tier 1 (Action with Deadline):** If the input JSON has an "objection_period" (e.g., "sixty (60) days", "30 days") or a "deadline", you MUST use this value.
              Example: "Submit objections within sixty (60) days from the notice date."
              **CRITICAL: Do NOT say 'check the gazette' if a specific period is provided in the JSON.**
            - **Tier 2 (Action without Deadline):** If the input has an action but NO "objection_period" or "deadline", advise checking official sources.
              Example: "Provide feedback. Check NTSA website for details."
            - **Tier 3 (Informational):** For appointments etc., provide context. Ex: "Note the new EPRA board leadership."

        3.  **Content Requirements:**
            - title: Clear, engaging headline.
            - summary: One-sentence key takeaway.
            
            - ****** PHASE 2 FIX ******
            - article: Detailed, human-readable article (200-400 words).
              CRITICAL: This field MUST be plain, human-readable text. Do NOT use any markdown formatting (like '*', '#', or '-' for lists). Write in full paragraphs.
            - ****** END OF FIX ******
            
            - xSummary: Engagement friendly but informative summary under 276 characters,don't include Hashtags.
            - actionableInfo: Text from tiered logic above.
            - significance: A 1-10 rating (integer) of how important this notice is to the general public (1=low, 10=critical).
        **CRITICAL: Your entire response MUST be a single, valid JSON object starting with `{` and ending with `}`. Do NOT include any text before or after the JSON object.**

        **STRUCTURED DATA TO USE:**
        %s

        **OUTPUT JSON FORMAT:**
        {
          "title": "...",
          "summary": "...",
          "article": "...",
          "xSummary": "...",
          "actionableInfo": "...",
          "significance": 5
        }
        """.formatted(extractedData.toString());

        log.info("Attempting Generation for category {}...", category);
        String generatedContentResponse = generateWithRetry(geminiProModel, generationPrompt);

        return parseSafeJson(generatedContentResponse);
    }


    private String generateWithRetry(GenerativeModel model, String prompt) {
        int maxRetries = 3;
        long retryDelaySeconds = 2;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                if (attempt > 1) {
                    log.warn("Waiting {} seconds before retry attempt {}...", retryDelaySeconds, attempt);
                    TimeUnit.SECONDS.sleep(retryDelaySeconds);
                    retryDelaySeconds *= 2;
                }

                log.debug("Sending text request to model: {} (Attempt {})", model.getModelName(), attempt);
                GenerateContentResponse response = model.generateContent(prompt);
                return ResponseHandler.getText(response).trim();

            } catch (Exception e) {
                log.warn("Vertex AI call failed (Attempt {}/{}): {}", attempt, maxRetries, e.getMessage());
                if (attempt == maxRetries) {
                    log.error("Max retries reached. Giving up.", e);
                    return null;
                }
                if (e.getMessage().contains("PERMISSION_DENIED")) {
                    log.error("Authentication error. Not retrying.", e);
                    return null;
                }
            }
        }
        return null;
    }

    private String generateWithRetry(GenerativeModel model, List<Part> partsList) {
        int maxRetries = 3;
        long retryDelaySeconds = 5;

        Content content = Content.newBuilder().setRole("user").addAllParts(partsList).build();

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                if (attempt > 1) {
                    log.warn("Waiting {} seconds before retry attempt {}...", retryDelaySeconds, attempt);
                    TimeUnit.SECONDS.sleep(retryDelaySeconds);
                    retryDelaySeconds *= 2;
                }

                log.debug("Sending vision request to model: {} (Attempt {})", model.getModelName(), attempt);
                GenerateContentResponse response = model.generateContent(content);

                String text = ResponseHandler.getText(response).trim();
                if (text.isEmpty()) {
                    log.warn("Vertex AI response had no text content. (Attempt {})", attempt);
                    continue;
                }
                return text;

            } catch (Exception e) {
                log.warn("Vertex AI vision call failed (Attempt {}/{}): {}", attempt, maxRetries, e.getMessage());
                if (attempt == maxRetries) {
                    log.error("Max retries reached. Giving up.", e);
                    return null;
                }
                if (e.getMessage().contains("PERMISSION_DENIED")) {
                    log.error("Authentication error. Not retrying.", e);
                    return null;
                }
            }
        }
        return null;
    }


    private JSONObject parseSafeJson(String text) {
        if (text == null || text.isBlank()) {
            return null;
        }
        text = text.replaceAll("(?s)```json\\s*(.*?)\\s*```", "$1").trim();

        int start = text.indexOf('{');
        int end = text.lastIndexOf('}');
        if (start == -1 || end == -1 || end < start) {
            log.warn("Failed to parse JSON: No valid '{{...}}' structure found.");
            return null;
        }
        text = text.substring(start, end + 1);

        String fixedText = fixCommonJsonErrors(text);

        try {
            return new JSONObject(fixedText);
        } catch (JSONException e) {
            log.warn("Failed to parse JSON after first fix: {} - Attempting aggressive recovery...", e.getMessage());

            String aggressiveFixedText = aggressiveJsonClean(fixedText);
            try {
                return new JSONObject(aggressiveFixedText);
            } catch (JSONException e2) {
                log.error("JSON parsing FAILED even after aggressive recovery. Error: {}", e2.getMessage());
                log.error("--- BAD JSON (first 500 chars) --- \n{}", fixedText.substring(0, Math.min(500, fixedText.length())));
                return null;
            }
        }
    }

    private String fixCommonJsonErrors(String json) {
        json = json.replaceAll(",\\s*}", "}");
        json = json.replaceAll(",\\s*]", "]");
        json = json.replaceAll("([{,]\\s*)([a-zA-Z_][a-zA-Z0-9_]*)\\s*:", "$1\"$2\":");
        return json;
    }

    private String aggressiveJsonClean(String json) {
        Pattern pattern = Pattern.compile("(?s)\"([^\"]*)\"");
        Matcher matcher = pattern.matcher(json);
        StringBuffer sb = new StringBuffer();

        while (matcher.find()) {
            String content = matcher.group(1);
            content = content.replace("\n", "\\n")
                    .replace("\t", "\\t")
                    .replace("\r", "\\r");
            matcher.appendReplacement(sb, "\"" + Matcher.quoteReplacement(content) + "\"");
        }
        matcher.appendTail(sb);
        json = sb.toString();

        json = json.replaceAll(",\\s*}", "}");
        json = json.replaceAll(",\\s*]", "]");
        return json;
    }


    @Async
    public void retryFailedNotices() {
        if (!isProcessing.compareAndSet(false, true)) {
            log.warn("Cannot start RETRY job. Another job (like a PDF upload) is already in progress.");
            return;
        }

        stopProcessing.set(false);

        log.info("Starting retry process for FAILED notices...");

        List<Gazette> failedNotices = gazetteRepository.findAllFailedWithCorrectSorting();
        if (failedNotices.isEmpty()) {
            log.info("No FAILED notices found to retry.");
            isProcessing.set(false);
            return;
        }

        log.info("Found {} FAILED notices to retry.", failedNotices.size());

        for (Gazette notice : failedNotices) {
            try {
                if (stopProcessing.get()) {
                    log.warn("Retry processing manually stopped by admin.");
                    break;
                }

                if (notice.getTitle().startsWith("[GENERATION FAILED]")) {
                    log.info("Retrying notice #{} (GENERATION failure)...", notice.getId());

                    Object extractedData = null;
                    String articleJson = notice.getArticle();
                    articleJson = articleJson.replaceAll("(?s)```json\\s*(.*?)\\s*```", "$1").trim();

                    try {
                        extractedData = new JSONObject(articleJson);
                    } catch (JSONException e) {
                        try {
                            extractedData = new JSONArray(articleJson);
                        } catch (JSONException e2) {
                            log.error("Could not retry notice #{}: Failed to parse extracted JSON. Content: {}", notice.getId(), articleJson);
                            continue;
                        }
                    }

                    if (extractedData == null) {
                        log.error("Could not retry notice #{}: Failed to parse extracted JSON.", notice.getId());
                        continue;
                    }

                    // --- FIX: Pass all required arguments to runGenerationStep ---
                    Gazette generatedNotice = runGenerationStep(extractedData, notice.getContent(), notice.getCategory(), notice.getSourceOrder(), null, notice.getOriginalPdfPath());

                    if (generatedNotice != null && generatedNotice.getStatus() == ProcessingStatus.SUCCESS) {
                        updateExistingNotice(notice, generatedNotice);
                        log.info("SUCCESS: Retry for notice #{} was successful.", notice.getId());
                    } else {
                        log.warn("FAIL: Retry for notice #{} (GENERATION) failed again.", notice.getId());
                    }
                }

                TimeUnit.MILLISECONDS.sleep(500);

            } catch (Exception e) {
                log.error("Unhandled exception while retrying notice #{}: {}", notice.getId(), e.getMessage());
            }
        }

        log.info("Finished retry process.");
        isProcessing.set(false);
        stopProcessing.set(false);
        log.info("Retry job finished. Processing lock released.");
    }

    // Helper method to run ONLY the Generation step (Step 3)
    private Gazette runGenerationStep(Object extractedData, String rawContent, String category, int sourceOrder, JSONObject overallGazetteDetails, String originalPdfPath) {
        log.info("Attempting Generation for retried notice...");

        String generationPrompt = """
        You are an expert editorial assistant for Smart Gazette. Your goal is to simplify government notices for Kenyan youth.
        Based ONLY on the structured JSON data provided below, generate a JSON object containing five fields: "title", "summary", "article", "xSummary", and "actionableInfo".

        **Your Instructions:**
        1.  **Grouping Logic:**
            - If the category is `Land_Property` or `Tenders` and the input data contains multiple similar items (look for arrays), create a single "digest" article. Title like "Land Transfer Notices" or "New Tenders". Article uses markdown lists. Otherwise, create a normal article.
        
        2.  **Actionable Info Logic (Tiered Approach):**
            - **Tier 1 (Action with Deadline):** If the input JSON has an "objection_period" (e.g., "sixty (60) days", "30 days") or a "deadline", you MUST use this value.
              Example: "Submit objections within sixty (60) days from the notice date."
              **CRITICAL: Do NOT say 'check the gazette' if a specific period is provided in the JSON.**
            - **Tier 2 (Action without Deadline):** If the input has an action but NO "objection_period" or "deadline", advise checking official sources.
              Example: "Provide feedback. Check NTSA website for details."
            - **Tier 3 (Informational):** For appointments etc., provide context. Ex: "Note the new EPRA board leadership."

        3.  **Content Requirements:**
            - title: Clear, engaging headline.
            - summary: One-sentence key takeaway.
            - article: Detailed, human-readable article (200-400 words), markdown formatted.
              CRITICAL: This field MUST be plain, human-readable text. Do NOT use any markdown formatting (like '*', '#', or '-' for lists). Write in full paragraphs.
            - xSummary:a Engagement friendly but informative summary under 276 characters, similar in tone to Moe (moneycademyke) on X.
            - actionableInfo: Text from tiered logic above.

        **CRITICAL: Your entire response MUST be a single, valid JSON object starting with `{` and ending with `}`. Do NOT include any text before or after the JSON object.**

        **STRUCTURED DATA TO USE:**
        %s

        **OUTPUT JSON FORMAT:**
        {
          "title": "...",
          "summary": "...",
          "article": "...",
          "xSummary": "...",
          "actionableInfo": "..."
        }
        """.formatted(extractedData.toString());

        String generatedContentResponse = generateWithRetry(geminiProModel, generationPrompt);
        JSONObject generatedContent = parseSafeJson(generatedContentResponse);

        if (generatedContent == null) {
            log.error("Generation step failed on retry.");
            return null;
        }
        log.info("Generation complete on retry.");

        // --- FIX: Pass originalPdfPath to final creator method ---
        return createGazetteFromJson(extractedData, generatedContent, rawContent, category, sourceOrder, overallGazetteDetails, originalPdfPath);
    }

    private void updateExistingNotice(Gazette oldNotice, Gazette newNotice) {
        oldNotice.setTitle(newNotice.getTitle());
        oldNotice.setSummary(newNotice.getSummary());
        oldNotice.setArticle(newNotice.getArticle());
        oldNotice.setActionableInfo(newNotice.getActionableInfo());
        oldNotice.setXSummary(newNotice.getXSummary());
        oldNotice.setNoticeNumber(newNotice.getNoticeNumber());
        oldNotice.setSignatory(newNotice.getSignatory());
        oldNotice.setPublishedDate(newNotice.getPublishedDate());
        oldNotice.setGazetteVolume(newNotice.getGazetteVolume());
        oldNotice.setGazetteNumber(newNotice.getGazetteNumber());
        oldNotice.setGazetteDate(newNotice.getGazetteDate());
        oldNotice.setCategory(newNotice.getCategory());
        oldNotice.setContent(newNotice.getContent());
        oldNotice.setStatus(ProcessingStatus.SUCCESS);

        gazetteRepository.save(oldNotice);
    }

    private Gazette createGazetteFromJson(Object extractedData, JSONObject generatedContent, String rawContent, String category, int order, JSONObject overallGazetteDetails, String originalPdfPath) {
        boolean isNull = extractedData == null;
        boolean isEmptyArray = (extractedData instanceof JSONArray) && ((JSONArray) extractedData).isEmpty();

        if (isNull || isEmptyArray) {
            log.error("Cannot create Gazette object: extractedData is null or empty for order {}", order);
            // --- FIX: Pass all required arguments, including the new path ---
            return createFallbackGazette(rawContent, order, overallGazetteDetails, "Extraction failed: AI returned null or empty 'items'", originalPdfPath);
        }

        Gazette gazette = new Gazette();
        String sanitizedRawContent = rawContent.replace("\u0000", "");
        gazette.setContent(sanitizedRawContent);
        gazette.setCategory(category);
        gazette.setSourceOrder(order);
        gazette.setOriginalPdfPath(originalPdfPath);

        if (overallGazetteDetails != null) {
            gazette.setGazetteVolume(overallGazetteDetails.optString("gazetteVolume", ""));
            gazette.setGazetteNumber(overallGazetteDetails.optString("gazetteNumber", ""));
            try {
                String dateStr = overallGazetteDetails.optString("gazetteDate");
                if (dateStr != null && !dateStr.isBlank()) {
                    gazette.setGazetteDate(LocalDate.parse(dateStr));
                }
            } catch (DateTimeParseException | JSONException e) {
                log.warn("Could not parse gazetteDate from header: {}", overallGazetteDetails.optString("gazetteDate"));
            }
        }

        if (generatedContent != null) {
            gazette.setStatus(ProcessingStatus.SUCCESS);
            gazette.setTitle(generatedContent.optString("title", "Untitled Notice").replace("\u0000", ""));
            gazette.setSummary(generatedContent.optString("summary", "No summary provided.").replace("\u0000", ""));
            gazette.setArticle(generatedContent.optString("article", extractedData.toString()).replace("\u0000", ""));
            gazette.setXSummary(generatedContent.optString("xSummary", "").replace("\u0000", ""));
            gazette.setActionableInfo(generatedContent.optString("actionableInfo", "").replace("\u0000", ""));
            gazette.setSignificanceRating(generatedContent.optInt("significance", 3));
        } else {
            gazette.setStatus(ProcessingStatus.FAILED);
            gazette.setTitle("[GENERATION FAILED] " + category + " Notice (Review Extracted Data)");
            gazette.setSummary("AI failed to generate summary. Review extracted data below.");
            gazette.setArticle("## Extracted Data (Generation Failed):\n\n```json\n" + extractedData.toString().replace("\u0000", "") + "\n```");
            gazette.setXSummary("");
            gazette.setActionableInfo("Review needed");
            gazette.setSignificanceRating(3);
        }

        String noticeNumber = "";
        String signatory = "";
        String dateStr = "";

        if (extractedData instanceof JSONObject singleItem) {
            noticeNumber = singleItem.optString("notice_id", singleItem.optString("reference_number", ""));
            signatory = singleItem.optString("signatory", "");
            dateStr = singleItem.optString("publication_date", singleItem.optString("effective_date", ""));
        } else if (extractedData instanceof JSONArray itemArray) {
            if (!itemArray.isEmpty()) {
                JSONObject firstItem = itemArray.getJSONObject(0);
                noticeNumber = firstItem.optString("notice_id", firstItem.optString("reference_number", ""));
                signatory = firstItem.optString("signatory", "");
                dateStr = firstItem.optString("publication_date", firstItem.optString("effective_date", ""));
            }
        }

        if (noticeNumber.isEmpty()) {
            // Look for pattern "GAZETTE NOTICE NO. 1234" in the raw text
            Pattern p = Pattern.compile("GAZETTE NOTICE NO\\.\\s*(\\d+)", Pattern.CASE_INSENSITIVE);
            Matcher m = p.matcher(rawContent);
            if (m.find()) {
                noticeNumber = m.group(1); // Capture just the digits
                log.info("Recovered missing notice number using Regex: {}", noticeNumber);
            }
        }

        gazette.setNoticeNumber(noticeNumber.replace("\u0000", ""));
        gazette.setSignatory(signatory.replace("\u0000", ""));

        try {
            if (!dateStr.isBlank()) gazette.setPublishedDate(LocalDate.parse(dateStr));
            else if (gazette.getGazetteDate() != null) gazette.setPublishedDate(gazette.getGazetteDate());
            else gazette.setPublishedDate(LocalDate.now());
        } catch (DateTimeParseException e) {
            gazette.setPublishedDate(LocalDate.now());
        }

        // --- IMPLEMENT AUTONOMOUS POSTING ---
        if (gazette.getStatus() == ProcessingStatus.SUCCESS && gazette.getSignificanceRating() >= 8) {
            log.info("Autonomous Posting: Notice #{} has High Significance ({}). Posting to X...", gazette.getId(), gazette.getSignificanceRating());
            iftttWebhookService.postTweet(gazette.getXSummary());
        }

        return gazette;
    }

    private Gazette createFallbackGazette(String text, int order, JSONObject overallGazetteDetails, String reason, String originalPdfPath) {
        Gazette g = new Gazette();
        g.setStatus(ProcessingStatus.FAILED);
        g.setTitle("[PROCESSING FAILED] Review Needed");
        g.setSummary("The AI failed during processing. Reason: " + reason);
        g.setXSummary("Processing error. Needs manual review.");
        g.setContent(text != null ? text.replace("\u0000", "") : "Content was null.");
        g.setArticle("## AI PROCESSING FAILED\n\n**Reason:** " + reason + "\n\nThe original text has been saved. You can try to fix it manually or use the 'Retry FAILED Notices' button.");
        g.setCategory("Uncategorized");
        g.setPublishedDate(LocalDate.now());
        g.setSourceOrder(order);
        // Set the permanent path to the file
        g.setOriginalPdfPath(originalPdfPath);

        if (overallGazetteDetails != null) {
            g.setGazetteVolume(overallGazetteDetails.optString("gazetteVolume", ""));
            g.setGazetteNumber(overallGazetteDetails.optString("gazetteNumber", ""));
            try {
                String dateStr = overallGazetteDetails.optString("gazetteDate");
                if (dateStr != null && !dateStr.isBlank()) {
                    g.setGazetteDate(LocalDate.parse(dateStr));
                }
            } catch (DateTimeParseException | JSONException e) {
                // Ignore
            }
        }
        return g;
    }

    private String loadSchemaFile(String path) {
        try (InputStream is = getClass().getResourceAsStream(path)) {
            if (is == null) {
                log.error("Schema file not found: {}", path);
                return "";
            }
            return new String(is.readAllBytes());
        } catch (IOException e) {
            log.error("Failed to load schema file: {}", path, e);
            return "";
        }
    }

    public void addThumbUp(Long id) {
        Gazette gazette = gazetteRepository.findById(id).orElse(null);
        if (gazette != null) {
            gazette.setThumbsUp(gazette.getThumbsUp() + 1);
            gazetteRepository.save(gazette);
            log.info("Added Thumbs Up for article ID: {}", id);
        }
    }

    public void addThumbDown(Long id) {
        Gazette gazette = gazetteRepository.findById(id).orElse(null);
        if (gazette != null) {
            gazette.setThumbsDown(gazette.getThumbsDown() + 1);
            gazetteRepository.save(gazette);
            log.info("Added Thumbs Down for article ID: {}", id);
        }
    }
    // This is the implementation for incrementing the view count
    public Gazette incrementViewCount(Long id) {
        Gazette gazette = gazetteRepository.findById(id).orElse(null);
        if (gazette != null) {
            gazette.setViewCount(gazette.getViewCount() + 1);
            return gazetteRepository.save(gazette);
        }
        return null;
    }

    // --- ADD THIS HELPER METHOD (Required by processSingleNotice) ---
// This method implements the logic for processing short notices or chunks.
    private Gazette processTextSegment(String textSegment, int sourceOrder, JSONObject overallGazetteDetails, String originalPdfPath) {
        // --- STEP 1: AI Triage ---
        String category = triageNoticeCategory(textSegment);

        if (category == null) {
            log.warn("Triage failed for notice segment {}. Creating fallback.", sourceOrder);
            return createFallbackGazette(textSegment, sourceOrder, overallGazetteDetails, "Triage failed", originalPdfPath);
        }

        // --- STEP 2: AI Extraction ---
        String schemaPath = "/schemas/field/" + category.toLowerCase() + ".json";
        String schemaContent = loadSchemaFile(schemaPath);
        if (schemaContent.isEmpty()) {
            log.error("Schema file not found for category '{}' (Notice {}). Creating fallback.", category, sourceOrder);
            return createFallbackGazette(textSegment, sourceOrder, overallGazetteDetails, "Schema file not found", originalPdfPath);
        }

        String extractionPrompt = """
    Extract structured data from the text below according to the provided JSON schema.
    ... (omitted prompt text for brevity) ...
    """.formatted(schemaContent, textSegment);

        String jsonResponse = generateWithRetry(geminiProModel, extractionPrompt);
        JSONObject extractedDataWrapper = parseSafeJson(jsonResponse);

        if (extractedDataWrapper == null || !extractedDataWrapper.has("items")) {
            log.error("Extraction failed for notice segment {}. AI did not return a valid 'items' wrapper.", sourceOrder);
            return createFallbackGazette(textSegment, sourceOrder, overallGazetteDetails, "Extraction failed: no 'items' wrapper", originalPdfPath);
        }
        Object extractedData = extractedDataWrapper.get("items");

        // ... (rest of the validation logic) ...

        // --- STEP 3: AI Generation ---
        JSONObject generatedContent = generateNarrativeContent(extractedData, category);

        // Final Step: Map everything to our Gazette entity
        return createGazetteFromJson(extractedData, generatedContent, textSegment, category, sourceOrder, overallGazetteDetails, originalPdfPath);
    }

    // --- BULK DELETE METHOD ---
// This is a minimal helper to allow the Controller to call bulk delete
    public void deleteGazetteInBulk(List<Long> ids) {
        gazetteRepository.deleteAllById(ids);
        log.info("Bulk deleted {} notices.", ids.size());
    }
    // --- Export Batch to Excel Stream ---
    public ByteArrayInputStream exportBatchToExcel(String originalPdfPath) {
        List<Gazette> batchNotices = gazetteRepository.findAllByOriginalPdfPath(originalPdfPath);
        log.info("Exporting batch for path: {}. Found {} notices.", originalPdfPath, batchNotices.size());
        return excelExportService.generateExcelReport(batchNotices);
    }
}