package com.smartgazette.smartgazette.service;

// --- VERTEX AI SDK UPGRADE: ADDED IMPORTS ---
import com.google.cloud.vertexai.VertexAI;
import com.google.cloud.vertexai.api.Blob;
import com.google.cloud.vertexai.api.Content;
import com.google.cloud.vertexai.api.GenerateContentResponse;
import com.google.cloud.vertexai.api.GenerationConfig;
import com.google.cloud.vertexai.api.Part;
import com.google.cloud.vertexai.generativeai.GenerativeModel;
import com.google.cloud.vertexai.generativeai.ResponseHandler;
import com.google.protobuf.ByteString;
// --- END OF IMPORTS ---

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
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class GazetteService {

    private static final Logger log = LoggerFactory.getLogger(GazetteService.class);
    private final GazetteRepository gazetteRepository;
    private final String PDF_STORAGE_PATH = "storage/gazettes/";

    private final AtomicBoolean isProcessing = new AtomicBoolean(false);
    private final AtomicBoolean stopProcessing = new AtomicBoolean(false);

    // --- VERTEX AI SDK UPGRADE: NEW MEMBER VARIABLES ---
    private final VertexAI vertexAI;
    private final GenerativeModel geminiProModel;
    private final GenerativeModel geminiFlashModel;

    // We get these model names from your application.properties file
    @Value("${gemini.model.pro:gemini-2.5-pro}")
    private String geminiProModelName;

    @Value("${gemini.model.flash:gemini-2.5-flash")
    private String geminiFlashModelName;
    // --- END OF VERTEX AI SDK UPGRADE ---

    // --- VERTEX AI SDK UPGRADE: CONSTRUCTOR ---
    // The constructor is now "injected" with the GCP project ID and location
    // and it initializes the Vertex AI SDK clients.
    public GazetteService(GazetteRepository gazetteRepository,
                          @Value("${gcp.project.id}") String projectId,
                          @Value("${gcp.location}") String location) {
        this.gazetteRepository = gazetteRepository;

        log.info("Initializing Vertex AI SDK for project '{}' in location '{}'", projectId, location);
        this.vertexAI = new VertexAI(projectId, location);

        // Build generation configs
        GenerationConfig textGenConfig = GenerationConfig.newBuilder()
                .setTemperature(0.2f)
                .setMaxOutputTokens(4096) // Increased token limit
                .setTopP(0.95f)
                .build();

        GenerationConfig visionGenConfig = GenerationConfig.newBuilder()
                .setMaxOutputTokens(8192)
                .setTemperature(0.1f)
                .build();

        // Initialize models
        // Note: We use @Value fields for model names, but initialize them here.
        // A more robust way would be to pass model names directly, but this works.
        this.geminiProModel = new GenerativeModel.Builder()
                .setModelName("gemini-2.5-pro") // Hardcoding from your properties for safety
                .setVertexAi(this.vertexAI)
                .setGenerationConfig(textGenConfig)
                .build();

        this.geminiFlashModel = new GenerativeModel.Builder()
                .setModelName("gemini-2.5-flash") // Hardcoding from your properties for safety
                .setVertexAi(this.vertexAI)
                .setGenerationConfig(visionGenConfig) // Use vision config for Flash
                .build();

        log.info("✅ Vertex AI SDK initialization complete!");
    }
    // --- END OF VERTEX AI SDK UPGRADE ---

    // --- Core Public Methods (Unchanged from T018) ---
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

    @Async
    public void processAndSavePdf(File file) {
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
                // This method is now powered by the Vertex AI SDK
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
                // This method is now powered by the Vertex AI SDK
                overallGazetteDetails = extractGazetteHeaderDetails(highQualityFullText);
            }

            List<String> notices = segmentTextByNotices(highQualityFullText);
            log.info("PDF segmented into {} potential notices.", notices.size());

            // --- LOGIC FIX (from Test T016/T017) ---
            if (notices.isEmpty() && highQualityFullText != null && !highQualityFullText.isBlank()) {
                log.warn("Segmentation found 0 notices. Assuming a single-notice document.");
                notices.add(highQualityFullText);
                log.info("Processing document as 1 single notice.");
            }
            // --- END OF LOGIC FIX ---

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
                    // This method now uses the Vertex AI SDK for all its sub-calls
                    gazette = processSingleNotice(noticeText, sourceOrder, overallGazetteDetails);

                    if (gazette != null) {
                        log.info("Saving {} article: '{}' (Cat: '{}', Num: {}, GazDate: {})",
                                gazette.getStatus(), gazette.getTitle(), gazette.getCategory(), gazette.getNoticeNumber(), gazette.getGazetteDate());
                        gazetteRepository.save(gazette);
                    }
                } catch (Exception e) {
                    log.error("Error processing or checking notice #{}. Creating a fallback.", sourceOrder, e);
                    gazetteRepository.save(createFallbackGazette(noticeText, sourceOrder, overallGazetteDetails, "Unhandled pipeline error"));
                }

                try {
                    // --- P0 FIX (T017): API Rate Limiting ---
                    log.debug("Pausing for 500ms to respect rate limits...");
                    TimeUnit.MILLISECONDS.sleep(500); // 500ms delay
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Rate limit pause interrupted.");
                }
            } // End of for loop

            log.info("<<<< Successfully finished processing PDF file: {}", file.getName());
        } catch (Exception e) {
            log.error("Critical error during PDF processing pipeline for file: {}", file.getName(), e);
        } finally {
            // --- THIS IS THE NEW LOGIC TO SAVE THE PDF ---
            try {
                File storageDir = new File(PDF_STORAGE_PATH);
                if (!storageDir.exists()) {
                    storageDir.mkdirs();
                }

                Path sourcePath = file.toPath();
                // Create a unique name to prevent conflicts
                String originalFileName = file.getName().replaceAll("sg-upload-.*\\.pdf", "gazette-" + System.currentTimeMillis() + ".pdf");
                Path destinationPath = new File(storageDir, originalFileName).toPath();

                Files.move(sourcePath, destinationPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                log.info("Moved processed PDF to permanent storage: {}", destinationPath);

                // Now, find all notices we just saved (that don't have a path) and update them
                if (overallGazetteDetails != null && overallGazetteDetails.has("gazetteDate") && !overallGazetteDetails.getString("gazetteDate").isEmpty()) {
                    LocalDate gazetteDate = LocalDate.parse(overallGazetteDetails.getString("gazetteDate"));
                    List<Gazette> justProcessed = gazetteRepository.findAllByGazetteDateAndOriginalPdfPathIsNull(gazetteDate);

                    for (Gazette g : justProcessed) {
                        g.setOriginalPdfPath(destinationPath.toString());
                        gazetteRepository.save(g);
                    }
                    log.info("Updated {} notices with the PDF path: {}", justProcessed.size(), destinationPath);
                }

            } catch (IOException e) {
                log.error("Failed to move processed PDF to storage: {}", e.getMessage());
                // Ensure temp file is deleted even if move fails
                try {
                    Files.deleteIfExists(file.toPath());
                } catch (IOException ignored) {
                }
            }
            // --- END OF NEW LOGIC ---

            isProcessing.set(false);
            stopProcessing.set(false);
            log.info("Processing lock released.");
        }
    }

    // --- VERTEX AI SDK UPGRADE: "First Page Only" Vision OCR ---
    // This method is replaced with the Vertex AI SDK version.
    // It's more reliable and uses the SDK's retry logic.
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
            BufferedImage bim = pdfRenderer.renderImageWithDPI(0, 300); // 300 DPI
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

        // Call the Vertex AI SDK
        String firstPageCleanText = generateWithRetry(geminiFlashModel, partsList); // Use Flash model

        if (firstPageCleanText != null) {
            StringBuilder fullCleanText = new StringBuilder(firstPageCleanText).append("\n\n");
            log.info("Successfully extracted high-fidelity text for Page 1.");

            if (document.getNumberOfPages() > 1) {
                log.info("Using fast PDFTextStripper for pages 2 through {}.", document.getNumberOfPages());
                PDFTextStripper stripper = new PDFTextStripper();
                stripper.setStartPage(2); // Start from page 2
                stripper.setEndPage(document.getNumberOfPages());
                String restOfDocumentText = stripper.getText(document);
                fullCleanText.append(restOfDocumentText);
            }
            return fullCleanText.toString();
        } else {
            log.error("Vision OCR call failed for Page 1.");
            return null; // This will trigger the fallback in the main method
        }
    }
    // --- END OF VERTEX AI SDK UPGRADE ---

    // This segmentation logic is from your T018 file and is proven to work.
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

    // --- VERTEX AI SDK UPGRADE: This method now uses generateWithRetry ---
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

        String jsonResponse = generateWithRetry(geminiFlashModel, prompt); // SDK Call
        JSONObject headerDetails = parseSafeJson(jsonResponse);

        if (headerDetails == null) {
            log.error("Failed to extract Gazette header details from text.");
            return null;
        }
        log.info("Successfully extracted Gazette header details: {}", headerDetails.toString());
        return headerDetails;
    }
    // --- END OF VERTEX AI SDK UPGRADE ---

    // This is our 3-Step "Assembly Line"
    // It now uses the SDK for its sub-calls
    // --- THIS IS THE FINAL, T020-COMPLIANT VERSION ---
    private Gazette processSingleNotice(String noticeText, int sourceOrder, JSONObject overallGazetteDetails) {

        // --- STEP 1: AI Triage ---
        String category = triageNoticeCategory(noticeText); // Uses SDK
        if (category == null) {
            log.warn("Triage failed for notice segment {}. Creating fallback.", sourceOrder);
            return createFallbackGazette(noticeText, sourceOrder, overallGazetteDetails, "Triage failed");
        }
        log.info("Triage complete for notice segment {}. Category: {}", sourceOrder, category);

        // --- STEP 2: AI Extraction ---
        String schemaPath = "/schemas/field/" + category.toLowerCase() + ".json";
        String schemaContent = loadSchemaFile(schemaPath);
        if (schemaContent.isEmpty()) {
            schemaPath = "/schemas/field/" + category.substring(0, 1).toUpperCase() + category.substring(1).toLowerCase() + ".json";
            schemaContent = loadSchemaFile(schemaPath);
            if(schemaContent.isEmpty()) {
                log.error("Schema file not found for category '{}' (Notice {}). Searched two paths. Creating fallback.", category, sourceOrder);
                return createFallbackGazette(noticeText, sourceOrder, overallGazetteDetails, "Schema file not found");
            }
        }

        // --- THIS IS THE NEW, T020-RECOMMENDED PROMPT ---
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
        // --- END OF NEW PROMPT ---

        String jsonResponse = generateWithRetry(geminiProModel, extractionPrompt); // SDK Call

        // The new robust parser is used here
        JSONObject extractedDataWrapper = parseSafeJson(jsonResponse);

        if (extractedDataWrapper == null || !extractedDataWrapper.has("items")) {
            log.error("Extraction failed for notice segment {}. AI did not return a valid 'items' wrapper.", sourceOrder);
            return createFallbackGazette(noticeText, sourceOrder, overallGazetteDetails, "Extraction failed: no 'items' wrapper");
        }
        Object extractedData = extractedDataWrapper.get("items");

        boolean isNull = extractedData == null || extractedData == JSONObject.NULL;
        boolean isEmptyObject = (extractedData instanceof JSONObject) && ((JSONObject) extractedData).isEmpty();

        if (isNull || isEmptyObject) {
            log.error("Extraction failed for notice segment {}. AI returned 'items' as null or an empty object.", sourceOrder);
            return createFallbackGazette(noticeText, sourceOrder, overallGazetteDetails, "Extraction failed: 'items' was null or empty");
        }

        log.info("Extraction complete for notice segment {}.", sourceOrder);

        // --- STEP 3: AI Generation ---
        JSONObject generatedContent = generateNarrativeContent(extractedData, category); // Uses SDK

        if (generatedContent == null) {
            log.error("Generation step failed for notice segment {}. Saving with extracted data only.", sourceOrder);
        } else {
            log.info("Generation complete for notice segment {}.", sourceOrder);
        }

        return createGazetteFromJson(extractedData, generatedContent, noticeText, category, sourceOrder, overallGazetteDetails);
    }

    // --- VERTEX AI SDK UPGRADE: This method now uses generateWithRetry ---
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

        String category = generateWithRetry(geminiFlashModel, triagePrompt); // SDK Call

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
    // --- END OF VERTEX AI SDK UPGRADE ---

    // --- VERTEX AI SDK UPGRADE: This method now uses generateWithRetry ---
    private JSONObject generateNarrativeContent(Object extractedData, String category) {
        String generationPrompt = """
        You are an expert editorial assistant for Smart Gazette. Your goal is to simplify government notices for Kenyan youth.
        Based ONLY on the structured JSON data provided below, generate a JSON object containing five fields: "title", "summary", "article", "xSummary", and "actionableInfo".

        **Your Instructions:**
        1.  **Grouping Logic:**
            - If the input data is a JSON array, create a single "digest" article. Title like "Land Transfer Notices" or "New Tenders".
            - Otherwise, create a normal article.
        
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
            
            - xSummary: Engagement friendly but informative summary under 276 characters, similar in tone to Moe (moneycademyke) on X.
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

        log.info("Attempting Generation for category {}...", category);
        String generatedContentResponse = generateWithRetry(geminiProModel, generationPrompt); // SDK Call

        return parseSafeJson(generatedContentResponse);
    }
    // --- END OF VERTEX AI SDK UPGRADE ---


    // --- VERTEX AI SDK UPGRADE: NEW HELPER METHODS ---
    // These replace the old `makeGeminiApiCall` methods.

    /**
     * Calls the Vertex AI SDK with built-in retry logic for text prompts.
     */
    private String generateWithRetry(GenerativeModel model, String prompt) {
        int maxRetries = 3;
        long retryDelaySeconds = 2;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                if (attempt > 1) {
                    log.warn("Waiting {} seconds before retry attempt {}...", retryDelaySeconds, attempt);
                    TimeUnit.SECONDS.sleep(retryDelaySeconds);
                    retryDelaySeconds *= 2; // Exponential backoff
                }

                log.debug("Sending text request to model: {} (Attempt {})", model.getModelName(), attempt);
                GenerateContentResponse response = model.generateContent(prompt);
                return ResponseHandler.getText(response).trim();

            } catch (Exception e) { // Catch a broad range of exceptions from the SDK
                log.warn("Vertex AI call failed (Attempt {}/{}): {}", attempt, maxRetries, e.getMessage());
                if (attempt == maxRetries) {
                    log.error("Max retries reached. Giving up.", e);
                    return null;
                }
                // Check for specific "stop" conditions if needed, e.g., auth errors
                if (e.getMessage().contains("PERMISSION_DENIED")) {
                    log.error("Authentication error. Not retrying.", e);
                    return null;
                }
            }
        }
        return null;
    }

    /**
     * Calls the Vertex AI SDK with built-in retry logic for multimodal (vision) prompts.
     */
    private String generateWithRetry(GenerativeModel model, List<Part> partsList) {
        int maxRetries = 3;
        long retryDelaySeconds = 5; // Longer delay for vision

        Content content = Content.newBuilder().setRole("user").addAllParts(partsList).build();

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                if (attempt > 1) {
                    log.warn("Waiting {} seconds before retry attempt {}...", retryDelaySeconds, attempt);
                    TimeUnit.SECONDS.sleep(retryDelaySeconds);
                    retryDelaySeconds *= 2; // Exponential backoff
                }

                log.debug("Sending vision request to model: {} (Attempt {})", model.getModelName(), attempt);
                GenerateContentResponse response = model.generateContent(content);

                // --- P0 FIX (T017): Fix JSONObject["content"] not found ---
                // The new SDK handles this gracefully. We just need to check the text.
                String text = ResponseHandler.getText(response).trim();
                if (text.isEmpty()) {
                    log.warn("Vertex AI response had no text content. (Attempt {})", attempt);
                    continue; // Go to next retry
                }
                return text;
                // --- END OF P0 FIX ---

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
    // --- END OF VERTEX AI SDK UPGRADE ---


    // --- NEW ROBUST JSON PARSER (Fixes T018+ errors) ---
    private JSONObject parseSafeJson(String text) {
        if (text == null || text.isBlank()) {
            return null;
        }

        // 1. Remove markdown code blocks
        text = text.replaceAll("(?s)```json\\s*(.*?)\\s*```", "$1").trim();

        // 2. Find the first '{' and last '}'
        int start = text.indexOf('{');
        int end = text.lastIndexOf('}');
        if (start == -1 || end == -1 || end < start) {
            log.warn("Failed to parse JSON: No valid '{{...}}' structure found.");
            return null;
        }
        text = text.substring(start, end + 1);

        // 3. Run the first cleaning pass (fixes common syntax errors)
        String fixedText = fixCommonJsonErrors(text);

        try {
            // 4. Try to parse the fixed text
            return new JSONObject(fixedText);
        } catch (JSONException e) {
            log.warn("Failed to parse JSON after first fix: {} - Attempting aggressive recovery...", e.getMessage());

            // 5. If it still fails, run the aggressive cleaner
            String aggressiveFixedText = aggressiveJsonClean(fixedText);
            try {
                // 6. Try to parse one last time
                return new JSONObject(aggressiveFixedText);
            } catch (JSONException e2) {
                log.error("JSON parsing FAILED even after aggressive recovery. Error: {}", e2.getMessage());
                log.error("--- BAD JSON (first 500 chars) --- \n{}", fixedText.substring(0, Math.min(500, fixedText.length())));
                return null;
            }
        }
    }

    /**
     * Helper for parseSafeJson: Fixes common, simple AI syntax errors.
     */
    private String fixCommonJsonErrors(String json) {
        // Fixes trailing commas before a } or ]
        json = json.replaceAll(",\\s*}", "}");
        json = json.replaceAll(",\\s*]", "]");

        // Fixes missing quotes on property keys (e.g., { key: "value" } -> { "key": "value" })
        json = json.replaceAll("([{,]\\s*)([a-zA-Z_][a-zA-Z0-9_]*)\\s*:", "$1\"$2\":");

        return json;
    }

    /**
     * Helper for parseSafeJson: Aggressively strips invalid characters.
     * --- THIS IS THE COMPATIBLE FIX ---
     */
    private String aggressiveJsonClean(String json) {
        // Remove all newline and tab characters from inside strings
        // This is a common failure point

        // Use Pattern and Matcher for universal Java compatibility
        Pattern pattern = Pattern.compile("(?s)\"([^\"]*)\"");
        Matcher matcher = pattern.matcher(json);
        StringBuffer sb = new StringBuffer();

        while (matcher.find()) {
            String content = matcher.group(1);
            // Escape newlines, tabs, and carriage returns within the string
            content = content.replace("\n", "\\n")
                    .replace("\t", "\\t")
                    .replace("\r", "\\r");
            // Manually append the fixed string with quotes
            matcher.appendReplacement(sb, "\"" + Matcher.quoteReplacement(content) + "\"");
        }
        matcher.appendTail(sb);
        json = sb.toString();

        // Re-run the trailing comma fix after newline removal
        json = json.replaceAll(",\\s*}", "}");
        json = json.replaceAll(",\\s*]", "]");

        return json;
    }
    // --- END OF NEW JSON PARSER ---

    // This retry logic is from your T018 file and is proven to work.
    @Async
    public void retryFailedNotices() {
        if (!isProcessing.compareAndSet(false, true)) {
            log.warn("Cannot start RETRY job. Another job is already in progress.");
            return;
        }
        stopProcessing.set(false);
        log.info("Starting retry process for FAILED notices...");

        List<Gazette> failedNotices = gazetteRepository.findAllFailedWithCorrectSorting();
        if (failedNotices.isEmpty()) {
            log.info("No FAILED notices found to retry.");
            isProcessing.set(false); // Release lock
            return;
        }

        log.info("Found {} FAILED notices to retry.", failedNotices.size());

        for (Gazette notice : failedNotices) {
            try {
                if (stopProcessing.get()) {
                    log.warn("Retry processing manually stopped by admin.");
                    break;
                }

                log.info("Retrying notice #{} (Full Pipeline)...", notice.getId());
                // This call now uses the full Vertex AI SDK pipeline
                Gazette retriedNotice = processSingleNotice(notice.getContent(), notice.getSourceOrder(), null);

                if (retriedNotice != null && retriedNotice.getStatus() == ProcessingStatus.SUCCESS) {
                    updateExistingNotice(notice, retriedNotice);
                    log.info("SUCCESS: Retry for notice #{} was successful.", notice.getId());
                } else {
                    log.warn("FAIL: Retry for notice #{} failed again.", notice.getId());
                    if(retriedNotice != null) {
                        notice.setArticle(notice.getArticle() + "\n\n--- RETRY FAILED ---\n" + retriedNotice.getArticle());
                        gazetteRepository.save(notice);
                    }
                }

                try {
                    // --- P0 FIX (T017): API Rate Limiting ---
                    log.debug("Pausing for 500ms to respect rate limits...");
                    TimeUnit.MILLISECONDS.sleep(500); // 500ms delay
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Rate limit pause interrupted.");
                }

            } catch (Exception e) {
                log.error("Unhandled exception while retrying notice #{}: {}", notice.getId(), e.getMessage());
            }
        }

        log.info("Finished retry process.");
        isProcessing.set(false);
        stopProcessing.set(false);
        log.info("Retry job finished. Processing lock released.");
    }

    // This helper is from your T018 file and is proven to work.
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

    // This helper is from your T018 file and is proven to work.
    private Gazette createGazetteFromJson(Object extractedData, JSONObject generatedContent, String rawContent, String category, int order, JSONObject overallGazetteDetails) {
        // --- FIX for T016/T017 Log ---
        boolean isNull = extractedData == null || extractedData == JSONObject.NULL;
        boolean isEmptyArray = (extractedData instanceof JSONArray) && ((JSONArray) extractedData).isEmpty();
        boolean isEmptyObject = (extractedData instanceof JSONObject) && ((JSONObject) extractedData).isEmpty();

        if (isNull || isEmptyArray || isEmptyObject) {
            log.error("Cannot create Gazette object: extractedData is null, empty array, or empty object for order {}", order);
            return createFallbackGazette(rawContent, order, overallGazetteDetails, "Extraction failed: AI returned null or empty 'items'");
        }
        // --- End of FIX ---

        Gazette gazette = new Gazette();
        gazette.setContent(rawContent.replace("\u0000", ""));
        gazette.setCategory(category);
        gazette.setSourceOrder(order);

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
        } else {
            gazette.setStatus(ProcessingStatus.FAILED);
            gazette.setTitle("[GENERATION FAILED] " + category + " Notice (Review Extracted Data)");
            gazette.setSummary("AI failed to generate summary. Review extracted data below.");
            gazette.setArticle("## Extracted Data (Generation Failed):\n\n```json\n" + extractedData.toString().replace("\u0000", "") + "\n```");
            gazette.setXSummary("");
            gazette.setActionableInfo("Review needed");
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

        gazette.setNoticeNumber(noticeNumber.replace("\u0000", ""));
        gazette.setSignatory(signatory.replace("\u0000", ""));

        try {
            if (!dateStr.isBlank()) gazette.setPublishedDate(LocalDate.parse(dateStr));
            else if (gazette.getGazetteDate() != null) gazette.setPublishedDate(gazette.getGazetteDate());
            else gazette.setPublishedDate(LocalDate.now()); // Fallback
        } catch (DateTimeParseException e) {
            gazette.setPublishedDate(LocalDate.now()); // Fallback
        }

        return gazette;
    }

    // This helper is from your T018 file and is proven to work.
    private Gazette createFallbackGazette(String text, int order, JSONObject overallGazetteDetails, String reason) {
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

    // This helper is from your T018 file and is proven to work.
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

    // --- (METRIC COLLECTION) ---
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
    public Gazette incrementViewCount(Long id) {
        Gazette gazette = gazetteRepository.findById(id).orElse(null);
        if (gazette != null) {
            gazette.setViewCount(gazette.getViewCount() + 1);
            return gazetteRepository.save(gazette);
        }
        return null;
    }

    // -METHOD FOR PAGINATION ---
    public Page<Gazette> listSuccessfulGazettesPaginated(int pageNum, int pageSize) {
        // We start pages from 0, so we subtract 1
        Pageable pageable = PageRequest.of(pageNum - 1, pageSize);
        return gazetteRepository.findAllSuccessfulWithCorrectSorting(pageable);
    }

    // --- NEW METHOD FOR CATEGORY PAGE ---
    public Page<Gazette> listSuccessfulGazettesByCategory(String category, int pageNum, int pageSize) {
        Pageable pageable = PageRequest.of(pageNum - 1, pageSize);
        return gazetteRepository.findAllSuccessfulByCategory(category, pageable);
    }
}