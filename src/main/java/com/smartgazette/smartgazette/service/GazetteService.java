package com.smartgazette.smartgazette.service;

import org.springframework.web.client.RestTemplate;
import org.springframework.http.*;
import java.util.Base64;
import java.util.Map;

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

    private final RestTemplate restTemplate;

    // Groq — all text calls (triage, extraction, generation)
    @Value("${groq.api.key}")
    private String groqApiKey;

    @Value("${groq.model.pro:llama-3.3-70b-versatile}")
    private String groqProModelName;

    @Value("${groq.model.flash:llama-3.1-8b-instant}")
    private String groqFlashModelName;

    // Gemini)
    @Value("${gemini.api.key}")
    private String geminiApiKey;

    @Value("${gemini.model.ocr:gemini-2.5-flash}")
    private String geminiOcrModelName;

    @Value("${gemini.model.flashLite:gemini-3.1-flash-lite}")
    private String geminiFlashLiteModelName;

    // Gemini 3.1 Flash Lite free tier: 15 RPM, 500 RPD. We track both
    // ourselves so we can skip straight to Groq when we know we'd be
    // rate-limited, instead of burning 3 retries discovering it via 429.
    private final java.util.Deque<Long> geminiCallTimestamps = new java.util.concurrent.ConcurrentLinkedDeque<>();
    private static final int GEMINI_RPM_LIMIT = 15;
    private static final int GEMINI_RPD_BUFFER_LIMIT = 480; // leave headroom under 500

    private final java.util.concurrent.atomic.AtomicInteger geminiDailyCount = new java.util.concurrent.atomic.AtomicInteger(0);
    private volatile LocalDate geminiDailyResetDate = LocalDate.now();

    private final IftttWebhookService iftttWebhookService;
    private final ExcelExportService excelExportService;


    public GazetteService(GazetteRepository gazetteRepository,
                          IftttWebhookService iftttWebhookService,
                          ExcelExportService excelExportService) {
        this.gazetteRepository = gazetteRepository;
        this.iftttWebhookService = iftttWebhookService;
        this.excelExportService = excelExportService;
        this.restTemplate = new RestTemplate();
        log.info("✅ GazetteService initialized with AI Studio REST client.");
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
    public Page<Gazette> listSuccessfulGazettesPaginated(int pageNum, int pageSize, String filter) {
        Pageable pageable = PageRequest.of(pageNum - 1, pageSize);

        if ("popular".equals(filter)) {
            return gazetteRepository.findAllSuccessfulOrderByPopularity(pageable);
        } else if ("significant".equals(filter)) {
            return gazetteRepository.findAllSuccessfulOrderBySignificance(pageable);
        } else {
            // DEFAULT: "latest" now means "Recently Processed" (ID DESC)
            return gazetteRepository.findAllSuccessfulOrderByRecentlyProcessed(pageable);
        }
    }

    public Page<Gazette> listSuccessfulGazettesByCategory(String category, int pageNum, int pageSize, String filter) {
        Pageable pageable = PageRequest.of(pageNum - 1, pageSize);

        if ("popular".equals(filter)) {
            return gazetteRepository.findAllSuccessfulOrderByPopularity(pageable);
        } else if ("significant".equals(filter)) {
            return gazetteRepository.findAllSuccessfulOrderBySignificance(pageable);
        } else {
            // DEFAULT: "latest" now means "Recently Processed" (ID DESC)
            return gazetteRepository.findAllSuccessfulOrderByRecentlyProcessed(pageable);
        }
    }

    public List<Gazette> getAllGazettes(String filter) {
        if ("oldest".equals(filter)) {
            return gazetteRepository.findAllWithCorrectSorting();
        } else if ("popular".equals(filter)) {
            return gazetteRepository.findAllOrderByPopularity();
        } else if ("significant".equals(filter)) {
            return gazetteRepository.findAllOrderBySignificance();
        } else {
            // DEFAULT: "latest" (Recently Processed / ID DESC)
            return gazetteRepository.findAllOrderByRecentlyProcessed();
        }
    }

    // --- NEW: Batch Filter ---
    public List<Gazette> getGazettesByBatch(String gazetteNumber) {
        return gazetteRepository.findAllByGazetteNumber(gazetteNumber);
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

            List<NoticeSegment> notices = segmentTextByNotices(highQualityFullText);
            log.info("PDF segmented into {} potential notices.", notices.size());

            if (notices.isEmpty() && highQualityFullText != null && !highQualityFullText.isBlank()) {
                log.warn("Segmentation found 0 notices. Assuming a single-notice document.");
                notices.add(NoticeSegment.of(1, highQualityFullText));
            }

// --- Task 2.4a: pre-categorize every segment up front ---
// This was previously done inside processSingleNotice per-notice; doing it
// here instead means triage runs exactly once per segment (no behavior
// change in cost) but now ALSO lets us group same-category segments
// before extraction, which is what makes batching possible.
            List<NoticeSegment> categorized = new ArrayList<>();
            for (NoticeSegment segment : notices) {
                String category = triageNoticeCategory(segment.rawText());
                categorized.add(segment.withCategory(category));
                try { TimeUnit.MILLISECONDS.sleep(500); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            }
            notices = categorized;

// --- Partition: batchable Land_Property reissuance vs everything else ---
            List<NoticeSegment> batchEligible = new ArrayList<>();
            List<NoticeSegment> normalQueue = new ArrayList<>();
            for (NoticeSegment segment : notices) {
                boolean isLandProperty = "Land_Property".equals(segment.preFilterCategory());
                boolean isMultiCase = segment.isMultiCase(); // multi-case always routes separately, never batched
                if (isLandProperty && !isMultiCase && isBatchableLandReissuance(segment.rawText())) {
                    batchEligible.add(segment);
                } else {
                    normalQueue.add(segment);
                }
            }

            if (!batchEligible.isEmpty()) {
                log.info("Found {} batch-eligible Land_Property reissuance notices in this issue.", batchEligible.size());
                for (int i = 0; i < batchEligible.size(); i += BATCH_GROUP_SIZE) {
                    List<NoticeSegment> chunk = batchEligible.subList(i, Math.min(i + BATCH_GROUP_SIZE, batchEligible.size()));
                    if (chunk.size() < 2) {
                        // Not worth batching a single leftover notice — process normally
                        normalQueue.addAll(chunk);
                        continue;
                    }
                    List<Gazette> batchResults = processBatchedGroup(chunk, "Land_Property", overallGazetteDetails, originalPdfPath);
                    for (Gazette g : batchResults) {
                        if (g != null) {
                            log.info("Saving {} batched article: '{}'", g.getStatus(), g.getTitle());
                            gazetteRepository.save(g);
                        }
                    }
                    try { TimeUnit.MILLISECONDS.sleep(500); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                }
            }

            log.info("Routing {} notices through normal pipeline ({} were batch-processed above).",
                    normalQueue.size(), batchEligible.size());

            for (NoticeSegment segment : normalQueue) {
                log.info("-----> Processing Notice {}/{} ({} chars, ~{} tokens, oversized={}, multiCase={})...",
                        segment.sourceOrder(), normalQueue.size(),
                        segment.charCount(), segment.estimatedTokens(), segment.isOversized(), segment.isMultiCase());
                if (stopProcessing.get()) {
                    log.warn("Processing manually stopped by admin at notice #{}", segment.sourceOrder());
                    break;
                }
                try {
                    if (segment.isMultiCase()) {
                        List<Gazette> subCases = processMultiCaseNotice(segment, overallGazetteDetails, originalPdfPath);
                        for (Gazette g : subCases) {
                            log.info("Saving {} sub-case: '{}' (Parent: {})",
                                    g.getStatus(), g.getTitle(), g.getParentNoticeNumber());
                            gazetteRepository.save(g);
                        }
                    } else {
                        Gazette gazette = processSingleNotice(segment, overallGazetteDetails, originalPdfPath);
                        if (gazette != null) {
                            log.info("Saving {} article: '{}' (Cat: '{}', Num: {}, GazDate: {})",
                                    gazette.getStatus(), gazette.getTitle(), gazette.getCategory(),
                                    gazette.getNoticeNumber(), gazette.getGazetteDate());
                            gazetteRepository.save(gazette);
                        }
                    }
                } catch (Exception e) {
                    log.error("Error processing notice #{}. Creating a fallback.", segment.sourceOrder(), e);
                    gazetteRepository.save(createFallbackGazette(
                            segment.rawText(), segment.sourceOrder(),
                            overallGazetteDetails, "Unhandled pipeline error", originalPdfPath));
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
            isProcessing.set(false);
            stopProcessing.set(false);
            log.info("Processing lock released.");
        }
    }


    private String extractHighFidelityTextFromPdf(PDDocument document) throws IOException, InterruptedException {
        log.info("Starting Vision OCR for FIRST PAGE ONLY...");
        PDFRenderer pdfRenderer = new PDFRenderer(document);
        List<Object> visionParts = new ArrayList<>();

        visionParts.add(Map.of("text", """
        You are a high-fidelity OCR service.
        Extract all text from this page image, preserving all original line breaks and formatting.
        Return ONLY the extracted text, no commentary.
        """));

        BufferedImage bim = pdfRenderer.renderImageWithDPI(0, 300);
        byte[] imageBytes;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            ImageIO.write(bim, "jpeg", baos);
            imageBytes = baos.toByteArray();
        }

        visionParts.add(Map.of(
                "inline_data", Map.of(
                        "mime_type", "image/jpeg",
                        "data", Base64.getEncoder().encodeToString(imageBytes)
                )
        ));

        String firstPageCleanText = generateWithRetry(geminiOcrModelName, visionParts);



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

    private List<NoticeSegment> segmentTextByNotices(String fullText) {
        List<NoticeSegment> notices = new ArrayList<>();
        final Pattern pattern = Pattern.compile("(?m)^GAZETTE NOTICE NO\\.\\s*\\d+", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(fullText);
        int lastEnd = 0;
        while (matcher.find()) {
            if (matcher.start() > lastEnd) {
                String notice = fullText.substring(lastEnd, matcher.start()).trim();
                if (!notice.isEmpty()) {
                    notices.add(NoticeSegment.of(notices.size() + 1, notice));
                }
            }
            lastEnd = matcher.start();
        }
        if (lastEnd < fullText.length()) {
            String lastNotice = fullText.substring(lastEnd).trim();
            if (!lastNotice.isEmpty()) {
                notices.add(NoticeSegment.of(notices.size() + 1, lastNotice));
            }
        }

        if (!notices.isEmpty() && !pattern.matcher(notices.get(0).rawText()).find()) {
            log.info("Removing potential header text from segmentation.");
            notices.remove(0);
        }
        return notices;
    }


    private JSONObject extractGazetteHeaderDetails(String headerText) {
        log.info("Attempting to extract Gazette header details from clean text...");
        String prompt = """
TASK: Find and copy three values from the text below into the JSON template.

OUTPUT TEMPLATE — copy values exactly as they appear:
{
  "gazetteVolume": "[e.g. Vol. CXXVIII — copy exactly]",
  "gazetteNumber": "[e.g. No. 21 — copy exactly]",
  "gazetteDate": "[convert to YYYY-MM-DD format — e.g. 2026-01-30]"
}

RULES:
- First character must be { and last must be }.
- No text before or after the JSON.
- If a value is not found, use empty string "".
- gazetteDate must always be YYYY-MM-DD format.

TEXT:
%s
""".formatted(headerText.substring(0, Math.min(headerText.length(), 2000)));

        String jsonResponse = generateWithRetry(groqFlashModelName, prompt, 150);
        JSONObject headerDetails = parseSafeJson(jsonResponse);

        if (headerDetails == null) {
            log.error("Failed to extract Gazette header details from text.");
            return null;
        }
        log.info("Successfully extracted Gazette header details: {}", headerDetails.toString());
        return headerDetails;
    }

    /**
     * Task 3.1 — ModelRouter: selectExtractionModel()
     *
     * WHY THIS EXISTS:
     * The 70B model (groqProModelName) has a 100,000 token/day limit.
     * The 8B model (groqFlashModelName) has a ~500,000 token/day limit.
     *
     * Simple, repetitive categories (probate notices, land title notices)
     * follow rigid templates. The 8B model handles them correctly and costs
     * 5x less of the daily budget. The 70B model is reserved for notices
     * with complex, ambiguous, or multi-part structure.
     *
     * ANALOGY: You don't call a senior lawyer to review a standard form.
     * A junior with a checklist handles it faster and cheaper.
     */
    private String selectExtractionModel(String category) {
        return switch (category) {
            case "Court_Legal",
                 "Land_Property",
                 "Company_Registrations",
                 "Licensing" -> {
                log.debug("ModelRouter: Using Flash (8b) for category {}", category);
                yield groqFlashModelName;
            }
            default -> {
                log.debug("ModelRouter: Using Pro (70b) for category {}", category);
                yield groqProModelName;
            }
        };
    }

    // --- NEW: processSingleNotice with correct signature ---
    private Gazette processSingleNotice(NoticeSegment segment, JSONObject overallGazetteDetails, String originalPdfPath) {
        int sourceOrder = segment.sourceOrder();

        if (segment.isOversized()) {
            log.warn("Notice {} is oversized ({} chars, ~{} tokens). Using sentence-boundary truncation.",
                    sourceOrder, segment.charCount(), segment.estimatedTokens());
        }

        // Category was already determined in the pre-categorization pass —
        // reuse it instead of paying for a second triage call.
        String category = segment.preFilterCategory();
        if (category == null || category.isBlank()) {
            log.warn("Segment {} reached processSingleNotice with no pre-computed category. Triaging now as a fallback.", sourceOrder);
            category = triageNoticeCategory(segment.rawText());
        }

        if (category == null) {
            log.warn("Triage failed for notice segment {}. Creating fallback.", sourceOrder);
            return createFallbackGazette(segment.rawText(), sourceOrder, overallGazetteDetails, "Triage failed", originalPdfPath);
        }
        log.info("Processing notice segment {}. Category: {}", sourceOrder, category);

        String schemaPath = "/schemas/field/" + category.toLowerCase() + ".json";
        String schemaContent = loadSchemaFile(schemaPath);
        if (schemaContent.isEmpty()) {
            log.error("Schema file not found for category '{}' (Notice {}). Creating fallback.", category, sourceOrder);
            return createFallbackGazette(segment.rawText(), sourceOrder, overallGazetteDetails, "Schema file not found", originalPdfPath);
        }

        // textForProcessing() handles oversized notices via sentence-boundary truncation
        // rawText() is preserved for DB storage — we never lose the original
        String cleanNoticeText = stripGazetteHeader(segment.textForProcessing());

        String extractionPrompt = """
    TASK: Extract fields from the notice text into the JSON schema below.

    RULES:
    - Output must be raw JSON only. First character must be { and last character must be }.
    - Do not write any text before or after the JSON.
    - Do not include the schema in the output — only the filled values.
    - If a field value is not present in the text, use an empty string "".
    - Do not invent, infer, or guess values not present in the text.
    - Preserve names, dates, and reference numbers exactly as written.
    - Root key must be "items". Value is either an object (one item) or array (multiple items).

    SCHEMA:
    %s

    NOTICE TEXT:
    %s
    """.formatted(schemaContent, cleanNoticeText);

        String extractionModel = selectExtractionModel(category);
        String jsonResponse = generateWithRetry(extractionModel, extractionPrompt);
        JSONObject extractedDataWrapper = parseSafeJson(jsonResponse);

        if (extractedDataWrapper == null || !extractedDataWrapper.has("items")) {
            log.error("Extraction failed for notice segment {}. AI did not return a valid 'items' wrapper.", sourceOrder);
            return createFallbackGazette(segment.rawText(), sourceOrder, overallGazetteDetails, "Extraction failed: no 'items' wrapper", originalPdfPath);
        }
        Object extractedData = extractedDataWrapper.get("items");

        boolean isNull = extractedData == null || extractedData == JSONObject.NULL;
        boolean isEmptyObject = (extractedData instanceof JSONObject) && ((JSONObject) extractedData).isEmpty();

        if (isNull || isEmptyObject) {
            log.error("Extraction failed for notice segment {}. AI returned 'items' as null or empty.", sourceOrder);
            return createFallbackGazette(segment.rawText(), sourceOrder, overallGazetteDetails, "Extraction failed: 'items' was null or empty", originalPdfPath);
        }

        log.info("Extraction complete for notice segment {}.", sourceOrder);

        JSONObject generatedContent = generateNarrativeContent(extractedData, category);

        if (generatedContent == null) {
            log.error("Generation step failed for notice segment {}. Saving with extracted data only.", sourceOrder);
        } else {
            log.info("Generation complete for notice segment {}.", sourceOrder);
        }

        // rawText() preserved for DB — never the truncated version
        return createGazetteFromJson(extractedData, generatedContent, segment.rawText(), category, sourceOrder, overallGazetteDetails, originalPdfPath);
    }

    /**
     * Handles a notice segment that bundles multiple court cases under one
     * shared GAZETTE NOTICE NO. (the probate/"PROBATE AND ADMINISTRATION"
     * pattern — see NoticeSegment.isMultiCase()).
     *
     * Splits the segment into per-case chunks (each carrying the shared
     * court header), runs the normal triage/extraction/generation pipeline
     * on each chunk independently, and tags every resulting Gazette row
     * with parentNoticeNumber so they can be found as a group later.
     *
     * Each sub-case gets its own sourceOrder offset (e.g. case 3 of notice
     * segment 50 becomes sourceOrder 50, 51, 52...) is NOT what we do here —
     * we keep the parent segment's sourceOrder on every sub-case and rely
     * on parentNoticeNumber + a per-case suffix in noticeNumber to keep
     * them distinguishable, since sourceOrder's job is "position in the
     * gazette issue", not "which sub-case is this".
     */
    private List<Gazette> processMultiCaseNotice(NoticeSegment segment, JSONObject overallGazetteDetails, String originalPdfPath) {
        List<Gazette> results = new ArrayList<>();
        int sourceOrder = segment.sourceOrder();

        // Multi-case notices are the probate pattern — the parent segment was
        // already triaged before splitting, so reuse that category for every
        // sub-case instead of re-triaging each chunk individually.
        String knownCategory = segment.preFilterCategory();

        String parentNoticeNumber = "";
        Matcher gnMatcher = Pattern.compile("GAZETTE NOTICE NO\\.\\s*(\\d+)", Pattern.CASE_INSENSITIVE)
                .matcher(segment.rawText());
        if (gnMatcher.find()) {
            parentNoticeNumber = gnMatcher.group(1);
        }

        List<String> caseChunks = segment.splitByCauseNumber();
        log.info("Notice {} is multi-case (parent G.N. {}, category {}): splitting into {} sub-cases.",
                sourceOrder, parentNoticeNumber, knownCategory, caseChunks.size());

        for (int i = 0; i < caseChunks.size(); i++) {
            String caseText = caseChunks.get(i);
            int caseIndex = i + 1;

            log.info("-----> Processing sub-case {}/{} of parent G.N. {}...",
                    caseIndex, caseChunks.size(), parentNoticeNumber);

            Gazette gazette;
            try {
                gazette = processTextSegment(caseText, sourceOrder, overallGazetteDetails, originalPdfPath, knownCategory);
            } catch (Exception e) {
                log.error("Error processing sub-case {}/{} of parent G.N. {}.",
                        caseIndex, caseChunks.size(), parentNoticeNumber, e);
                gazette = createFallbackGazette(caseText, sourceOrder, overallGazetteDetails,
                        "Unhandled error in multi-case sub-processing", originalPdfPath);
            }

            if (gazette != null) {
                gazette.setParentNoticeNumber(parentNoticeNumber);
            }
            results.add(gazette);

            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }

        return results;
    }

    /**
     * Strips the Kenya Gazette boilerplate header from a notice segment.
     *
     * WHY: The header ("THE KENYA GAZETTE Published by Authority...")
     * appears on the first page of every gazette. After segmentation,
     * the first notice segment often carries this header text.
     * We already extract volume/date/number from it separately.
     * Sending it again to extraction wastes ~300-500 tokens per notice
     * and introduces noise into the schema extraction.
     */
    private String stripGazetteHeader(String noticeText) {
        if (noticeText == null) return "";

        // Find where the actual notice content starts (GAZETTE NOTICE NO. XXXX)
        // Everything before the first notice marker is boilerplate header
        int noticeStart = noticeText.indexOf("GAZETTE NOTICE NO.");
        if (noticeStart == -1) {
            noticeStart = noticeText.indexOf("GAZETTE NOTICE NO .");
        }

        if (noticeStart > 0) {
            // Only strip if there's actually content before the notice marker
            // and that content looks like a header (short, under 1000 chars)
            String potentialHeader = noticeText.substring(0, noticeStart);
            if (potentialHeader.length() < 1000) {
                return noticeText.substring(noticeStart).trim();
            }
        }

        return noticeText.trim();
    }

    /**
     * Stage 0: Rule-based pre-classification using keyword patterns.
     *
     * WHY THIS EXISTS:
     * AI triage costs tokens. Kenya Gazette notices use highly predictable
     * legal language — "letters of administration" appears in nearly every
     * Court_Legal notice, without exception. We exploit this predictability
     * to classify ~80% of notices for free, before any AI call is made.
     *
     * DESIGN PRINCIPLE: "Heuristic rules" — the pre-AI approach to classification.
     * Rules are fast, free, and deterministic. AI handles only ambiguous cases.
     *
     * RETURNS: category string if confident match found, null if ambiguous.
     * null means "I don't know — ask the AI".
     */
    private String keywordPreFilter(String noticeText) {
        String text = noticeText.toLowerCase();

        // Court_Legal — probate, succession, administration of estates
        if (text.contains("letters of administration")
                || text.contains("grant of probate")
                || text.contains("succession cause")
                || text.contains("insolvency")
                || text.contains("dissolution of marriage")) {
            return "Court_Legal";
        }

        // Land_Property — title deeds, leases, EIA reports
        if (text.contains("title deed")
                || text.contains("certificate of lease")
                || text.contains("land title")
                || text.contains("land registrar")
                || text.contains("environmental impact")
                || text.contains("provisional certificate")) {
            return "Land_Property";
        }

        // Tenders — procurement, disposal of assets
        if (text.contains("invitation to tender")
                || text.contains("request for proposal")
                || text.contains("procurement")
                || text.contains("disposal of assets")
                || text.contains("expression of interest")) {
            return "Tenders";
        }

        // Appointments
        if (text.contains("is hereby appointed")
                || text.contains("are hereby appointed")
                || text.contains("appointment of")) {
            return "Appointments";
        }

        // Company_Registrations — incorporation and dissolution
        if (text.contains("hereby incorporated")
                || text.contains("certificate of incorporation")
                || text.contains("dissolution of")) {
            return "Company_Registrations";
        }

        // Legislation — bills, acts, regulations
        if (text.contains("bill, 20")
                || text.contains("act, 20")
                || text.contains("regulations, 20")
                || text.contains("legal notice")) {
            return "Legislation";
        }

        // Licensing
        if (text.contains("licence")
                || text.contains("license")
                || text.contains("permit")) {
            return "Licensing";
        }

        // No confident match — return null so AI triage handles it
        return null;
    }

    /**
     * Task 2.4a — sub-pattern detector for batchable Land_Property notices.
     *
     * WHY: Land_Property is not one homogeneous thing. Lost-document
     * reissuance ("ISSUE OF A PROVISIONAL CERTIFICATE", "RECONSTRUCTION OF
     * A GREEN CARD", "REPLACEMENT TITLE") is the same owner getting a
     * replacement for a lost document — nothing changes hands, near-zero
     * individual news value, near-identical boilerplate. Ownership transfer
     * ("REGISTRATION OF INSTRUMENT", "CANCELLATION OF A TITLE DEED") is a
     * genuine "this happened" event and must never be batched.
     *
     * This tag is pipeline-internal only — it never touches the category
     * enum, the DB, or the schema files. It only decides whether a
     * Land_Property segment is eligible for the batched extraction path.
     */
    private boolean isBatchableLandReissuance(String rawText) {
        String text = rawText.toUpperCase();

        // Explicit exclusion first — ownership change must never batch,
        // even if it also happens to contain a reissuance-sounding word.
        if (text.contains("REGISTRATION OF INSTRUMENT") || text.contains("CANCELLATION OF")) {
            return false;
        }

        return text.contains("PROVISIONAL CERTIFICATE")
                || text.contains("REPLACEMENT TITLE")
                || text.contains("RECONSTRUCTION OF")
                || text.contains("NEW LAND TITLE DEED")
                || text.contains("NEW LAND REGISTER")
                || text.contains("GREEN CARD")
                || text.contains("WHITE CARD")
                || text.contains("CERTIFICATE OF LEASE");
    }

    // Max notices combined into one batched extraction call. Keeps the
// combined prompt well under the token ceiling that caused the
// daily-quota failures in the original test run.
    private static final int BATCH_GROUP_SIZE = 5;


    private String triageNoticeCategory(String noticeText) {
        // Stage 0: Free keyword pre-filter — no AI call, no tokens used
        String preFiltered = keywordPreFilter(noticeText);
        if (preFiltered != null) {
            log.debug("Pre-filter classified notice as {} (AI triage skipped)", preFiltered);
            return preFiltered;
        }
        log.debug("Pre-filter inconclusive. Sending to AI triage...");

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
""".formatted(noticeText.substring(0, Math.min(noticeText.length(), 1500)));

        // Try Gemini Flash Lite first, but only if our own RPM/RPD tracking
        // says we have headroom — avoids burning retries discovering a 429.
        String category = null;
        if (canCallGeminiNow()) {
            category = generateWithRetry(geminiFlashLiteModelName, List.of(Map.of("text", triagePrompt)));
            if (category != null) {
                recordGeminiCall();
            }
        }

        if (category == null) {
            log.warn("Gemini unavailable or rate-limited. Falling back to Groq for this notice's triage.");
            category = generateWithRetry(groqFlashModelName, triagePrompt, 20);
        }

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
        log.warn("Both Gemini and Groq triage failed. Defaulting to Miscellaneous.");
        return "Miscellaneous";
    }

    /**
     * Checks whether we have headroom to call Gemini right now, under both
     * its RPM and RPD ceilings. Does NOT call Gemini — just tells the caller
     * whether trying is worthwhile, so we can skip straight to Groq when not.
     */
    private synchronized boolean canCallGeminiNow() {
        LocalDate today = LocalDate.now();
        if (!today.equals(geminiDailyResetDate)) {
            geminiDailyResetDate = today;
            geminiDailyCount.set(0);
            log.info("Gemini daily call counter reset for {}.", today);
        }

        if (geminiDailyCount.get() >= GEMINI_RPD_BUFFER_LIMIT) {
            log.debug("Gemini RPD buffer ({}) reached for today. Skipping Gemini, going straight to Groq.", GEMINI_RPD_BUFFER_LIMIT);
            return false;
        }

        long now = System.currentTimeMillis();
        long oneMinuteAgo = now - 60_000;
        while (!geminiCallTimestamps.isEmpty() && geminiCallTimestamps.peekFirst() < oneMinuteAgo) {
            geminiCallTimestamps.pollFirst();
        }

        if (geminiCallTimestamps.size() >= GEMINI_RPM_LIMIT) {
            log.debug("Gemini RPM limit ({}) reached for this minute. Skipping Gemini, going straight to Groq.", GEMINI_RPM_LIMIT);
            return false;
        }

        return true;
    }

    /** Call this only AFTER a Gemini call you're about to make/just made succeeds, to record it. */
    private void recordGeminiCall() {
        geminiCallTimestamps.addLast(System.currentTimeMillis());
        geminiDailyCount.incrementAndGet();
    }


    private JSONObject generateNarrativeContent(Object extractedData, String category) {
        String generationPrompt = """
TASK: Fill the output template below using only the data provided. Do not add information not present in the data.

OUTPUT TEMPLATE — fill every field, no exceptions:
{
  "title": "[One clear headline, max 12 words, no punctuation at end]",
  "summary": "[One sentence. What happened and who it affects. Max 25 words.]",
  "article": "[Three paragraphs. Paragraph 1: what the notice says. Paragraph 2: who is affected and what it means. Paragraph 3: what action is needed and by when. Plain text only, no markdown, no bullet points, no headers.]",
  "xSummary": "[Max 240 characters. Same information as summary but conversational. No hashtags.]",
  "actionableInfo": "[One sentence. If a deadline exists in the data, state it exactly. If no deadline, state the key action required.]",
  "significance": [integer 1-10, where 1=routine administrative, 5=affects a specific group, 10=affects all Kenyans]
}

RULES:
- First character of response must be { and last must be }.
- No text before or after the JSON.
- Fill every field. No field may be null or empty.
- Do not use markdown formatting anywhere inside the JSON values.
- Do not use bullet points, asterisks, or hyphens inside article or any other field.
- significance must be a bare integer, not a string.

DATA:
%s
""".formatted(extractedData.toString());

        log.info("Attempting Generation for category {}...", category);

        // Try Gemini 3.1 Flash Lite first, but only if our own RPM/RPD
        // tracking says we have headroom.
        String generatedContentResponse = null;
        if (canCallGeminiNow()) {
            generatedContentResponse = generateWithRetry(
                    geminiFlashLiteModelName, List.of(Map.of("text", generationPrompt)));
            if (generatedContentResponse != null) {
                recordGeminiCall();
            }
        }

        if (generatedContentResponse == null) {
            log.warn("Gemini unavailable or rate-limited. Falling back to Groq for this notice's generation.");
            String genModel = List.of("Court_Legal", "Land_Property", "Company_Registrations", "Licensing")
                    .contains(category) ? groqFlashModelName : groqProModelName;
            generatedContentResponse = generateWithRetry(genModel, generationPrompt);
        }

        return parseSafeJson(generatedContentResponse);
    }


    private String generateWithRetry(String modelName, String prompt, int maxTokens) {
        int maxRetries = 3;
        long retryDelaySeconds = 2;
        String url = "https://api.groq.com/openai/v1/chat/completions";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("Authorization", "Bearer " + groqApiKey);

        Map<String, Object> requestBody = Map.of(
                "model", modelName,
                "messages", List.of(Map.of("role", "user", "content", prompt)),
                "temperature", modelName.contains("8b") ? 0.1 : 0.2,
                "max_tokens", maxTokens
        );

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(requestBody, headers);

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                if (attempt > 1) {
                    log.warn("Waiting {}s before retry attempt {}...", retryDelaySeconds, attempt);
                    TimeUnit.SECONDS.sleep(retryDelaySeconds);
                    retryDelaySeconds *= 2;
                }
                log.debug("Sending text request to {} (Attempt {})", modelName, attempt);
                ResponseEntity<Map> response = restTemplate.postForEntity(url, request, Map.class);
                return extractTextFromResponse(response.getBody());

            } catch (Exception e) {
                log.warn("Groq call failed (Attempt {}/{}): {}", attempt, maxRetries, e.getMessage());
                if (attempt == maxRetries) {
                    log.error("Max retries reached for model {}. Giving up.", modelName);
                    return null;
                }
                long waitSeconds = retryDelaySeconds;
                if (e.getMessage() != null) {
                    java.util.regex.Matcher m = java.util.regex.Pattern
                            .compile("(?:retry in|try again in) (\\d+(?:\\.\\d+)?)(ms)?")
                            .matcher(e.getMessage());
                    if (m.find()) {
                        double value = Double.parseDouble(m.group(1));
                        boolean isMilliseconds = "ms".equals(m.group(2));
                        waitSeconds = isMilliseconds ? Math.max(1, (long)(value / 1000)) + 2 : (long)value + 2;
                        log.warn("API requested retry after {}s. Waiting...", waitSeconds);
                    }
                }
                try { TimeUnit.SECONDS.sleep(waitSeconds); }
                catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                retryDelaySeconds *= 2;
            }
        }
        return null;
    }

    // Existing call sites that need the full 4096 (extraction/generation JSON)
// keep working unchanged via this overload.
    private String generateWithRetry(String modelName, String prompt) {
        return generateWithRetry(modelName, prompt, 4096);
    }


    private String generateWithRetry(String modelName, List<Object> parts) {
        int maxRetries = 3;
        long retryDelaySeconds = 5;


        String url = "https://generativelanguage.googleapis.com/v1beta/models/"
                + modelName + ":generateContent?key=" + geminiApiKey;

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        // No Authorization header — key is in the URL above

        Map<String, Object> requestBody = Map.of(
                "contents", List.of(Map.of("parts", parts)),
                "generationConfig", Map.of(
                        "maxOutputTokens", 8192,
                        "temperature", 0.1
                )
        );

        HttpEntity<Map<String, Object>> request = new HttpEntity<>(requestBody, headers);

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                if (attempt > 1) {
                    log.warn("Waiting {}s before vision retry {}...", retryDelaySeconds, attempt);
                    TimeUnit.SECONDS.sleep(retryDelaySeconds);
                    retryDelaySeconds *= 2;
                }
                log.debug("Sending vision request to {} (Attempt {})", modelName, attempt);
                ResponseEntity<Map> response = restTemplate.postForEntity(url, request, Map.class);
                // Vision uses Gemini format — different parser from text calls
                String text = extractGeminiTextFromResponse(response.getBody());
                if (text == null || text.isBlank()) {
                    log.warn("Vision response had no text (Attempt {})", attempt);
                    continue;
                }
                log.info("Gemini call succeeded for model {} (Attempt {}).", modelName, attempt);
                return text;


            } catch (Exception e) {
                log.warn("Vision call failed (Attempt {}/{}): {}", attempt, maxRetries, e.getMessage());
                if (attempt == maxRetries) {
                    log.error("Max retries reached for vision call. Giving up.");
                    return null;
                }
                long waitSeconds = retryDelaySeconds;
                if (e.getMessage() != null) {
                    java.util.regex.Matcher m = java.util.regex.Pattern
                            .compile("retry in (\\d+)")
                            .matcher(e.getMessage());
                    if (m.find()) {
                        waitSeconds = Long.parseLong(m.group(1)) + 2;
                        log.warn("API requested retry after {}s. Waiting...", waitSeconds);
                    }
                }
                try { TimeUnit.SECONDS.sleep(waitSeconds); }
                catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                retryDelaySeconds *= 2;
            }
        }
        return null;
    }

    // Parses Groq / OpenAI format: choices[0].message.content
    @SuppressWarnings("unchecked")
    private String extractTextFromResponse(Map<?, ?> responseBody) {
        if (responseBody == null) return null;
        try {
            List<Map<?, ?>> choices = (List<Map<?, ?>>) responseBody.get("choices");
            if (choices == null || choices.isEmpty()) {
                log.warn("Groq response had no choices — possible error.");
                return null;
            }
            Map<?, ?> message = (Map<?, ?>) choices.get(0).get("message");
            return message.get("content").toString().trim();
        } catch (Exception e) {
            log.error("Failed to parse Groq response: {}", e.getMessage());
            return null;
        }
    }

    // Parses Gemini format: candidates[0].content.parts[0].text
// Used only by the vision generateWithRetry overload
    @SuppressWarnings("unchecked")
    private String extractGeminiTextFromResponse(Map<?, ?> responseBody) {
        if (responseBody == null) return null;
        try {
            List<Map<?, ?>> candidates = (List<Map<?, ?>>) responseBody.get("candidates");
            if (candidates == null || candidates.isEmpty()) {
                log.warn("Gemini vision response had no candidates — possible safety block.");
                return null;
            }
            Map<?, ?> content = (Map<?, ?>) candidates.get(0).get("content");
            List<Map<?, ?>> parts = (List<Map<?, ?>>) content.get("parts");
            return parts.get(0).get("text").toString().trim();
        } catch (Exception e) {
            log.error("Failed to parse Gemini vision response: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Task 2.4a (hybrid) — one call does BOTH extraction and generation for
     * the whole group of fungible Land_Property reissuance notices. Bigger
     * token saving than extraction-only batching, while still staying safe
     * because reissuance articles are themselves near-templated — low risk
     * of the AI mixing details between unrelated people in one response.
     *
     * Falls back to one-by-one normal processing if the batch call fails
     * or returns a mismatched item count.
     */
    private List<Gazette> processBatchedGroup(List<NoticeSegment> group, String category,
                                              JSONObject overallGazetteDetails, String originalPdfPath) {
        List<Gazette> results = new ArrayList<>();

        String schemaPath = "/schemas/field/" + category.toLowerCase() + ".json";
        String schemaContent = loadSchemaFile(schemaPath);
        if (schemaContent.isEmpty()) {
            log.error("Schema file not found for batched category '{}'. Falling back to individual processing.", category);
            for (NoticeSegment segment : group) {
                results.add(processSingleNotice(segment, overallGazetteDetails, originalPdfPath));
            }
            return results;
        }

        StringBuilder noticesBlock = new StringBuilder();
        for (int i = 0; i < group.size(); i++) {
            noticesBlock.append("NOTICE ").append(i + 1).append(":\n")
                    .append(stripGazetteHeader(group.get(i).textForProcessing()))
                    .append("\n\n");
        }

        String batchPrompt = """
    TASK: For EACH of the %d notices below, extract fields per the schema AND write the article content, combined into one output per notice.

    RULES:
    - Output must be raw JSON only. First character must be { and last character must be }.
    - Do not write any text before or after the JSON.
    - Root key must be "items". Value MUST be a JSON ARRAY with exactly %d elements.
    - Element order MUST match notice order exactly (element 1 = NOTICE 1, etc.).
    - Each element must contain ALL extraction fields from the schema below, PLUS these five
      generation fields: "title", "summary", "article", "xSummary", "actionableInfo", "significance".
    - title: clear headline, max 12 words, no punctuation at end.
    - summary: one sentence, max 25 words.
    - article: three short paragraphs, plain text, no markdown.
    - xSummary: max 240 characters, conversational, no hashtags.
    - actionableInfo: one sentence — state the deadline exactly if one exists in that notice's data.
    - significance: integer 1-10, bare integer not a string.
    - If a field is not present in a given notice's text, use an empty string "" for that field.
    - Do not invent, infer, or guess values not present in the text.
    - Do not mix details between notices — each element describes only its own notice.

    EXTRACTION SCHEMA (applies to every item, in addition to the five generation fields above):
    %s

    %s
    """.formatted(group.size(), group.size(), schemaContent, noticesBlock.toString());

        log.info("Sending hybrid batched extraction+generation for {} {} notices via Gemini Flash Lite (1 call instead of {}).",
                group.size(), category, group.size() * 2);

// Batched calls are large single prompts (multiple notices combined) —
// Gemini 3.1 Flash Lite's 250K TPM ceiling handles this comfortably,
// versus Groq's 6,000 TPM which a single batch could exhaust in one call.
        List<Object> textParts = List.of(Map.of("text", batchPrompt));
        String jsonResponse = generateWithRetry(geminiFlashLiteModelName, textParts);
        JSONObject wrapper = parseSafeJson(jsonResponse);

        JSONArray items = null;
        if (wrapper != null && wrapper.has("items") && wrapper.get("items") instanceof JSONArray) {
            items = wrapper.getJSONArray("items");
        }

        if (items == null || items.length() != group.size()) {
            log.warn("Hybrid batch failed or returned mismatched item count ({} expected, got {}). " +
                            "Falling back to individual processing for this group.",
                    group.size(), items == null ? "null" : items.length());
            for (NoticeSegment segment : group) {
                results.add(processSingleNotice(segment, overallGazetteDetails, originalPdfPath));
            }
            return results;
        }

        log.info("Hybrid batched extraction+generation succeeded for {} notices.", group.size());

        for (int i = 0; i < group.size(); i++) {
            NoticeSegment segment = group.get(i);
            JSONObject itemData = items.getJSONObject(i);

            // The same object IS both the extracted data and the generated content —
            // createGazetteFromJson reads extraction fields off extractedData and
            // generation fields off generatedContent, so we pass itemData as both.
            Gazette gazette = createGazetteFromJson(itemData, itemData, segment.rawText(),
                    category, segment.sourceOrder(), overallGazetteDetails, originalPdfPath);
            results.add(gazette);
        }

        return results;
    }

    private JSONObject parseSafeJson(String text) {
        if (text == null || text.isBlank()) {
            return null;
        }

        // Stage 0: Strip markdown code fences (```json ... ```)
        // Gemini sometimes wraps its JSON in markdown, even when told not to.
        text = text.replaceAll("(?s)```json\\s*(.*?)\\s*```", "$1").trim();

        // Stage 0b: Extract the outermost { ... } boundary.
        // This discards any conversational prefix like "Here is the JSON:"
        // Find the LAST complete {...} block — handles model returning template + real JSON
        // The real data is always last when a preamble exists
        int end = text.lastIndexOf('}');
        if (end == -1) {
            log.warn("parseSafeJson: No closing brace found.");
            return null;
        }

        // Walk backwards from the last } to find its matching opening {
        // This is more reliable than indexOf('{') which finds the preamble
        int depth = 0;
        int start = -1;
        for (int i = end; i >= 0; i--) {
            char c = text.charAt(i);
            if (c == '}') depth++;
            else if (c == '{') {
                depth--;
                if (depth == 0) { start = i; break; }
            }
        }

        if (start == -1) {
            log.warn("parseSafeJson: Could not find matching opening brace.");
            return null;
        }
        text = text.substring(start, end + 1);

        // Stage 1: Try the clean string first.
        // Most responses (especially after T020 prompt improvements) will parse here.
        try {
            return new JSONObject(text);
        } catch (JSONException e1) {
            log.warn("parseSafeJson [Stage 1 FAIL]: {}. Trying cosmetic fixes...",
                    e1.getMessage());
        }

        // Stage 2: Cosmetic fixes — trailing commas, unquoted keys.
        // Regex is APPROPRIATE here: these are cosmetic, pattern-based errors.
        // A trailing comma ",}" is always wrong regardless of context.
        String stage2 = fixCommonJsonErrors(text);
        try {
            return new JSONObject(stage2);
        } catch (JSONException e2) {
            log.warn("parseSafeJson [Stage 2 FAIL]: {}. Trying structural repair...",
                    e2.getMessage());
        }

        // Stage 3: Structural repair — missing commas between sibling objects.
        // This is the NEW stage. Uses a character scanner because regex cannot
        // detect nesting-context-sensitive missing commas.
        String stage3 = repairJsonStructure(stage2);
        try {
            return new JSONObject(stage3);
        } catch (JSONException e3) {
            log.warn("parseSafeJson [Stage 3 FAIL]: {}. Trying aggressive clean...",
                    e3.getMessage());
        }

        // Stage 4: Aggressive string cleaning — escape unescaped control chars
        // inside string values (newlines, tabs that break the JSON parser).
        String stage4 = aggressiveJsonClean(stage3);
        try {
            return new JSONObject(stage4);
        } catch (JSONException e4) {
            log.error("parseSafeJson [ALL STAGES FAILED]: {}", e4.getMessage());
            log.error("--- Original bad JSON (first 500 chars) ---\n{}",
                    text.substring(0, Math.min(500, text.length())));
            return null;
        }
    }


    private String repairJsonStructure(String json) {
        StringBuilder result = new StringBuilder(json.length() + 16);

        boolean inString = false;   // are we inside "..." ?
        boolean escaped = false;    // was the previous char a backslash?
        char prevMeaningful = 0;    // last non-whitespace char outside a string

        for (int i = 0; i < json.length(); i++) {
            char c = json.charAt(i);

            // --- Handle escape sequences inside strings ---
            // If we saw a backslash last iteration, this char is escaped.
            // It's a literal character — don't interpret it as structure.
            if (escaped) {
                result.append(c);
                escaped = false;
                continue;
            }

            if (c == '\\' && inString) {
                result.append(c);
                escaped = true;
                continue;
            }

            // --- Handle string boundaries ---
            // A quote toggles us in and out of "string mode".
            // When we EXIT a string, the closing quote is itself a meaningful char.
            if (c == '"') {
                inString = !inString;
                result.append(c);
                if (!inString) {
                    prevMeaningful = '"'; // closing quote = "a value just ended"
                }
                continue;
            }

            // --- Inside a string: append literally, touch nothing ---
            if (inString) {
                result.append(c);
                continue;
            }

            // --- We are now outside strings, in structural territory ---

            // Whitespace: invisible to structure, just carry it forward
            if (Character.isWhitespace(c)) {
                result.append(c);
                continue;
            }

            // --- THE KEY CHECK ---
            // If we are about to start a new value ('{' or '['),
            // AND the previous meaningful char signals that a value just ended,
            // THEN a comma was supposed to be there. Insert one.
            if ((c == '{' || c == '[') && isValueEndChar(prevMeaningful)) {
                log.warn("repairJsonStructure: Inserted missing comma before '{}' " +
                        "(prev meaningful char was '{}')", c, prevMeaningful);
                result.append(',');
            }

            result.append(c);
            prevMeaningful = c;
        }

        return result.toString();
    }


    private boolean isValueEndChar(char c) {
        return c == '}'
                || c == ']'
                || c == '"'
                || Character.isDigit(c)
                || c == 'e'   // tru-e, fals-e
                || c == 'l';  // nul-l
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
TASK: Fill the output template below using only the data provided. Do not add information not present in the data.

OUTPUT TEMPLATE — fill every field, no exceptions:
{
  "title": "[One clear headline, max 12 words, no punctuation at end]",
  "summary": "[One sentence. What happened and who it affects. Max 25 words.]",
  "article": "[Three paragraphs. Paragraph 1: what the notice says. Paragraph 2: who is affected and what it means. Paragraph 3: what action is needed and by when. Plain text only, no markdown, no bullet points, no headers.]",
  "xSummary": "[Max 240 characters. Same information as summary but conversational. No hashtags.]",
  "actionableInfo": "[One sentence. If a deadline exists in the data, state it exactly. If no deadline, state the key action required.]",
  "significance": [integer 1-10, where 1=routine administrative, 5=affects a specific group, 10=affects all Kenyans]
}

RULES:
- First character of response must be { and last must be }.
- No text before or after the JSON.
- Fill every field. No field may be null or empty.
- Do not use markdown formatting anywhere inside the JSON values.
- Do not use bullet points, asterisks, or hyphens inside article or any other field.
- significance must be a bare integer, not a string.

DATA:
%s
""".formatted(extractedData.toString());

        String generatedContentResponse = generateWithRetry(
                List.of("Court_Legal", "Land_Property", "Company_Registrations", "Licensing")
                        .contains(category) ? groqFlashModelName : groqProModelName,
                generationPrompt);
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
            } catch (DateTimeParseException e) {
                log.warn("Could not parse gazetteDate from header: {}", overallGazetteDetails.optString("gazetteDate"));
            } catch (JSONException e) {
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
            } catch (DateTimeParseException e) {
                // Ignore
            } catch (JSONException e) {
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
    private Gazette processTextSegment(String textSegment, int sourceOrder, JSONObject overallGazetteDetails,
                                       String originalPdfPath, String knownCategory) {
        String category = (knownCategory != null && !knownCategory.isBlank())
                ? knownCategory
                : triageNoticeCategory(textSegment);

        if (category == null) {
            log.warn("Triage failed for notice segment {}. Creating fallback.", sourceOrder);
            return createFallbackGazette(textSegment, sourceOrder, overallGazetteDetails, "Triage failed", originalPdfPath);
        }

        String schemaPath = "/schemas/field/" + category.toLowerCase() + ".json";
        String schemaContent = loadSchemaFile(schemaPath);
        if (schemaContent.isEmpty()) {
            log.error("Schema file not found for category '{}' (Notice {}). Creating fallback.", category, sourceOrder);
            return createFallbackGazette(textSegment, sourceOrder, overallGazetteDetails, "Schema file not found", originalPdfPath);
        }

        String cleanTextSegment = stripGazetteHeader(textSegment);

        String extractionPrompt = """
    TASK: Extract fields from the notice text into the JSON schema below.

    RULES:
    - Output must be raw JSON only. First character must be { and last character must be }.
    - Do not write any text before or after the JSON.
    - Do not include the schema in the output — only the filled values.
    - If a field value is not present in the text, use an empty string "".
    - Do not invent, infer, or guess values not present in the text.
    - Preserve names, dates, and reference numbers exactly as written.
    - Root key must be "items". Value is either an object (one item) or array (multiple items).

    SCHEMA:
    %s

    NOTICE TEXT:
    %s
    """.formatted(schemaContent, cleanTextSegment);

        String extractionModel = selectExtractionModel(category);
        String jsonResponse = generateWithRetry(extractionModel, extractionPrompt);
        JSONObject extractedDataWrapper = parseSafeJson(jsonResponse);

        if (extractedDataWrapper == null || !extractedDataWrapper.has("items")) {
            log.error("Extraction failed for notice segment {}. AI did not return a valid 'items' wrapper.", sourceOrder);
            return createFallbackGazette(textSegment, sourceOrder, overallGazetteDetails, "Extraction failed: no 'items' wrapper", originalPdfPath);
        }
        Object extractedData = extractedDataWrapper.get("items");

        boolean isNull = extractedData == null || extractedData == JSONObject.NULL;
        boolean isEmptyObject = (extractedData instanceof JSONObject) && ((JSONObject) extractedData).isEmpty();
        if (isNull || isEmptyObject) {
            log.error("Extraction failed for notice segment {}. AI returned 'items' as null or empty.", sourceOrder);
            return createFallbackGazette(textSegment, sourceOrder, overallGazetteDetails, "Extraction failed: 'items' was null or empty", originalPdfPath);
        }

        JSONObject generatedContent = generateNarrativeContent(extractedData, category);
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