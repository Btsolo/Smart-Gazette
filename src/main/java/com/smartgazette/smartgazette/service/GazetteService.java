package com.smartgazette.smartgazette.service;

import com.smartgazette.smartgazette.model.Gazette;
import com.smartgazette.smartgazette.model.ProcessingStatus;
import com.smartgazette.smartgazette.repository.GazetteRepository;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;
import java.util.concurrent.atomic.AtomicBoolean;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class GazetteService {

    private static final Logger log = LoggerFactory.getLogger(GazetteService.class);
    private final GazetteRepository gazetteRepository;
    private final RestTemplate restTemplate;

//    Add a "Mutex Lock" to prevent concurrent processing (Fix T015) ---
    private final AtomicBoolean isProcessing = new AtomicBoolean(false);
    private final AtomicBoolean stopProcessing = new AtomicBoolean(false);

    @Value("${gemini.api.key:}")
    private String geminiApiKey;

    // =====================================================================================
    // CHANGE 1: We now define BOTH models. Pro is for complex tasks, Flash for simple ones.
    // =====================================================================================
    @Value("${gemini.model.pro:gemini-2.5-pro}")
    private String geminiProModel;

    @Value("${gemini.model.flash:gemini-2.5-flash}")
    private String geminiFlashModel;


    public GazetteService(GazetteRepository gazetteRepository) {
        this.gazetteRepository = gazetteRepository;
        SimpleClientHttpRequestFactory rf = new SimpleClientHttpRequestFactory();
        rf.setConnectTimeout(60_000);
        rf.setReadTimeout(300_000);
        this.restTemplate = new RestTemplate(rf);
    }

    // --- Core Public Methods ---

    // --- FIX for T014 Ordering Issue ---
    public List<Gazette> getAllGazettes() {
        // We now use the new method that sorts by date first.
        // This CALL now matches the simple METHOD NAME in the repository.
        return gazetteRepository.findAllWithCorrectSorting();
    }
    // --- END OF FIX ---

    public Gazette getGazetteById(Long id) { return gazetteRepository.findById(id).orElse(null); }
    public void deleteGazette(Long id) { gazetteRepository.deleteById(id); }
    public Gazette saveGazette(Gazette gazette) { return gazetteRepository.save(gazette); }

    // --- NEW: Method for the "Stop Processing" Button ---
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
        // --- FIX T015: Check and set the processing lock ---
        if (!isProcessing.compareAndSet(false, true)) {
            log.warn("Cannot start PDF processing. Another job (like a retry) is already in progress.");
            return;
        }

//  Reset stop flag at the beginning of a new job ---
        stopProcessing.set(false);

        JSONObject overallGazetteDetails = null; // Variable to store header info

        try (PDDocument document = PDDocument.load(file)) {
            log.info(">>>> Starting async PDF processing for file: {}", file.getName());
            String fullText = new PDFTextStripper().getText(document);

            // =====================================================================================
            // NEW STEP: Extract header details *before* segmenting notices
            // =====================================================================================
            if (fullText != null && !fullText.isBlank()) {
                overallGazetteDetails = extractGazetteHeaderDetails(fullText);
            }

            List<String> notices = segmentTextByNotices(fullText);
            log.info("PDF segmented into {} potential notices.", notices.size());

            for (int i = 0; i < notices.size(); i++) {
                String noticeText = notices.get(i);
                int sourceOrder = i + 1;
                log.info("-----> Processing Notice {}/{}...", sourceOrder, notices.size());
                // --- NEW: Check for admin stop signal ---
                if (stopProcessing.get()) {
                    log.warn("Processing manually stopped by admin at notice #{}", sourceOrder);
                    break; // Exit the for-loop
                }
                Gazette gazette = null;
                try {
                    // Pass overallGazetteDetails to the processing method
                    gazette = processSingleNotice(noticeText, sourceOrder, overallGazetteDetails);

                    if (gazette != null) {
                        // ... (Deduplication logic remains the same) ...
                        // TODO: Add deduplication logic here if needed

                        // Save only if gazette is not null
                        log.info("Saving {} article: '{}' (Cat: '{}', Num: {}, GazDate: {})", // Added GazDate log
                                gazette.getStatus(), gazette.getTitle(), gazette.getCategory(), gazette.getNoticeNumber(), gazette.getGazetteDate());
                        gazetteRepository.save(gazette);
                    }
                } catch (Exception e) {
                    log.error("Error processing or checking notice #{}. Creating a fallback.", sourceOrder, e);
                    // Pass header details to fallback too
                    gazetteRepository.save(createFallbackGazette(noticeText, sourceOrder, overallGazetteDetails));
                }
                // =====================================================================================
                // EDUCATIONAL NOTE: Implementing Rate Limiting Pause.
                // We pause for a calculated duration AFTER processing each notice.
                // This prevents us from sending too many requests too quickly and hitting API limits.
                // Target: ~20 notices per minute (60 calls/min / 3 calls/notice)
                //Pause: 60 seconds / 20 notices = 3 seconds pause per notice.
                // =====================================================================================
                try {
                    log.debug("Pausing for 3 seconds to respect rate limits...");
                    TimeUnit.SECONDS.sleep(3); // Pause for 4 seconds
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // Restore interruption status
                    log.warn("Rate limit pause interrupted.");
                    // Optionally, break the loop or handle the interruption
                }

            } // End of for loop

            log.info("<<<< Successfully finished processing PDF file: {}", file.getName());
        } catch (Exception e) {
            log.error("Critical error during PDF processing pipeline for file: {}", file.getName(), e);
        } finally {
            try {
                Files.deleteIfExists(file.toPath());
            } catch (IOException ignored) {}

            // --- FIX T015: Release the processing lock ---
            isProcessing.set(false);
            stopProcessing.set(false); // Also reset stop flag
            log.info("Processing lock released.");
        }
    }

    private List<String> segmentTextByNotices(String fullText) {
        // ... (This method is unchanged)
        List<String> notices = new ArrayList<>();
        final Pattern pattern = Pattern.compile("(GAZETTE NOTICE NO\\.\\s*\\d+)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(fullText);
        int lastEnd = 0;
        while (matcher.find()) {
            if (matcher.start() > lastEnd) {
                notices.add(fullText.substring(lastEnd, matcher.start()).trim());
            }
            lastEnd = matcher.start();
        }
        if (lastEnd < fullText.length()) {
            notices.add(fullText.substring(lastEnd).trim());
        }
        if (!notices.isEmpty() && !pattern.matcher(notices.get(0)).find()) {
            notices.remove(0);
        }
        return notices;
    }

    // =====================================================================================
    // CHANGE 2: This is our new "Chunking" logic to handle huge notices.
    // It detects if a notice is too long and processes it in smaller pieces if needed.
    // =====================================================================================
    private Gazette processSingleNotice(String noticeText, int sourceOrder, JSONObject overallGazetteDetails) {
        // A safe character limit (approx. 4000 tokens) to avoid API errors.
        final int SAFE_CHAR_LIMIT = 15000;

        if (noticeText.length() < SAFE_CHAR_LIMIT) {
            // If the notice is short, process it the normal way.
            return processTextSegment(noticeText, sourceOrder, overallGazetteDetails); // Pass details down
        } else {
            // If the notice is VERY long, we process it in chunks.
            log.warn("Notice {} is very long ({} chars). Processing in chunks.", sourceOrder, noticeText.length());
            List<String> chunks = chunkText(noticeText, SAFE_CHAR_LIMIT, 200); // 200 char overlap

            // We process the first chunk to get the main details (title, summary, etc.)
            Gazette initialGazette = processTextSegment(chunks.get(0), sourceOrder, overallGazetteDetails); // Pass details down

            if (initialGazette != null && initialGazette.getStatus() == ProcessingStatus.SUCCESS) {
                // We then append the rest of the original text to the article for completeness.
                StringBuilder fullArticle = new StringBuilder(initialGazette.getArticle());
                for (int i = 1; i < chunks.size(); i++) {
                    fullArticle.append("\n\n--- (Continuation from Gazette) ---\n\n");
                    fullArticle.append(chunks.get(i));
                }
                initialGazette.setArticle(fullArticle.toString());
                log.info("Successfully combined {} chunks for notice {}.", chunks.size(), sourceOrder);
                return initialGazette;
            }
            // If the first chunk fails, the whole notice fails.
            return createFallbackGazette(noticeText, sourceOrder, overallGazetteDetails);
        }
    }

    // This new method contains our original 3-step pipeline, now operating on a safe-sized text segment.
    private Gazette processTextSegment(String textSegment, int sourceOrder, JSONObject overallGazetteDetails) {

        // --- STEP 1: AI Triage ---
        String category = triageNoticeCategory(textSegment);

        // --- THIS IS THE FIX (P0-B from Report T013.2) ---
        // Per Report T012/T013, Triage returning null (which defaults to "Miscellaneous")
        // should NOT be a failure. We must allow "Miscellaneous" to process as a safety net.
        // We only fail if Triage *itself* fails and returns null (which our code prevents).
        // This 'if' check is now simplified to only catch a true null,
        // allowing "Miscellaneous" to proceed.
        if (category == null) {
            log.warn("Triage failed AND fallback failed for notice segment {}. Creating fallback.", sourceOrder);
            return createFallbackGazette(textSegment, sourceOrder, overallGazetteDetails);
        }
        // --- END OF FIX ---

        log.info("Triage complete for notice segment {}. Category: {}", sourceOrder, category);

        // --- STEP 2: AI Extraction ---
        String schemaPath = "/schemas/field/" + category.toLowerCase() + ".json";
        String schemaContent = loadSchemaFile(schemaPath);
        if (schemaContent.isEmpty()) {
            log.error("Schema file not found for category '{}' (Notice {}). Creating fallback.", category, sourceOrder);
            return createFallbackGazette(textSegment, sourceOrder, overallGazetteDetails);
        }

        // --- THIS IS THE NEW EXTRACTION PROMPT (P2-A from Report T012) ---
        // It tells the AI to use the "items" wrapper.
        String extractionPrompt = """
        You are a precise data extraction assistant. Your task is to extract structured data from the provided text.
        Your entire response MUST be a single JSON object.
        This object MUST have one key: "items".
        
        - If the text is about a SINGLE item (e.g., one appointment, one land parcel), the value for "items" should be that SINGLE JSON OBJECT.
        - If the text is a DIGEST or list of MULTIPLE items (e.g., multiple tenders, multiple land parcels), the value for "items" should be a JSON ARRAY of those objects.
        
        SCHEMA (for the individual items):
        %s
        
        EXAMPLE (Single Item):
        {
          "items": { "person_name": "Jane Doe", "position": "Director" }
        }
        
        EXAMPLE (Multiple Items):
        {
          "items": [
            { "parcel_id": "1", "location": "Nairobi" },
            { "parcel_id": "2", "location": "Nakuru" }
          ]
        }
        
        TEXT TO PARSE:
        %s
        """.formatted(schemaContent, textSegment);
        // --- END OF NEW PROMPT ---

        String jsonResponse = makeGeminiApiCall(null, extractionPrompt, geminiProModel); // Use PRO model
        JSONObject extractedDataWrapper = parseSafeJson(jsonResponse);

        // --- NEW LOGIC: Handle the wrapper object ---
        if (extractedDataWrapper == null || !extractedDataWrapper.has("items")) {
            log.error("Extraction failed for notice segment {}. AI did not return a valid 'items' wrapper.", sourceOrder);
            return createFallbackGazette(textSegment, sourceOrder, overallGazetteDetails);
        }

        // This variable will hold EITHER a JSONObject or a JSONArray
        Object extractedData = extractedDataWrapper.get("items");
        // --- END OF NEW LOGIC ---

        log.info("Extraction complete for notice segment {}.", sourceOrder);

        // --- STEP 3: AI Generation (Now in a separate, resilient method) ---
        // We pass the raw extractedData object (JSONObject or JSONArray)
        JSONObject generatedContent = generateNarrativeContent(extractedData, category);

        if (generatedContent == null) {
            log.error("Generation step failed for notice segment {}. Saving with extracted data only.", sourceOrder);
            // We will proceed. createGazetteFromJson will handle the fallback state.
        } else {
            log.info("Generation complete for notice segment {}.", sourceOrder);
        }

        // Final Step: Map everything to our Gazette entity
        // We pass the raw extractedData object (JSONObject or JSONArray)
        return createGazetteFromJson(extractedData, generatedContent, textSegment, category, sourceOrder, overallGazetteDetails);
    }

    /**
     * NEW HELPER METHOD (from Report Recommendation)
     * This is Step 3 (Generation). It is now a separate, robust AI call.
     * This isolates the Extraction logic from the Generation logic.
     */
    private JSONObject generateNarrativeContent(Object extractedData, String category) {

        // Use the exact same prompt as before, but now it's in its own method.
        // We pass 'category' in case we want to customize the prompt by category later.

        // --- THIS IS THE NEW GENERATION PROMPT (P3-A / "Deadline Fix") ---
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
        """.formatted(extractedData.toString()); // <-- BUG FIX 1: Removed (2)
        // --- END OF NEW GENERATION PROMPT ---

        log.info("Attempting Generation for category {} with prompt:\n{}", category, generationPrompt);
        String generatedContentResponse = makeGeminiApiCall(null, generationPrompt, geminiProModel); // Use PRO model
        log.info("Received RAW Generation response:\n{}", generatedContentResponse);

        return parseSafeJson(generatedContentResponse);
    }

    // New helper method to slice text into manageable pieces.
    private List<String> chunkText(String text, int chunkSize, int overlap) {
        List<String> chunks = new ArrayList<>();
        if (text == null || text.length() <= chunkSize) {
            chunks.add(text);
            return chunks;
        }
        int i = 0;
        while (i < text.length()) {
            int end = Math.min(i + chunkSize, text.length());
            chunks.add(text.substring(i, end));
            i += chunkSize - overlap;
        }
        return chunks;
    }

    // Add this new helper method inside GazetteService.java

    /**
     * Extracts overall Gazette issue details (Volume, Number, Date) from the header text.
     * Uses a dedicated AI call for robustness.
     * @param headerText The first few hundred characters of the PDF text.
     * @return A JSONObject containing "gazetteVolume", "gazetteNumber", and "gazetteDate", or null if extraction fails.
     */
    private JSONObject extractGazetteHeaderDetails(String headerText) {
        log.info("Attempting to extract Gazette header details...");
        String prompt = """
        Analyze the following text from the beginning of a Kenya Gazette PDF.
        Extract the Volume (e.g., "Vol. CXXVII"), the Issue Number (e.g., "No. 36"), and the Publication Date (e.g., "21st February, 2025").
        Return ONLY a valid JSON object with three keys: "gazetteVolume", "gazetteNumber", and "gazetteDate" (in YYYY-MM-DD format).

        TEXT:
        %s

        OUTPUT JSON:
        {
          "gazetteVolume": "...",
          "gazetteNumber": "...",
          "gazetteDate": "YYYY-MM-DD"
        }
        """.formatted(headerText.substring(0, Math.min(headerText.length(), 1000))); // Use first 1000 chars

        // Use the cheaper Flash model for this simple extraction task
        String jsonResponse = makeGeminiApiCall(null, prompt, geminiFlashModel);
        JSONObject headerDetails = parseSafeJson(jsonResponse);

        if (headerDetails == null) {
            log.error("Failed to extract Gazette header details from text.");
            return null;
        }

        // Basic validation (optional but good)
        if (!headerDetails.has("gazetteVolume") || !headerDetails.has("gazetteNumber") || !headerDetails.has("gazetteDate")) {
            log.warn("Extracted header details JSON is missing required keys: {}", headerDetails.toString());
            // Attempt to salvage if possible, otherwise return null or partial object
        }

        log.info("Successfully extracted Gazette header details: {}", headerDetails.toString());
        return headerDetails;
    }

    private String triageNoticeCategory(String noticeText) {
        // =====================================================================================
        // EDUCATIONAL NOTE: This is our new, stricter prompt.
        // We've added "Your response MUST be a single word" and "Do not add any explanation."
        // This is a powerful prompt engineering technique to force the AI to be concise.
        // =====================================================================================
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
        """.formatted(noticeText.substring(0, Math.min(noticeText.length(), 2000)));

        String category = makeGeminiApiCall(null, triagePrompt, geminiFlashModel);

        if (category != null) {
            // Cleanup to ensure only the word remains
            String cleanedCategory = category.replaceAll("[^a-zA-Z_]", "").trim();

            // **NEW VALIDATION**: Check if the cleaned response is one of our expected categories
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

    // Update makeGeminiApiCall to accept a model parameter
    private String makeGeminiApiCall(String systemPrompt, String userPrompt, String modelName) {
        if (geminiApiKey == null || geminiApiKey.isBlank()) {
            log.error("Gemini API key is not configured.");
            return null;
        }
        String apiUrl = "https://generativelanguage.googleapis.com/v1beta/models/" + modelName + ":generateContent?key=" + geminiApiKey;
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        JSONObject requestBody = new JSONObject();
        JSONArray contents = new JSONArray();
        JSONObject partsContainer = new JSONObject();
        JSONArray parts = new JSONArray().put(new JSONObject().put("text", userPrompt));
        partsContainer.put("parts", parts);
        contents.put(partsContainer);
        requestBody.put("contents", contents);
        if (systemPrompt != null && !systemPrompt.isBlank()) {
            requestBody.put("system_instruction", new JSONObject().put("parts", new JSONArray().put(new JSONObject().put("text", systemPrompt))));
        }
        HttpEntity<String> entity = new HttpEntity<>(requestBody.toString(), headers);

        // =====================================================================================
        // EDUCATIONAL NOTE: Implementing Retry Logic.
        // This loop will try the API call up to 'maxRetries' times if it fails
        // with a network error or a server-side error (5xx).
        // =====================================================================================
        int maxRetries = 3;
        int retryDelaySeconds = 2; // Start with a 2-second delay

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                // Add a small delay BEFORE each attempt (except the first) to avoid overwhelming the API
                if (attempt > 1) {
                    log.warn("Waiting {} seconds before retry attempt {}...", retryDelaySeconds, attempt);
                    TimeUnit.SECONDS.sleep(retryDelaySeconds);
                    retryDelaySeconds *= 2; // Exponential backoff: Wait longer each time
                }

                log.debug("Sending request to Gemini model: {} (Attempt {})", modelName, attempt);
                ResponseEntity<String> resp = restTemplate.postForEntity(apiUrl, entity, String.class);

                if (resp.getStatusCode().is2xxSuccessful() && resp.getBody() != null) {
                    JSONObject jsonResponse = new JSONObject(resp.getBody());
                    // Check for safety ratings or other potential issues in the response
                    if (jsonResponse.has("promptFeedback") &&
                            jsonResponse.getJSONObject("promptFeedback").has("blockReason")) {
                        log.error("Gemini API blocked the request. Reason: {}", jsonResponse.getJSONObject("promptFeedback").getString("blockReason"));
                        return null; // Don't retry if blocked
                    }
                    log.debug("Gemini Raw Response Received (Attempt {}).", attempt);
                    // Ensure candidates array exists and has content
                    if (!jsonResponse.has("candidates") || jsonResponse.getJSONArray("candidates").isEmpty()) {
                        log.error("Gemini API returned success but no candidates found in response (Attempt {}).", attempt);
                        // Consider retrying or failing based on your needs, for now fail
                        continue; // Go to next retry attempt
                    }
                    return jsonResponse.getJSONArray("candidates").getJSONObject(0).getJSONObject("content").getJSONArray("parts").getJSONObject(0).getString("text");
                } else {
                    // Log unexpected success codes
                    log.error("Gemini API returned non-2xx status: {} (Attempt {})", resp.getStatusCode(), attempt);
                    // Decide if you want to retry non-2xx codes or just fail
                    // continue; // Uncomment to retry non-2xx codes
                }

                // Catch specific exceptions that warrant a retry
            } catch (ResourceAccessException | HttpServerErrorException e) { // Network errors or 5xx server errors
                log.warn("Gemini API call failed (Attempt {}/{}) with error: {}. Retrying...", attempt, maxRetries, e.getMessage());
                if (attempt == maxRetries) {
                    log.error("Max retries ({}) reached for Gemini API call. Giving up.", maxRetries, e);
                    return null; // Failed after all retries
                }
                // Catch client errors (4xx) - usually means something wrong with OUR request, don't retry
            } catch (HttpClientErrorException e) {
                log.error("Gemini client error: {} - {} (Attempt {}). Not retrying.", e.getStatusCode(), e.getResponseBodyAsString(), attempt, e);
                return null; // Do not retry client errors
                // Catch JSON parsing errors - means the response structure was wrong, don't retry
            } catch (JSONException e) {
                log.error("Error parsing Gemini JSON response (Attempt {}): {}. Response body might be invalid. Not retrying.", attempt, e.getMessage(), e);
                return null;
                // Catch sleep interruption
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Restore interruption status
                log.error("API call retry delay interrupted (Attempt {}).", attempt, e);
                return null;
                // Catch any other unexpected errors
            } catch (Exception e) {
                log.error("Unexpected error during Gemini call (Attempt {}): {}", attempt, e.getMessage(), e);
                // Decide if you want to retry unexpected errors
                if (attempt == maxRetries) return null; // Give up after max retries
                // continue; // Uncomment to retry unexpected errors
            }
        } // End of retry loop

        return null; // Should not be reached if loop logic is correct, but needed for compilation
    }

    private JSONObject parseSafeJson(String text) {
        // ... (This method is unchanged)
        if (text == null || text.isBlank()) return null;
        text = text.replaceAll("(?s)```json\\s*(.*?)\\s*```", "$1").trim();
        if (!text.startsWith("{")) {
            int start = text.indexOf('{');
            if (start == -1) return null;
            text = text.substring(start);
        }
        try {
            return new JSONObject(text);
        } catch (JSONException e) {
            log.warn("Failed to parse JSON from model output: {}", e.getMessage());
            return null;
        }
    }

    //    /**
//     * EDUCATIONAL NOTE: This method implements your retry strategy.
//     * It finds all FAILED notices, checks why they failed, and re-runs the
//     * specific part of the pipeline that broke.
//     */
    @Async
    public void retryFailedNotices() {
        // --- FIX T015: Check and set the processing lock ---
        if (!isProcessing.compareAndSet(false, true)) {
            log.warn("Cannot start RETRY job. Another job (like a PDF upload) is already in progress.");
            return;
        }

        // --- NEW: Reset stop flag at the beginning of a new job ---
        stopProcessing.set(false);
        // --- END OF FIX ---

        log.info("Starting retry process for FAILED notices...");

        // 1. Find all failed notices
        // --- FIX: This now calls the correctly named, correctly sorted method ---
        List<Gazette> failedNotices = gazetteRepository.findAllWithStatusAndCorrectSorting(ProcessingStatus.FAILED);
        if (failedNotices.isEmpty()) {
            log.info("No FAILED notices found to retry.");
            return;
        }

        log.info("Found {} FAILED notices to retry.", failedNotices.size());

        for (Gazette notice : failedNotices) {
            try {
                // --- 1. Check for admin stop signal ---
                if (stopProcessing.get()) {
                    log.warn("Retry processing manually stopped by admin.");
                    break; // Exit the for-loop
                }
                // 2. Determine the failure type based on the title
                if (notice.getTitle().startsWith("[TRIAGE/SCHEMA FAILED]")) {
                    // This failed at Step 1. We have the raw content. Re-run the full process.
                    log.info("Retrying notice #{} (TRIAGE failure)...", notice.getId());
                    // We pass null for overallGazetteDetails as we don't have it saved separately yet
                    Gazette retriedNotice = processSingleNotice(notice.getContent(), notice.getSourceOrder(), null);

                    if (retriedNotice != null && retriedNotice.getStatus() == ProcessingStatus.SUCCESS) {
                        // Update the existing notice with the new, successful data
                        updateExistingNotice(notice, retriedNotice);
                        log.info("SUCCESS: Retry for notice #{} was successful.", notice.getId());
                    } else {
                        log.warn("FAIL: Retry for notice #{} (TRIAGE) failed again.", notice.getId());
                    }

                } else if (notice.getTitle().startsWith("[GENERATION FAILED]")) {
                    // This failed at Step 3. We have the extracted JSON. Re-run only Generation.
                    log.info("Retrying notice #{} (GENERATION failure)...", notice.getId());

                    // --- BUG FIX: The stored JSON could be an Object OR an Array. ---
                    // We can't just parse as JSONObject. We'll try one, then the other.
                    Object extractedData = null;
                    // We must get the article's text, which contains the JSON
                    String articleJson = notice.getArticle();

                    // First, we must strip the "## Extracted Data..." markdown
                    articleJson = articleJson.replaceAll("(?s)```json\\s*(.*?)\\s*```", "$1").trim();

                    try {
                        // First, try to parse as the most common case (a single object)
                        extractedData = new JSONObject(articleJson);
                    } catch (JSONException e) {
                        try {
                            // If that fails, try to parse as an array (for digest notices)
                            extractedData = new JSONArray(articleJson);
                        } catch (JSONException e2) {
                            log.error("Could not retry notice #{}: Failed to parse extracted JSON from article field. Content: {}", notice.getId(), articleJson);
                            continue;
                        }
                    }
                    // --- END OF BUG FIX ---


                    if (extractedData == null) {
                        log.error("Could not retry notice #{}: Failed to parse extracted JSON.", notice.getId());
                        continue;
                    }

                    // Call Step 3 (Generation) directly
                    Gazette generatedNotice = runGenerationStep(extractedData, notice.getContent(), notice.getCategory(), notice.getSourceOrder(), null); // Pass null for header details

                    if (generatedNotice != null && generatedNotice.getStatus() == ProcessingStatus.SUCCESS) {
                        updateExistingNotice(notice, generatedNotice);
                        log.info("SUCCESS: Retry for notice #{} was successful.", notice.getId());
                    } else {
                        log.warn("FAIL: Retry for notice #{} (GENERATION) failed again.", notice.getId());
                    }
                }

                // Add a pause to respect rate limits even during retries
                TimeUnit.SECONDS.sleep(3);

            } catch (Exception e) {
                log.error("Unhandled exception while retrying notice #{}: {}", notice.getId(), e.getMessage());
            }
        }// end of for loop

        log.info("Finished retry process.");

        // --- FIX T015: Release the processing lock ---
        isProcessing.set(false);
        stopProcessing.set(false); // Also reset stop flag
        log.info("Retry job finished. Processing lock released.");
    }

    // Helper method to run ONLY the Generation step (Step 3)
    // --- FIX: Change signature to accept Object ---
    private Gazette runGenerationStep(Object extractedData, String rawContent, String category, int sourceOrder, JSONObject overallGazetteDetails) {
        log.info("Attempting Generation for retried notice...");

        // --- BUG FIX 2: Replace old prompt with the NEW, CORRECT Generation Prompt ---
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
        """.formatted(extractedData.toString()); // <-- BUG FIX 3: Removed (2)
        // --- END OF FIX ---

        String generatedContentResponse = makeGeminiApiCall(null, generationPrompt, geminiProModel);
        JSONObject generatedContent = parseSafeJson(generatedContentResponse);

        if (generatedContent == null) {
            log.error("Generation step failed on retry.");
            // Return null or a FAILED gazette object to indicate failure
            return null;
        }
        log.info("Generation complete on retry.");
        // Map and return the new, successful object
        return createGazetteFromJson(extractedData, generatedContent, rawContent, category, sourceOrder, overallGazetteDetails);
    }

    // Helper method to update the old notice with new data
    private void updateExistingNotice(Gazette oldNotice, Gazette newNotice) {
        // Copy all fields from the new, successful notice to the old one
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
        oldNotice.setStatus(ProcessingStatus.SUCCESS); // Most important!

        gazetteRepository.save(oldNotice); // This will perform an UPDATE, not an INSERT
    }

    // =====================================================================================
    // CHANGE 4: Add the data sanitization step here to fix the database crash.
    // =====================================================================================
    private Gazette createGazetteFromJson(Object extractedData, JSONObject generatedContent, String rawContent, String category, int order, JSONObject overallGazetteDetails) {
        // We absolutely need extractedData to proceed.
        boolean isNull = extractedData == null;
        boolean isEmptyArray = (extractedData instanceof JSONArray) && ((JSONArray) extractedData).isEmpty();

        if (isNull || isEmptyArray) {
            log.error("Cannot create Gazette object: extractedData is null or empty for order {}", order);
            // ... (fallback logic) ...
            Gazette failedGazette = new Gazette();
            failedGazette.setStatus(ProcessingStatus.FAILED);
            failedGazette.setTitle("[EXTRACTION FAILED] Review Needed");
            failedGazette.setCategory(category); // Use the category we found
            failedGazette.setSourceOrder(order);
            failedGazette.setContent(rawContent != null ? rawContent.replace("\u0000", "") : "");
            failedGazette.setArticle("Extraction failed: AI returned null or empty 'items'. Original text in content field.");
            failedGazette.setPublishedDate(LocalDate.now());
            return failedGazette;
        }

        Gazette gazette = new Gazette();
        // Sanitize raw content once
        String sanitizedRawContent = rawContent.replace("\u0000", "");
        gazette.setContent(sanitizedRawContent);
        gazette.setCategory(category);
        gazette.setSourceOrder(order);

        // Set overall Gazette details if available
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
                // Leave gazetteDate as null or set a default? For now, null is fine.
            }
        }

        // --- Populate fields, handling the case where generation might have failed ---
        if (generatedContent != null) {
            // Generation succeeded! Populate all fields normally and set status to SUCCESS.
            gazette.setStatus(ProcessingStatus.SUCCESS);
            gazette.setTitle(generatedContent.optString("title", "Untitled Notice").replace("\u0000", ""));
            gazette.setSummary(generatedContent.optString("summary", "No summary provided.").replace("\u0000", ""));

            // --- BUG FIX 4 (in case generation succeeds but we still want to show the JSON) ---
            // Let's decide: Article is the AI text. If we want to see the JSON, we need a new field.
            // For now, the report says the AI Article is the goal.
            gazette.setArticle(generatedContent.optString("article", extractedData.toString()).replace("\u0000", "")); // <-- BUG FIX 4: Removed (2)

            gazette.setXSummary(generatedContent.optString("xSummary", "").replace("\u0000", ""));
            gazette.setActionableInfo(generatedContent.optString("actionableInfo", "").replace("\u0000", ""));
        } else {
            // Generation failed. Populate minimally, show extracted data, and set status to FAILED.
            gazette.setStatus(ProcessingStatus.FAILED);
            gazette.setTitle("[GENERATION FAILED] " + category + " Notice (Review Extracted Data)");
            gazette.setSummary("AI failed to generate summary. Review extracted data below.");
            // Store the extracted JSON in the article field for admin review
            // --- FIX: We must store the raw extractedData (Object or Array) ---
            gazette.setArticle("## Extracted Data (Generation Failed):\n\n```json\n" + extractedData.toString().replace("\u0000", "") + "\n```"); // <-- BUG FIX 5: Removed (2)
            gazette.setXSummary(""); // No tweet summary if generation failed
            gazette.setActionableInfo("Review needed");
        }

        // --- Populate remaining fields from EXTRACTED data ---
        // This logic handles both a single item (JSONObject) and multiple items (JSONArray)

        String noticeNumber = "";
        String signatory = "";
        String dateStr = "";

        if (extractedData instanceof JSONObject singleItem) {
            // It's a single item, extract directly
            noticeNumber = singleItem.optString("notice_id", singleItem.optString("reference_number", ""));
            signatory = singleItem.optString("signatory", "");
            dateStr = singleItem.optString("publication_date", singleItem.optString("effective_date", ""));

        } else if (extractedData instanceof JSONArray itemArray) {
            // It's multiple items. We'll take the info from the FIRST item as representative.
            // This is perfect for "Digest" articles (e.g., Tenders, Land Transfers).
            if (!itemArray.isEmpty()) {
                JSONObject firstItem = itemArray.getJSONObject(0);
                noticeNumber = firstItem.optString("notice_id", firstItem.optString("reference_number", ""));
                signatory = firstItem.optString("signatory", "");
                dateStr = firstItem.optString("publication_date", firstItem.optString("effective_date", ""));
            }
        }

        gazette.setNoticeNumber(noticeNumber.replace("\u0000", ""));
        gazette.setSignatory(signatory.replace("\u0000", ""));

        // Safely parse the date
        // --- THIS IS THE FIX ---
        // We DELETE the old, buggy line:
        // String dateStr = extractedData.optString("publication_date", extractedData.optString("effective_date", ""));
        // We now use the 'dateStr' variable we safely extracted above.
        // --- END OF FIX ---
        try {
            if (!dateStr.isBlank()) gazette.setPublishedDate(LocalDate.parse(dateStr));
            else gazette.setPublishedDate(LocalDate.now());
        } catch (DateTimeParseException e) {
            gazette.setPublishedDate(LocalDate.now());
        }

        return gazette;
    }

    // Ensure the main fallback also sets FAILED status
    private Gazette createFallbackGazette(String text, int order, JSONObject overallGazetteDetails) {
        Gazette g = new Gazette();
        g.setStatus(ProcessingStatus.FAILED); // Correctly sets FAILED
        g.setTitle("[TRIAGE/SCHEMA FAILED] Review Needed"); // Made title more specific
        g.setSummary("The AI failed early processing (Triage or Schema load). Raw text in Article field.");
        g.setXSummary("Processing error. Needs manual review.");
        g.setContent(text.replace("\u0000", ""));
        g.setArticle("## AI PROCESSING FAILED (TRIAGE)\n\nThe system could not categorize this notice. The original raw text has been saved in the 'content' field, which is visible in the 'Edit' page. You can try to fix it manually or use the 'Retry FAILED Notices' button in the admin panel.");
        g.setCategory("Uncategorized");
        g.setPublishedDate(LocalDate.now());
        g.setSourceOrder(order);

        // Set overall Gazette details if available, even for fallbacks
        if (overallGazetteDetails != null) {
            g.setGazetteVolume(overallGazetteDetails.optString("gazetteVolume", ""));
            g.setGazetteNumber(overallGazetteDetails.optString("gazetteNumber", ""));
            try {
                String dateStr = overallGazetteDetails.optString("gazetteDate");
                if (dateStr != null && !dateStr.isBlank()) {
                    g.setGazetteDate(LocalDate.parse(dateStr));
                }
            } catch (DateTimeParseException | JSONException e) {
                // Ignore parsing error for fallback
            }
        }

        return g;
    }

    private String loadSchemaFile(String path) {
        // ... (This method is unchanged)
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
    private String truncate(String s, int max) {
        return (s == null || s.length() <= max) ? s : s.substring(0, max - 3) + "...";
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
}