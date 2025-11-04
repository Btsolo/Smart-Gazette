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

    // --- Core Public Methods (Unchanged) ---
    public List<Gazette> getAllGazettes() { return gazetteRepository.findAllByOrderBySourceOrderAsc(); }
    public Gazette getGazetteById(Long id) { return gazetteRepository.findById(id).orElse(null); }
    public void deleteGazette(Long id) { gazetteRepository.deleteById(id); }
    public Gazette saveGazette(Gazette gazette) { return gazetteRepository.save(gazette); }

    @Async
    public void processAndSavePdf(File file) {
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
        // STEP 1: AI Triage
        String category = triageNoticeCategory(textSegment);
        if (category == null || category.equalsIgnoreCase("Miscellaneous")) {
            log.warn("Triage failed for notice segment {}. Creating fallback.", sourceOrder); // Added log
            return createFallbackGazette(textSegment, sourceOrder, overallGazetteDetails);
        }
        log.info("Triage complete for notice segment {}. Category: {}", sourceOrder, category);

        // STEP 2: AI Extraction
        String schemaPath = "/schemas/field/" + category.toLowerCase() + ".json";
        String schemaContent = loadSchemaFile(schemaPath);
        if (schemaContent.isEmpty()) {
            log.error("Schema file not found for category '{}' (Notice {}). Creating fallback.", category, sourceOrder); // Added log
            return createFallbackGazette(textSegment, sourceOrder, overallGazetteDetails);
        }
        String extractionPrompt = """
            You are a precise data extraction assistant...
            SCHEMA: %s
            TEXT TO PARSE: %s
            """.formatted(schemaContent, textSegment);
        String jsonResponse = makeGeminiApiCall(null, extractionPrompt, geminiProModel); // Use PRO model
        JSONObject extractedData = parseSafeJson(jsonResponse);
        if (extractedData == null) {
            log.error("Extraction failed for notice segment {}. Creating fallback.", sourceOrder);
            return createFallbackGazette(textSegment, sourceOrder, overallGazetteDetails);
        }
        log.info("Extraction complete for notice segment {}.", sourceOrder);

        // STEP 3: AI Generation
        String generationPrompt = """
        You are an expert editorial assistant for Smart Gazette. Your goal is to simplify government notices for Kenyan youth.
        Based ONLY on the structured JSON data provided below, generate a JSON object containing five fields: "title", "summary", "article", "xSummary", and "actionableInfo".

        **Your Instructions:**
        1.  **Grouping Logic:**
            - If the category is `Land_Property` or `Tenders` and the input data contains multiple similar items (look for arrays), create a single "digest" article. Title like "Land Transfer Notices" or "New Tenders". Article uses markdown lists. Otherwise, create a normal article.
        2.  **Actionable Info Logic (Tiered Approach):**
            - Tier 1 (Action with Deadline): If input has action & deadline, state it. Ex: "Submit comments within 30 days."
            - Tier 2 (Action without Deadline): If input has action, no deadline, state action & advise checking official sources. Ex: "Provide feedback. Check NTSA website for details."
            - Tier 3 (Informational): For appointments etc., provide context. Ex: "Note the new EPRA board leadership."
        3.  **Content Requirements:**
            - title: Clear, engaging headline.
            - summary: One-sentence key takeaway.
            - article: Detailed, human-readable article (200-400 words), markdown formatted.
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
        """.formatted(extractedData.toString(2)); // Still using pretty print for input clarity
        // =====================================================================================
        // EDUCATIONAL NOTE: Adding detailed logging BEFORE the Generation API call.
        // We log the prompt we are about to send.
        // =====================================================================================
        log.info("Attempting Generation for notice segment {} with prompt:\n{}", sourceOrder, generationPrompt);

        String generatedContentResponse = makeGeminiApiCall(null, generationPrompt, geminiProModel); // Use PRO model
        // =====================================================================================
        // EDUCATIONAL NOTE: Adding detailed logging AFTER the Generation API call.
        // We log the RAW response string we received from the API, BEFORE parsing.
        // This will tell us exactly what Gemini is sending back.
        // =====================================================================================
        log.info("Received RAW Generation response for notice segment {}:\n{}", sourceOrder, generatedContentResponse);
        JSONObject generatedContent = parseSafeJson(generatedContentResponse);
        // =====================================================================================
        // EDUCATIONAL NOTE: Adding specific log message here if generation fails.
        // This helps us debug if Step 3 is the problem.
        // =====================================================================================
        if (generatedContent == null) {
            log.error("Generation step failed for notice segment {}. Saving with extracted data only.", sourceOrder);
            // We will proceed but createGazetteFromJson will handle the fallback state
        } else {
            log.info("Generation complete for notice segment {}.", sourceOrder);
        }

        // Final Step: Map everything to our Gazette entity
        // This method now handles the case where generatedContent might be null
        return createGazetteFromJson(extractedData, generatedContent, textSegment, category, sourceOrder, overallGazetteDetails);
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
        Tenders
        Land_Property
        Court_Legal
        Public_Service_HR
        Licensing
        Company_Registrations
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

    // =====================================================================================
    // CHANGE 4: Add the data sanitization step here to fix the database crash.
    // =====================================================================================
    private Gazette createGazetteFromJson(JSONObject extractedData, JSONObject generatedContent, String rawContent, String category, int order, JSONObject overallGazetteDetails) {
        // We absolutely need extractedData to proceed.
        if (extractedData == null) {
            log.error("Cannot create Gazette object: extractedData is null for order {}", order);
            // Return null or throw an exception if this case should be fatal
            // For now, let's create a minimal FAILED object
            Gazette failedGazette = new Gazette();
            failedGazette.setStatus(ProcessingStatus.FAILED);
            failedGazette.setTitle("[EXTRACTION FAILED] Review Needed");
            failedGazette.setCategory("Uncategorized");
            failedGazette.setSourceOrder(order);
            failedGazette.setContent(rawContent.replace("\u0000", ""));
            failedGazette.setArticle("Extraction failed. Original text in content field.");
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
            gazette.setArticle(generatedContent.optString("article", extractedData.toString(2)).replace("\u0000", ""));
            gazette.setXSummary(generatedContent.optString("xSummary", "").replace("\u0000", ""));
            gazette.setActionableInfo(generatedContent.optString("actionableInfo", "").replace("\u0000", ""));
        } else {
            // Generation failed. Populate minimally, show extracted data, and set status to FAILED.
            gazette.setStatus(ProcessingStatus.FAILED);
            gazette.setTitle("[GENERATION FAILED] " + category + " Notice (Review Extracted Data)");
            gazette.setSummary("AI failed to generate summary. Review extracted data below.");
            // Store the extracted JSON in the article field for admin review
            gazette.setArticle("## Extracted Data (Generation Failed):\n\n```json\n" + extractedData.toString(2).replace("\u0000", "") + "\n```");
            gazette.setXSummary(""); // No tweet summary if generation failed
            gazette.setActionableInfo("Review needed");
        }

        // --- Populate remaining fields from EXTRACTED data ---
        gazette.setNoticeNumber(extractedData.optString("notice_id", extractedData.optString("reference_number", "")).replace("\u0000", ""));
        gazette.setSignatory(extractedData.optString("signatory", "").replace("\u0000", ""));

        // Safely parse the date
        String dateStr = extractedData.optString("publication_date", extractedData.optString("effective_date", ""));
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
        g.setArticle(text.replace("\u0000", ""));
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
}