package com.smartgazette.smartgazette.service;

import com.smartgazette.smartgazette.model.Gazette;
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
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
public class GazetteService {

    private static final Logger log = LoggerFactory.getLogger(GazetteService.class);
    private final GazetteRepository gazetteRepository;
    private final RestTemplate restTemplate;

    @Value("${gemini.api.key:}")
    private String geminiApiKey;
    @Value("${gemini.model:gemini-pro}")
    private String geminiModel;

    public GazetteService(GazetteRepository gazetteRepository) {
        this.gazetteRepository = gazetteRepository;
        SimpleClientHttpRequestFactory rf = new SimpleClientHttpRequestFactory();
        rf.setConnectTimeout(30_000);// 30s
        rf.setReadTimeout(300_000);// 5 minutes for very complex tasks
        this.restTemplate = new RestTemplate(rf);
    }

    // --- Core Public Methods ---
    public List<Gazette> getAllGazettes() {
        return gazetteRepository.findAllByOrderBySourceOrderAsc();
    }

    public Gazette getGazetteById(Long id) {
        return gazetteRepository.findById(id).orElse(null);
    }
    public void deleteGazette(Long id) {
        gazetteRepository.deleteById(id);
    }
    public Gazette saveGazette(Gazette gazette) {
        return gazetteRepository.save(gazette);
    }

    // --- Main PDF Processing Pipeline ---
    @Async
    public void processAndSavePdf(File file) {
        PDDocument document = null;
        try {
            log.info(">>>> Starting async PDF processing for file: {}", file.getName());
            document = PDDocument.load(file);

            // 1. Segment the document into smaller text chunks first
            List<String> segments = segmentDocumentByPages(document, 4); // Use a safe 4-page window
            if (segments.isEmpty()) {
                log.warn("No text could be extracted from the PDF.");
                return;
            }

            log.info("PDF split into {} segments.", segments.size());
            int sourceOrderCounter = 0;

            // 2. Loop through each small segment and process it
            for (int i = 0; i < segments.size(); i++) {
                String segment = segments.get(i);
                log.info("-----> Processing Segment {}/{}...", i + 1, segments.size());

                JSONObject triageJson = triageGazetteContent(segment);
                if (triageJson == null) {
                    log.warn("Triage failed for segment {}. Creating a fallback entry.", i + 1);
                    gazetteRepository.save(createFallbackGazette(segment, sourceOrderCounter++));
                    continue; // Move to the next segment
                }

                // 3. Process the significant notices found in THIS segment
                JSONArray significantNotices = triageJson.optJSONArray("significant_notices");
                if (significantNotices != null && !significantNotices.isEmpty()) {
                    log.info("Found {} significant notices in segment {}.", significantNotices.length(), i + 1);
                    for (Object item : significantNotices) {
                        String noticeText = ((JSONObject) item).optString("raw_text", "");
                        if (noticeText.isBlank()) continue;
                        Gazette gazette = processIndividualNotice(noticeText);
                        if (gazette == null) continue;
                        gazette.setContent(noticeText);
                        gazette.setSourceOrder(sourceOrderCounter++);
                        log.info("Saving significant article: '{}'", gazette.getTitle());
                        gazetteRepository.save(gazette);
                    }
                }

                // 4. Process the routine notices found in THIS segment
                JSONArray routineNotices = triageJson.optJSONArray("routine_notices");
                if (routineNotices != null && !routineNotices.isEmpty()) {
                    log.info("Found {} routine notices in segment {}.", routineNotices.length(), i + 1);
                    List<JSONObject> routineList = new ArrayList<>();
                    routineNotices.forEach(obj -> routineList.add((JSONObject) obj));
                    Map<String, List<JSONObject>> groupedRoutines = routineList.stream()
                            .collect(Collectors.groupingBy(n -> n.optString("group_key", "General")));
                    for (Map.Entry<String, List<JSONObject>> entry : groupedRoutines.entrySet()) {
                        Gazette digestGazette = processDigest(entry.getKey(), entry.getValue());
                        if (digestGazette == null) continue;
                        String digestContent = entry.getValue().stream().map(n -> n.optString("raw_text")).collect(Collectors.joining("\n---\n"));
                        digestGazette.setContent(digestContent);
                        digestGazette.setSourceOrder(sourceOrderCounter++);
                        log.info("Saving digest article: '{}'", digestGazette.getTitle());
                        gazetteRepository.save(digestGazette);
                    }
                }
            }
            log.info("<<<< Successfully finished processing PDF file: {}", file.getName());
        } catch (Exception e) {
            log.error("Critical error during AI processing pipeline: {}", e.getMessage(), e);
        } finally {
            try {
                if (document != null) document.close();
                Files.deleteIfExists(file.toPath());
            } catch (IOException ignored) {}
        }
    }

    // --- Private AI Helper Methods ---
    private JSONObject triageGazetteContent(String segment) {
        log.info("  -> Sending segment to AI for triage...");
        String systemPrompt = "You are a JSON-only API. Read the text and identify distinct notices. " +
                "Return a JSON object with two arrays: 'significant_notices' and 'routine_notices'. Each notice object must have keys: " +
                "category, raw_text (the full notice), importance_score (0-100), and group_key. " +
                "If you find no notices, you MUST return the structure with empty arrays. " +
                "Do not include any text, explanations, or markdown outside of the single, valid JSON object.";
        String responseText = makeGeminiApiCall(systemPrompt, segment);
        if (responseText != null) log.info("  <- Received triage response from AI.");
        return parseSafeJson(responseText);
    }

    private Gazette processIndividualNotice(String noticeText) {
        log.info("    -> Sending significant notice to AI for article generation...");
        String systemPrompt = "You are a JSON-only API... (your individual notice prompt)";
        String responseText = makeGeminiApiCall(systemPrompt, noticeText);
        if (responseText != null) log.info("    <- Received article response from AI.");
        return createGazetteFromJson(parseSafeJson(responseText));
    }

    private Gazette processDigest(String category, List<JSONObject> notices) {
        log.info("    -> Sending {} routine notices to AI for digest generation...", notices.size());
        String combinedText = notices.stream().map(n -> n.optString("raw_text")).collect(Collectors.joining("\n---\n"));
        String systemPrompt = "You are a JSON-only API... (your digest prompt)";
        String responseText = makeGeminiApiCall(systemPrompt, combinedText);
        if (responseText != null) log.info("    <- Received digest response from AI.");
        return createGazetteFromJson(parseSafeJson(responseText));
    }

    private String makeGeminiApiCall(String systemPrompt, String userPrompt) {
        if (geminiApiKey == null || geminiApiKey.isBlank()) {
            log.error("Gemini API key is not configured.");
            return null;
        }
        String apiUrl = "https://generativelanguage.googleapis.com/v1beta/models/" + geminiModel + ":generateContent?key=" + geminiApiKey;
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        String fullPrompt = systemPrompt + "\n\n" + userPrompt;
        String requestBody = new JSONObject()
                .put("contents", new JSONArray().put(new JSONObject()
                        .put("parts", new JSONArray().put(new JSONObject()
                                .put("text", fullPrompt)))))
                .toString();

        HttpEntity<String> entity = new HttpEntity<>(requestBody, headers);

        try {
            log.info("Sending request to Gemini model: {}", geminiModel);
            ResponseEntity<String> resp = restTemplate.postForEntity(apiUrl, entity, String.class);

            // --- NEW LOGGING ---
            String body = resp.getBody();
            log.info("Gemini Raw Response: {}", body); // This will print the AI's exact response

            if (resp.getStatusCode().is2xxSuccessful() && body != null) {
                JSONObject jsonResponse = new JSONObject(body);
                return jsonResponse.getJSONArray("candidates").getJSONObject(0).getJSONObject("content").getJSONArray("parts").getJSONObject(0).getString("text");
            }
            log.warn("Gemini non-OK response: {}", resp.getStatusCode());
        } catch (Exception e) {
            log.error("An unexpected error occurred during Gemini call: {}", e.getMessage(), e);
        }
        return null;
    }

    // --- Helper Methods for Parsing and Data Creation ---
    private JSONObject parseSafeJson(String text) {
        if (text == null || text.isBlank()) return null;
        int start = text.indexOf('{');
        int end = text.lastIndexOf('}');
        if (start == -1 || end == -1 || end <= start) return null;
        try {
            return new JSONObject(text.substring(start, end + 1));
        } catch (JSONException e) {
            log.warn("Failed to parse JSON from model output: {}", e.getMessage());
            return null;
        }
    }

    private Gazette createGazetteFromJson(JSONObject json) {
        if (json == null) return null;
        Gazette gazette = new Gazette();
        gazette.setTitle(flattenJsonField(json, "title"));
        gazette.setSummary(flattenJsonField(json, "summary"));
        gazette.setXSummary(truncate(flattenJsonField(json, "xSummary"), 277));
        gazette.setArticle(flattenJsonField(json, "article"));
        gazette.setCategory(json.optString("category", "General"));
        gazette.setNoticeNumber(json.optString("noticeNumber", ""));
        gazette.setSignatory(json.optString("signatory", ""));
        gazette.setActionableInfo(flattenJsonField(json, "actionableInfo"));
        try {
            gazette.setPublishedDate(LocalDate.parse(json.optString("publishedDate")));
        } catch (Exception e) {
            gazette.setPublishedDate(LocalDate.now());
        }
        return gazette;
    }

    private Gazette createFallbackGazette(String text, int order) {
        Gazette g = new Gazette();
        g.setTitle("[TRIAGE FAILED] Review Needed");
        g.setSummary("The AI failed to process this section. The raw text is included below for manual review.");
        g.setXSummary("Processing error. Needs manual review.");
        g.setArticle(text);
        g.setCategory("Uncategorized");
        g.setPublishedDate(LocalDate.now());
        g.setSourceOrder(order);
        return g;
    }

    private String flattenJsonField(JSONObject json, String key) {
        if (!json.has(key)) return "";
        Object field = json.get(key);
        if (field instanceof JSONArray) {
            return IntStream.range(0, ((JSONArray) field).length()).mapToObj(((JSONArray) field)::getString).collect(Collectors.joining("; "));
        }
        return field.toString();
    }

    private List<String> segmentDocumentByPages(PDDocument document, int pageWindow) throws IOException {
        List<String> segments = new ArrayList<>();
        PDFTextStripper stripper = new PDFTextStripper();
        for (int start = 1; start <= document.getNumberOfPages(); start += pageWindow) {
            stripper.setStartPage(start);
            stripper.setEndPage(Math.min(start + pageWindow - 1, document.getNumberOfPages()));
            String part = stripper.getText(document);
            if (part != null && !part.isBlank()) segments.add(part);
        }
        return segments;
    }

    private String extractTitleFallback(String text) {
        return truncate(Arrays.stream(text.split("\n")).filter(l -> !l.isBlank()).findFirst().orElse("Notice"), 120);
    }

    private String truncate(String s, int max) {
        return (s == null || s.length() <= max) ? s : s.substring(0, max - 3) + "...";
    }
}