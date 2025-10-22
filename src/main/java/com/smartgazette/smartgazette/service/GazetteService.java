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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
public class GazetteService {

    private static final Logger log = LoggerFactory.getLogger(GazetteService.class);
    private final GazetteRepository gazetteRepository;
    private final RestTemplate restTemplate;

    @Value("${gemini.api.key:}")
    private String geminiApiKey;
    @Value("${gemini.model:gemini-1.5-pro-latest}")
    private String geminiModel;

    public GazetteService(GazetteRepository gazetteRepository) {
        this.gazetteRepository = gazetteRepository;
        SimpleClientHttpRequestFactory rf = new SimpleClientHttpRequestFactory();
        rf.setConnectTimeout(30_000);
        rf.setReadTimeout(300_000); // 5-minute timeout for complex AI tasks
        this.restTemplate = new RestTemplate(rf);
    }

    // --- Core Public Methods ---
    public List<Gazette> getAllGazettes() { return gazetteRepository.findAllByOrderBySourceOrderAsc(); }
    public Gazette getGazetteById(Long id) { return gazetteRepository.findById(id).orElse(null); }
    public void deleteGazette(Long id) { gazetteRepository.deleteById(id); }
    public Gazette saveGazette(Gazette gazette) { return gazetteRepository.save(gazette); }

    // --- Main PDF Processing Pipeline (SIMPLIFIED) ---
    @Async
    public void processAndSavePdf(File file) {
        PDDocument document = null;
        try {
            log.info(">>>> Starting async PDF processing for file: {}", file.getName());
            document = PDDocument.load(file);
            List<String> segments = segmentDocumentByPages(document, 4);
            if (segments.isEmpty()) {
                log.warn("No text could be extracted from the PDF.");
                return;
            }

            log.info("PDF split into {} segments.", segments.size());
            int sourceOrderCounter = 0;
            for (int i = 0; i < segments.size(); i++) {
                String segment = segments.get(i);
                log.info("-----> Processing Segment {}/{}...", i + 1, segments.size());

                // Make ONE powerful AI call for each segment
                List<Gazette> processedGazettes = processSegmentWithAI(segment);

                if (processedGazettes.isEmpty()) {
                    log.warn("AI returned no articles for this segment. Creating a fallback.");
                    gazetteRepository.save(createFallbackGazette(segment, sourceOrderCounter++));
                } else {
                    for (Gazette gazette : processedGazettes) {
                        gazette.setSourceOrder(sourceOrderCounter++);
                        gazette.setContent(segment); // Save the segment text for context
                        log.info("Saving article: '{}'", gazette.getTitle());
                        gazetteRepository.save(gazette);
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

    // --- NEW: Single, Powerful AI Method ---
    private List<Gazette> processSegmentWithAI(String segment) {
        String systemPrompt = "You are an expert editorial assistant. Your task is to parse a raw gazette text that may contain multiple notices. " +
                "Identify each distinct notice and process it. You MUST return a JSON object with a single key 'articles', which holds an array of JSON objects. " +
                "Each object in the array represents one notice and must have nine keys: 'title', 'summary', 'xSummary', 'article', 'category', 'publishedDate', 'noticeNumber', 'signatory', and 'actionableInfo'. " +
                "If a notice is routine (like a land transfer), the 'article' can be a brief, single paragraph. If it is significant (like a new bill), the 'article' should be 300-600 words. " +
                "For appointment gauge them on their significance to know if to produce an article explaining or not as significant can be grouped and a single article with paragraphs explaining each is produced(this can be excepted from the word limit if it exceeds)"+
                "For the xSummary use the twitter user by the name of Moe(moneycademyke) as a guide to know how to write them ,the 'xSummary' should be not more 276 characters."+
                "For routine notices that are very similar (e.g., multiple land title loss notices), you may group them into a single 'digest' article, using markdown lists in the 'article' field to present the information clearly. " +
                "Return only the valid JSON object and nothing else.";

        String responseText = makeGeminiApiCall(systemPrompt, segment);
        List<Gazette> processedGazettes = new ArrayList<>();
        JSONObject parsedJson = parseSafeJson(responseText);

        if (parsedJson != null && parsedJson.has("articles")) {
            JSONArray articlesArray = parsedJson.getJSONArray("articles");
            for (Object item : articlesArray) {
                Gazette gazette = createGazetteFromJson((JSONObject) item);
                if (gazette != null) {
                    processedGazettes.add(gazette);
                }
            }
        }
        return processedGazettes;
    }

    // --- Helper Methods ---
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
            if (resp.getStatusCode().is2xxSuccessful() && resp.getBody() != null) {
                JSONObject jsonResponse = new JSONObject(resp.getBody());
                log.info("Gemini Raw Response Received.");
                return jsonResponse.getJSONArray("candidates").getJSONObject(0).getJSONObject("content").getJSONArray("parts").getJSONObject(0).getString("text");
            }
        } catch (HttpClientErrorException e) {
            log.error("Gemini client error: {} - {}", e.getStatusCode(), e.getResponseBodyAsString());
        } catch (ResourceAccessException e) {
            log.error("Gemini network error: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Unexpected error during Gemini call: {}", e.getMessage(), e);
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