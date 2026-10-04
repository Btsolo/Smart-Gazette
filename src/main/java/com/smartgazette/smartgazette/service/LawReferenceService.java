package com.smartgazette.smartgazette.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The laws a notice cites, with the text of each cited provision, so the
 * notice page can show "Article 179 (2) of the Constitution" next to what
 * Article 179 (2) actually says.
 *
 * WHY THIS EXISTS
 * About a quarter of county notices (and most IEBC notices) rest on a
 * provision of the Constitution or the County Governments Act ("IN EXERCISE
 * of the powers conferred by Article 179 (2) (b) of the Constitution ...").
 * A reader cannot judge the notice without that provision.
 *
 * The law texts are reference/<key>.json, listed with the names notices print
 * them under in reference/laws.json (42 laws: the Constitution, the County
 * Governments Act and 40 Acts), built from Kenya Law by
 * tools/build_law_reference.py. Citations are found with the same rules as
 * tools/law_refs.py (keep the two in sync; parity-tested on 21,250 notices).
 * Resolved at view time from the stored notice text - nothing is persisted.
 */
@org.springframework.stereotype.Service
public class LawReferenceService {

    private static final Logger log = LoggerFactory.getLogger(LawReferenceService.class);

    /** A cited provision, ready to display. text is null when we do not hold the law. */
    public record LawRef(String law, String lawTitle, String provision, String clause,
                         String label, String title, String text, boolean found) {
    }

    // law key -> how notices name it (whitespace-tolerant: the joiner glues words),
    // from the catalog reference/laws.json (tools/build_law_reference.py pdfs);
    // longest names first, as tools/law_refs.py
    private record LawName(String key, Pattern pattern) {}
    private static final List<String> LAW_KEYS = new ArrayList<>();      // filled by loadNames(): declared first
    private static final List<LawName> LAW_NAMES = loadNames();

    private static List<LawName> loadNames() {
        List<String[]> pairs = new ArrayList<>();
        try (InputStream in = new ClassPathResource("reference/laws.json").getInputStream()) {
            for (JsonNode l : new ObjectMapper().readTree(in).path("laws")) {
                LAW_KEYS.add(l.path("key").asText());
                for (JsonNode n : l.path("names")) pairs.add(new String[]{l.path("key").asText(), n.asText()});
            }
        } catch (IOException e) {
            LoggerFactory.getLogger(LawReferenceService.class).error("Could not load reference/laws.json - no law names.", e);
        }
        pairs.sort((a, b) -> Integer.compare(b[1].length(), a[1].length()));        // stable, as Python's sort
        List<LawName> out = new ArrayList<>();
        for (String[] p : pairs)
            out.add(new LawName(p[0], Pattern.compile("\\s*" + p[1] + "\\b",Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CHARACTER_CLASS)));
        return out;
    }

    private static final String NUM = "\\d+[A-Z]?(?:\\s*\\(\\s*[0-9a-z]{1,4}\\s*\\))*";
    private static final Pattern CITE = Pattern.compile(
            "\\b(?<kind>Articles?|Art\\.|sections?|ss?\\.)\\s*(?<list>" + NUM
            + "(?:\\s*(?:,|and|or|&)\\s*" + NUM + ")*)"
            + "(?<gap>[^.;]{0,60}?)\\b(?:of|under|to|in)\\s+(?<law>[^.;\\n]{0,70})",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern ONE = Pattern.compile("(\\d+[A-Z]?)((?:\\s*\\(\\s*[0-9a-z]{1,4}\\s*\\))*)");
    private static final Pattern FIRST_SUB = Pattern.compile("^\\(\\d+\\)");
    private static final Pattern SCHED = Pattern.compile(
            "\\b(?<n>First|Second|Third|Fourth|Fifth|Sixth)\\s+Schedule\\s+(?:to|of)\\s+(?<law>(?:the\\s*)?Constitution)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern OTHER_ACT = Pattern.compile(
            "\\s*(?:the\\s+)?((?:[A-Z][A-Za-z'\\-]*\\s+){1,9}?Act)\\b");

    private final Map<String, JsonNode> laws = new LinkedHashMap<>();

    @PostConstruct
    void load() {
        for (String key : LAW_KEYS) {
            try (InputStream in = new ClassPathResource("reference/" + key + ".json").getInputStream()) {
                laws.put(key, new ObjectMapper().readTree(in));
            } catch (IOException e) {
                log.error("Could not load reference/{}.json - its citations will show without text.", key, e);
            }
        }
        log.info("Law references loaded: {}", laws.keySet());
    }

    /** Also usable outside Spring (parity test): load from a JSON per key. */
    public void put(String key, JsonNode doc) {
        laws.put(key, doc);
    }

    private static String whichLaw(String text) {
        for (LawName n : LAW_NAMES) {
            if (n.pattern().matcher(text).lookingAt()) {
                return n.key();
            }
        }
        Matcher m = OTHER_ACT.matcher(text);
        return m.lookingAt() ? "?" + m.group(1).replaceAll("\\s+", " ") : null;
    }

    /** Cited provisions in order of first mention, without duplicates. */
    public List<LawRef> refs(String notice) {
        List<LawRef> out = new ArrayList<>();
        if (notice == null || notice.isBlank()) {
            return out;
        }
        Set<String> seen = new HashSet<>();
        Matcher m = CITE.matcher(notice);
        while (m.find()) {
            String kind = m.group("kind").toLowerCase(Locale.ROOT);
            String key = whichLaw(m.group("law"));
            if (key == null) continue;
            if (kind.startsWith("art") && !key.equals("constitution")) continue;
            if (kind.startsWith("s") && key.equals("constitution")) continue;
            Matcher one = ONE.matcher(m.group("list"));
            while (one.find()) {
                String clause = one.group(2).replaceAll("\\s+", "");
                Matcher first = FIRST_SUB.matcher(clause);
                add(out, seen, key, one.group(1), first.find() ? first.group() : null);
            }
        }
        Matcher s = SCHED.matcher(notice);
        while (s.find()) {
            add(out, seen, "constitution", s.group("n").toUpperCase(Locale.ROOT) + " SCHEDULE", null);
        }
        return out;
    }

    private void add(List<LawRef> out, Set<String> seen, String key, String num, String clause) {
        if (!seen.add(key + "|" + num + "|" + clause)) {
            return;
        }
        JsonNode doc = key.startsWith("?") ? null : laws.get(key);
        String lawTitle = doc != null ? doc.path("title").asText() : key.substring(key.startsWith("?") ? 1 : 0);
        if (num.endsWith("SCHEDULE")) {
            JsonNode sched = null;
            if (doc != null) {
                for (JsonNode x : doc.path("schedules")) {
                    if (x.path("title").asText().equals(num)) sched = x;
                }
            }
            String label = titleCase(num);
            out.add(new LawRef(key, lawTitle, num, null, label,
                    sched != null ? sched.path("subtitle").asText() : null,
                    sched != null ? sched.path("text").asText() : null, sched != null));
            return;
        }
        JsonNode prov = doc != null ? doc.path("provisions").get(num) : null;
        String label = (doc != null ? doc.path("provision_label").asText() : "section") + " " + num
                + (clause != null ? clause : "");
        out.add(new LawRef(key, lawTitle, num, clause, label,
                prov != null ? prov.path("title").asText() : null,
                prov != null ? provisionText(prov, clause) : null, prov != null));
    }

    /** The whole provision, or one sub-article with its lettered paragraphs. */
    static String provisionText(JsonNode prov, String clause) {
        String text = prov.path("text").asText();
        if (clause == null) {
            return text;
        }
        String[] lines = text.split("\n");
        int i = -1;
        for (int k = 0; k < lines.length; k++) {
            if (lines[k].startsWith(clause)) { i = k; break; }
        }
        if (i < 0) {
            return text;
        }
        int j = i + 1;
        while (j < lines.length && lines[j].startsWith(" ")) j++;
        return String.join("\n", java.util.Arrays.copyOfRange(lines, i, j));
    }

    private static String titleCase(String s) {
        StringBuilder b = new StringBuilder();
        for (String w : s.toLowerCase(Locale.ROOT).split(" ")) {
            if (b.length() > 0) b.append(' ');
            b.append(Character.toUpperCase(w.charAt(0))).append(w.substring(1));
        }
        return b.toString();
    }
}
