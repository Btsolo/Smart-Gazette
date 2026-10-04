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
                         String label, String title, String text, boolean found,
                         String kind, String versionDate, String sourceUrl) {
        /** kind: "cited" (the notice cites it), "implied" (the section this kind of
         *  notice is issued under), "act" (a law named in the heading) */
        public boolean isImplied() { return "implied".equals(kind); }
        public boolean isAct() { return "act".equals(kind); }
    }

    // law key -> how notices name it (whitespace-tolerant: the joiner glues words),
    // from the catalog reference/laws.json (tools/build_law_reference.py pdfs);
    // longest names first, as tools/law_refs.py
    private record LawName(String key, Pattern pattern, Pattern bare) {}
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
            out.add(new LawName(p[0], Pattern.compile("\\s*" + p[1] + "\\b",Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CHARACTER_CLASS), Pattern.compile(p[1] + "\\b", Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CHARACTER_CLASS)));
        return out;
    }

    private static final String NUM = "\\d+[A-Z]?(?:\\s*\\(\\s*[0-9a-z]{1,4}\\s*\\))*";
    private static final Pattern CITE = Pattern.compile(
            "\\b(?<kind>Articles?|Art\\.|sections?|ss?\\.)\\s*(?<list>" + NUM
            + "(?:\\s*(?:,|and|or|&)\\s*" + NUM + ")*)"
            + "(?<gap>[^.;]{0,60}?)\\b(?:of|under|to|in)\\s+(?<law>[^.;\\n]{0,70})",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern ONE = Pattern.compile("(\\d+[A-Z]?)((?:\\s*\\(\\s*[0-9a-zA-Z]{1,4}\\s*\\))*)");
    private static final Pattern FIRST_SUB = Pattern.compile("^\\(\\d+\\)");
    private static final Pattern SCHED = Pattern.compile(
            "\\b(?<n>First|Second|Third|Fourth|Fifth|Sixth)\\s+Schedule\\s+(?:to|of)\\s+(?<law>(?:the\\s*)?Constitution)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern OTHER_ACT = Pattern.compile(
            "\\s*(?:the\\s+)?((?:[A-Z][A-Za-z'\\-]*\\s+){1,9}?Act)\\b");

    private final Map<String, JsonNode> laws = new LinkedHashMap<>();

    /** A law of the library (null when we do not hold it). */
    public JsonNode law(String key) {
        return laws.get(key);
    }

    /** Keys of every law in the library. */
    public List<String> lawKeys() {
        return new ArrayList<>(laws.keySet());
    }

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

    static String whichLaw(String text) {
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
                    sched != null ? sched.path("text").asText() : null, sched != null, "cited",
                    versionOf(doc), sourceOf(doc)));
            return;
        }
        JsonNode prov = doc != null ? doc.path("provisions").get(num) : null;
        String label = (doc != null ? doc.path("provision_label").asText() : "section") + " " + num
                + (clause != null ? clause : "");
        out.add(new LawRef(key, lawTitle, num, clause, label,
                prov != null ? prov.path("title").asText() : null,
                prov != null ? provisionText(prov, clause) : null, prov != null, "cited",
                versionOf(doc), sourceOf(doc)));
    }

    // ------------------------------------------------------------- implied sections and Act links (= law_refs.laws_for)

    /** Where a notice's heading ends: its first operative words. */
    private static final Pattern HEAD_END = Pattern.compile(
            "\\b(?:WHEREAS|IN EXERCISE|PURSUANT|NOTICE is|TAKE NOTICE|IT IS NOTIFIED|IN PURSUANCE)\\b",
            Pattern.UNICODE_CHARACTER_CLASS);

    /** A rule of reference/implied.json. */
    private record Implied(String law, String section, String clause, Pattern heading, Pattern text) {}

    private static final List<Implied> IMPLIED = loadImplied();

    private static List<Implied> loadImplied() {
        List<Implied> out = new ArrayList<>();
        int f = Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CHARACTER_CLASS;
        try (InputStream in = new ClassPathResource("reference/implied.json").getInputStream()) {
            for (JsonNode r : new ObjectMapper().readTree(in).path("rules")) {
                out.add(new Implied(r.path("law").asText(), r.path("section").asText(),
                        r.path("clause").isNull() ? null : r.path("clause").asText(),
                        Pattern.compile(r.path("heading").asText(), f), Pattern.compile(r.path("text").asText(), f)));
            }
        } catch (IOException e) {
            LoggerFactory.getLogger(LawReferenceService.class).error("Could not load reference/implied.json.", e);
        }
        return out;
    }

    static String headingOf(String notice) {
        String head = notice.length() > 600 ? notice.substring(0, 600) : notice;
        Matcher m = HEAD_END.matcher(head);
        if (m.find()) return notice.substring(0, m.start());
        return notice.length() > 400 ? notice.substring(0, 400) : notice;
    }

    /** Keys of the laws named in the heading, in order of position; a shorter name
     *  inside a longer one already matched is not counted. */
    static List<String> headingLaws(String notice) {
        String head = headingOf(notice);
        List<int[]> spans = new ArrayList<>();
        List<Object[]> found = new ArrayList<>();
        for (LawName n : LAW_NAMES) {                                   // longest first
            Matcher m = n.bare().matcher(head);
            while (m.find()) {
                boolean overlaps = false;
                for (int[] s : spans) if (s[0] < m.end() && m.start() < s[1]) { overlaps = true; break; }
                if (overlaps) continue;
                spans.add(new int[]{m.start(), m.end()});
                found.add(new Object[]{m.start(), n.key()});
            }
        }
        found.sort((a, b) -> Integer.compare((Integer) a[0], (Integer) b[0]));
        List<String> out = new ArrayList<>();
        for (Object[] x : found) if (!out.contains((String) x[1])) out.add((String) x[1]);
        return out;
    }

    /**
     * Everything the notice rests on, for the "Laws Cited" tab and the article:
     * kind "cited" (a provision it cites), "implied" (the section this kind of
     * notice is issued under, reference/implied.json, when the heading names the
     * Act and no section of it is cited) and "act" (a law named in the heading).
     */
    public List<LawRef> lawsFor(String notice) {
        List<LawRef> out = new ArrayList<>(refs(notice));
        if (notice == null || notice.isBlank()) return out;
        Set<String> cited = new HashSet<>();
        for (LawRef r : out) cited.add(r.law());
        List<String> named = headingLaws(notice);
        String head = headingOf(notice);
        for (Implied rule : IMPLIED) {
            if (named.contains(rule.law()) && !cited.contains(rule.law())
                    && rule.heading().matcher(head).find() && rule.text().matcher(notice).find()) {
                JsonNode doc = laws.get(rule.law());
                JsonNode prov = doc != null ? doc.path("provisions").get(rule.section()) : null;
                out.add(new LawRef(rule.law(), doc != null ? doc.path("title").asText() : rule.law(), rule.section(),
                        rule.clause(), "section " + rule.section() + (rule.clause() != null ? rule.clause() : ""),
                        prov != null ? prov.path("title").asText() : null,
                        prov != null ? provisionText(prov, rule.clause()) : null, prov != null, "implied",
                        versionOf(doc), sourceOf(doc)));
                cited.add(rule.law());
                break;
            }
        }
        for (String key : named) {
            if (cited.contains(key)) continue;
            JsonNode doc = laws.get(key);
            String title = doc != null ? doc.path("title").asText() : key;
            String citation = doc != null && doc.hasNonNull("citation") ? doc.path("citation").asText() : null;
            out.add(new LawRef(key, title, null, null, title, citation, null, doc != null, "act",
                    versionOf(doc), sourceOf(doc)));
            cited.add(key);
        }
        return out;
    }

    private static String versionOf(JsonNode doc) {
        return doc != null && doc.hasNonNull("version_date") ? doc.path("version_date").asText() : null;
    }

    private static String sourceOf(JsonNode doc) {
        return doc != null && doc.hasNonNull("source_url") ? doc.path("source_url").asText() : null;
    }

    // ------------------------------------------------------------- the law in the article (docs/specs/law-library.md §3.3)

    /** The provisions an article may quote: cited or implied ones whose text we hold (at most two). */
    static List<LawRef> quotable(List<LawRef> refs) {
        List<LawRef> out = new ArrayList<>();
        for (LawRef r : refs) {
            if (!r.isAct() && r.found() && r.text() != null && !r.text().isBlank()) out.add(r);
            if (out.size() == 2) break;
        }
        return out;
    }

    /** The LAW CONTEXT block for the article prompt: exact text, trimmed; "" when there is none. */
    public static String lawContext(List<LawRef> refs, int maxChars) {
        StringBuilder sb = new StringBuilder();
        for (LawRef r : quotable(refs)) {
            String head = r.label() + " of the " + r.lawTitle() + (r.title() != null ? " (" + r.title() + ")" : "") + ":\n";
            int room = maxChars - sb.length() - head.length();
            if (room < 200) break;
            sb.append(head).append(trimTo(r.text(), room)).append("\n\n");
        }
        return sb.toString().strip();
    }

    /** The fixed sentence that ends an article: which law the notice rests on and
     *  what it says, quoted from the stored text - no AI involved. "" when the
     *  notice names no law we hold. */
    public static String lawSentence(List<LawRef> refs) {
        List<LawRef> q = quotable(refs);
        if (!q.isEmpty()) {
            LawRef r = q.get(0);
            String verb = r.isImplied() ? "This notice is issued under " : "This notice relies on ";
            return verb + r.label() + " of the " + r.lawTitle()
                    + (r.title() != null ? " (" + r.title() + ")" : "")
                    // the label names the subsection, so its "(3)" is not repeated inside the quote
                    + ", which provides: \u201c" + trimTo(oneLine(r.text()).replaceFirst("^\\(\\w+\\)\\s*", ""), 600) + "\u201d"
                    + (r.versionDate() != null ? " (Kenya Law, text as at " + r.versionDate() + ")." : ".");
        }
        for (LawRef r : refs) {
            if (r.isAct() && r.found()) {
                return "This notice is issued under the " + r.lawTitle() + (r.title() != null ? " (" + r.title() + ")" : "") + ".";
            }
        }
        return "";
    }

    private static final Pattern QUOTE = Pattern.compile("[\u201c\"]([^\u201d\"]{20,}?)[\u201d\"]");

    /**
     * Removes every sentence whose quotation is not word for word in one of the
     * sources (the stored law text, the notice): an AI may explain the law, but
     * it may not put words in its mouth.
     */
    public static String checkQuotes(String text, List<String> sources) {
        if (text == null) return null;
        List<String> norm = new ArrayList<>();
        for (String s : sources) if (s != null) norm.add(normalise(s));
        StringBuilder out = new StringBuilder();
        for (String para : text.split("\n", -1)) {
            StringBuilder kept = new StringBuilder();
            for (String sentence : para.split("(?<=[.!?][\u201d\"]?)\\s+(?=[A-Z\u201c\"])")) {
                boolean ok = true;
                Matcher m = QUOTE.matcher(sentence);
                while (m.find() && ok) {
                    String q = normalise(m.group(1)).replaceAll("(?:\\.\\.\\.|\u2026)$", "").strip();
                    boolean found = false;
                    for (String s : norm) if (s.contains(q)) { found = true; break; }
                    ok = found;
                }
                if (ok) {
                    if (kept.length() > 0) kept.append(' ');
                    kept.append(sentence);
                }
            }
            if (out.length() > 0) out.append('\n');
            out.append(kept);
        }
        return out.toString();
    }

    /** The article with its quotes checked and the fixed law sentence at the end. */
    public String withLaw(String article, String noticeText) {
        if (article == null || noticeText == null) return article;
        List<LawRef> refs = lawsFor(noticeText);
        List<String> sources = new ArrayList<>();
        sources.add(noticeText);
        for (LawRef r : refs) if (r.text() != null) sources.add(r.text());
        String checked = checkQuotes(article, sources);
        String sentence = lawSentence(refs);
        if (sentence.isEmpty() || checked.contains(sentence)) return checked;
        return checked.stripTrailing() + "\n\n" + sentence;
    }

    private static String normalise(String s) {
        return s.replace('\u2019', '\'').replace('\u2018', '\'').replace('\u2014', '-').replace('\u2013', '-')
                .replaceAll("\\s+", " ").toLowerCase(Locale.ROOT).strip();
    }

    private static String oneLine(String s) {
        return s.replaceAll("\\s*\n\\s*", " ").strip();
    }

    /** At most n characters, cut at the end of a sentence or clause when possible. */
    static String trimTo(String s, int n) {
        if (s.length() <= n) return s;
        String cut = s.substring(0, n);
        int end = Math.max(cut.lastIndexOf(". "), Math.max(cut.lastIndexOf("; "), cut.lastIndexOf(".\n")));
        return (end > n / 2 ? cut.substring(0, end + 1) : cut.substring(0, cut.lastIndexOf(' ') > 0 ? cut.lastIndexOf(' ') : n)) + " \u2026";
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
