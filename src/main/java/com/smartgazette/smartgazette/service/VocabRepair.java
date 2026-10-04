package com.smartgazette.smartgazette.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Vocabulary-guided word repair (fix 2) - Java port of tools/vocab_repair.py.
 *
 * WHY
 * ---
 * The extractor and the fragment joiner cut words in the wrong places
 * ("a dministration", "20 21", "knownas", "ORDERSS PECIALS ITTINGO FTHE").
 * The letters are all correct; only the cuts are wrong. So we glue a short
 * run of tokens together and re-cut it into words from a Gazette dictionary
 * built from five years of real Gazette text (resources/vocab/gazette_vocab.txt).
 *
 * GUARDS (each one added after a real failure, see docs/JOURNAL.md 12.2)
 * - only words seen >= RARE times may be used to re-cut;
 * - the rarest new word must be >= RATIO times more common than the rarest old one;
 * - a common word of 3+ letters must survive a re-cut (pure merges exempt);
 * - more words out than tokens in only if every token is rare;
 * - short pieces must be real short words; at most one extra word.
 *
 * Keep in sync with tools/vocab_repair.py - the same 13 regression cases run in
 * tools/audit.py.
 */
public final class VocabRepair {

    private static final Logger log = LoggerFactory.getLogger(VocabRepair.class);

    static final int RARE = 20;
    static final int RATIO = 5;
    private static final int MAX_WORD = 30;

    private static final Set<String> SHORT = new HashSet<>(Arrays.asList(
            "a", "i", "of", "to", "in", "at", "by", "on", "as", "is", "an", "or", "no", "be", "it", "he", "we",
            "the", "and", "for", "was", "who", "are", "all", "has", "her", "his", "its", "not", "did", "box", "son"));

    private static final Pattern ORDINAL = Pattern.compile(
            "(\\d{1,2})\\s?(st|nd|rd|th)\\s?(January|February|March|April|May|June|July|August|September|October|November|December)");
    private static final Pattern TOKEN = Pattern.compile(
            "^([(\\[\"'“‘]*)(.*?)([)\\]\"'”’.,;:]*)$", Pattern.DOTALL);
    private static final Pattern CAMEL = Pattern.compile(
            "^((?!(?:Mc|Mac|De|Van|Von|La|Le)[A-Z])[A-Z][a-z]{2,})([A-Z][a-z]{2,})$");

    private final Map<String, Integer> vocab;

    VocabRepair(Map<String, Integer> vocab) {
        this.vocab = vocab;
    }

    /** Loads the corpus dictionary from the classpath; null if it is missing. */
    public static VocabRepair fromClasspath(String resource) {
        try (InputStream in = VocabRepair.class.getResourceAsStream(resource)) {
            if (in == null) {
                log.warn("VocabRepair: dictionary {} not found - word repair disabled.", resource);
                return null;
            }
            Map<String, Integer> v = new HashMap<>();
            try (BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                String line;
                while ((line = r.readLine()) != null) {
                    if (line.startsWith("#") || line.isEmpty()) continue;
                    int tab = line.indexOf('\t');
                    if (tab > 0) v.put(line.substring(0, tab), Integer.parseInt(line.substring(tab + 1).trim()));
                }
            }
            log.info("VocabRepair: loaded {} words.", v.size());
            return new VocabRepair(v);
        } catch (Exception e) {
            log.warn("VocabRepair: could not load {} - word repair disabled.", resource, e);
            return null;
        }
    }

    public String repair(String text) {
        String t = ORDINAL.matcher(text).replaceAll("$1$2 $3");
        String[] lines = t.split("\n", -1);
        StringBuilder sb = new StringBuilder(t.length());
        for (int k = 0; k < lines.length; k++) {
            if (k > 0) sb.append('\n');
            sb.append(repairLine(lines[k]));
        }
        return sb.toString();
    }

    /** Corpus frequency of a word (lowercased); 0 if unseen. Used by OcrTextFixer. */
    public int frequency(String w) {
        return f(w);
    }

    /** The 1-3 letter words a short token may legitimately be. */
    static boolean isShortWord(String w) {
        return SHORT.contains(w);
    }

    // ------------------------------------------------------------------ helpers
    private int f(String w) {
        Integer c = vocab.get(w.toLowerCase(Locale.ROOT));
        return c == null ? 0 : c;
    }

    private static boolean isAlpha(String s) {
        if (s.isEmpty()) return false;
        for (int i = 0; i < s.length(); i++) if (!Character.isLetter(s.charAt(i))) return false;
        return true;
    }

    private static boolean isDigits(String s) {
        if (s.isEmpty()) return false;
        for (int i = 0; i < s.length(); i++) if (!Character.isDigit(s.charAt(i))) return false;
        return true;
    }

    private boolean rare(String w) {
        return !w.isEmpty() && isAlpha(w) && f(w) < RARE;
    }

    /** {prefix punctuation, core, suffix punctuation} */
    private static String[] core(String tok) {
        Matcher m = TOKEN.matcher(tok);
        if (!m.matches()) return new String[]{"", tok, ""};
        return new String[]{m.group(1), m.group(2), m.group(3)};
    }

    /** Word-break: fewest words, then most frequent. Returns cut positions or null. */
    int[] segment(String letters) {
        String s = letters.toLowerCase(Locale.ROOT);
        int n = s.length();
        int[] words = new int[n + 1];
        int[] negf = new int[n + 1];
        int[] prev = new int[n + 1];
        boolean[] seen = new boolean[n + 1];
        seen[0] = true;
        for (int i = 1; i <= n; i++) {
            for (int j = Math.max(0, i - MAX_WORD); j < i; j++) {
                if (!seen[j]) continue;
                String w = s.substring(j, i);
                Integer fc = vocab.get(w);
                int fw = fc == null ? 0 : fc;
                if (fw < RARE || (w.length() <= 3 && !SHORT.contains(w))) continue;
                int cw = words[j] + 1, cn = Math.max(negf[j], -fw);
                if (!seen[i] || cw < words[i] || (cw == words[i] && cn < negf[i])) {
                    seen[i] = true; words[i] = cw; negf[i] = cn; prev[i] = j;
                }
            }
        }
        if (!seen[n]) return null;
        List<Integer> cuts = new ArrayList<>();
        for (int i = n; i > 0; i = prev[i]) cuts.add(0, i);
        return cuts.stream().mapToInt(Integer::intValue).toArray();
    }

    String repairLine(String line) {
        String[] toks = line.split(" ", -1);
        List<String> out = new ArrayList<>();
        int i = 0;
        while (i < toks.length) {
            String[] c1 = toks[i].isEmpty() ? new String[]{"", "", ""} : core(toks[i]);
            // digits split apart: "20 21" -> "2021"
            if (isDigits(c1[1]) && i + 1 < toks.length && !toks[i + 1].isEmpty() && c1[2].isEmpty()) {
                String[] c2 = core(toks[i + 1]);
                String joined = c1[1] + c2[1];
                if (isDigits(c2[1]) && c2[0].isEmpty() && f(joined) > 0 && joined.length() >= 4) {
                    toks[i + 1] = c1[0] + joined + c2[2];
                    i++;
                    continue;
                }
            }
            if (isAlpha(c1[1]) && (rare(c1[1])
                    || (i + 1 < toks.length && !toks[i + 1].isEmpty() && rare(core(toks[i + 1])[1])))) {
                int bestRare = -1, bestLow = -1, bestWidth = 0;
                List<String> bestWords = null;
                String[][] bestParts = null;
                for (int width = 1; width <= 4; width++) {
                    if (i + width > toks.length) break;
                    boolean empty = false;
                    for (int k = i; k < i + width; k++) if (toks[k].isEmpty()) empty = true;
                    if (empty) break;
                    String[][] parts = new String[width][];
                    for (int k = 0; k < width; k++) parts[k] = core(toks[i + k]);
                    boolean bad = false;
                    for (int k = 0; k < width; k++) {
                        if (k > 0 && !parts[k][0].isEmpty()) bad = true;
                        if (k < width - 1 && !parts[k][2].isEmpty()) bad = true;
                        if (!isAlpha(parts[k][1])) bad = true;
                    }
                    if (bad) break;
                    StringBuilder lb = new StringBuilder();
                    for (String[] p : parts) lb.append(p[1]);
                    String letters = lb.toString();
                    if (letters.length() < 3) continue;
                    int[] cuts = segment(letters);
                    if (cuts == null) continue;
                    List<String> words = new ArrayList<>();
                    int a = 0;
                    for (int c : cuts) { words.add(letters.substring(a, c)); a = c; }
                    boolean same = words.size() == width;
                    for (int k = 0; same && k < width; k++) if (!words.get(k).equals(parts[k][1])) same = false;
                    if (same || words.size() > width + 1) continue;
                    Set<String> lw = new HashSet<>();
                    for (String w : words) lw.add(w.toLowerCase(Locale.ROOT));
                    if (words.size() >= width) {
                        boolean lost = false;
                        for (String[] p : parts)
                            if (p[1].length() > 2 && !rare(p[1]) && !lw.contains(p[1].toLowerCase(Locale.ROOT))) lost = true;
                        if (lost) continue;
                    }
                    if (words.size() > width) {
                        boolean allRare = true;
                        for (String[] p : parts) if (!rare(p[1])) allRare = false;
                        if (!allRare) continue;
                    }
                    int low = Integer.MAX_VALUE;
                    for (String w : words) low = Math.min(low, f(w));
                    int oldLow = Integer.MAX_VALUE;
                    for (String[] p : parts) oldLow = Math.min(oldLow, f(p[1]));
                    if (low < RATIO * Math.max(1, oldLow)) continue;
                    int rareCount = 0;
                    for (String[] p : parts) if (rare(p[1])) rareCount++;
                    if (bestWords == null || rareCount > bestRare || (rareCount == bestRare && low > bestLow)) {
                        bestRare = rareCount; bestLow = low; bestWidth = width; bestWords = words; bestParts = parts;
                    }
                }
                if (bestWords != null) {
                    List<String> w = new ArrayList<>(bestWords);
                    w.set(0, bestParts[0][0] + w.get(0));
                    w.set(w.size() - 1, w.get(w.size() - 1) + bestParts[bestParts.length - 1][2]);
                    out.addAll(w);
                    i += bestWidth;
                    continue;
                }
            }
            // two names glued at a case boundary: "JohnDoe" -> "John Doe"
            if (isAlpha(c1[1])) {
                Matcher m = CAMEL.matcher(c1[1]);
                if (m.matches() && f(c1[1]) < RARE) {
                    out.add(c1[0] + m.group(1));
                    out.add(m.group(2) + c1[2]);
                    i++;
                    continue;
                }
            }
            out.add(toks[i]);
            i++;
        }
        return String.join(" ", out);
    }
}
