package com.smartgazette.smartgazette.service;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * OCR repair for the scan lane (fix 5) - a rule-for-rule port of
 * tools/scan_lane.py fix_headers() and ocr_fix(). Keep the two in sync; the
 * parity test runs both on every Tesseract page of the 2022-2025 scans.
 *
 * WHY: OCR text of a scanned gazette carries reading errors the born-digital
 * cleaner never sees: notice headers read as "GAZETTE N0TICE No, 8O6O",
 * "CAUSE NO. E139 oF 2022", specks read as quotes or colons, "tor a grant",
 * "Keriya". Fixing them before the normal cleaner raised probate cause blocks
 * templated on scans from 78.0% to 86.6% (born-digital: 93-97%).
 *
 * Word correction is deliberately narrow (review of 787 first-run corrections
 * found names being changed - Shaw->Show, Mboga->Mbogo): a word is corrected
 * only when it was never seen (capitalised) or seen <= 3 times (lowercase) in
 * five years of born-digital text, and ONE known OCR confusion turns it into a
 * clearly more common Gazette word.
 */
public final class OcrTextFixer {

    private static final int FLAGS = Pattern.UNICODE_CHARACTER_CLASS;

    private static final Pattern HDR = Pattern.compile(
            "G\\s?A\\s?Z\\s?E\\s?T\\s?T?\\s?E\\s+N\\s?[O0o]\\s?T\\s?[I1l|]\\s?C\\s?E\\s+N\\s?[O0o]\\s*[.,:;]?\\s*"
            + "(?<num>[0-9OoIl|SB](?:[0-9OoIl|SB ]{0,6}[0-9OoIl|SB])?)\\b(?!\\s*of\\s+\\d{4})",
            FLAGS | Pattern.CASE_INSENSITIVE);
    private static final Pattern CAUSE = Pattern.compile(
            "\\bC\\s?A\\s?U\\s?S\\s?E\\s+N\\s?[O0o]\\s*[.,:;]?\\s*(?=[A-Z]?\\s*[0-9OoIl])",
            FLAGS | Pattern.CASE_INSENSITIVE);

    private static final Pattern CAUSE_OF = Pattern.compile(
            "(CAUSE NO\\.\\s*[A-Z]?\\s*\\d+\\s*)[oO0][fF]\\s*(\\d{4})[\\s.,;:+|'`i]*?(?=\\s*\\(?By\\b)", FLAGS);
    private static final Pattern LEAD_QUOTE = Pattern.compile(
            "(?:(?<=\\s)|^)['\u2018\u2019`](?=[A-Za-z])", FLAGS | Pattern.MULTILINE);
    private static final Pattern MID_SPECK = Pattern.compile(
            "(?<=\\b[a-z]{1})[:;'](?=\\s+[a-z])|(?<=[a-z]{2})[:;'](?=\\s+[a-z])", FLAGS);
    private static final Pattern WORD_DOT = Pattern.compile(
            "\\b(who|the|deceased's|a|of|for)\\.(?=\\s+[a-z])", FLAGS);
    private static final Pattern BANG_ONE = Pattern.compile("[!|]\\s?(?=\\d)", FLAGS);
    private static final Pattern S_FIVE = Pattern.compile("\\bS(?=th\\b)", FLAGS);
    private static final Pattern TILDE = Pattern.compile("(?<=\\d)~(?=[-\\d])", FLAGS);
    private static final Pattern ALIAS = Pattern.compile("\\balias(?=[A-Z])", FLAGS);
    private static final Pattern WORD = Pattern.compile(
            "(?<![A-Za-z\uFFFD])[A-Za-z\uFFFD][A-Za-z0-9\uFFFD]{2,}(?![A-Za-z\uFFFD])", FLAGS);

    private static final String[][] CONFUSIONS = {
            {"t", "f"}, {"f", "t"}, {"s", "l"}, {"l", "s"}, {"i", "l"}, {"l", "i"}, {"rn", "m"},
            {"m", "rn"}, {"cl", "d"}, {"li", "h"}, {"b", "h"}, {"h", "b"}, {"ri", "n"}, {"c", "e"},
            {"e", "c"}, {"1", "l"}, {"0", "o"}, {"5", "s"}, {"u", "n"}, {"n", "u"},
            {"ii", "u"}, {"vv", "w"}};
    static final int COMMON = 200;
    static final int COMMON_CAP = 2000;
    static final int SEEN_MAX = 3;
    static final int MARGIN = 10;

    private final VocabRepair vocab;

    public OcrTextFixer(VocabRepair vocab) {
        this.vocab = vocab;
    }

    /** One OCR page -> repaired page text (headers, specks, digits, words). */
    public String fixPage(String page) {
        return ocrFix(fixHeaders(page));
    }

    // Headers Tesseract read but garbled beyond HDR ("GAZETTR NOTICE NO",
    // "GAZETTE NOTICENO", "GAZETTE Nomice No"): a line of letters within 3
    // edits of GAZETTENOTICENO, then the number and nothing else. No digits
    // before the number ("Gazette Notice No. 123 of 2024" is a cross-reference).
    // Raised Tesseract-only recall from 94.2% to 96.5% of notice slots, which
    // made a second OCR engine unnecessary (it reached 97.9%).
    private static final Pattern HDR_LINE = Pattern.compile(
            "^(?<w>[A-Za-z .,]{10,26}?)[\\s.,:;]*(?<num>[0-9][0-9OoIlSB]{1,5})\\s*['.,]?\\s*$", FLAGS);
    private static final String HDR_LETTERS = "GAZETTENOTICENO";

    static String fuzzyHeader(String line) {
        Matcher m = HDR_LINE.matcher(line);
        if (!m.matches()) return null;
        String letters = m.group("w").replaceAll("[^A-Za-z]", "").toUpperCase(Locale.ROOT);
        if (!letters.startsWith("GA") || edits(letters, HDR_LETTERS) > 3) return null;
        String num = m.group("num").replace('O', '0').replace('o', '0').replace('I', '1')
                .replace('l', '1').replace('S', '5').replace('B', '8');
        return isDigits(num) ? "GAZETTE NOTICE NO. " + new java.math.BigInteger(num) : null;
    }

    private static int edits(String a, String b) {
        int[] prev = new int[b.length() + 1];
        for (int j = 0; j <= b.length(); j++) prev[j] = j;
        for (int i = 1; i <= a.length(); i++) {
            int[] cur = new int[b.length() + 1];
            cur[0] = i;
            for (int j = 1; j <= b.length(); j++) {
                cur[j] = Math.min(Math.min(prev[j] + 1, cur[j - 1] + 1),
                        prev[j - 1] + (a.charAt(i - 1) == b.charAt(j - 1) ? 0 : 1));
            }
            prev = cur;
        }
        return prev[b.length()];
    }

    private static String stripTrailing(String s) {
        int e = s.length();
        while (e > 0 && (Character.isWhitespace(s.charAt(e - 1)) || Character.isSpaceChar(s.charAt(e - 1)))) e--;
        return s.substring(0, e);
    }

    static String fixHeaders(String text) {
        String[] lines = text.split("\n", -1);
        StringBuilder sb = new StringBuilder(text.length());
        for (int k = 0; k < lines.length; k++) {
            String line = lines[k];
            String s = stripLeading(line);
            if ((s.isEmpty() || s.charAt(0) == 'G' || s.charAt(0) == 'g')) {
                Matcher m = HDR.matcher(s);
                if (m.lookingAt()) {
                    String num = m.group("num").replace('O', '0').replace('o', '0').replace('I', '1')
                            .replace('l', '1').replace('|', '1').replace('S', '5').replace('B', '8')
                            .replaceAll("\\s", "");
                    String rep = isDigits(num) ? "GAZETTE NOTICE NO. " + new java.math.BigInteger(num) : m.group();
                    line = rep + s.substring(m.end());
                } else if (!s.isEmpty()) {
                    String fuzzy = fuzzyHeader(stripTrailing(s));
                    if (fuzzy != null) line = fuzzy;
                }
            }
            if (k > 0) sb.append('\n');
            sb.append(line);
        }
        return CAUSE.matcher(sb.toString()).replaceAll("CAUSE NO. ");
    }

    String ocrFix(String text) {
        text = CAUSE_OF.matcher(text).replaceAll("$1OF $2 ");
        text = LEAD_QUOTE.matcher(text).replaceAll("");
        text = MID_SPECK.matcher(text).replaceAll("");
        text = WORD_DOT.matcher(text).replaceAll("$1");
        text = BANG_ONE.matcher(text).replaceAll("1");
        text = S_FIVE.matcher(text).replaceAll("5");
        text = TILDE.matcher(text).replaceAll("");
        text = ALIAS.matcher(text).replaceAll("alias ");
        if (vocab == null) {
            return text;
        }
        Matcher m = WORD.matcher(text);
        StringBuilder sb = new StringBuilder(text.length());
        while (m.find()) {
            m.appendReplacement(sb, Matcher.quoteReplacement(correctWord(m.group())));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    String correctWord(String w) {
        boolean cap = Character.isUpperCase(w.charAt(0));
        int seen = vocab.frequency(w);
        if (w.indexOf('\uFFFD') < 0 && seen > (cap ? 0 : SEEN_MAX)) {
            return w;
        }
        List<Object[]> cands = new ArrayList<>();
        for (String c : candidates(w)) {
            if (w.length() <= 3 && !VocabRepair.isShortWord(c)) continue;
            cands.add(new Object[]{vocab.frequency(c), c});
        }
        // Python: sorted((freq, word), reverse=True) - freq desc, then word desc
        cands.sort((x, y) -> {
            int c = Integer.compare((Integer) y[0], (Integer) x[0]);
            return c != 0 ? c : ((String) y[1]).compareTo((String) x[1]);
        });
        if (cands.isEmpty() || (Integer) cands.get(0)[0] < (cap ? COMMON_CAP : COMMON)) {
            return w;
        }
        if (cands.size() > 1 && (Integer) cands.get(0)[0] < MARGIN * (Integer) cands.get(1)[0]) {
            return w;
        }
        String best = (String) cands.get(0)[1];
        if (isUpperPy(w)) {
            return best.toUpperCase(Locale.ROOT);
        }
        if (cap) {
            return Character.toUpperCase(best.charAt(0)) + best.substring(1);
        }
        return best;
    }

    private static Set<String> candidates(String word) {
        String w = word.toLowerCase(Locale.ROOT);
        Set<String> out = new LinkedHashSet<>();
        int q = w.indexOf('\uFFFD');
        if (q >= 0) {
            for (char c = 'a'; c <= 'z'; c++) out.add(w.substring(0, q) + c + w.substring(q + 1));
            return out;
        }
        for (String[] ab : CONFUSIONS) {
            int start = w.indexOf(ab[0]);
            while (start >= 0) {
                out.add(w.substring(0, start) + ab[1] + w.substring(start + ab[0].length()));
                start = w.indexOf(ab[0], start + 1);
            }
        }
        return out;
    }

    /** Python str.isupper(): at least one cased character and none lowercase. */
    private static boolean isUpperPy(String s) {
        boolean cased = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (Character.isLowerCase(c)) return false;
            if (Character.isUpperCase(c)) cased = true;
        }
        return cased;
    }

    private static boolean isDigits(String s) {
        if (s.isEmpty()) return false;
        for (int i = 0; i < s.length(); i++) if (!Character.isDigit(s.charAt(i))) return false;
        return true;
    }

    private static String stripLeading(String s) {
        int i = 0;
        while (i < s.length() && (Character.isWhitespace(s.charAt(i)) || Character.isSpaceChar(s.charAt(i)))) i++;
        return s.substring(i);
    }
}
