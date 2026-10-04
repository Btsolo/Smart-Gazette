package com.smartgazette.smartgazette.service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * What a gazette image is (docs/specs/figures.md, Stage A) - the Java port of
 * tools/figures.py {@code notice_of_markers} and {@code classify}.
 *
 * Every image is kept and given a kind; nothing is discarded (a stamp or seal
 * can carry authentication). The rules read the figure's size and place, the
 * text Tesseract reads on it, and the text of the notice it sits in.
 *
 * Kinds, in order: page_scan, coat_of_arms, table_symbol, form, map, chart,
 * table_image, prescribed_image, stamp_seal, mark, signature, logo, photo, other.
 */
public final class FigureClassifier {

    private FigureClassifier() {}

    private static final int F = Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE | Pattern.UNICODE_CHARACTER_CLASS;
    private static final int U = Pattern.UNICODE_CHARACTER_CLASS;

    public static final Pattern MARK = Pattern.compile("\\[\\[FIGURE:(\\d+)\\.(\\d+)\\]\\]");
    private static final Pattern HEADER = Pattern.compile("^\\s*GAZETTE\\s+NOTICE\\s+NO\\.?\\s*(\\d+)", F);

    // on the figure itself (OCR)
    private static final Pattern MAP_WORDS = Pattern.compile(
            "\\b(?:map|registry\\s+index|index\\s+series|block|scale|sheet|coordinates?|UTM|Arc\\s*1960|survey|plan\\s+no|R\\.?I\\.?M)\\b", F);
    // in the notice text: only explicit words (a long report says 'block' or 'scale' for other reasons)
    private static final Pattern MAP_NOTICE = Pattern.compile(
            "\\b(?:maps?|index\\s+series|registry\\s+index|index\\s+map|coordinates|UTM|Arc\\s*1960|concession\\s+(?:areas?|blocks?|maps?)|coal\\s+blocks?|petroleum\\s+blocks?)\\b", F);
    private static final Pattern CHART_WORDS = Pattern.compile(
            "\\b(?:figure\\s+\\d|fig\\.\\s*\\d|chart|graph|trend|performance|revenue|growth|percent|financial\\s+year|billion|actual|target|%)", F);
    private static final Pattern PRESCRIBED_WORDS = Pattern.compile(
            "\\b(?:images?\\s+set\\s+out|pictorial|health\\s+warning|graphic\\s+warning|prescribed\\s+images?)\\b", F);
    private static final Pattern STAMP_WORDS = Pattern.compile(
            "\\b(?:received|library|council|registrar|certified|seal|stamp|approved|law\\s+reporting|commissioner\\s+for\\s+oaths|date)\\b", F);
    private static final Pattern SIGN_CONTEXT = Pattern.compile(
            "\\b(?:Dated\\s+the|Signed|Chairperson|Registrar|Secretary|Cabinet\\s+Secretary|Governor)\\b", F);
    private static final Pattern FORM = Pattern.compile("\\W*FORM\\s*\\d", U);
    private static final Pattern FIGURE_CITED = Pattern.compile("\\b(?:Figure|Fig\\.)\\s*\\d", U);
    private static final Pattern WORD = Pattern.compile("[A-Za-z]{3,}");
    private static final Pattern AMOUNT = Pattern.compile("\\b\\d{1,3}(?:,\\d{3})+\\b", U);
    private static final Pattern DIGIT = Pattern.compile("\\d", U);     // Python \d is Unicode too

    /** Page area the rules measure against (A4 in points). */
    public static final double PAGE_AREA = 595.0 * 842.0;

    /** Where a marker sits: the notice above it (null on a cover), whether the
     *  line is a table row, and the line itself. */
    public record Context(String notice, boolean inTable, String line) {
        public static final Context NONE = new Context(null, false, "");
    }

    /** A placement on the page: 1-based page, box in page points (top-left origin). */
    public record Box(int page, double x0, double y0, double x1, double y1) {}

    /** marker id ("p.k") -> its context, in the cleaned reading order: the
     *  notice is the last header above the marker. */
    public static Map<String, Context> noticeOfMarkers(String text) {
        Map<String, Context> out = new LinkedHashMap<>();
        String num = null;
        for (String line : text.split("\n", -1)) {
            Matcher h = HEADER.matcher(line);
            if (h.find()) num = h.group(1);
            Matcher m = MARK.matcher(line);
            while (m.find()) {
                boolean inTable = line.contains(" | ") || line.stripTrailing().endsWith(" |");
                out.put(m.group(1) + "." + m.group(2), new Context(num, inTable, line.strip()));
            }
        }
        return out;
    }

    private static int count(Pattern p, String s) {
        Matcher m = p.matcher(s);
        int n = 0;
        while (m.find()) n++;
        return n;
    }

    private static boolean has(Pattern p, String s) {
        return s != null && p.matcher(s).find();
    }

    /** = figures.classify (the image argument there is unused by the rules). */
    public static String classify(Box f, Context ctx, String ocrText, int repeats, String noticeText) {
        String ocr = ocrText == null ? "" : ocrText;
        double wIn = (f.x1() - f.x0()) / 72.0, hIn = (f.y1() - f.y0()) / 72.0;
        double area = wIn * hIn;
        int words = count(WORD, ocr);
        int digits = count(DIGIT, ocr);
        if ((f.x1() - f.x0()) * (f.y1() - f.y0()) >= 0.85 * PAGE_AREA)
            return has(MAP_WORDS, ocr + " " + (noticeText == null ? "" : noticeText)) ? "map" : "page_scan";
        if (f.page() == 1 && f.y0() < 300 && wIn >= 1.5 && wIn <= 3.5 && ctx.notice() == null)
            return "coat_of_arms";
        if (ctx.inTable())
            return "table_symbol";
        // a prescribed form printed as an image (2026 No 75: EPRA Form 3 certificate)
        if (FORM.matcher(ocr).lookingAt())
            return "form";
        // a page-sized map: before the chart / table tests, a map's OCR is full of parcel numbers
        if (area >= 30 && (has(MAP_WORDS, ocr) || has(MAP_NOTICE, noticeText)))
            return "map";
        if (area >= 4 && has(CHART_WORDS, ocr) && digits >= 8)
            return "chart";
        // a picture of a table (financial statements printed as images, 2023 No 154): it prints
        // comma-grouped amounts, even a one-row strip; not a bare digit count (a county map's
        // block numbers 1-24 are 43 digits, 2026 No 75 p27)
        if (area >= 2 && words >= 2 && count(AMOUNT, ocr) >= 2)
            return "table_image";
        // a report that cites "Figure n": its images are charts
        if (area >= 4 && has(FIGURE_CITED, noticeText))
            return "chart";
        if ((area >= 12 && has(MAP_WORDS, ocr)) || (area >= 4 && has(MAP_NOTICE, noticeText)))
            return "map";
        if (has(PRESCRIBED_WORDS, noticeText))
            return "prescribed_image";
        if (has(STAMP_WORDS, ocr) && area < 12)
            return "stamp_seal";
        if (area < 0.25)
            return "mark";
        if (area < 3 && wIn > 1.6 * hIn && noticeText != null && !noticeText.isEmpty() && has(SIGN_CONTEXT, ctx.line()))
            return "signature";
        if (area < 4 && repeats >= 2)
            return "logo";
        if (area >= 2 && words < 15)
            return "photo";
        if (area < 4)
            return "logo";
        return "other";
    }

    /** Kinds made from a scan or photograph: these get a cleaned copy beside the original. */
    public static boolean scanned(String kind) {
        return switch (kind) {
            case "map", "page_scan", "stamp_seal", "photo" -> true;
            default -> false;
        };
    }
}
