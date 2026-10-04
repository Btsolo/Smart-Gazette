package com.smartgazette.smartgazette.service;

import org.json.JSONObject;

import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Issue details from the cover text - port of tools/gazette_header.py.
 *
 * WHY: every issue prints "Vol. CXXVIII-No. 166 ... NAIROBI, 18th September,
 * 2026" on page 1 (all 264 born-digital issues of 2022-2026 read correctly).
 * The app asked the AI for these; in the 3 Oct 2026 test run that call
 * failed and every notice of the issue was saved without its gazette number
 * and date. The AI is now only the fallback.
 */
public final class GazetteHeaderParser {

    private static final int F = Pattern.UNICODE_CHARACTER_CLASS | Pattern.UNIX_LINES;
    private static final Pattern VOL_NO = Pattern.compile(
            "Vol\\.?\\s*(?<vol>[CXLVI]+)\\s*[-–—]?\\s*No\\.?\\s*(?<no>\\d{1,3})", F | Pattern.CASE_INSENSITIVE);
    private static final Pattern DATE = Pattern.compile(
            "NAIROBI,?\\s*(?<d>\\d{1,2})\\s*(?:st|nd|rd|th)?\\s*(?<m>[A-Za-z]+),?\\s*(?<y>(?:19|20)\\d\\d)", F);
    private static final List<String> MONTHS = List.of("january", "february", "march", "april", "may", "june", "july",
            "august", "september", "october", "november", "december");

    private GazetteHeaderParser() {
    }

    /** {"gazetteVolume": "Vol. CXXVIII", "gazetteNumber": "No. 166", "gazetteDate": "2026-09-18"} or null. */
    public static JSONObject parse(String text) {
        if (text == null) return null;
        String head = text.substring(0, Math.min(3000, text.length()));
        Matcher v = VOL_NO.matcher(head);
        Matcher d = DATE.matcher(head);
        if (!v.find() || !d.find()) return null;
        int month = MONTHS.indexOf(d.group("m").toLowerCase(Locale.ROOT)) + 1;
        if (month == 0) return null;
        JSONObject o = new JSONObject();
        o.put("gazetteVolume", "Vol. " + v.group("vol").toUpperCase(Locale.ROOT));
        o.put("gazetteNumber", "No. " + Integer.parseInt(v.group("no")));
        o.put("gazetteDate", String.format("%s-%02d-%02d", d.group("y"), month, Integer.parseInt(d.group("d"))));
        return o;
    }
}
