package com.smartgazette.smartgazette.service.templates;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.smartgazette.smartgazette.service.templates.Py.*;

/**
 * Rule-based extractor for corrigenda - a port of tools/corrigenda_template.py
 * (keep in sync; parity-tested). Two shapes:
 *   extract(block)          a probate correction with CAUSE NO.:
 *                           amend the X printed as "A" to read "B"
 *   extractNotice(notice)   any correction naming the corrected notice and at
 *                           least one change (loose quotes, numbered
 *                           delete / insert instructions)
 */
public final class CorrigendaTemplate {

    private static final Pattern RE_CAUSE = re("CAUSE NO\\.\\s*(?<cause>[A-Z]?\\s*[\\d\\s]+?)\\s*of\\s*(?<causeYear>[\\d\\s]{4,6}?)\\s*,", I);
    private static final Pattern RE_REF = re("(?:IN\\s+)?Gazette\\s+Notice\\s+No\\.\\s*(?<ref>[\\d\\s]+?)\\s*(?:of\\s*(?<refYear>[\\d\\s]{4,6}?))?\\s*[,\\n]", I);
    private static final Pattern RE_AMEND = re(
            "amend\\s*the\\s+(?<field>.+?)\\s*printed\\s+as\\s*\"\\s*(?<old>[^\"]*?)\\s*\""
            + "\\s*to\\s+read\\s*\"\\s*(?<new>[^\"]*?)\\s*\"", I | S);
    private static final Pattern RE_TARGET = re(
            "(?:IN|in|further\\s+to)\\s+(?:the\\s+)?(?:Kenya\\s+)?Gaz+et+e\\s+Notices?\\s+Nos?\\.?\\s*(?<ref>\\d[\\d\\s]{0,6}\\d|\\d)"
            + "(?:\\s*of\\s*(?<year>(?:19|20)\\d\\d))?", I);
    private static final Pattern RE_AMEND_LOOSE = re(
            "amend\\s*the\\s+(?<field>.+?)\\s*printed\\s+as\\s*\"?\\s*(?<old>[^\"\\n]+?)\\s*\"?\\s*to\\s+read\\s*\"?\\s*(?<new>[^\"\\n]+?)\\s*\"?\\s*(?:\\.|;|$)",
            I | M);
    private static final Pattern RE_INSTR = re("^\\s*\\d{1,3}\\.\\s+(?<text>[^\\n]*\\b(?:delete|insert|replace|substitute|amend)\\b[^\\n]*)", I | M);
    private static final Pattern SPACE = re("\\s", 0);

    private static final Pattern OWN_HEADER = re("^\\s*GAZETTE NOTICE NO\\.[^\\n]*\\n", 0);

    private CorrigendaTemplate() {
    }

    /** Python _t: may return "" (not null) when only punctuation remains. */
    private static String t(String s) {
        return isEmpty(s) ? null : strip(squash(s), " ,.");
    }

    private static String digits(String s) {
        return isEmpty(s) ? null : sub(SPACE, "", s);
    }

    private static Map<String, Object> amendment(String field, String old, String neu) {
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("field", field);
        a.put("printed_as", old);
        a.put("to_read", neu);
        return a;
    }

    /** block = the CAUSE NO. segment; notice = the whole notice (the corrected
     *  notice is named before the cause, outside the block), or null. */
    public static Map<String, Object> extract(String block, String notice) {
        Matcher c = find(RE_CAUSE, block);
        Matcher a = find(RE_AMEND, block);
        if (c == null || a == null) return null;
        List<Map<String, Object>> amendments = new ArrayList<>();
        Matcher m = RE_AMEND.matcher(block);
        while (m.find()) amendments.add(amendment(t(m.group("field")), t(m.group("old")), t(m.group("new"))));
        Matcher r = find(RE_REF, block);
        if (r == null && notice != null) r = find(RE_REF, sub(OWN_HEADER, "", notice));   // not its own header line
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("notice_subtype", "Corrigendum");
        out.put("cause_reference", digits(c.group("cause")) + " of " + digits(c.group("causeYear")));
        out.put("amends_notice", r != null ? digits(r.group("ref")) : null);
        out.put("amends_notice_year", r != null && !isEmpty(r.group("refYear")) ? digits(r.group("refYear")) : null);
        out.put("amendments", amendments);
        return out;
    }

    public static Map<String, Object> extractNotice(String notice) {
        Matcher tg = find(RE_TARGET, notice);
        if (tg == null) return null;
        List<Map<String, Object>> amendments = new ArrayList<>();
        Matcher m = RE_AMEND_LOOSE.matcher(notice);
        while (m.find()) amendments.add(amendment(t(m.group("field")), t(m.group("old")), t(m.group("new"))));
        if (amendments.isEmpty()) {
            Matcher i = RE_INSTR.matcher(notice);
            while (i.find()) amendments.add(amendment("instruction", null, t(i.group("text"))));
        }
        if (amendments.isEmpty()) return null;
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("notice_subtype", "Corrigendum");
        out.put("cause_reference", null);
        out.put("amends_notice", digits(tg.group("ref")));
        out.put("amends_notice_year", tg.group("year"));
        out.put("amendments", amendments);
        return out;
    }
}
