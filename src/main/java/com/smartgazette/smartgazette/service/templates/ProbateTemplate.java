package com.smartgazette.smartgazette.service.templates;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.smartgazette.smartgazette.service.templates.Py.*;

/**
 * Rule-based extractor for probate (Court_Legal) cause blocks - a port of
 * tools/probate_template.py (keep in sync; parity-tested on 2022-2026).
 * Returns null when a required field is missing, so the caller falls back
 * to the AI path. Field names follow schemas/field/court_legal.json.
 */
public final class ProbateTemplate {

    private static final Pattern RE_CAUSE = re(
            "CAUSE NO\\.\\s*(?<caseRef>[A-Z]?\\s*\\d+)\\s*OF\\s*(?<caseYear>\\d[\\d\\s]{2,5}?)\\s*"
            + "(?:\\([^)]*\\)|\\))?\\s*"
            + "By\\s+(?<body>.+?)(?=\\s*(?:CAUSE NO\\.|GAZETTE NOTICE NO\\.|$))", S);
    private static final Pattern RE_ACTION = re(
            "for\\s+(?:a\\s+|the\\s+)?(?<action>"
            + "(?:limited\\s+)?grant\\s+of\\s+letters\\s+of\\s+administration"
            + "(?:\\s+(?:intestate|testate|with\\s+(?:the\\s+)?will\\s+annexed|ad\\s+colligenda\\s+bona|ad\\s+litem))?|"
            + "grant\\s+of\\s+probate|"
            + "resealing\\s+of\\s+(?:a\\s+|the\\s+)?grant[a-z\\s]*?|"
            + "limited\\s+grant[a-z\\s]*?"
            + ")\\s*(?:of\\s+(?:the\\s+)?(?:last\\s+)?(?:written\\s+|oral\\s+)?will(?:\\s+and\\s+testament)?\\s+(?:to\\s+the\\s+estate\\s+)?of"
            + "|to\\s+the\\s+estate(?:\\s+of)?"
            + "|in\\s+respect\\s+of\\s+the\\s+estate\\s+of"
            + "|(?:written|last)?\\s*will\\s+of)\\s+(?<tail>.+)$", S | I);
    private static final String DATE = "\\d{1,2}\\s*(?:st|nd|rd|th)?\\s*[A-Za-z]+\\s*,?\\s*\\d{4}";
    private static final Pattern RE_DEATH = re(
            "^(?<deceased>.+?)"
            + "(?:\\s*,?\\s*late\\s+of\\s+(?<residence>.+?))?"
            + "[,\\s]*who\\s+died\\s*(?:(?:at|in|along|near|on\\s+the)\\s+(?<place>.+?)|(?<there>there))??"
            + "(?:[,\\s]*(?:on|n)?\\s*(?<date>" + DATE + ")"
            + "|(?:\\s+in)?\\s+(?<year>(?:18|19|20)\\d\\d)(?=\\s*[.,]?\\s*(?:\\n|$))"
            + "|\\s*(?=\\.\\s*(?:\\n|$)|\\n|$))", S | I);
    private static final Pattern INNER_CAUSE = re("(\\((?:\\s*formerly|\\s*as\\s+consolidated\\s+with)\\s*)CAUSE\\s+NO\\.", I);
    private static final Pattern SPLIT = re("(?=CAUSE NO\\.)", 0);
    private static final Pattern RE_COURT = re(
            "IN\\s+THE\\s+(?<court>(?:HIGH\\s*COURT|CHIEF\\s*MAGISTRATE|SENIOR\\s*PRINCIPAL\\s*MAGISTRATE|PRINCIPAL\\s*MAGISTRATE"
            + "|SENIOR\\s*RESIDENT\\s*MAGISTRATE|RESIDENT\\s*MAGISTRATE)[^\\n]{0,60}?)\\s*(?:PROBATE|\\n)", I);
    private static final Pattern RE_DEADLINE = re(
            "within\\s+(?<deadline>[a-z\\-]+\\s*\\(\\s*\\d+\\s*\\)\\s*days(?:\\s+from\\s+the\\s+date\\s+of\\s+publication)?)", I);
    private static final Pattern RE_ADVOCATE = re(
            "through\\s+(?:Messrs\\.?|M/s\\.?)?\\s*(?<advocates>.+?)"
            + "(?:,?\\s+advocates?\\b"
            + "|,?\\s+(?:of|in)\\s+[A-Z][A-Za-z'-]+(?:\\s+[A-Z][A-Za-z'-]+)?\\s*,?\\s*(?=for\\b)"
            + "|,\\s*(?=for\\s+(?:a\\s+|the\\s+)?(?:grant|resealing|limited|confirmation)))", I | S);
    private static final Pattern RE_ADDRESS = re("(?:all\\s+of|of)\\s+(?<address>P\\.?\\s*O\\.?\\s*Box[^,]*(?:,\\s*[^,]*)?)", I);
    private static final Pattern RE_RELATION = re("the\\s+deceased'?s?\\s+(?<relationship>[a-z\\s\\-]+?)\\s*,", I);
    private static final Pattern EXECUTOR = re("the\\s+(executors?|executrix)\\s+named", I);
    private static final Pattern WILL_END = re("will$", I);
    private static final Pattern FALLBACK_OF = re(",\\s*(?:both\\s+|all\\s+)?of\\s+", 0);
    private static final Pattern WHO_DIED = re("\\bwho\\s+died\\b", I);
    private static final Pattern ORDINAL_GLUE = re("(\\d(?:st|nd|rd|th))([A-Z])", 0);
    private static final Pattern CAMEL = re("([a-z])([A-Z])(?=[a-z])", 0);
    private static final Pattern DIGIT = re("\\d", 0);
    private static final Pattern QUANT_END = re("[,\\s]+(both|all)$", I);
    private static final Pattern NUM_PAREN = re("\\(\\d+\\)", 0);
    private static final Pattern AND = re("\\s+and\\s+", 0);
    private static final Pattern SPACE = re("\\s", 0);

    private ProbateTemplate() {
    }

    /** CAUSE NO. blocks of a probate notice, not split at a cause number quoted
     *  inside a parenthesis ("(Formerly CAUSE NO. ...)"). */
    public static List<String> splitCauses(String notice) {
        Matcher m = INNER_CAUSE.matcher(notice);
        StringBuilder sb = new StringBuilder();
        while (m.find()) m.appendReplacement(sb, Matcher.quoteReplacement(m.group(1) + "CAUSE NO."));
        m.appendTail(sb);
        List<String> out = new ArrayList<>();
        for (String b : SPLIT.split(sb.toString(), -1)) {
            if (b.startsWith("CAUSE NO.")) out.add(b.replace("CAUSE NO.", "CAUSE NO."));
        }
        return out;
    }

    static String tidy(String s) {
        if (isEmpty(s)) return null;
        s = strip(squash(s), " ,.");
        s = sub(ORDINAL_GLUE, "$1 $2", s);
        if (search(DIGIT, s)) s = sub(CAMEL, "$1 $2", s);
        s = strip(sub(QUANT_END, "", s), " ,.");
        return s.isEmpty() ? null : s;
    }

    private static List<String> names(String raw) {
        if (isEmpty(raw)) return new ArrayList<>();
        raw = sub(NUM_PAREN, "|", raw);
        raw = sub(AND, "|", raw);
        return Names.cleanNames(Arrays.asList(raw.split("\\|", -1)));
    }

    private static String group(Matcher m, String name) {
        return m == null ? null : m.group(name);
    }

    /** block = one CAUSE NO. segment; notice = the whole notice (court name and
     *  objection deadline are stated once per notice), or null. */
    public static Map<String, Object> extract(String block, String notice) {
        String ctx = !isEmpty(notice) ? notice : block;
        Matcher m = find(RE_CAUSE, block);
        if (m == null) return null;
        String body = m.group("body");
        Matcher a = find(RE_ACTION, body);
        if (a == null) return null;
        String tail = a.group("tail");
        Matcher d = find(RE_DEATH, tail);
        if (d == null) return null;

        String petSeg = body.substring(0, a.start());
        Matcher adv = find(RE_ADVOCATE, body);
        if (adv != null && adv.start() >= a.start()) adv = null;
        Matcher addr = find(RE_ADDRESS, petSeg);
        Matcher rel = find(RE_RELATION, petSeg);
        String relText = rel != null ? tidy(rel.group("relationship")) : null;
        Matcher ex = find(EXECUTOR, petSeg);
        if (ex != null && (isEmpty(relText) || search(WILL_END, relText))) {
            relText = ex.group(1).toLowerCase(Locale.ROOT);
        }
        List<Integer> cuts = new ArrayList<>();
        for (Matcher x : new Matcher[]{adv, addr, rel}) if (x != null) cuts.add(x.start());
        Matcher fb = find(FALLBACK_OF, petSeg);
        if (fb != null) cuts.add(fb.start());
        int cut = cuts.isEmpty() ? petSeg.length() : cuts.stream().min(Integer::compare).get();
        List<String> names = names(petSeg.substring(0, cut));

        String act = tidy(a.group("action")).toLowerCase(Locale.ROOT);
        Matcher court = find(RE_COURT, ctx);
        Matcher dead = find(RE_DEADLINE, ctx);
        String date = tidy(d.group("date"));
        if (isEmpty(date)) date = tidy(d.group("year"));
        String place;
        if (!isEmpty(d.group("place"))) place = tidy(d.group("place"));
        else place = !isEmpty(d.group("there")) ? tidy(d.group("residence")) : null;

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("court_name", court != null ? tidy(court.group("court")) : null);
        out.put("case_reference", tidy(sub(SPACE, "", m.group("caseRef")) + " OF " + sub(SPACE, "", m.group("caseYear"))));
        out.put("notice_subtype", title(act));
        out.put("deceased_name", tidy(d.group("deceased")));
        out.put("date_of_death", date);
        out.put("place_of_death", place);
        out.put("deceased_residence", tidy(d.group("residence")));
        out.put("petitioner_names", names);
        out.put("petitioner_relationship", relText);
        out.put("action_type", act);
        out.put("filing_deadline", dead != null ? tidy(dead.group("deadline")) : null);
        out.put("judgment_summary", null);
        out.put("advocate_firm", adv != null ? tidy(adv.group("advocates")) : null);

        String deceased = (String) out.get("deceased_name");
        if (isEmpty(deceased) || names.isEmpty()) return null;
        if (deceased.length() > 80 || search(WHO_DIED, deceased)) return null;
        return out;
    }
}
