package com.smartgazette.smartgazette.service.templates;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.smartgazette.smartgazette.service.templates.Py.*;

/**
 * Rule-based extractor for land title notices (lost title / provisional
 * certificate / replacement / reconstruction) - a port of
 * tools/land_template.py (keep in sync; parity-tested on 2022-2026).
 * Returns null when parties or a clean parcel id are missing (-> AI path).
 * Field names follow schemas/field/land_property.json.
 */
public final class LandTemplate {

    private static final int F = I | S;

    /** Whitespace-tolerant word: the joiner may split "proprie tors". */
    private static String tol(String word) {
        StringBuilder b = new StringBuilder();
        for (int i = 0; i < word.length(); i++) {
            if (i > 0) b.append("\\s*");
            b.append(Pattern.quote(String.valueOf(word.charAt(i))));
        }
        return b.toString();
    }

    private static final Pattern RE_ACT = re("THE\\s*LAND\\s+(?:REGISTRATION|TITLES?)\\s+ACT", F);
    private static final Pattern RE_SUBJ = re("ISSUE\\s+OF\\s+A\\s+(?<subject>(?:NEW\\s+)?(?:PROVISIONAL\\s+)?"
            + "(?:LAND\\s+)?(?:CERTIFICATE|TITLE|LEASE)[A-Z\\s]*?)(?=WHEREAS|\\s*$)", F);
    private static final Pattern RE_PROP = re(
            "WHEREAS\\s+(?<names>.+?)\\s*,?\\s*"
            + "(?:of\\s+(?<address>P\\.?\\s*O\\.?\\s*Box[^,]*(?:,\\s*[^,]*?)?)\\s*,?\\s*)?"
            + "(?:in\\s+the\\s+Republic\\s+of\\s+Kenya\\s*,?\\s*)?"
            + "(?:is|are)?\\s*(?:the\\s+)?(?:directors?\\s+of\\s+[^,]+,\\s*)?" + tol("registered")
            + "(?:\\s+as\\s+" + tol("proprietor") + "s?)?"
            + "(?<tenure>.{0,70}?)\\s*of\\s*(?:all\\s*)?th(?:at|ose)", F);
    private static final Pattern RE_LR = re("known\\s+as\\s+(?<lr>.+?)(?:\\s*,\\s*|\\s+)"
            + "(?=containing|situate|measuring|(?:by\\s+)?virtue|and\\s+whereas|" + tol("registered") + ")", F);
    private static final Pattern RE_LR2 = re("(?:" + tol("registered") + "\\s+under\\s*(?:the\\s*)?)?ti?tle\\s+Nos?\\.?\\s*"
            + "(?<lr>.+?)\\s*(?:,?\\s*respectively)?\\s*"
            + "(?:(?:\\s*,\\s*|\\s+)(?=and\\s+whereas|(?:by\\s+)?virtue|containing|situate|measuring)|(?<!\\b[A-Z])\\.\\s|$)", F);
    private static final Pattern RE_LR3 = re(tol("registered") + "\\s+as\\s+(?<lr>(?:C|I)\\.?\\s*R\\.?\\s*(?:No\\.?\\s*)?\\d+(?:\\s*/\\s*\\d+)+)", F);
    private static final Pattern RE_AREA = re("containing\\s+(?<area>[\\d\\.]+\\s*(?:hectares?|acres?))", F);
    private static final Pattern RE_LOC = re("situate\\s+in\\s+(?:the\\s+)?(?<location>.+?)\\s+in\\s+(?:the\\s+)?(?<district>[\\w\\s\\-]+?)\\s*"
            + "(?:District|Area|County)", F);
    private static final Pattern RE_LOC2 = re("situate\\s+in\\s+the\\s+(?:district|county)\\s+of\\s+(?<district>[\\w\\s\\-]+?)\\s*(?:,|\\.|$)", F);
    private static final Pattern RE_LOC3 = re("situate\\s+in\\s+(?:the\\s+)?(?<district>[\\w\\s\\-]+?)\\s+(?:County|District)", F);
    private static final Pattern RE_IR = re(tol("registered") + "\\s+as\\s+(?:I\\.?\\s*R\\.?|C\\.?\\s*R\\.?)\\s*(?<ir>[\\w/\\-\\.]+)", F);
    private static final Pattern RE_LOST = re("show\\s+that\\s+the\\s+said\\s*(?<lost>.+?)\\s*(?:issued\\s+thereof\\s*)?has\\s+been\\s+lost", F);
    private static final Pattern RE_DAYS = re("expiration\\s+of\\s+(?<period>[a-z\\-]+)\\s*\\(\\s*(?<days>\\d+)\\s*\\)\\s*days", F);
    private static final Pattern RE_REGISTRAR = re("([A-Z][A-Za-z\\.\\s']{3,40}?)\\s*,\\s*(?:MR\\s*/?\\s*\\d+\\s*)?(?:Land\\s*|Deputy\\s*|District\\s*)?Registrar(?:\\s*of\\s*Titles)?", F);
    private static final Pattern RE_CAUSE_REF = re("(?:succession\\s+)?cause\\s+no\\.?\\s*(?<cause>[A-Z]?\\d+\\s*of\\s*\\d{4})", F);
    private static final Pattern NUM_PAREN = re("\\(\\d+\\)", 0);
    private static final Pattern AND = re("\\s+and\\s+", 0);
    private static final Pattern DIGIT = re("\\d", 0);

    private LandTemplate() {
    }

    static String t(String s) {
        if (isEmpty(s)) return null;
        s = strip(squash(s), " ,.");
        return s.isEmpty() ? null : s;
    }

    private static List<String> names(String raw) {
        if (isEmpty(raw)) return new ArrayList<>();
        raw = sub(NUM_PAREN, "|", raw);
        raw = sub(AND, "|", raw);
        return Names.cleanNames(Arrays.asList(raw.split("\\|", -1)));
    }

    /** notice = full text of one GAZETTE NOTICE block. */
    public static Map<String, Object> extract(String notice) {
        if (!search(RE_ACT, notice)) return null;
        Matcher p = find(RE_PROP, notice);
        if (p == null) return null;

        Matcher lr = find(RE_LR, notice);
        if (lr == null) lr = find(RE_LR2, notice);
        if (lr == null) lr = find(RE_LR3, notice);
        Matcher loc = find(RE_LOC, notice);
        Matcher loc2 = null;
        if (loc == null) {
            loc2 = find(RE_LOC2, notice);
            if (loc2 == null) loc2 = find(RE_LOC3, notice);
        }
        Matcher ir = RE_IR.matcher(notice);
        boolean irFound = ir.find(p.end());
        Matcher lost = find(RE_LOST, notice);
        Matcher days = find(RE_DAYS, notice);
        Matcher subj = find(RE_SUBJ, notice);
        Matcher cause = find(RE_CAUSE_REF, notice);
        Matcher reg = find(RE_REGISTRAR, notice);
        Matcher area = find(RE_AREA, notice);

        String pd = days != null ? t(days.group("period")) : null;
        String period = days != null ? (pd == null ? "None" : pd) + " (" + days.group("days") + ") days" : null;

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("notice_subtype", subj != null ? t(subj.group("subject")) : "Issue of a Provisional Certificate");
        out.put("is_ownership_change", false);
        out.put("parcel_id", lr != null ? t(lr.group("lr")) : null);
        out.put("document_type", lost != null ? t(lost.group("lost")) : null);
        out.put("parties", names(p.group("names")));
        out.put("new_owner", null);
        out.put("succession_cause_number", cause != null ? t(cause.group("cause")) : null);
        out.put("action_type", "replacement of lost document");
        out.put("county", loc != null ? t(loc.group("district")) : (loc2 != null ? t(loc2.group("district")) : null));
        out.put("registrar_name", reg != null ? t(reg.group(1)) : null);
        out.put("reference_docs", irFound ? t(ir.group("ir")) : null);
        out.put("valuation", area != null ? t(area.group("area")) : null);
        out.put("objection_period", period);

        // the parcel id is the key a reader looks the notice up by: a fragment
        // stores an unfindable record, so it is refused (-> AI path)
        String pid = (String) out.get("parcel_id");
        @SuppressWarnings("unchecked")
        List<String> parties = (List<String>) out.get("parties");
        if (parties.isEmpty() || isEmpty(pid) || pid.length() < 4 || !search(DIGIT, pid)) return null;
        return out;
    }
}
