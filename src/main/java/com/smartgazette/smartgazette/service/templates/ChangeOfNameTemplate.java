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
 * Change-of-name (deed poll) notices - a port of tools/change_of_name_template.py
 * (keep in sync). Spec: docs/specs/change-of-name-template.md.
 *
 * Reads 98% of the 1,536 deed polls of 2022-2026 into the change_of_name
 * schema fields, including the ones the collective change-of-name table needs
 * (registry, presentation number, volume, folio, file number, filed by,
 * applicants, minor). Refuses (null -> AI path) anything that is not a deed
 * poll, and any notice whose two printed former names disagree.
 */
public final class ChangeOfNameTemplate {

    private static final Pattern RE_DEED_POLL = re("\\bdeed[\\s-]*poll\\b", I | S);
    private static final Pattern RE_DEED = re("deed\\s*poll\\s*(?:dated\\s*)?,?\\s*(?:the\\s*)?(?<date>\\d{1,2}\\s*(?:st|nd|rd|th)?\\s*(?:day\\s+of\\s+)?[A-Za-z]+\\s*,?\\s*\\d{4})", I | S);
    private static final String NAME_END = "(?=for\\s+all\\s+purposes|for\\s+all|and\\s+authori[sz]es|,\\s*[a-z]|\\.?\\s*\\n|\\.?\\s*$)";
    private static final Pattern RE_ASSUMED = re("(?:assumed\\s+and|lieu\\s+thereof)\\s+(?:re-?)?adopted\\s+the\\s*(?:names?\\s*)?(?:of\\s+)?(?<name>[A-Z].+?)\\s*,?\\s*" + NAME_END, S);
    private static final Pattern RE_REGISTRY = re("(?i:Registry\\s+of\\s+Documents\\s+(?:at|in))\\s+(?<v>[A-Z][a-z]+)", S);
    private static final Pattern RE_PRES = re("Presentation\\s+No\\.?\\s*(?<v>\\d+[\\w/]*)", I | S);
    private static final Pattern RE_VOL = re("Volume\\s+(?<v>[A-Z]{1,2}\\s?\\d*|\\d+)\\b", I | S);
    private static final Pattern RE_FOLIO = re("Folio\\.?\\s*(?<v>\\d+(?:\\s*/\\s*\\d+)?)", I | S);
    private static final Pattern RE_FILE = re("File\\s+(?:No\\.?\\s*)?(?<v>[A-Z0-9][A-Z0-9/\\-]*)\\s*,", I | S);
    private static final Pattern RE_FILER = re("\\bby\\s+(?<who>our\\s+clients?|my\\s+clients?|me|us)\\s*,?\\s*(?<applicants>.+?)\\s*,?\\s*"
            + "(?=\\bof\\s+P\\.?\\s*O|\\bformerly\\s+known|\\bon\\s+behalf\\s+of|\\bin\\s+the\\s+Republic)", I | S);
    private static final Pattern RE_ADDRESS = re("\\bof\\s+(?<addr>P\\.?\\s*O\\.?\\s*Box\\s*[^,]*,\\s*[^,]*?)"
            + "(?:\\s+in\\s+the\\s+Republic\\s+of\\s+(?<country>[A-Z][a-z]+))?\\s*,", I | S);
    private static final Pattern RE_MINOR = re("on\\s+behalf\\s+of\\s+(?:the\\s+)?(?:minor\\s*,?\\s*)?(?<minor>.+?)\\s*\\(\\s*(?:a\\s+)?minors?\\s*\\)", I | S);
    private static final Pattern RE_FORMER1 = re("formerly\\s+known\\s+as\\s+(?<name>.+?)\\s*,?\\s*"
            + "(?=formally|and\\s+absolutely|absolutely|renounced|who\\s+has|has\\s+formally|do\\s+hereby)", I | S);
    private static final Pattern RE_FORMER2 = re("former\\s+names?\\s+(?:of\\s+)?(?<name>.+?)\\s*,?\\s*(?=and\\s+in\\s+lieu|in\\s+lieu)", I | S);
    private static final Pattern RE_FIRM = re("(?<firm>[A-Z][A-Z0-9&.,'\\-\\s]{2,90}?)\\s*,?\\s*Advocates?\\s+for\\b", S);
    private static final Pattern RE_REPUBLIC = re("in\\s+the\\s+Republic\\s+of\\s+(?<c>[A-Z][a-z]+)", I | S);
    private static final Pattern LEAD_AS = re("^\\s*(?:known\\s+)?as\\s+", I);
    private static final Pattern RUN_ON = re(",\\s*(?=[a-z])", 0);
    private static final Pattern NOT_A_NAME = re("\\b(?:formerly|assumed|renounced|deed\\s+poll)\\b", I);
    private static final Pattern GUARDIAN = re("\\(\\s*(?:guardians?|parents?|mother|father)\\s*\\)", I);
    private static final Pattern NUM_PAREN = re("\\(\\d+\\)", 0);
    private static final Pattern AND = re("\\s+and\\s+", 0);
    private static final Pattern BOTH_END = re("[,\\s]+(?:both|all)$", I);
    private static final Pattern NOT_LETTER = re("[^a-z]", 0);
    private static final Pattern SPACE = re("\\s", 0);
    private static final Pattern OWN_NUMBER = re("\\s*GAZETTE NOTICE NO\\.\\s*(\\d+)", 0);

    private ChangeOfNameTemplate() {
    }

    private static String t(String s) {
        if (isEmpty(s)) return null;
        s = strip(squash(s), " ,.;:");
        return s.isEmpty() ? null : s;
    }

    private static String key(String s) {
        return s == null ? "" : sub(NOT_LETTER, "", s.toLowerCase(Locale.ROOT));
    }

    private static String former(String raw) {
        if (isEmpty(raw)) return null;
        raw = sub(LEAD_AS, "", raw);
        return RUN_ON.split(raw, -1)[0];
    }

    private static List<String> applicants(String raw) {
        if (isEmpty(raw)) return new ArrayList<>();
        raw = sub(GUARDIAN, "", raw);
        raw = sub(NUM_PAREN, "|", raw);
        raw = sub(AND, "|", raw);
        raw = sub(BOTH_END, "", strip(raw));
        return Names.cleanNames(Arrays.asList(raw.split("\\|", -1)));
    }

    private static String group(Pattern p, String notice) {
        Matcher m = p.matcher(notice);
        return m.find() ? t(m.group("v")) : null;
    }

    public static Map<String, Object> extract(String notice) {
        Matcher deed = RE_DEED.matcher(notice);
        boolean hasDate = deed.find();
        Matcher assumed = RE_ASSUMED.matcher(notice);
        if (!search(RE_DEED_POLL, notice)) return null;
        if (!assumed.find()) return null;
        Matcher f1 = find(RE_FORMER1, notice);
        Matcher f2 = find(RE_FORMER2, notice);
        String n1 = f1 != null ? former(f1.group("name")) : null;
        String n2 = f2 != null ? former(f2.group("name")) : null;
        String formerRaw = !isEmpty(n1) ? n1 : n2;
        // the two printed statements of the former name are a built-in witness
        if (!isEmpty(n1) && !isEmpty(n2) && !key(Names.cleanName(t(n1))).equals(key(Names.cleanName(t(n2))))) return null;
        String formerName = !isEmpty(formerRaw) ? Names.cleanName(t(formerRaw)) : null;
        String assumedName = Names.cleanName(t(assumed.group("name")));
        if (isEmpty(formerName) || isEmpty(assumedName)) return null;
        for (String n : new String[]{formerName, assumedName}) {
            if (n.length() > 80 || search(NOT_A_NAME, n)) return null;
        }

        Matcher filer = find(RE_FILER, notice);
        String who = filer != null ? filer.group("who").toLowerCase(Locale.ROOT) : "";
        boolean advocates = who.startsWith("our") || who.startsWith("my");
        Matcher addr = find(RE_ADDRESS, notice);
        Matcher rep = find(RE_REPUBLIC, notice);
        String firm = null;
        Matcher fm = RE_FIRM.matcher(notice);
        while (fm.find()) firm = t(fm.group("firm"));
        String folio = group(RE_FOLIO, notice);
        Matcher own = OWN_NUMBER.matcher(notice);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("former_name", formerName);
        out.put("assumed_name", assumedName);
        out.put("aliases", Names.aliases(t(formerRaw)));
        out.put("person_address", addr != null ? t(addr.group("addr")) : null);
        out.put("citizenship", rep != null ? rep.group("c") : null);
        out.put("deed_poll_date", hasDate ? t(deed.group("date")) : null);
        out.put("registration_date", null);
        out.put("advocate_firm", advocates ? firm : null);
        out.put("registry", group(RE_REGISTRY, notice));
        out.put("presentation_number", group(RE_PRES, notice));
        out.put("volume", group(RE_VOL, notice));
        out.put("folio", folio != null ? sub(SPACE, "", folio) : null);
        out.put("file_number", group(RE_FILE, notice));
        out.put("filed_by", advocates ? "advocates" : (who.equals("me") || who.equals("us") ? "self" : null));
        out.put("applicants", filer != null ? applicants(filer.group("applicants")) : new ArrayList<>());
        out.put("on_behalf_of_minor", find(RE_MINOR, notice) != null);
        out.put("notice_id", own.lookingAt() ? own.group(1) : null);
        return out;
    }
}
