package com.smartgazette.smartgazette.service.templates;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.smartgazette.smartgazette.service.templates.Py.*;

/**
 * Appointment notices - a port of tools/appointments_template.py (keep in
 * sync). Spec: docs/specs/appointments-template.md.
 *
 * Reads the "IN EXERCISE of the powers conferred by ... appoints- X to be Y"
 * formula (78% of the 641 notices of 2022-2026) into one record per appointed
 * person. Refuses (null -> AI path) tables and schedules, taskforces, customs
 * places, and any notice where one name in the list does not look like a
 * personal name - never a partial list.
 */
public final class AppointmentsTemplate {

    private static final String DASH = "\\x{2013}";
    private static final String ROLE_END = "(?=,\\s*for\\s|\\s+for\\s+(?:a\\s+|the\\s+|another\\s+)?(?:further\\s+)?(?:period|term)"
            + "|,?\\s*with\\s+effect|\\.\\s*\\n|\\.\\s*Dated|\\.\\s*$|\\n\\s*Dated)";

    private static final Pattern JUNK = re("(?<=\\s)\\d{8,}(?=\\s)|\\[\\d+\\s+\\d+\\S*\\s+\\d{1,2}:\\d{2}\\s*[AP]M", 0);
    private static final Pattern RE_POWER = re("(?:exercise\\s+of\\s+(?:the\\s+)?powers?\\s+conferred\\s*(?:by|on|under|upon)?|pursuant\\s+to)\\s+"
            + "(?:the\\s+provisions\\s+of\\s+)?(?<prov>(?:section|article|regulation|paragraph|rule|sections)\\b.+?)"
            + "\\s+of\\s+(?:the\\s+)?(?:of\\s+the\\s+)?(?<act>(?:[A-Z][^,;]*?\\s+)??(?:Act|Constitution(?:\\s+of\\s+Kenya)?|Order|Regulations)\\b(?:,?\\s*\\d{4}\\b)?)", I | S);
    private static final Pattern RE_VERB = re("(?<verb>re-?\\s*appoints?|appoints?|makes\\s+the\\s+following\\s+appointments?|revokes?\\s+the\\s+\\*?appointments?\\s+of)"
            + "(?:\\s*[-:" + DASH + "]+\\s*|\\s+(?=[A-Z]))", S);
    private static final Pattern RE_ROLE = re("(?:\\s*,?\\s*(?<![A-Za-z])|(?<=[A-Za-z.)]))to\\s*(?:be|serve\\s+as)\\s+(?<r1>.+?)" + ROLE_END
            + "|(?:\\s*,?\\s+|(?<=[A-Z.)]))as\\s+(?!read\\b)(?<r2>[A-Za-z].+?)" + ROLE_END, S);
    private static final Pattern RE_TERM = re("for\\s+(?:a\\s+|the\\s+|another\\s+)?(?:further\\s+)?(?:period|term)\\s+of\\s+(?<t>[a-z\\- ]+\\(\\s*\\d+\\s*\\)\\s*(?:years?|months?))", I | S);
    private static final Pattern RE_EFF = re("with\\s+effect\\s+from\\s*(?:the\\s*)?(?<d>\\d{1,2}\\s*(?:st|nd|rd|th)?\\s*(?:of\\s+)?[A-Za-z]+,?\\s*\\d{4})", I | S);
    private static final Pattern RE_DATED = re("Dated\\s+(?:the\\s*)?(?<d>\\d{1,2}\\s*(?:st|nd|rd|th)?\\s*(?:day\\s+of\\s+)?[A-Za-z]+,?\\s*\\d{4})\\.?\\s*"
            + "(?<name>[A-Z][A-Z.'\\- ]*[A-Z])\\s*,\\s*(?<title>[^\\n]+)", S);
    private static final Pattern RE_GN = re("(?:Gazette\\s+Notice\\s+Nos?\\.?|G\\.\\s*N\\.?\\s*(?:No\\.?)?)\\s*(?<n>\\d+)\\s*(?:of|/)\\s*(?<y>\\d{4})", I | S);
    private static final Pattern RE_LABEL = re("Under\\s+(?<l>(?:paragraph|sub-?section|section|para\\.?|regulation|rule|(?:first|second|third)\\s+schedule)\\s*[^-"
            + DASH + "]{0,40}?)\\s*[-" + DASH + "]+\\s*", I | S);
    private static final String ROLES = "(?:Vice[- ]?)?Chair(?:person|man|woman)|Members?|Registrar|Secretary";
    private static final Pattern RE_ROLE_PREFIX = re("\\b(?<r>" + ROLES + ")\\s*:\\s*|\\b(?<m>Members)\\s+(?=[A-Z])", I);
    private static final Pattern RE_CHAIR_AND_MEMBERS = re("^(?:Non-Executive\\s+)?Chair\\w*\\s+and\\s+Members?$", I);
    private static final Pattern RE_ROLE_SUFFIX = re("\\s*[-" + DASH + "]\\s*(?<r>" + ROLES + ")$", I);
    private static final Pattern RE_UNDER_AFTER = re("\\s+under\\s+(?<l>(?:section|paragraph|regulation)\\b.*)$", I);
    private static final Pattern PLACE = re("customs\\s+area|transit\\s+shed|for\\s+the\\s+purposes?\\s+of|for\\s+purposes\\s+of", I | S);
    private static final Pattern MIXED = re("\\band\\b|,|respectively", I | S);
    private static final String HON = "(?:Dr|Prof|Eng|Amb|Hon|Rtd|Gen|Brig|Col|Maj|Lt|Captain|Capt|Canon|Justice|Lady\\s+Justice|Bishop|Rev|Ms|Mrs|Mr|Arch|F?CPA|Sen|Fr|Amb|Ambassador|Gen\\.?\\s*\\(Rtd\\))";
    private static final Pattern RE_HON_PRE = re("^(?<h>(?:" + HON + "\\.?\\s*(?:\\(\\s*Rtd\\.?\\s*\\)\\s*)?)+)\\s+(?=[A-Z])", I);
    private static final Pattern RE_HON_POST = re("\\s*\\(\\s*(?<h>(?:" + HON + ")\\.?(?:\\s*(?:" + HON + ")\\.?)*(?:\\s*\\(?\\s*Rtd\\.?\\s*\\)?)?)\\s*\\)\\s*(?<g>(?:Gen|Brig|Col|Maj)\\.?)?\\s*$", I);
    private static final Pattern NOT_A_NAME = re("\\b(?:board|council|act|the|of|following|persons?|members?|under|paragraph|section|schedule|"
            + "chair\\w*|committee|authority|appoint\\w*|to|be|serve|as|is|whose|names?|for|with|effect|period|vide|gazette|notice|"
            + "republic|kenya\\s+(?:defence|forces))\\b|\\d", I);
    private static final Pattern SING_IES = re("ies$", 0);
    private static final Pattern SING_S = re("(?<=[^s])s$", 0);
    private static final Pattern MEMBERS_WORD = re("members?", I);
    private static final Pattern HON_THEN_NAME = re("(\\([^)]{1,12}\\))\\s+(?=[A-Z][a-z])", 0);
    private static final Pattern SPLIT_NAMES = re("[,;\\n]|\\s+and\\s+(?=[A-Z])", 0);
    private static final Pattern LEAD_ARTICLE = re("^(?:a|an|the)\\s+", I);
    private static final Pattern ROLE_OF = re("\\s+(?:of|to)\\s+(?:the\\s+)?", 0);
    private static final Pattern I_FORM = re("(?:^|,\\s*|\\s)I,?\\s+[^,]+,\\s*(?<a>.+)$", 0);
    private static final Pattern PRESIDENT = re("President\\b", 0);
    private static final Pattern I_TITLE_END = re(",\\s*(?=and\\b)|\\s+and\\s+Commander", 0);
    private static final Pattern THE_AUTHORITY = re("(?:^|,\\s*)the\\s+(?=[A-Z])", 0);
    private static final Pattern REVOK = re("revok", I);
    private static final Pattern OWN_NUMBER = re("\\s*GAZETTE NOTICE NO\\.\\s*(\\d+)", 0);

    private AppointmentsTemplate() {
    }

    /** One appointee as read from the list: name, honorific, provision label, own role. */
    private record Person(String name, String hon, String label, String role) {
    }

    private static String t(String s) {
        if (isEmpty(s)) return null;
        s = strip(squash(s), " ,.;:");
        return s.isEmpty() ? null : s;
    }

    private static String capFirst(String s) {
        return s.isEmpty() ? s : s.substring(0, 1).toUpperCase(Locale.ROOT) + s.substring(1);
    }

    private static String[] honorific(String name) {
        List<String> hs = new ArrayList<>();
        Matcher m = RE_HON_POST.matcher(name);
        while (m.find()) {                                   // "(DR.) (BISHOP)"
            hs.add(0, t(m.group("h") + (m.group("g") != null ? " " + m.group("g") : "")));
            name = name.substring(0, m.start());
            m = RE_HON_POST.matcher(name);
        }
        m = RE_HON_PRE.matcher(name);
        if (m.lookingAt()) {
            hs.add(0, t(m.group("h")));
            name = name.substring(m.end());
        }
        StringBuilder h = new StringBuilder();
        for (String x : hs) {
            if (isEmpty(x)) continue;
            if (h.length() > 0) h.append(' ');
            h.append(x);
        }
        return new String[]{t(name), h.length() > 0 ? h.toString() : null};
    }

    private static String roleWord(String r) {
        r = t(r);
        return MEMBERS_WORD.matcher(r).matches() ? "Member" : capFirst(r);
    }

    private static List<Person> names(String raw) {
        raw = HON_THEN_NAME.matcher(raw).replaceAll("$1, ");
        // "Under paragraph (d)-" labels and "Chairperson:" / "Members:" switches,
        // in the order printed; each applies to the names after it
        List<Object[]> marks = new ArrayList<>();
        Matcher m = RE_LABEL.matcher(raw);
        while (m.find()) marks.add(new Object[]{m.start(), m.end(), "label", t(m.group("l"))});
        m = RE_ROLE_PREFIX.matcher(raw);
        while (m.find()) marks.add(new Object[]{m.start(), m.end(), "role", roleWord(m.group("r") != null ? m.group("r") : m.group("m"))});
        marks.sort(Comparator.<Object[]>comparingInt(a -> (Integer) a[0]).thenComparingInt(a -> (Integer) a[1])
                .thenComparing(a -> (String) a[2]).thenComparing(a -> String.valueOf(a[3])));
        List<String[]> pieces = new ArrayList<>();
        String label = null, role = null;
        int pos = 0;
        for (Object[] mk : marks) {
            int start = (Integer) mk[0];
            if (start < pos) continue;
            pieces.add(new String[]{raw.substring(pos, start), label, role});
            if ("label".equals(mk[2])) label = (String) mk[3];
            else role = (String) mk[3];
            pos = (Integer) mk[1];
        }
        pieces.add(new String[]{raw.substring(pos), label, role});
        List<Person> out = new ArrayList<>();
        for (String[] piece : pieces) {
            for (String p0 : SPLIT_NAMES.split(piece[0], -1)) {
                String p = t(p0);
                if (p == null) continue;
                String own = piece[2], plab = piece[1];
                Matcher sm = RE_ROLE_SUFFIX.matcher(p);
                if (sm.find()) {
                    own = roleWord(sm.group("r"));
                    p = p.substring(0, sm.start());
                }
                Matcher um = RE_UNDER_AFTER.matcher(p);
                if (um.find()) {
                    plab = t(um.group("l"));
                    p = p.substring(0, um.start());
                }
                String[] nh = honorific(p);
                if (nh[0] == null) {
                    if (nh[1] != null && !out.isEmpty() && out.get(out.size() - 1).hon() == null) {
                        Person last = out.get(out.size() - 1);     // "Jane Doe, (Dr.)"
                        out.set(out.size() - 1, new Person(last.name(), nh[1], last.label(), last.role()));
                        continue;
                    }
                    return null;
                }
                String clean = Names.cleanName(nh[0]);
                if (clean == null || search(NOT_A_NAME, clean) || clean.length() > 60) return null;
                int w = words(clean);
                if (w < 2 || w > 6) return null;
                out.add(new Person(clean, nh[1], plab, own));
            }
        }
        return out.isEmpty() ? null : out;
    }

    private static String[] splitRole(String role, boolean several) {
        role = sub(LEAD_ARTICLE, "", role);
        Matcher m = ROLE_OF.matcher(role);
        String pos, agency;
        if (m.find()) {
            pos = role.substring(0, m.start());
            agency = role.substring(m.end());
        } else {
            pos = role;
            agency = null;
        }
        if (several) {
            if (SING_IES.matcher(pos).find()) pos = sub(SING_IES, "y", pos);
            else if (SING_S.matcher(pos).find()) pos = sub(SING_S, "", pos);
        }
        pos = t(pos);
        return new String[]{pos != null ? capFirst(pos) : null, t(agency)};
    }

    private static String authority(String pre) {
        pre = t(pre);
        if (pre == null) pre = "";
        Matcher m = I_FORM.matcher(pre);
        if (m.find()) {
            String a = m.group("a");
            if (PRESIDENT.matcher(a).lookingAt()) return "President";
            return t(I_TITLE_END.split(a, -1)[0]);
        }
        // the first ", the <Authority>" after the power clause: a later ", the"
        // is inside the title ("for Information, Communications and the Digital Economy")
        m = THE_AUTHORITY.matcher(pre);
        if (!m.find()) return null;
        return t(pre.substring(m.end()));
    }

    public static List<Map<String, Object>> extract(String notice) {
        String tx = sub(JUNK, " ", notice);
        if (tx.contains(" | ")) return null;
        Matcher verb = RE_VERB.matcher(tx);
        if (!verb.find()) return null;
        String rest = tx.substring(verb.end());
        Matcher role = RE_ROLE.matcher(rest);
        if (!role.find()) return null;
        String namesRaw = rest.substring(0, role.start());
        String roleTxt = t(role.group("r1") != null ? role.group("r1") : role.group("r2"));
        if (roleTxt == null || search(PLACE, roleTxt)) return null;
        List<Person> people = names(namesRaw);
        if (people == null || people.size() > 40) return null;
        boolean several = people.size() > 1;
        String[] pa = splitRole(roleTxt, several);
        String position = pa[0], agency = pa[1];
        // a combined role ("a Member and Chairperson") belongs to a single person;
        // "the Chairperson and Members of X" is read when every name carries its
        // role, or when exactly one is labelled chair (the others are then the
        // members, as printed) - never by guessing who the chair is
        if (position != null && search(MIXED, position) && several && !people.stream().allMatch(p -> p.role() != null)) {
            List<Person> chairs = people.stream()
                    .filter(p -> p.role() != null && p.role().toLowerCase(Locale.ROOT).startsWith("chair")).toList();
            boolean others = people.stream().allMatch(p -> p.role() == null || "Member".equals(p.role()) || chairs.contains(p));
            if (RE_CHAIR_AND_MEMBERS.matcher(position).find() && chairs.size() == 1 && others) {
                List<Person> filled = new ArrayList<>();
                for (Person p : people) filled.add(p.role() != null ? p : new Person(p.name(), p.hon(), p.label(), "Member"));
                people = filled;
            } else {
                return null;
            }
        }
        if (position == null) return null;

        String before = tx.substring(0, verb.start());
        Matcher power = RE_POWER.matcher(before);
        boolean hasPower = power.find();
        String pre = hasPower ? tx.substring(power.end(), verb.start()) : before.substring(before.lastIndexOf('\n') + 1);
        String v = verb.group("verb").toLowerCase(Locale.ROOT);
        String kind = v.startsWith("revoke") ? "revocation" : (v.startsWith("re") ? "re-appointment" : "appointment");
        Matcher term = find(RE_TERM, rest);
        Matcher eff = find(RE_EFF, rest);
        Matcher dated = find(RE_DATED, tx);
        LinkedHashSet<String> gns = new LinkedHashSet<>();
        if (search(REVOK, tx)) {
            Matcher g = RE_GN.matcher(tx);
            while (g.find()) gns.add(g.group("n") + "/" + g.group("y"));
        }
        Matcher own = OWN_NUMBER.matcher(notice);
        String nid = own.lookingAt() ? own.group(1) : null;
        String baseProv = hasPower ? t(power.group("prov")) : null;
        String act = hasPower ? t(power.group("act")) : null;
        String authority = authority(pre);

        List<Map<String, Object>> out = new ArrayList<>();
        for (Person p : people) {
            List<String> prov = new ArrayList<>();
            if (!isEmpty(baseProv)) prov.add(baseProv);
            if (!isEmpty(p.label())) prov.add(p.label());
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("person_name", p.name());
            r.put("honorific", p.hon());
            r.put("position", p.role() != null ? p.role() : position);
            r.put("agency", agency);
            r.put("appointment_type", kind);
            r.put("appointing_authority", authority);
            r.put("term_length", term != null ? t(term.group("t")) : null);
            r.put("effective_date", eff != null ? t(eff.group("d")) : null);
            r.put("revokes_gn_number", gns.isEmpty() ? null : String.join("; ", gns));
            r.put("act", act);
            r.put("legal_provision", prov.isEmpty() ? null : String.join("; ", prov));
            r.put("signatory", dated != null ? t(dated.group("name")) : null);
            r.put("date_signed", dated != null ? t(dated.group("d")) : null);
            r.put("notice_id", nid);
            out.add(r);
        }
        return out;
    }
}
