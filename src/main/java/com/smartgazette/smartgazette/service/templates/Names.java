package com.smartgazette.smartgazette.service.templates;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static com.smartgazette.smartgazette.service.templates.Py.*;

/**
 * Proper-noun cleaning shared by every template - port of tools/names.py
 * (clean_name / clean_names). Names are the load-bearing field of a notice:
 * a wrong deceased name or proprietor makes the record unsearchable.
 * (tools/proper_nouns.py is still a pass-through placeholder, so it has no
 * Java counterpart.)
 */
final class Names {

    private static final Pattern CAPACITY = re(
            "\\s*,?\\s*\\bas\\s+(?:the\\s+|duly\\s+appointed\\s+|lawfully\\s+appointed\\s+)?"
            + "(?:administrators?|administratrix|executors?|executrix|trustees?|attorneys?|"
            + "tenants?\\s+in\\s+common|joint\\s+tenants?|legal\\s+representatives?|"
            + "personal\\s+representatives?)\\b.*$", I);
    private static final Pattern ALIAS = re("\\s+alias\\s+", I);
    private static final Pattern ADDRESS = re("\\s*,?\\s*\\bof\\s+P\\.?\\s*O\\.?\\s*Box.*$", I);
    private static final Pattern ID_PAREN = re("\\s*\\(\\s*(?:ID|I\\.D\\.)\\s*/?\\s*(?<id>[\\w/\\-]+)\\)?\\s*", I);
    private static final Pattern PAREN_ANY = re("\\s*\\([^)]*\\)\\s*", 0);
    private static final Pattern QUANTIFIER = re("[,\\s]+(?:both|all|each|jointly)$", I);
    private static final Pattern DANGLING = re("[,\\s]+(?:is|are|was|were)(?:\\s+the)?$", I);
    private static final Pattern LEAD_QUANT = re("^(?:both|all|each)\\s+", I);
    private static final Pattern COMMAS = re("\\s*,\\s*,\\s*", 0);
    private static final Pattern HYPHEN = re("\\s*-\\s*", 0);
    private static final Pattern TWO_LETTERS = re("[A-Za-z]{2}", 0);

    private Names() {
    }

    /** One personal or corporate name, or null when nothing name-like remains. */
    static String cleanName(String raw) {
        if (isEmpty(raw)) return null;
        String s = strip(squash(raw), " ,.;:");
        String[] parts = ALIAS.split(s, -1);
        if (parts.length > 1) s = parts[0];
        s = sub(ID_PAREN, " ", s);
        s = sub(CAPACITY, "", s);
        s = sub(ADDRESS, "", s);
        s = sub(PAREN_ANY, " ", s);
        s = sub(LEAD_QUANT, "", s);
        s = sub(QUANTIFIER, "", s);
        s = sub(DANGLING, "", s);
        s = sub(COMMAS, ", ", s);
        s = sub(HYPHEN, "-", s);
        s = strip(squash(s), " ,.;:-");
        if (s.length() < 3 || !search(TWO_LETTERS, s)) return null;
        return s;
    }

    /** The "alias" names peeled off a raw name (names.py clean_name extras['aliases']). */
    static List<String> aliases(String raw) {
        List<String> out = new ArrayList<>();
        if (isEmpty(raw)) return out;
        String s = strip(squash(raw), " ,.;:");
        String[] parts = ALIAS.split(s, -1);
        for (int i = 1; i < parts.length; i++) {
            if (!strip(parts[i]).isEmpty()) out.add(strip(squash(parts[i]), " ,."));
        }
        return out;
    }

    static List<String> cleanNames(List<String> raw) {
        List<String> out = new ArrayList<>();
        if (raw == null) return out;
        for (String r : raw) {
            String n = cleanName(r);
            if (n != null) out.add(n);
        }
        return out;
    }
}
