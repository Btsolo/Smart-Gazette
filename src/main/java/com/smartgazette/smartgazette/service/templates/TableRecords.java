package com.smartgazette.smartgazette.service.templates;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.smartgazette.smartgazette.service.templates.Py.*;

/**
 * Table rows as records - a port of tools/table_records.py (keep in sync).
 * Header words name the columns (parcel_id, owner, area_ha, old_lr /
 * new_parcel, name, position); print defects are repaired; validity tests
 * let a template refuse a table it cannot read cleanly.
 */
public final class TableRecords {

    private static final String[] ROLE_NAMES = {"old_lr", "new_parcel", "parcel_id", "owner", "area_ha", "position", "name"};
    private static final Pattern[] ROLE_PATTERNS = {
            re("\\bold\\b.*\\bl\\.?\\s*r\\b", I),
            re("\\bnew\\b.*\\b(?:parcel|plot)", I),
            re("\\b(?:parcel|plot|l\\.?\\s*r\\.?\\s*no|title\\s*no|land\\s*ref)", I),
            re("\\b(?:owner|proprietor)", I),
            re("\\barea\\b|\\(\\s*ha|\\bhectares?\\b|\\bacq", I),
            re("\\b(?:position|role|designation|representation|capacity)\\b", I),
            re("^\\s*(?:full\\s+)?names?\\b|\\bname\\s+of\\b", I),
    };
    private static final Pattern AREA = re("^\\d+(?:\\s?[.,]\\s?\\d+)?$", 0);
    private static final Pattern DECIMAL = re("(?<![\\d/])\\d+\\.\\d{2,}(?![\\d/])", 0);
    private static final Pattern TRAIL_AREA = re("\\s(\\d+\\.\\d{2,})$", 0);
    private static final Pattern SLASH = re("\\s*/\\s*", 0);
    private static final Pattern DIGIT = re("\\d", 0);
    private static final Pattern PY_FLOAT = Pattern.compile("[+-]?(?:\\d+\\.?\\d*|\\.\\d+)(?:[eE][+-]?\\d+)?");

    private TableRecords() {
    }

    /** column index -> role, first matching role per column, each role once. */
    public static Map<Integer, String> roles(List<String> columns) {
        Map<Integer, String> out = new LinkedHashMap<>();
        List<String> used = new ArrayList<>();
        if (columns == null) return out;
        for (int i = 0; i < columns.size(); i++) {
            for (int k = 0; k < ROLE_NAMES.length; k++) {
                if (!used.contains(ROLE_NAMES[k]) && ROLE_PATTERNS[k].matcher(columns.get(i)).find()) {
                    out.put(i, ROLE_NAMES[k]);
                    used.add(ROLE_NAMES[k]);
                    break;
                }
            }
        }
        return out;
    }

    static Double areaValue(String v) {
        String x = (v == null ? "" : v).replace(" ", "").replace(",", ".");
        return PY_FLOAT.matcher(x).matches() ? Double.valueOf(x) : null;
    }

    public static List<Map<String, String>> records(List<String> columns, List<List<String>> rows) {
        Map<Integer, String> rs = roles(columns);
        List<Map<String, String>> out = new ArrayList<>();
        if (rs.isEmpty()) return out;
        for (List<String> row : rows) {
            // a group label printed across the table ("Wednesday, 2nd July, 2025 at
            // Likoni Chief's office ...", fix 7b): one filled cell of >= 4 words is
            // not a record (a parcel number alone is 1-3 tokens)
            List<String> filled = row.stream().filter(c -> !c.isBlank()).toList();
            if (filled.size() == 1 && words(filled.get(0)) >= 4) continue;
            Map<String, String> rec = new LinkedHashMap<>();
            for (Map.Entry<Integer, String> e : rs.entrySet()) {
                rec.put(e.getValue(), strip(e.getKey() < row.size() ? row.get(e.getKey()) : ""));
            }
            // area slid into the owner column, or glued to its end ("TBD 0.0107")
            if (rec.containsKey("owner") && isEmpty(rec.get("area_ha"))) {
                String owner = rec.get("owner");
                if (AREA.matcher(owner).matches()) {
                    rec.put("area_ha", owner);
                    rec.put("owner", "");
                } else {
                    Matcher m = TRAIL_AREA.matcher(owner);
                    if (m.find()) {
                        rec.put("area_ha", m.group(1));
                        rec.put("owner", strip(owner.substring(0, m.start())));
                    }
                }
            }
            for (String k : new String[]{"parcel_id", "old_lr", "new_parcel"}) {
                if (!isEmpty(rec.get(k))) rec.put(k, sub(SLASH, "/", rec.get(k)));
            }
            out.add(rec);
        }
        return out;
    }

    /** A parcel id, not a row glued into one cell. */
    public static boolean parcelOk(String p) {
        if (isEmpty(p) || !search(DIGIT, p) || search(DECIMAL, p)) return false;
        String[] words = strip(p).split("\\s+");
        if (words.length > 6) return false;
        int last = -1;
        for (int i = 0; i < words.length; i++) if (search(DIGIT, words[i])) last = i;
        return words.length - 1 - last <= 2;
    }

    public static boolean valid(Map<String, String> rec) {
        String p = firstNonEmpty(rec.get("parcel_id"), rec.get("new_parcel"), rec.get("old_lr"));
        if (!parcelOk(p)) return false;
        if (!isEmpty(rec.get("owner")) && search(DECIMAL, rec.get("owner"))) return false;
        if (rec.containsKey("area_ha") && !isEmpty(rec.get("area_ha")) && areaValue(rec.get("area_ha")) == null) return false;
        return true;
    }

    private static String firstNonEmpty(String... v) {
        for (String s : v) if (!isEmpty(s)) return s;
        return null;
    }
}
