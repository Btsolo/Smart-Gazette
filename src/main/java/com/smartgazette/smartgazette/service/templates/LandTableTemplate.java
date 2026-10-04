package com.smartgazette.smartgazette.service.templates;

import com.smartgazette.smartgazette.service.TableExtractor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.smartgazette.smartgazette.service.templates.Py.*;

/**
 * Land notices whose content is a parcel table - compulsory acquisition
 * schedules, their "DELETION, CORRIGENDA AND ADDENDUM" corrections, and
 * registration-unit conversions. Port of tools/land_table_template.py.
 * Null unless >= 80% of the parcel rows are valid records; rows it cannot
 * read are left out and counted (rows_not_read).
 */
public final class LandTableTemplate {

    private static final Pattern RE_ACT = re("THE\\s+LAND\\s+(?:ACT|REGISTRATION\\s+ACT)|NATIONAL\\s+LAND\\s+COMMISSION", I);
    private static final Pattern RE_PROJECT = re("\\b((?:CONSTRUCTION|REHABILITATION|UPGRADING|EXPANSION|DUALLING|IMPROVEMENT|DEVELOPMENT)"
            + "\\s+OF\\s+[^a-z]{5,160}?(?:PROJECT|ROAD|LINE|DAM|SCHEME|STATION)S?)\\b", 0);
    private static final Pattern RE_CONVERSION = re("CONVERSION\\s+AND\\s+MIGRATION|REGISTRATION\\s+UNITS", I);
    private static final Pattern RE_BODY = re("on\\s+behalf\\s+of\\s+(?:the\\s+)?(.+?)\\s*(?:\\(|,|\\bgives\\b|\\bintends\\b|\\bhereby\\b)", I | S);
    private static final Pattern RE_FURTHER = re("further\\s+to\\s+Gazette\\s+Notices?\\s+Nos?\\.?\\s*(.{0,120}?)(?:,\\s*the\\b|\\bthe\\s+National)", I | S);
    private static final Pattern RE_REF = re("(\\d{1,5})\\s*(?:of|/)\\s*(\\d{4})", 0);
    private static final Pattern RE_CORR = re("\\bDELETION|ADDENDUM|CORRIGEND", I);
    private static final Pattern TRAIL_NUM = re("\\s+\\d{1,5}$", 0);

    private LandTableTemplate() {
    }

    public static Map<String, Object> extract(String notice) {
        if (!search(RE_ACT, notice.substring(0, Math.min(600, notice.length())))) return null;
        List<Map<String, Object>> sections = new ArrayList<>();
        int total = 0, ok = 0;
        for (TableExtractor.Table t : TableExtractor.tables(notice)) {
            Map<Integer, String> rs = TableRecords.roles(t.columns());
            if (!(rs.containsValue("parcel_id") || rs.containsValue("new_parcel") || rs.containsValue("old_lr"))) continue;
            List<Map<String, String>> all = TableRecords.records(t.columns(), t.rows());
            total += all.size();
            List<Map<String, String>> recs = new ArrayList<>();
            for (Map<String, String> r : all) if (TableRecords.valid(r)) recs.add(r);
            ok += recs.size();
            Matcher m = TableExtractor.SECTION.matcher(t.caption() == null ? "" : t.caption());
            String cap = m.find() ? title(m.group()) : null;
            if (cap == null && !sections.isEmpty()) {
                @SuppressWarnings("unchecked")
                List<Map<String, String>> prev = (List<Map<String, String>>) sections.get(sections.size() - 1).get("parcels");
                prev.addAll(recs);
                continue;
            }
            Map<String, Object> sec = new LinkedHashMap<>();
            sec.put("section", cap);
            sec.put("parcels", recs);
            sections.add(sec);
        }
        if (sections.isEmpty() || total == 0 || ok < 0.8 * total) return null;
        String head = notice.substring(0, Math.min(900, notice.length()));
        Matcher proj = find(RE_PROJECT, head);
        Matcher body = find(RE_BODY, head);
        Matcher fur = find(RE_FURTHER, head);
        List<String> related = new ArrayList<>();
        if (fur != null) {
            Matcher r = RE_REF.matcher(fur.group(1));
            while (r.find()) related.add(r.group(1) + " of " + r.group(2));
        }
        String subtype;
        if (search(RE_CONVERSION, head)) subtype = "Conversion of land reference numbers to new parcel numbers";
        else if (search(RE_CORR, head)) subtype = "Correction of a compulsory acquisition schedule";
        else subtype = "Compulsory acquisition of land";

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("notice_subtype", subtype);
        out.put("project", proj != null ? strip(squash(proj.group(1))) : null);
        out.put("acquiring_body", body != null ? sub(TRAIL_NUM, "", strip(squash(body.group(1)))) : null);
        out.put("related_notices", related);
        out.put("sections", sections);
        out.put("parcel_count", ok);
        out.put("rows_not_read", total - ok);
        return out;
    }
}
