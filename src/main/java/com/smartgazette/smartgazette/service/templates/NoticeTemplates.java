package com.smartgazette.smartgazette.service.templates;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The rule-based templates, by category - mirrors category_census.template_for
 * in tools/. A result replaces the AI extraction call for that notice; null
 * means "no template could read it" and the notice takes the AI path as before.
 *
 *   Court_Legal    every CAUSE NO. block must be read (all or nothing): a list
 *                  of one record per cause
 *   Land_Property  lost-title template, then the parcel-table template
 *   Corrigenda     probate correction (CAUSE NO.), then any correction naming
 *                  its notice, then an acquisition-schedule correction table
 *   Change_of_Name deed polls
 *   Appointments   a list of one record per appointed person
 *
 * Measured on 2022-2026 born-digital text: probate ~96% of cause blocks,
 * land ~93% of notices, corrigenda ~28%, deed polls 98%, appointments 77.5%.
 */
public final class NoticeTemplates {

    private NoticeTemplates() {
    }

    public static Object extract(String category, String notice) {
        if (category == null || notice == null) return null;
        switch (category) {
            case "Court_Legal": {
                List<String> blocks = ProbateTemplate.splitCauses(notice);
                if (blocks.isEmpty()) return null;
                List<Map<String, Object>> recs = new ArrayList<>();
                for (String b : blocks) {
                    Map<String, Object> r = ProbateTemplate.extract(b, notice);
                    if (r == null) return null;
                    recs.add(r);
                }
                return recs;
            }
            case "Land_Property": {
                Map<String, Object> r = LandTemplate.extract(notice);
                return r != null ? r : LandTableTemplate.extract(notice);
            }
            case "Change_of_Name":
                return ChangeOfNameTemplate.extract(notice);
            case "Appointments":
                return AppointmentsTemplate.extract(notice);   // one record per person
            case "Corrigenda": {
                List<String> blocks = new ArrayList<>();
                for (String b : notice.split("(?=CAUSE NO\\.)", -1)) if (b.startsWith("CAUSE NO.")) blocks.add(b);
                Map<String, Object> r = blocks.isEmpty() ? null : CorrigendaTemplate.extract(blocks.get(0), notice);
                if (r == null) r = CorrigendaTemplate.extractNotice(notice);
                if (r == null) r = LandTableTemplate.extract(notice);
                return r;
            }
            default:
                return null;
        }
    }
}
