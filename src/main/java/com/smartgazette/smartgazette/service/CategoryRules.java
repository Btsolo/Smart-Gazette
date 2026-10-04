package com.smartgazette.smartgazette.service;

import java.util.regex.Pattern;

/**
 * Heading rules for the keyword pre-filter. Kept apart from GazetteService so
 * they can be tested against the Python categoriser (tools/category_census.py)
 * without starting Spring. Keep the two in sync.
 */
public final class CategoryRules {

    private CategoryRules() {
    }

    // Case-sensitive on purpose: the heading of a county notice is printed in
    // capitals ("THE COUNTY GOVERNMENTS ACT", "COUNTY ASSEMBLY OF KISUMU",
    // "THE KIAMBU COUNTY FINANCE ACT"); national notices only mention "the
    // County Assembly of ..." in their body. Same regex as Python COUNTY_HEAD.
    private static final Pattern COUNTY_HEAD = Pattern.compile(
            "C\\s*O?\\s*UNTY\\s*GOVERNMENTS?\\s*(?:\\(\\s*AMENDMENT\\s*\\)\\s*)?S?\\s*ACT"
            + "|URBAN\\s*AREAS\\s*AND\\s*CITIES"
            + "|COUNTY\\s*ASSEMBLY\\s*(?:OF\\s+[A-Z]|STANDING\\s*ORDERS)"
            + "|COUNTY\\s*GOVERNMENT\\s*OF\\s+[A-Z]"
            + "|\\bTHE\\s+[A-Z'\\-]+(?:\\s+[A-Z'\\-]+)?\\s+COUNTY\\s+[A-Z ,()\\-]*?ACT\\b");
    // probate, elections and physical-planning notices can carry a county
    // heading but are filed by their subject (planning stays Land_Property)
    private static final Pattern NOT_COUNTY_HEAD = Pattern.compile(
            "PROBATE\\s*AND\\s*ADMINISTRATION|ELECTIONS?\\s*ACT|PHYSICAL\\s*AND\\s*LAND\\s*USE\\s*PLANNING",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern LAND_TITLE_HEAD = Pattern.compile(
            "THE\\s*LAND\\s*(?:R\\s*E\\s*G\\s*I\\s*S\\s*T\\s*R\\s*A\\s*T\\s*I\\s*O\\s*N|TITLES?)\\s*ACT",
            Pattern.CASE_INSENSITIVE);

    public static boolean isCountyHeading(String noticeText) {
        String head = noticeText.length() > 400 ? noticeText.substring(0, 400) : noticeText;
        String head300 = noticeText.length() > 300 ? noticeText.substring(0, 300) : noticeText;
        return COUNTY_HEAD.matcher(head).find()
                && !NOT_COUNTY_HEAD.matcher(head).find()
                && !LAND_TITLE_HEAD.matcher(head300).find();
    }

    private static final Pattern PROBATE_HEAD = Pattern.compile(
            "PROBATE\\s*AND\\s*ADMINISTRATION", Pattern.CASE_INSENSITIVE);

    // New categories (fix 6 step 2), read from the UPPERCASE heading only, in
    // priority order. Same regexes as Python category_census.HEADING_CATEGORIES.
    // Body keywords are not used: the IEBC's name and "Registrar of Political
    // Parties" also appear in Treasury exchequer tables.
    private record HeadingRule(String category, Pattern pattern, Pattern unless) {
    }

    private static final java.util.List<HeadingRule> HEADING_CATEGORIES = java.util.List.of(
            new HeadingRule("Elections", Pattern.compile(
                    "ELECTIONS?\\s*ACT|ELECTORAL\\s*AND\\s*BOUNDARIES|INDEPENDENT\\s*ELECTORAL|POLITICAL\\s*PARTIES"
                    + "|ELECTION\\s*PETITION|BY-?\\s*ELECTION|GENERAL\\s*ELECTION|REGISTRATION\\s*OF\\s*VOTERS"
                    + "|ELECTION\\s*OFFENCES|ELECTION\\s*CAMPAIGN"), null),
            new HeadingRule("Uncollected_Goods", Pattern.compile("UNCOLLECTED\\s*GOODS"), null),
            new HeadingRule("Environment", Pattern.compile(
                    "ENVIRONMENTAL\\s*MANAGEMENT|ENVIRONMENTAL\\s*IMPACT"
                    + "|NATIONAL\\s*ENVIRONMENT(?:AL)?\\s*(?:MANAGEMENT|TRIBUNAL|COMPLAINTS)|STRATEGIC\\s*ENVIRONMENTAL"), null),
            // a regulator's board appointment stays Appointments
            new HeadingRule("Utility_Tariffs", Pattern.compile(
                    "TARIFF|WATER\\s*SERVICES\\s*REGULATORY|ENERGY\\s*AND\\s*PETROLEUM\\s*REGULATORY|RETURN\\s*ON\\s*ASSETS"),
                    Pattern.compile("APPOINTMENT")));

    /** Category decided by the heading alone (new categories, then county), or null. */
    public static String headingCategory(String noticeText) {
        String head = noticeText.length() > 400 ? noticeText.substring(0, 400) : noticeText;
        String head300 = noticeText.length() > 300 ? noticeText.substring(0, 300) : noticeText;
        if (LAND_TITLE_HEAD.matcher(head300).find() && !PROBATE_HEAD.matcher(head300).find()) {
            return null;                    // a land title notice is land (fix 4)
        }
        for (HeadingRule r : HEADING_CATEGORIES) {
            if (r.pattern().matcher(head).find() && (r.unless() == null || !r.unless().matcher(head).find())) {
                return r.category();
            }
        }
        return isCountyHeading(noticeText) ? "County_Government" : null;
    }
}
