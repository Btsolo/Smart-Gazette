package com.smartgazette.smartgazette.service;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Tables of a notice as data (fix 8) - a rule-for-rule port of
 * tools/table_extract.py (keep in sync).
 *
 * WHY: the table lane (inspect_positions.js + the cleaner) keeps every table
 * row on its own line with " | " between cells and a "--- | ---" line under
 * a header row. This turns those lines into {columns, rows} so the notice
 * page can show a real table instead of a run of numbers - a land
 * acquisition schedule as parcel / owner / area, a committee as name /
 * position.
 *
 * Built at view time from the stored notice text; nothing is persisted.
 */
public final class TableExtractor {

    /** One table; columns is null when no header was recognised; caption is the
     *  short label line above it ("Deletion", "Addendum"), or null. */
    public record Table(List<String> columns, List<List<String>> rows, String caption) {
        public int width() {
            return columns != null ? columns.size() : (rows.isEmpty() ? 0 : rows.get(0).size());
        }
    }

    private static final int F = Pattern.UNICODE_CHARACTER_CLASS | Pattern.UNIX_LINES;
    // cell separator: a bar with whitespace (or the line start) before it and
    // whitespace (or the line end) after it - so empty cells survive the cleaner's
    // squashing: "a | | c", "text | |" (fix 7b); "a|b" inside text is not split
    // a line holding only figure markers (docs/specs/figures.md)
    private static final Pattern FIG_LINE = Pattern.compile("^(?:\\[\\[FIGURE:\\d+\\.\\d+\\]\\]\\s*)+$", F);
    private static final Pattern CELL_SEP = Pattern.compile("(?:^|(?<=\\s))\\|(?=\\s|$)", F);
    private static final Pattern DASH_ROW = Pattern.compile("^-{3}(?: \\| -{3})+$", F);
    /** Words that open a new section of a schedule (acquisition corrigenda). */
    public static final Pattern SECTION = Pattern.compile(
            "\\b(?:deletion|addendum|corrigend\\w*|amendment|schedule|part|annex|appendix)\\b",
            F | Pattern.CASE_INSENSITIVE);
    private static final Pattern CONTD = Pattern.compile("cont(?:d|inued)", F | Pattern.CASE_INSENSITIVE);
    private static final Pattern SENT_END = Pattern.compile("[.;:]\\s*$", F);
    private static final Pattern DIGIT = Pattern.compile("\\d", F);
    private static final Pattern WORD = Pattern.compile("[a-z]+");
    private static final Set<String> STOP = Set.of("of", "in", "and", "the", "to", "for", "a", "by", "or", "on", "at");
    private static volatile Set<String> headerWords;

    private TableExtractor() {
    }

    /** Words seen in >= 3 typeface-marked headers (vocab/table_header_words.txt). */
    static Set<String> headerWords() {
        if (headerWords == null) {
            Set<String> w = new HashSet<>();
            try (InputStream in = TableExtractor.class.getResourceAsStream("/vocab/table_header_words.txt")) {
                if (in != null) {
                    BufferedReader r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
                    String l;
                    while ((l = r.readLine()) != null) {
                        l = l.trim();
                        if (!l.isEmpty() && !l.startsWith("#")) w.add(l);
                    }
                }
            } catch (Exception ignored) {
                // no header vocabulary: only typeface-marked headers are used
            }
            headerWords = w;
        }
        return headerWords;
    }

    private static String caption(List<String> lines) {
        String t = String.join(" ", lines).strip();
        return !t.isEmpty() && t.split("\\s+").length <= 12 ? t : null;
    }

    /** Mutable state of the table being read (mirrors the Python locals). */
    private static final class Open {
        List<List<String>> cur;            // rows of the open table, null = none
        List<List<String>> pre = new ArrayList<>();   // short lines seen just before each row
        List<String> header;
        String caption;
    }

    public static List<Table> tables(String notice) {
        List<Table> out = new ArrayList<>();
        Open o = new Open();
        List<String> pending = new ArrayList<>();
        List<String> lastText = new ArrayList<>();

        for (String line : notice.split("\n", -1)) {
            String s = line.strip();
            if (FIG_LINE.matcher(s).matches()) continue;   // a figure marker between rows is not a wrapped cell
            if (DASH_ROW.matcher(s).matches()) {
                // the row above is the header; rows before it in the same run
                // are upper header levels (no digits) or a previous table
                if (o.cur != null && !o.cur.isEmpty()) {
                    List<String> h = o.cur.remove(o.cur.size() - 1);
                    List<String> hpre = o.pre.remove(o.pre.size() - 1);
                    String cap = caption(hpre);
                    if (cap == null) cap = o.caption;
                    boolean data = false;
                    for (List<String> r : o.cur) if (DIGIT.matcher(String.join(" ", r)).find()) data = true;
                    if (data) close(out, o);
                    o.cur = new ArrayList<>();
                    o.pre = new ArrayList<>();
                    o.header = h;
                    o.caption = cap;
                    pending = new ArrayList<>();
                }
                continue;
            }
            if (s.contains(" | ") || s.endsWith(" |")) {        // last cell empty: "text |" (fix 7b)
                List<String> cells = new ArrayList<>();
                // an empty cell arrives as "a | | c" (the cleaner squashes "a |  | c")
                for (String c : CELL_SEP.split(s, -1)) cells.add(c.strip());
                if (o.cur == null) {
                    o.cur = new ArrayList<>();
                    o.pre = new ArrayList<>();
                    o.caption = caption(lastText);
                }
                o.cur.add(cells);
                o.pre.add(pending);
                pending = new ArrayList<>();
            } else if (o.cur != null && !s.isEmpty() && s.split("\\s+").length <= 8 && !SENT_END.matcher(s).find()
                    && !s.startsWith("GAZETTE NOTICE NO.") && pending.size() < 3) {
                pending.add(s);
            } else {
                close(out, o);
                pending = new ArrayList<>();
                if (!s.isEmpty()) {
                    lastText = new ArrayList<>(List.of(s));
                }
            }
        }
        close(out, o);

        // a headerless piece directly after a table of the same width is that
        // table continued (page break, "(Contd.)", long wrapped cell) - unless
        // it carries its own section caption ("Addendum" after "Deletion")
        List<Table> merged = new ArrayList<>();
        for (Table t : out) {
            Table prev = merged.isEmpty() ? null : merged.get(merged.size() - 1);
            boolean own = t.caption() != null && SECTION.matcher(t.caption()).find() && !CONTD.matcher(t.caption()).find();
            int prevWidth = prev == null ? -1
                    : (!prev.rows().isEmpty() ? prev.rows().get(0).size() : (prev.columns() != null ? prev.columns().size() : 0));
            if (prev != null && t.columns() == null && !own && !t.rows().isEmpty() && t.rows().get(0).size() == prevWidth) {
                prev.rows().addAll(t.rows());
            } else {
                merged.add(t);
            }
        }
        return merged;
    }

    private static void close(List<Table> out, Open o) {
        List<List<String>> cur = o.cur;
        if (cur != null && !cur.isEmpty() && (cur.size() >= 2 || (o.header != null && !o.header.isEmpty()))) {
            for (int i = 1; i < cur.size(); i++) {
                List<String> p = o.pre.get(i);
                if (!p.isEmpty()) {                  // wrapped text of the row above
                    List<String> last = cur.get(i - 1);
                    int tgt = 0;
                    for (int k = 1; k < last.size(); k++) if (letters(last.get(k)) > letters(last.get(tgt))) tgt = k;
                    last.set(tgt, (last.get(tgt) + " " + String.join(" ", p)).strip());
                }
            }
            out.add(shape(cur, o.header, o.caption));
        }
        o.cur = null;
        o.pre = new ArrayList<>();
        o.header = null;
        o.caption = null;
    }

    static boolean looksLikeHeader(List<String> row) {
        String all = String.join(" ", row);
        if (DIGIT.matcher(all).find()) return false;
        for (String c : row) if (!c.isBlank() && c.strip().split("\\s+").length > 5) return false;
        List<String> words = new ArrayList<>();
        Matcher m = WORD.matcher(all.toLowerCase(Locale.ROOT));
        while (m.find()) if (!STOP.contains(m.group())) words.add(m.group());
        if (words.size() < 2) return false;
        int hit = 0;
        for (String w : words) if (headerWords().contains(w)) hit++;
        return hit >= 0.7 * words.size();
    }

    private static Table shape(List<List<String>> rows, List<String> cols, String caption) {
        List<List<String>> rs = new ArrayList<>(rows);
        if (rs.isEmpty()) rs.add(new ArrayList<>());
        if (cols == null && rs.size() > 1 && looksLikeHeader(rs.get(0))) {
            cols = rs.remove(0);
        }
        int width;
        if (cols != null) {
            width = cols.size();
        } else {
            Map<Integer, Integer> count = new LinkedHashMap<>();
            for (List<String> r : rs) count.merge(r.size(), 1, Integer::sum);
            width = -1;
            int best = -1;
            for (Map.Entry<Integer, Integer> e : count.entrySet()) {
                if (e.getValue() > best) { best = e.getValue(); width = e.getKey(); }
            }
        }
        // two records side by side on one line (the page's two columns read as
        // one row): exactly twice the header width -> two rows
        List<List<String>> split = new ArrayList<>();
        for (List<String> r : rs) {
            if (cols != null && !cols.isEmpty() && width >= 2 && r.size() == 2 * width) {
                split.add(new ArrayList<>(r.subList(0, width)));
                split.add(new ArrayList<>(r.subList(width, r.size())));
            } else {
                split.add(r);
            }
        }
        rs = split;
        List<List<String>> shaped = new ArrayList<>();
        for (List<String> r : rs) {
            List<String> x = new ArrayList<>(r);
            if (x.size() < width) {
                while (x.size() < width) x.add("");
            } else if (x.size() > width) {
                List<String> head = new ArrayList<>(x.subList(0, Math.max(0, width - 1)));
                head.add(String.join(" ", x.subList(Math.max(0, width - 1), x.size())));
                x = head;
            }
            shaped.add(x);
        }
        return new Table(cols, shaped, caption);
    }

    private static int letters(String s) {
        int n = 0;
        for (int i = 0; i < s.length(); i++) if (Character.isLetter(s.charAt(i))) n++;
        return n;
    }

    @SuppressWarnings("unused")
    private static List<String> list(String... s) {
        return new ArrayList<>(Arrays.asList(s));
    }
}
