package com.smartgazette.smartgazette.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Cleans raw pdf-inspector output into text that can be segmented and matched
 * by rules rather than only by an LLM.
 *
 * WHY THIS EXISTS
 * ---------------
 * pdf-inspector gives correct multi-column reading order, but it emits each
 * text run on its own line. Over half of all lines come back 1-4 characters
 * long, because gazette headings are set in small caps and the extractor
 * treats the large first letter as a separate run:
 *
 *     G / AZETTE / N / OTICE / N / O / . / 1216
 *
 * Words are also cut mid-token ("Nairob" + "i"), and the running header
 * ("THE KENYA GAZETTE", the dateline, the page number) lands in the middle of
 * a notice wherever a page break falls.
 *
 * Three stages fix that:
 *   Stage 0  repair mojibake; canonicalise the fragmented markers BEFORE any
 *            joining, so they cannot be glued into surrounding prose
 *   Stage 1  strip running headers and footers, context-aware so that a year
 *            on its own line is not mistaken for a page number
 *   Stage 2  join fragments, deciding glue-vs-space by whether the previous
 *            line's trailing token is already a complete word
 *
 * applyAscendingLock() then decides which candidate headers are real notices.
 *
 * This is a direct port of tools/gazette_clean.py, validated against 20
 * gazettes and 1,945 notices with 100% precision and recall.
 */
@Service
public class GazetteTextCleaner {

    private static final Logger log = LoggerFactory.getLogger(GazetteTextCleaner.class);

    // ---------------------------------------------------------------- stage 0
    //
    // Whitespace is allowed between EVERY letter because the extractor splits
    // small-caps headings unpredictably: "NOTICE" has been seen as "N OTICE",
    // "NOTIC E" and "N OT ICE" in the same document.
    //
    // Deliberately CASE-SENSITIVE. Real headers are set in small caps and come
    // out as all-uppercase fragments. Cross-references inside corrigenda are
    // ordinary mixed case ("IN Gazette Notice No. 5520 of 2026, amend ..."),
    // so requiring uppercase keeps them out of the candidate set entirely.
    // This single detail took precision from 99.29% to 100%.
    // The number is followed by the title on the next line OR on the same line
    // ("NO. 7653 THE PUBLIC HOLIDAYS ACT", 2026 No 90) - but never by
    // "OF <year>": that is an uppercase cross-reference ("NO. 6865 OF 2017").
    // Mirrors tools/gazette_clean.py (lessons 21, 29).
    private static final Pattern P_HEADER = Pattern.compile(
            "G\\s*A\\s*Z\\s*E\\s*T\\s*T?\\s*E\\s*N\\s*O\\s*T\\s*I\\s*C\\s*E\\s*N\\s*O\\s*\\.\\s*"
                    + "([\\d\\s]*\\d)(?!\\s*OF\\s+\\d{4})(?=\\s*\\n\\s*[A-Z]|[ \\t]+[A-Z]{2})");

    private static final Pattern P_CAUSE = Pattern.compile(
            "C\\s*A\\s*U\\s*S\\s*E\\s*N\\s*O\\s*\\.\\s*", Pattern.CASE_INSENSITIVE);

    private static final Pattern P_TAKE_NOTICE =
            Pattern.compile("T\\s*AKE\\s+N\\s*OTICE", Pattern.CASE_INSENSITIVE);
    private static final Pattern P_PROBATE_HEAD = Pattern.compile(
            "P\\s*ROBATE\\s+AND\\s+A\\s*DMINISTRATION", Pattern.CASE_INSENSITIVE);

    // ---------------------------------------------------------------- stage 1
    private static final Pattern P_RUNNING_HEADER =
            Pattern.compile("^\\s*THE KENYA GAZETTE\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern P_DATELINE = Pattern.compile(
            "^\\s*\\d{1,2}(st|nd|rd|th)\\s+\\w+,?\\s+\\d{4}\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern P_BARE_NUMBER = Pattern.compile("^\\s*\\[?(\\d{1,5})\\]?\\s*$");
    // A date line that completes a sentence ("... who died on" / "Dated the")
    // is content, not a running-header dateline (lesson 22).
    private static final Pattern P_CONTENT_BEFORE_DATE = Pattern.compile(
            "(?:\\bon|\\bthe|\\bof|\\bdated|\\bdied|,)\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern P_PRINTER =
            Pattern.compile("PRINTED AND|GOVERNMENT PRINTER", Pattern.CASE_INSENSITIVE);

    // ---------------------------------------------------------------- stage 2
    private static final Pattern P_LIST_ITEM =
            Pattern.compile("^(\\d+\\.|\\([a-z]\\)|\\([ivx]+\\))\\s");
    private static final Pattern P_SENTENCE_END = Pattern.compile("[.;:]$");

    /**
     * Words used to decide glue-vs-space when joining a fragmented line. If the
     * previous line's trailing token is a complete word we insert a space; if it
     * is a cut-off fragment we glue with no space. This distinguishes
     * "Nairob" + "i" (glue) from "shown to" + "the contrary" (space) — a length
     * heuristic cannot, because "to" is short but complete.
     */
    private static final Set<String> COMMON_WORDS = new HashSet<>(Arrays.asList(
            ("a an and are as at be been by for from had has have he her his if in into is it "
           + "its of on or that the their there they this to was were which who will with within would "
           + "court estate grant letters administration intestate testate probate notice cause deceased "
           + "died late who widow son sons daughter children advocates through messrs "
           + "gazette kenya publication days thirty date same issue unless shown contrary appearance "
           + "respect entered proceed application applications having made this order box po "
           + "registrar district deputy senior principal magistrate chief high resident county "
           + "act cap constitution government public service board members chairperson secretary "
           + "notified pursuant provisions section reference period appointment following persons "
           + "amend printed read land title parcel situate proprietor lost replacement objection")
                    .split("\\s+")));

    private static boolean isCompleteWord(String token) {
        String t = token.replaceAll("^[\\.,;:()'\"-]+|[\\.,;:()'\"-]+$", "").toLowerCase();
        return COMMON_WORDS.contains(t) || t.length() > 6;
    }

    /** Full clean: stages 0-2, then the notice-boundary lock. */
    public String clean(String raw) {
        if (raw == null || raw.isBlank()) {
            return "";
        }
        if (isOneCharPerLine(raw)) {
            // Some scans give ~1 character per line: unreadable, and stage 2
            // would never flush its buffer. Empty result = route to OCR (lesson 20).
            log.warn("Text layer is one character per line ({} chars) - needs the scan lane.", raw.length());
            return "";
        }
        String staged = joinFragments(stripRunningHeaders(canonicalise(raw)));
        // fix 2: re-cut broken words with the Gazette dictionary, before the
        // lock - same place as tools/pipeline.py
        VocabRepair repair = repairer();
        if (repair != null) {
            staged = repair.repair(staged);
        }
        return applyAscendingLock(staged);
    }

    private static volatile VocabRepair REPAIR;
    private static volatile boolean repairLoaded;

    /** Loaded once; null (repair skipped) if the dictionary resource is missing. */
    static VocabRepair repairer() {
        if (!repairLoaded) {
            synchronized (GazetteTextCleaner.class) {
                if (!repairLoaded) {
                    REPAIR = VocabRepair.fromClasspath("/vocab/gazette_vocab.txt");
                    repairLoaded = true;
                }
            }
        }
        return REPAIR;
    }

    /** Mirrors gazette_clean.one_char_per_line. */
    static boolean isOneCharPerLine(String text) {
        long lines = 0, chars = 0;
        for (String l : text.split("\n")) {
            String s = l.trim();
            if (!s.isEmpty()) {
                lines++;
                chars += s.length();
            }
        }
        return lines > 20000 && (double) chars / lines < 3;
    }

    // ==================================================================== 0
    private String canonicalise(String input) {
        // A NUL after a header hid the notice (2023 No 26 / 1150, lesson 21).
        String t = input.replace("\u0000", "");

        // Mojibake: UTF-8 punctuation decoded as windows-1252.
        t = t.replace("\u00d4\u00c7\u00d6", "'")
             .replace("\u00d4\u00c7\u00f4", "-")
             .replace("\u00d4\u00c7\u00f6", "\u2014")
             .replace("\u00d4\u00c7\u00a3", "\"")
             .replace("\u00d4\u00c7\u00d8", "\"");

        // Real Unicode punctuation as well: the templates match ASCII quotes and
        // apostrophes ("deceased's", printed as "X" to read "Y") - lesson 25.
        t = t.replace('\u2019', '\'').replace('\u2018', '\'')
             .replace('\u201c', '"').replace('\u201d', '"')
             .replace('\u2013', '-').replace('\u2014', '-');

        // Markers are canonicalised to sentinels first, so the joiner in stage 2
        // cannot glue them into neighbouring prose. The digit group tolerates
        // internal whitespace because notice numbers get split too ("1900" + "3").
        t = P_HEADER.matcher(t).replaceAll(mr ->
                "\n@@HDR@@" + mr.group(1).replaceAll("\\s", "") + "\n");
        t = P_CAUSE.matcher(t).replaceAll("\n@@CAUSE@@ ");
        t = P_TAKE_NOTICE.matcher(t).replaceAll("TAKE NOTICE");
        t = P_PROBATE_HEAD.matcher(t).replaceAll("PROBATE AND ADMINISTRATION");
        return t;
    }

    // ==================================================================== 1
    /**
     * A bare number is only a page number if a running header appeared just
     * before it. Without that context rule, a year split onto its own line
     * ("2025") matches the page-number pattern and is deleted — which silently
     * removed 11 real notices from one gazette during development.
     */
    private String stripRunningHeaders(String input) {
        String[] lines = input.split("\n", -1);
        StringBuilder out = new StringBuilder(input.length());
        int sinceHeader = 99;

        // Line indices within 3 lines of a THE KENYA GAZETTE running header.
        Set<Integer> nearGazette = new HashSet<>();
        for (int i = 0; i < lines.length; i++) {
            if (P_RUNNING_HEADER.matcher(lines[i].trim()).matches()) {
                for (int k = i - 3; k <= i + 3; k++) nearGazette.add(k);
            }
        }
        String prev = "";

        for (int i = 0; i < lines.length; i++) {
            String line = lines[i];
            String s = line.trim();
            if (s.isEmpty()) {
                out.append(line).append('\n');
                sinceHeader++;
                continue;
            }
            if (P_RUNNING_HEADER.matcher(s).matches()) {
                // page break marker: lets Stage 2 recognise a table header
                // repeated at the top of the next page (fix 7)
                sinceHeader = 0;
                out.append("@@PAGE@@\n");
                continue;
            }
            // A date line is a dateline only next to THE KENYA GAZETTE and not
            // when it completes a sentence; otherwise it is a date of death or
            // a signature date (lesson 22: ~480 a year were being deleted).
            if (P_DATELINE.matcher(s).matches() && nearGazette.contains(i)
                    && !P_CONTENT_BEFORE_DATE.matcher(prev).find()) {
                sinceHeader = 0;
                continue;
            }
            prev = s;
            if (P_PRINTER.matcher(s).find()) {
                continue;
            }
            Matcher num = P_BARE_NUMBER.matcher(s);
            if (num.matches()) {
                int n = Integer.parseInt(num.group(1));
                boolean yearLike = n >= 1900 && n <= 2100;
                if (sinceHeader <= 6 && !yearLike) {
                    continue;                       // page number after a header
                }
                if (sinceHeader <= 6 && yearLike && n > 2030) {
                    continue;                       // implausible year => page number
                }
            }
            sinceHeader++;
            out.append(line).append('\n');
        }
        return out.toString();
    }

    // ==================================================================== 2
    private String joinFragments(String input) {
        List<String> out = new ArrayList<>();
        StringBuilder buf = new StringBuilder();
        boolean pageTop = false;
        Set<Integer> pageFirst = new HashSet<>();      // first table row on a page

        for (String line : input.split("\n", -1)) {
            String s = line.trim();
            if (s.isEmpty()) {
                continue;
            }
            if (s.equals("@@PAGE@@")) {
                pageTop = true;
                continue;
            }
            if (s.startsWith("@@HDR@@")) {
                pageTop = false;
                flush(buf, out);
                out.add("GAZETTE NOTICE NO. " + s.substring("@@HDR@@".length()).trim());
                continue;
            }
            if (s.startsWith("@@CAUSE@@")) {
                flush(buf, out);
                buf.append("CAUSE NO. ").append(s.substring("@@CAUSE@@".length()).trim());
                continue;
            }
            // A table row (fix 7: cells marked " | " by inspect_positions.js)
            // stands on its own line; joined into a paragraph its row boundary
            // is lost. Before the list-item rule: rows start "1. | 233426 | ...".
            // (a row whose last cell is empty ends " |" once trailing spaces go, fix 7b)
            if (s.contains(" | ") || s.endsWith(" |")) {
                flush(buf, out);
                if (pageTop) {
                    pageFirst.add(out.size());
                    pageTop = false;
                }
                out.add(s);
                continue;
            }
            // top-of-page text other than a short caption: no continued table
            if (pageTop && s.split("\\s+").length > 6) {
                pageTop = false;
            }
            if (P_LIST_ITEM.matcher(s).find()) {
                flush(buf, out);
                buf.append(s);
                continue;
            }
            if (buf.length() == 0) {
                buf.append(s);
            } else {
                char prev = buf.charAt(buf.length() - 1);
                char next = s.charAt(0);
                // Last word only. Splitting the whole buffer on every line was
                // quadratic and hung for hours on unpunctuated text (lesson 20).
                int end = buf.length();
                while (end > 0 && Character.isWhitespace(buf.charAt(end - 1))) end--;
                int startTok = end;
                while (startTok > 0 && !Character.isWhitespace(buf.charAt(startTok - 1))) startTok--;
                String lastTok = buf.substring(startTok, end);

                if (".,;:)]".indexOf(next) >= 0 || "([".indexOf(prev) >= 0) {
                    buf.append(s);
                } else if (Character.isLetter(prev) && Character.isLetter(next)
                        && !isCompleteWord(lastTok)) {
                    buf.append(s);                  // previous token is a fragment
                } else {
                    buf.append(' ').append(s);
                }
            }
            if (s.length() > 2 && P_SENTENCE_END.matcher(s).find()) {
                flush(buf, out);
            }
        }
        flush(buf, out);
        return String.join("\n", dropRepeatedTableHeaders(out, pageFirst)).replaceAll("\n{3,}", "\n\n");
    }

    private static final Pattern P_DIGIT = Pattern.compile("\\d");
    // the separator line inspect_positions.js writes under a table header row
    private static final Pattern P_DASH_ROW = Pattern.compile("^-{3}(?: \\| -{3})+$");

    /**
     * Mirrors gazette_clean.drop_repeated_table_headers: a table continued on
     * the next page repeats its header row as the FIRST row of the page; that
     * row (no digits, identical to an earlier cell row of the same notice) is
     * dropped. Only page-first rows: data rows without digits repeat
     * legitimately ("Ward Administrator | Ex-Officio Member").
     */
    static List<String> dropRepeatedTableHeaders(List<String> lines, Set<Integer> pageFirst) {
        Set<String> seen = new HashSet<>();
        List<String> out = new ArrayList<>(lines.size());
        boolean dropped = false;
        for (int i = 0; i < lines.size(); i++) {
            String l = lines.get(i);
            if (dropped && P_DASH_ROW.matcher(l).matches()) {   // its "--- | ---" separator (fix 8)
                dropped = false;
                continue;
            }
            dropped = false;
            if (l.startsWith("GAZETTE NOTICE NO.")) {
                seen = new HashSet<>();
            } else if (l.contains(" | ") && !P_DIGIT.matcher(l).find() && !P_DASH_ROW.matcher(l).matches()) {
                String key = l.replaceAll("\\s+", " ").trim().toLowerCase(Locale.ROOT);
                if (seen.contains(key) && pageFirst.contains(i)) {
                    dropped = true;
                    continue;
                }
                seen.add(key);
            }
            out.add(l);
        }
        return out;
    }

    private void flush(StringBuilder buf, List<String> out) {
        String s = buf.toString().replaceAll("\\s{2,}", " ").trim();
        if (!s.isEmpty()) {
            out.add(s);
        }
        buf.setLength(0);
    }

    // ============================================================ boundary lock
    private static final Pattern P_CANDIDATE =
            Pattern.compile("^GAZETTE NOTICE NO\\. (\\d+)\\s*(.*)$");

    /**
     * Gazette notice numbers ascend monotonically through an issue, but the text
     * also contains cross-references to earlier notices ("IN Gazette Notice No.
     * 14410 of 2023, amend ..."). Those look identical to real headers.
     *
     * A simple "must exceed the previous" rule fails when a cross-reference
     * appears BEFORE the first real notice, as in issues that open with a
     * CORRIGENDA section: the high reference number becomes the baseline and
     * every genuine header after it is rejected. During development this
     * silently discarded about 900 notices across the test corpus.
     *
     * So we keep the largest strictly-ascending subset of candidates instead —
     * a longest increasing subsequence in document order. Real headers form one
     * long ascending run; cross-references are isolated outliers that fall
     * outside it. Each candidate is also considered in a repaired form, for
     * headers whose trailing digits were split onto the following line
     * ("NO. 1900" then "3 HIGH COURT" is really notice 19003).
     */
    public String applyAscendingLock(String cleaned) {
        String[] lines = cleaned.split("\n", -1);

        List<int[]> flat = new ArrayList<>();       // {lineIndex, value, absorbNext}
        List<String> tails = new ArrayList<>();
        for (int i = 0; i < lines.length; i++) {
            Matcher m = P_CANDIDATE.matcher(lines[i]);
            if (!m.matches()) {
                continue;
            }
            String numStr = m.group(1);
            String rest = m.group(2);
            flat.add(new int[]{i, Integer.parseInt(numStr), 0});
            tails.add(rest);

            String nextText = !rest.isEmpty() ? rest
                    : (i + 1 < lines.length ? lines[i + 1] : "");
            Matcher dm = Pattern.compile("^(\\d+)\\s+(.*)$").matcher(nextText.trim());
            if (dm.matches()) {
                flat.add(new int[]{i, Integer.parseInt(numStr + dm.group(1)),
                        rest.isEmpty() ? 1 : 0});
                tails.add(dm.group(2));
            }
        }
        if (flat.isEmpty()) {
            return cleaned;
        }

        // O(n^2) longest strictly-increasing subsequence over (candidate, variant).
        int n = flat.size();
        int[] best = new int[n];
        int[] prev = new int[n];
        int endK = 0;
        for (int k = 0; k < n; k++) {
            best[k] = 1;
            prev[k] = -1;
            for (int j = 0; j < k; j++) {
                if (flat.get(j)[0] < flat.get(k)[0]
                        && flat.get(j)[1] < flat.get(k)[1]
                        && best[j] + 1 > best[k]) {
                    best[k] = best[j] + 1;
                    prev[k] = j;
                }
            }
            if (best[k] > best[endK]) {
                endK = k;
            }
        }

        java.util.Map<Integer, int[]> keep = new java.util.HashMap<>();
        java.util.Map<Integer, String> keepTail = new java.util.HashMap<>();
        for (int k = endK; k != -1; k = prev[k]) {
            keep.put(flat.get(k)[0], flat.get(k));
            keepTail.put(flat.get(k)[0], tails.get(k));
        }

        StringBuilder out = new StringBuilder(cleaned.length());
        Set<Integer> skip = new HashSet<>();
        int demoted = 0;
        for (int i = 0; i < lines.length; i++) {
            if (skip.contains(i)) {
                continue;
            }
            Matcher m = P_CANDIDATE.matcher(lines[i]);
            if (!m.matches()) {
                out.append(lines[i]).append('\n');
                continue;
            }
            int[] kept = keep.get(i);
            if (kept != null) {
                out.append("GAZETTE NOTICE NO. ").append(kept[1]).append('\n');
                String tail = keepTail.get(i);
                if (kept[2] == 1) {
                    skip.add(i + 1);
                }
                if (tail != null && !tail.isBlank()) {
                    out.append(tail).append('\n');
                }
            } else {
                // Cross-reference: demoted to body text in mixed case so it can
                // never start a notice, while the information is preserved.
                out.append("Gazette Notice No. ").append(m.group(1))
                   .append(m.group(2).isBlank() ? "" : " " + m.group(2)).append('\n');
                demoted++;
            }
        }
        if (demoted > 0) {
            log.info("Notice boundary lock: kept {} headers, demoted {} cross-references.",
                    keep.size(), demoted);
        }
        return out.toString();
    }
}
