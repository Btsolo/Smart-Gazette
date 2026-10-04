package com.smartgazette.smartgazette.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Tables in scanned gazettes - a port of tools/scan_tables.py (keep in sync).
 * Spec: docs/specs/scan-tables.md.
 *
 * A scanned page is read as Tesseract's word boxes (TSV). Without tables the
 * page text is exactly Tesseract's own text. Ruled tables get rows and cells
 * from the ruling lines found in the page image; unruled tables get a cell per
 * group of words between wide gaps; elsewhere a standalone "|" never becomes a
 * cell marker.
 */
public final class ScanTableReader {

    private ScanTableReader() {
    }

    /** One Tesseract word: its line (block, paragraph, line), box and text. */
    public static final class Word {
        final int block, par, line;
        final int x, y, w, h;
        final String text;

        Word(int block, int par, int line, int x, int y, int w, int h, String text) {
            this.block = block; this.par = par; this.line = line;
            this.x = x; this.y = y; this.w = w; this.h = h; this.text = text;
        }

        Word with(String text, int x, int w) {
            return new Word(block, par, line, x, y, w, h, text);
        }

        String key() {
            return block + "/" + par + "/" + line;
        }
    }

    /** Ruling lines in word-box coordinates: h = {y, x0, x1}, v = {x, y0, y1}. */
    public record Rules(List<double[]> h, List<double[]> v, double width, double height) {
    }

    private record Lattice(List<Double> xs, List<Double> ys, List<double[]> walls) {
    }

    private static final String RULE_CHARS = "|[]{}_";

    // ------------------------------------------------------------ word boxes

    /** Tesseract TSV text -> words in reading order. */
    public static List<Word> readWords(String tsv) {
        List<Word> out = new ArrayList<>();
        String[] lines = tsv.split("\n", -1);
        for (int i = 1; i < lines.length; i++) {
            String[] c = lines[i].replace("\r", "").split("\t", -1);
            if (c.length < 12 || !c[0].equals("5") || c[11].strip().isEmpty()) continue;
            out.add(new Word(Integer.parseInt(c[2]), Integer.parseInt(c[3]), Integer.parseInt(c[4]),
                    Integer.parseInt(c[6]), Integer.parseInt(c[7]), Integer.parseInt(c[8]), Integer.parseInt(c[9]), c[11]));
        }
        return out;
    }

    private static int cmpKey(Word a, Word b) {
        int c = Integer.compare(a.block, b.block);
        if (c != 0) return c;
        c = Integer.compare(a.par, b.par);
        return c != 0 ? c : Integer.compare(a.line, b.line);
    }

    /** lines in reading order (key -> words) */
    private static List<List<Word>> linesOf(List<Word> words) {
        Map<String, List<Word>> d = new LinkedHashMap<>();
        for (Word w : words) d.computeIfAbsent(w.key(), k -> new ArrayList<>()).add(w);
        return new ArrayList<>(d.values());
    }

    // ------------------------------------------------------------ ruling lines

    /** runs of true of length >= minLen: {start, end} */
    private static List<int[]> runs(boolean[] row, int minLen) {
        List<int[]> out = new ArrayList<>();
        int s = -1;
        for (int i = 0; i <= row.length; i++) {
            boolean v = i < row.length && row[i];
            if (v && s < 0) s = i;
            else if (!v && s >= 0) {
                if (i - s >= minLen) out.add(new int[]{s, i});
                s = -1;
            }
        }
        return out;
    }

    /** segments {c, a0, a1} -> lines {mean c, a0, a1} (see scan_tables._join) */
    private static List<double[]> join(List<int[]> segs, int alongTol, int acrossTol) {
        segs.sort(Comparator.<int[]>comparingInt(s -> s[0]).thenComparingInt(s -> s[1]).thenComparingInt(s -> s[2]));
        List<long[]> lines = new ArrayList<>();          // {a0, a1, sumC, n, cEnd}
        for (int[] s : segs) {
            long[] hit = null;
            for (long[] l : lines) {
                if (Math.abs(l[4] - s[0]) <= acrossTol && s[1] <= l[1] + alongTol && s[2] >= l[0] - alongTol) {
                    hit = l;
                    break;
                }
            }
            if (hit != null) {
                hit[0] = Math.min(hit[0], s[1]);
                hit[1] = Math.max(hit[1], s[2]);
                hit[2] += s[0];
                hit[3] += 1;
                hit[4] = s[0];
            } else {
                lines.add(new long[]{s[1], s[2], s[0], 1, s[0]});
            }
        }
        List<double[]> out = new ArrayList<>();
        for (long[] l : lines) out.add(new double[]{(double) l[2] / l[3], l[0], l[1]});
        return out;
    }

    /**
     * Ruling lines of a grey page image (gray[y][x], 0-255) at the given dpi,
     * in word-box coordinates (pixel * scale). Pixel sizes are given at 150
     * dpi and scale with dpi.
     */
    public static Rules imageRules(int[][] gray, double scale, int dpi) {
        int k = dpi / 150;
        int H = gray.length, W = gray[0].length;
        boolean[][] dark = new boolean[H][W];
        for (int y = 0; y < H; y++) for (int x = 0; x < W; x++) dark[y][x] = gray[y][x] < 185;
        List<int[]> hs = new ArrayList<>(), vs = new ArrayList<>();
        for (int y = 0; y < H; y++) for (int[] r : runs(dark[y], 25 * k)) hs.add(new int[]{y, r[0], r[1]});
        boolean[] col = new boolean[H];
        for (int x = 0; x < W; x++) {
            for (int y = 0; y < H; y++) col[y] = dark[y][x];
            for (int[] r : runs(col, 25 * k)) vs.add(new int[]{x, r[0], r[1]});
        }
        List<double[]> h = new ArrayList<>(), v = new ArrayList<>();
        for (double[] l : join(hs, 12 * k, 2 * k)) if (l[2] - l[1] >= 0.06 * W) h.add(new double[]{l[0] * scale, l[1] * scale, l[2] * scale});
        for (double[] l : join(vs, 12 * k, 2 * k)) if (l[2] - l[1] >= 0.03 * H) v.add(new double[]{l[0] * scale, l[1] * scale, l[2] * scale});
        return new Rules(h, v, W * scale, H * scale);
    }

    private static List<Double> uniq(List<Double> vals, double tol) {
        List<Double> s = new ArrayList<>(vals);
        Collections.sort(s);
        List<Double> out = new ArrayList<>();
        for (double v : s) if (out.isEmpty() || v - out.get(out.size() - 1) > tol) out.add(v);
        return out;
    }

    private static List<Lattice> lattices(Rules R) {
        double W = R.width(), H = R.height();
        List<double[]> walls = new ArrayList<>();
        for (double[] v : R.v()) {
            boolean crossed = false;
            for (double[] h : R.h()) {
                if (v[1] + 4 < h[0] && h[0] < v[2] - 4 && h[1] < v[0] - 4 && h[2] > v[0] + 4) { crossed = true; break; }
            }
            boolean gutter = v[2] - v[1] >= 0.4 * H && Math.abs(v[0] - W / 2) <= 0.04 * W && !crossed;
            if (!gutter) walls.add(v);
        }
        walls.sort(Comparator.comparingDouble(v -> v[1]));
        List<double[]> gBounds = new ArrayList<>();
        List<List<double[]>> gWalls = new ArrayList<>();
        for (double[] w : walls) {
            int hit = -1;
            for (int i = 0; i < gBounds.size(); i++) {
                double[] g = gBounds.get(i);
                if (w[1] < g[1] - 8 && w[2] > g[0] + 8) { hit = i; break; }
            }
            if (hit >= 0) {
                gWalls.get(hit).add(w);
                double[] g = gBounds.get(hit);
                g[0] = Math.min(g[0], w[1]);
                g[1] = Math.max(g[1], w[2]);
            } else {
                gBounds.add(new double[]{w[1], w[2]});
                gWalls.add(new ArrayList<>(List.of(w)));
            }
        }
        List<Lattice> out = new ArrayList<>();
        for (int i = 0; i < gBounds.size(); i++) {
            double[] g = gBounds.get(i);
            List<Double> xsIn = new ArrayList<>();
            for (double[] w : gWalls.get(i)) xsIn.add(w[0]);
            List<Double> xs = uniq(xsIn, 8);
            if (xs.size() < 3) continue;
            List<Double> ysIn = new ArrayList<>();
            for (double[] h : R.h()) {
                if (g[0] - 6 <= h[0] && h[0] <= g[1] + 6 && h[1] <= xs.get(1) + 4 && h[2] >= xs.get(xs.size() - 2) - 4) ysIn.add(h[0]);
            }
            List<Double> ys = uniq(ysIn, 6);
            if (ys.size() < 3) continue;
            out.add(new Lattice(xs, ys, gWalls.get(i)));
        }
        return out;
    }

    // ---------------------------------------------------------------- tables

    private static boolean onlyRuleChars(String t) {
        for (int i = 0; i < t.length(); i++) if (RULE_CHARS.indexOf(t.charAt(i)) < 0) return false;
        return true;
    }

    private static String rstrip(String t, String chars) {
        int e = t.length();
        while (e > 0 && chars.indexOf(t.charAt(e - 1)) >= 0) e--;
        return t.substring(0, e);
    }

    private static String lstrip(String t, String chars) {
        int s = 0;
        while (s < t.length() && chars.indexOf(t.charAt(s)) >= 0) s++;
        return t.substring(s);
    }

    private static String cleanCellWord(Word w, List<Double> wallsX) {
        String t = w.text;
        if (onlyRuleChars(t)) return "";
        double tol = 1.5 * Math.max(w.h, 10);
        if ("|]}".indexOf(t.charAt(t.length() - 1)) >= 0 && wallsX.stream().anyMatch(x -> Math.abs(w.x + w.w - x) <= tol)) {
            t = rstrip(t, "|]}_");
        }
        if (!t.isEmpty() && "|[{_".indexOf(t.charAt(0)) >= 0 && wallsX.stream().anyMatch(x -> Math.abs(w.x - x) <= tol)) {
            t = lstrip(t, "|[{_");
        }
        return t;
    }

    private static List<Word> splitAtBar(Word w, List<Double> xs) {
        String t = w.text;
        int i = t.indexOf('|');
        if (i <= 0 || i >= t.length() - 1 || xs.stream().noneMatch(x -> w.x + 4 < x && x < w.x + w.w - 4)) return List.of(w);
        String a = rstrip(t.substring(0, i), "_"), b = lstrip(t.substring(i + 1), "_");
        if (a.isEmpty() || b.isEmpty()) return List.of(w);
        double cut = w.x + (double) w.w * i / t.length();
        return List.of(w.with(a, w.x, Math.max(1, (int) (cut - w.x))), w.with(b, (int) cut, Math.max(1, (int) (w.x + w.w - cut))));
    }

    private record RuledResult(List<List<String>> rows, List<Word> inside) {
    }

    private static int countLe(List<Double> vals, double v) {
        int n = 0;
        for (double x : vals) if (x <= v) n++;
        return n;
    }

    private static RuledResult ruledRows(Lattice L, List<Word> words, Rules R) {
        List<Double> xs = L.xs(), ys = L.ys();
        List<double[]> walls = L.walls();
        List<Word> inside = new ArrayList<>();
        for (Word w : words) {
            double cx = w.x + w.w / 2.0, cy = w.y + w.h / 2.0;
            if (xs.get(0) <= cx && cx <= xs.get(xs.size() - 1) && ys.get(0) <= cy && cy <= ys.get(ys.size() - 1)) inside.add(w);
        }
        if (inside.size() < 6) return null;
        List<Word> pieces = new ArrayList<>();
        for (Word w : inside) pieces.addAll(splitAtBar(w, xs));
        int nc = xs.size() - 1;
        List<Double> allIn = new ArrayList<>(ys);
        for (double[] h : R.h()) {
            if (ys.get(0) - 3 <= h[0] && h[0] <= ys.get(ys.size() - 1) + 3) {
                for (int c = 0; c < nc; c++) {
                    if (h[1] <= xs.get(c) + 6 && h[2] >= xs.get(c + 1) - 6) { allIn.add(h[0]); break; }
                }
            }
        }
        List<Double> allY = uniq(allIn, 6);
        TreeMap<Integer, List<List<Word>>> grid = new TreeMap<>();
        for (Word w : pieces) {
            double cx = w.x + w.w / 2.0, cy = w.y + w.h / 2.0;
            int k = -1;
            for (int i = 0; i < nc; i++) if (xs.get(i) <= cx && cx < xs.get(i + 1)) { k = i; break; }
            if (k < 0) k = cx < xs.get(0) ? 0 : nc - 1;
            int r = Math.min(Math.max(0, countLe(allY, cy) - 1), allY.size() - 2);
            grid.computeIfAbsent(r, x -> { List<List<Word>> l = new ArrayList<>(); for (int i = 0; i < nc; i++) l.add(new ArrayList<>()); return l; })
                    .get(k).add(w);
        }
        List<Double> wallsX = new ArrayList<>();
        for (double[] v : walls) wallsX.add(v[0]);
        int key = 0;
        if (!grid.isEmpty()) {
            int[] filled = new int[nc];
            for (List<List<Word>> byCol : grid.values()) for (int c = 0; c < nc; c++) if (!byCol.get(c).isEmpty()) filled[c]++;
            int mx = Arrays.stream(filled).max().orElse(0);
            for (int c = 0; c < nc; c++) if (filled[c] >= 0.5 * mx) { key = c; break; }
        }
        List<Integer> bandR = new ArrayList<>();
        List<List<List<Word>>> bandCols = new ArrayList<>();
        for (Map.Entry<Integer, List<List<Word>>> e : grid.entrySet()) {
            List<List<Word>> byCol = e.getValue();
            Map<String, List<Word>> byKey = new LinkedHashMap<>();
            for (List<Word> ps : byCol) for (Word w : ps) byKey.computeIfAbsent(w.key(), x -> new ArrayList<>()).add(w);
            List<List<Word>> lines = new ArrayList<>(byKey.values());
            lines.sort(Comparator.comparingInt(ws -> ws.stream().mapToInt(w -> w.y).min().orElse(0)));
            Set<Word> firstCol = Collections.newSetFromMap(new IdentityHashMap<>());
            firstCol.addAll(byCol.get(key));
            int starts = 0;
            for (int i = 0; i < lines.size(); i++) {
                if (i == 0 || lines.get(i).stream().anyMatch(firstCol::contains)) starts++;
            }
            if (starts <= 1) {
                bandR.add(e.getKey());
                bandCols.add(byCol);
                continue;
            }
            for (List<Word> l : lines) {
                Set<Word> ids = Collections.newSetFromMap(new IdentityHashMap<>());
                ids.addAll(l);
                List<List<Word>> sub = new ArrayList<>();
                for (List<Word> ps : byCol) { List<Word> f = new ArrayList<>(); for (Word w : ps) if (ids.contains(w)) f.add(w); sub.add(f); }
                bandR.add(e.getKey());
                bandCols.add(sub);
            }
        }
        List<List<String>> rows = new ArrayList<>();
        for (int b = 0; b < bandR.size(); b++) {
            int r = bandR.get(b);
            List<List<Word>> byCol = bandCols.get(b);
            double bm = (allY.get(r) + allY.get(r + 1)) / 2;
            List<Double> ws = new ArrayList<>();
            for (double[] v : walls) if (v[1] < bm && bm < v[2]) ws.add(v[0]);
            List<List<Word>> cells = new ArrayList<>();
            for (int k = 0; k < byCol.size(); k++) {
                final double xk = xs.get(k);
                if (k > 0 && ws.stream().noneMatch(x -> Math.abs(x - xk) <= 8)) cells.get(cells.size() - 1).addAll(byCol.get(k));
                else cells.add(new ArrayList<>(byCol.get(k)));
            }
            List<String> texts = new ArrayList<>();
            for (List<Word> c : cells) {
                Map<String, Integer> tops = new LinkedHashMap<>();
                for (Word w : c) tops.merge(w.key(), w.y, Math::min);
                c.sort(Comparator.<Word>comparingInt(w -> tops.get(w.key())).thenComparing(ScanTableReader::cmpKey).thenComparingInt(w -> w.x));
                List<String> parts = new ArrayList<>();
                for (Word w : c) { String t = cleanCellWord(w, wallsX); if (!t.isEmpty()) parts.add(t); }
                texts.add(String.join(" ", parts));
            }
            if (texts.stream().anyMatch(t -> !t.isEmpty())) rows.add(texts);
        }
        long full = rows.stream().filter(t -> t.stream().filter(x -> !x.isEmpty()).count() >= 2).count();
        if (rows.size() < 2 || full * 2 < rows.size()) return null;
        return new RuledResult(rows, inside);
    }

    private static int medianHigh(List<Word> ws) {
        int[] hs = ws.stream().mapToInt(w -> w.h).sorted().toArray();
        return hs[hs.length / 2];
    }

    private static List<List<Word>> groups(List<Word> lineWords) {
        List<Word> ws = new ArrayList<>(lineWords);
        ws.sort(Comparator.comparingInt(w -> w.x));
        int h = medianHigh(ws);
        List<List<Word>> groups = new ArrayList<>();
        List<Word> cur = new ArrayList<>();
        Integer end = null;
        for (Word w : ws) {
            if (onlyRuleChars(w.text)) {
                if (!cur.isEmpty()) groups.add(cur);
                cur = new ArrayList<>();
                end = null;
                continue;
            }
            if (!cur.isEmpty() && w.x - end > 2 * h) {
                groups.add(cur);
                cur = new ArrayList<>();
            }
            cur.add(w);
            end = w.x + w.w;
        }
        if (!cur.isEmpty()) groups.add(cur);
        return groups;
    }

    private static String rowText(List<List<Word>> groups) {
        List<String> cells = new ArrayList<>();
        for (List<Word> g : groups) {
            List<String> t = new ArrayList<>();
            for (Word w : g) t.add(w.text);
            cells.add(String.join(" ", t));
        }
        return String.join(" | ", cells);
    }

    private static String strayBars(List<Word> ws) {
        List<String> out = new ArrayList<>();
        boolean glue = false;
        for (int i = 0; i < ws.size(); i++) {
            Word w = ws.get(i);
            if (w.text.equals("|")) {
                String nxt = i + 1 < ws.size() ? ws.get(i + 1).text : "";
                if (!nxt.isEmpty() && Character.isLowerCase(nxt.codePointAt(0))) out.add("I");
                else if (!nxt.isEmpty() && Character.isDigit(nxt.codePointAt(0))) glue = true;
                continue;
            }
            out.add((glue ? "|" : "") + w.text);
            glue = false;
        }
        return String.join(" ", out);
    }

    /** The page as text: Tesseract's own text, with tables as " | " rows. */
    public static String pageText(List<Word> words, Rules R) {
        List<List<Word>> lines = linesOf(words);
        Set<Word> taken = Collections.newSetFromMap(new IdentityHashMap<>());
        Map<Integer, List<String>> rowsAt = new TreeMap<>();
        if (R != null) {
            for (Lattice L : lattices(R)) {
                List<Word> avail = new ArrayList<>();
                for (Word w : words) if (!taken.contains(w)) avail.add(w);
                RuledResult res = ruledRows(L, avail, R);
                if (res == null) continue;
                Set<Word> ids = Collections.newSetFromMap(new IdentityHashMap<>());
                ids.addAll(res.inside());
                taken.addAll(ids);
                int first = 0;
                for (int i = 0; i < lines.size(); i++) if (lines.get(i).stream().anyMatch(ids::contains)) { first = i; break; }
                List<String> rs = rowsAt.computeIfAbsent(first, x -> new ArrayList<>());
                for (List<String> t : res.rows()) {
                    String s = String.join(" | ", t) + (t.size() == 1 ? " |" : "");
                    rs.add(s.replaceAll(" {2,}", " ").strip());
                }
            }
        }
        // unruled tables
        List<List<List<Word>>> groups = new ArrayList<>();
        for (List<Word> ws : lines) {
            List<Word> rest = new ArrayList<>();
            for (Word w : ws) if (!taken.contains(w)) rest.add(w);
            groups.add(rest.isEmpty() ? new ArrayList<>() : groups(rest));
        }
        int minX = words.stream().mapToInt(w -> w.x).min().orElse(0);
        int maxX = words.stream().mapToInt(w -> w.x + w.w).max().orElse(1);
        double pageW = maxX - minX;
        for (int i = 0; i < groups.size(); i++) {
            boolean cellish = true;
            for (List<Word> g : groups.get(i)) {
                Word a = g.get(0), z = g.get(g.size() - 1);
                if (z.x + z.w - a.x > 0.34 * pageW) { cellish = false; break; }
            }
            if (!cellish) groups.set(i, new ArrayList<>());
        }
        List<List<Integer>> starts = new ArrayList<>();
        for (List<List<Word>> gs : groups) { List<Integer> s = new ArrayList<>(); for (List<Word> g : gs) s.add(g.get(0).x); starts.add(s); }
        int[] hgt = new int[lines.size()];
        for (int i = 0; i < lines.size(); i++) hgt[i] = medianHigh(lines.get(i));
        boolean[] multi = new boolean[lines.size()];
        for (int i = 0; i < lines.size(); i++) {
            if (groups.get(i).size() < 3) continue;
            for (int j : new int[]{i - 1, i + 1}) {
                if (j < 0 || j >= lines.size() || groups.get(j).size() < 3) continue;
                double tol = 2 * Math.max(hgt[i], hgt[j]);
                List<Integer> a = starts.get(i).subList(1, starts.get(i).size()), b = starts.get(j).subList(1, starts.get(j).size());
                int n = 0;
                for (int x : a) if (b.stream().anyMatch(y -> Math.abs(x - y) <= tol)) n++;
                if (n >= 2) { multi[i] = true; break; }
            }
        }
        boolean[] inTable = new boolean[lines.size()];
        int i = 0;
        while (i < lines.size()) {
            int j = i;
            while (j < lines.size() && multi[j]) j++;
            if (j - i >= 3) for (int k = i; k < j; k++) inTable[k] = true;
            i = Math.max(j, i + 1);
        }
        List<String> out = new ArrayList<>();
        Word prev = null;
        for (int n = 0; n < lines.size(); n++) {
            List<Word> ws = lines.get(n);
            Word k = ws.get(0);
            if (prev != null && (k.block != prev.block || k.par != prev.par)) out.add("");
            prev = k;
            if (rowsAt.containsKey(n)) out.addAll(rowsAt.get(n));
            List<Word> rest = new ArrayList<>();
            for (Word w : ws) if (!taken.contains(w)) rest.add(w);
            if (rest.isEmpty()) continue;
            out.add(inTable[n] ? rowText(groups.get(n)) : strayBars(rest));
        }
        return String.join("\n", out);
    }
}
