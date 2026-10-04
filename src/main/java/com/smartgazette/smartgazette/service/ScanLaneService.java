package com.smartgazette.smartgazette.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Scan lane (fix 5): reads a scanned gazette with Tesseract and repairs the
 * OCR text so the normal cleaner, categoriser and templates work on it.
 *
 * WHY THIS EXISTS
 * 21 of 287 gazettes in 2022-2025 are scans (Foxit, OmniPage, PFU). Their text
 * layer is missing or a poor hidden OCR guess, so pdf-inspector finds 0-1
 * notices in each - about 3,800 notices the system could not see. Measured on
 * those scans (tools/scan_lane.py, the Python reference): 97.9% of notices
 * found, probate cause blocks templated 86.6%, land 79.5%.
 *
 * Detection mirrors tools/ocr_check.py: a page is a scan when it carries a
 * raster image at least ~A4 in printed size (pdfimages -list: pixels / ppi,
 * not pixels - 2024 No 95 is scanned at 150 ppi); a gazette is a scan when at
 * least half of its pages are. In the corpus that split is clean: every scan
 * has an image on every page, every born-digital issue on none.
 *
 * Needs the poppler tools (pdfimages, pdftoppm) and tesseract on the PATH, or
 * their paths in application.properties. Every step fails soft: on any error
 * the caller keeps the text-layer path.
 */
@Service
public class ScanLaneService {

    private static final Logger log = LoggerFactory.getLogger(ScanLaneService.class);

    @Value("${extraction.scan.enabled:true}")
    private boolean enabled;

    /** Parallel Tesseract processes; each is single-threaded (OMP_THREAD_LIMIT=1), measured fastest. */
    @Value("${extraction.scan.workers:4}")
    private int workers;

    @Value("${extraction.scan.tesseract:tesseract}")
    private String tesseract;

    @Value("${extraction.scan.pdftoppm:pdftoppm}")
    private String pdftoppm;

    @Value("${extraction.scan.pdfimages:pdfimages}")
    private String pdfimages;

    @Value("${extraction.scan.dpi:300}")
    private int dpi;

    /** Per page; Tesseract takes ~2.5 s at 300 dpi. */
    @Value("${extraction.scan.page-timeout-seconds:120}")
    private long pageTimeoutSeconds;

    private volatile OcrTextFixer fixer;

    private OcrTextFixer fixer() {
        if (fixer == null) {
            fixer = new OcrTextFixer(GazetteTextCleaner.repairer());
        }
        return fixer;
    }

    public boolean isEnabled() {
        return enabled;
    }

    /** True when at least half the pages carry a page-sized raster image. */
    public boolean isScanned(File pdf, int pageCount) {
        if (!enabled || pageCount <= 0) {
            return false;
        }
        try {
            String out = run(List.of(pdfimages, "-list", pdf.getAbsolutePath()), 60);
            Set<Integer> scanPages = new HashSet<>();
            String[] lines = out.split("\n");
            for (int i = 2; i < lines.length; i++) {
                String[] f = lines[i].trim().split("\\s+");
                try {
                    int page = Integer.parseInt(f[0]);
                    double w = Double.parseDouble(f[3]), h = Double.parseDouble(f[4]);
                    double xppi = Double.parseDouble(f[12]), yppi = Double.parseDouble(f[13]);
                    if (xppi > 0 && yppi > 0 && w / xppi >= 6.5 && h / yppi >= 9.0) {
                        scanPages.add(page);
                    }
                } catch (RuntimeException ignored) {
                    // header or a line in another format
                }
            }
            boolean scanned = scanPages.size() >= 0.5 * pageCount;
            log.info("Scan check {}: {} of {} pages carry a page-sized image -> {}",
                    pdf.getName(), scanPages.size(), pageCount, scanned ? "SCAN LANE" : "text layer");
            return scanned;
        } catch (Exception e) {
            log.warn("Scan check failed for {} - using the text layer.", pdf.getName(), e);
            return false;
        }
    }

    /**
     * OCR text of every page, repaired page by page, joined like the Python
     * reference (pages separated by a blank line). Null on failure.
     */
    public String ocrText(File pdf, int pageCount) {
        long t0 = System.currentTimeMillis();
        Path tmp = null;
        ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, workers));
        try {
            tmp = Files.createTempDirectory("sg-scan-");
            final Path dir = tmp;
            List<Future<String>> pages = new ArrayList<>();
            for (int p = 1; p <= pageCount; p++) {
                final int page = p;
                pages.add(pool.submit(() -> ocrPage(pdf, page, dir)));
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < pages.size(); i++) {
                String text = pages.get(i).get(pageTimeoutSeconds * 2, TimeUnit.SECONDS);
                if (i > 0) sb.append("\n\n");
                sb.append(fixer().fixPage(text));
            }
            log.info("Scan lane: OCR of {} pages in {} s ({} workers).",
                    pageCount, (System.currentTimeMillis() - t0) / 1000, workers);
            return sb.toString();
        } catch (Exception e) {
            log.error("Scan lane failed for {} - falling back to the text layer.", pdf.getName(), e);
            return null;
        } finally {
            pool.shutdownNow();
            deleteQuietly(tmp);
        }
    }

    private static final java.util.regex.Pattern WORD = java.util.regex.Pattern.compile("[A-Za-z]{3,}");
    /** a figure marker (docs/specs/figures.md): not a word, and kept when OCR replaces its page */
    private static final java.util.regex.Pattern FIG = java.util.regex.Pattern.compile("\\[\\[FIGURE:\\d+\\.\\d+\\]\\]");

    /** Pages (1-based) of the extractor's text with fewer than 5 words - no text layer. */
    public static List<Integer> textlessPages(String raw) {
        List<Integer> out = new ArrayList<>();
        String[] pages = raw.split("\f", -1);
        for (int i = 0; i < pages.length; i++) {
            java.util.regex.Matcher m = WORD.matcher(FIG.matcher(pages[i]).replaceAll(""));
            int n = 0;
            while (m.find() && n < 5) n++;
            if (n < 5) out.add(i + 1);
        }
        return out;
    }

    /** The result of {@link #fill}: the text and the pages filled by OCR. */
    public record Filled(String text, List<Integer> pages) {
    }

    /**
     * Mirrors tools/mixed_pages.fill: every textless page replaced by its OCR
     * text (a page keeps the "\n" that ends it before the next form feed).
     */
    public static Filled fill(String raw, java.util.function.IntFunction<String> ocrPage) {
        String[] pages = raw.split("\f", -1);
        List<Integer> done = new ArrayList<>();
        for (int n : textlessPages(raw)) {
            String text = ocrPage.apply(n);
            if (text == null || text.strip().isEmpty()) continue;
            String t = text.replaceAll("^\n+|\n+$", "");
            List<String> marks = new ArrayList<>();
            java.util.regex.Matcher fm = FIG.matcher(pages[n - 1]);
            while (fm.find()) marks.add(fm.group());
            String body = (marks.isEmpty() ? "" : String.join("\n", marks) + "\n") + t;
            pages[n - 1] = body + (n < pages.length ? "\n" : "");
            done.add(n);
        }
        return new Filled(String.join("\f", pages), done);
    }

    /**
     * Pages of a born-digital issue that have no text layer (scanned maps or
     * pages, docs/specs/mixed-pages.md): each is read like a scanned page
     * (Tesseract word boxes + image rules + OCR repair) and its text replaces
     * the empty page. Returns the input unchanged when there is none or OCR
     * fails.
     */
    public Filled fillTextlessPages(File pdf, String raw) {
        if (!enabled || raw == null) return new Filled(raw, List.of());
        List<Integer> todo = textlessPages(raw);
        if (todo.isEmpty()) return new Filled(raw, List.of());
        Path tmp = null;
        ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, workers));
        try {
            tmp = Files.createTempDirectory("sg-pages-");
            final Path dir = tmp;
            // the pages are read in parallel, as the scan lane reads a whole scan
            Map<Integer, Future<String>> ocr = new java.util.HashMap<>();
            for (int page : todo) ocr.put(page, pool.submit(() -> fixer().fixPage(ocrPage(pdf, page, dir))));
            Filled f = fill(raw, page -> {
                try {
                    return ocr.get(page).get(pageTimeoutSeconds * 2, TimeUnit.SECONDS);
                } catch (Exception e) {
                    log.warn("OCR of page {} of {} failed - the page stays empty.", page, pdf.getName(), e);
                    return null;
                }
            });
            log.info("Pages without a text layer in {}: {} found, {} read by OCR.", pdf.getName(), todo.size(), f.pages().size());
            return f;
        } catch (IOException e) {
            log.warn("Page OCR not possible for {} - text layer only.", pdf.getName(), e);
            return new Filled(raw, List.of());
        } finally {
            pool.shutdownNow();
            deleteQuietly(tmp);
        }
    }

    private static final java.util.regex.Pattern CLEAN_HDR =
            java.util.regex.Pattern.compile("GAZETTE NOTICE NO\\. (\\d+)");

    /**
     * Mirrors scan_lane.drop_outliers: a header whose number is far from the
     * issue's run (a cross-reference "(GAZETTE NOTICE NO. 963)" in an issue
     * of 9618-9771, or a misread "1623382") is lowered to plain text so it
     * stays inside the notice it was printed in. Run on the cleaned scan text.
     */
    public static String dropOutliers(String text) {
        java.util.regex.Matcher m = CLEAN_HDR.matcher(text);
        List<Long> nums = new ArrayList<>();
        while (m.find()) nums.add(Long.parseLong(m.group(1)));
        Set<Long> keep = nums.size() >= 3 ? mainBlock(nums) : new HashSet<>(nums);
        m = CLEAN_HDR.matcher(text);
        StringBuilder sb = new StringBuilder(text.length());
        while (m.find()) {
            String rep = keep.contains(Long.parseLong(m.group(1))) ? m.group() : "Gazette Notice No. " + m.group(1);
            m.appendReplacement(sb, java.util.regex.Matcher.quoteReplacement(rep));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    /** Mirrors ocr_check.main_block: runs split at jumps > 100; keep the largest run and any run of >= 3. */
    static Set<Long> mainBlock(List<Long> values) {
        java.util.TreeSet<Long> vs = new java.util.TreeSet<>(values);
        List<List<Long>> blocks = new ArrayList<>();
        List<Long> cur = new ArrayList<>();
        for (Long v : vs) {
            if (!cur.isEmpty() && v - cur.get(cur.size() - 1) > 100) {
                blocks.add(cur);
                cur = new ArrayList<>();
            }
            cur.add(v);
        }
        if (!cur.isEmpty()) blocks.add(cur);
        List<Long> big = blocks.get(0);
        for (List<Long> b : blocks) if (b.size() > big.size()) big = b;     // first largest, like max()
        Set<Long> keep = new HashSet<>();
        for (List<Long> b : blocks) if (b == big || b.size() >= 3) keep.addAll(b);
        return keep;
    }

    private String ocrPage(File pdf, int page, Path dir) throws IOException, InterruptedException {
        Path img = dir.resolve(String.format("p%04d", page));
        run(List.of(pdftoppm, "-r", String.valueOf(dpi), "-gray", "-png", "-singlefile",
                "-f", String.valueOf(page), "-l", String.valueOf(page), pdf.getAbsolutePath(), img.toString()),
                pageTimeoutSeconds);
        Path png = dir.resolve(img.getFileName() + ".png");
        Path base = dir.resolve(img.getFileName() + "_ocr");
        // word boxes (tsv) instead of plain text: tables are rebuilt from them and
        // from the ruling lines in the same page image (ScanTableReader, scan tables)
        run(List.of(tesseract, png.toString(), base.toString(), "-l", "eng", "tsv"), pageTimeoutSeconds);
        String tsv = Files.readString(dir.resolve(base.getFileName() + ".tsv"), StandardCharsets.UTF_8);
        ScanTableReader.Rules rules = null;
        try {
            java.awt.image.BufferedImage bi = javax.imageio.ImageIO.read(png.toFile());
            if (bi != null) {
                int[][] gray = new int[bi.getHeight()][bi.getWidth()];
                java.awt.image.Raster r = bi.getRaster();
                for (int y = 0; y < gray.length; y++) for (int x = 0; x < gray[0].length; x++) gray[y][x] = r.getSample(x, y, 0);
                rules = ScanTableReader.imageRules(gray, 1.0, dpi);
            }
        } catch (RuntimeException e) {
            log.warn("Ruling lines not read on page {} - tables read without them.", page, e);
        }
        String text = ScanTableReader.pageText(ScanTableReader.readWords(tsv), rules);
        Files.deleteIfExists(png);
        return text;
    }

    private static String run(List<String> cmd, long timeoutSeconds) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(cmd).redirectErrorStream(true);
        pb.environment().put("OMP_THREAD_LIMIT", "1");      // measured: faster than Tesseract's own threads
        Process proc = pb.start();
        byte[] out = proc.getInputStream().readAllBytes();
        if (!proc.waitFor(timeoutSeconds, TimeUnit.SECONDS)) {
            proc.destroyForcibly();
            throw new IOException("timed out: " + cmd.get(0));
        }
        if (proc.exitValue() != 0) {
            throw new IOException(cmd.get(0) + " exit " + proc.exitValue() + ": "
                    + new String(out, StandardCharsets.UTF_8).trim());
        }
        return new String(out, StandardCharsets.UTF_8);
    }

    private static void deleteQuietly(Path dir) {
        if (dir == null) return;
        try (var s = Files.walk(dir)) {
            s.sorted(java.util.Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        } catch (IOException ignored) {
            // temp dir; the OS cleans it eventually
        }
    }
}
