package com.smartgazette.smartgazette.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartgazette.smartgazette.model.NoticeFigure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.imageio.ImageIO;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Figures, Stage A in production (docs/specs/figures.md) - the Java side of
 * tools/figures.py.
 *
 * For every image placement the extractor lists ({@code inspect_positions.js
 * --figures}): crop it from the page rendered at 200 dpi, read its text with
 * PP-OCRv6 ({@link PpOcr}; Tesseract when the models cannot load), classify it
 * ({@link FigureClassifier}), and for faded / brown-paper scans write a cleaned
 * copy beside the original. Nothing is dropped.
 *
 * Files: {@code <figures.dir>/<first 16 hex of the PDF's SHA-256>/f<page>_<k>.png}
 * (+ {@code _clean.png}). Never throws: a failure costs figures, never notices.
 */
@Service
public class FigureService {

    private static final Logger log = LoggerFactory.getLogger(FigureService.class);

    @Value("${figures.enabled:true}")
    private boolean enabled;

    @Value("${figures.dir:figures}")
    private String figuresDir;

    @Value("${figures.dpi:200}")
    private int dpi;

    @Value("${extraction.scan.pdftoppm:pdftoppm}")
    private String pdftoppm;

    @Value("${extraction.scan.tesseract:tesseract}")
    private String tesseract;

    /** PP-OCRv6 models (tools/fetch_ocr_models.py) */
    @Value("${figures.ocr.models-dir:models/ppocr}")
    private String modelsDir;

    private volatile PpOcr rapid;
    private volatile boolean rapidFailed;

    public boolean isEnabled() { return enabled; }

    public Path root() { return Path.of(figuresDir).toAbsolutePath(); }

    /** One image placement from the extractor's figure list. */
    record Placement(String id, int page, double x0, double y0, double x1, double y1, int wPx, int hPx) {
        FigureClassifier.Box box() { return new FigureClassifier.Box(page, x0, y0, x1, y1); }
        double areaPt() { return (x1 - x0) * (y1 - y0); }
    }

    static List<Placement> readPlacements(String json) throws Exception {
        List<Placement> out = new ArrayList<>();
        for (JsonNode f : new ObjectMapper().readTree(json)) {
            JsonNode px = f.path("px");
            out.add(new Placement(f.path("id").asText(), f.path("page").asInt(),
                    f.path("x0").asDouble(), f.path("y0").asDouble(), f.path("x1").asDouble(), f.path("y1").asDouble(),
                    px.isArray() ? px.path(0).asInt() : 0, px.isArray() ? px.path(1).asInt() : 0));
        }
        return out;
    }

    /** notice number -> notice text, as GazetteService segments: the last
     *  "GAZETTE NOTICE NO. n" part wins (= the Python dict). */
    static Map<String, String> noticeTexts(String text) {
        Map<String, String> ns = new HashMap<>();
        Pattern hdr = Pattern.compile("GAZETTE NOTICE NO\\. (\\d+)");
        for (String part : text.split("(?=GAZETTE NOTICE NO\\. \\d+)")) {
            Matcher m = hdr.matcher(part);
            if (m.lookingAt()) ns.put(m.group(1), part);
        }
        return ns;
    }

    /**
     * Captures every figure of one gazette.
     *
     * @param figuresJson the extractor's figure list
     * @param text        the cleaned text of the whole gazette (with its markers)
     * @return one record per placement (not saved), or an empty list on failure
     */
    public List<NoticeFigure> capture(File pdf, String originalPdfPath, String figuresJson, String text) {
        List<NoticeFigure> out = new ArrayList<>();
        if (!enabled || figuresJson == null || figuresJson.isBlank() || text == null) return out;
        try {
            List<Placement> figs = readPlacements(figuresJson);
            if (figs.isEmpty()) return out;
            Path dir = root().resolve(pdfKey(pdf));
            Files.createDirectories(dir);
            Map<String, FigureClassifier.Context> ctx = FigureClassifier.noticeOfMarkers(text);
            Map<String, String> notices = noticeTexts(text);
            // the same image placed several times (a logo, a party symbol) has the same pixel size
            Map<String, Integer> repeats = new HashMap<>();
            for (Placement f : figs) repeats.merge(f.wPx() + "x" + f.hPx(), 1, Integer::sum);
            long start = System.currentTimeMillis();
            for (Placement f : figs) {
                Path png = dir.resolve("f" + f.id().replace('.', '_') + ".png");
                if (!Files.exists(png) && !crop(pdf, f, png)) continue;
                BufferedImage img = ImageIO.read(png.toFile());
                if (img == null) continue;
                FigureClassifier.Context c = ctx.getOrDefault(f.id(), FigureClassifier.Context.NONE);
                Reading reading = f.areaPt() >= 0.25 * 72 * 72 ? read(png) : new Reading("", List.of());
                String ocr = reading.text();
                String noticeText = c.notice() == null ? null : notices.get(c.notice());
                String kind = FigureClassifier.classify(f.box(), c, ocr, repeats.get(f.wPx() + "x" + f.hPx()), noticeText);
                int rotate = ("map".equals(kind) || "page_scan".equals(kind)) && upsideDown(img, reading.lines()) ? 180 : 0;
                String clean = null;
                if (FigureClassifier.scanned(kind) && (paperTone(img) > 25 || rotate != 0)) {
                    Path cp = dir.resolve("f" + f.id().replace('.', '_') + "_clean.png");
                    if (!Files.exists(cp)) ImageIO.write(clean(img, rotate), "png", cp.toFile());
                    clean = root().relativize(cp).toString().replace('\\', '/');
                }
                NoticeFigure nf = new NoticeFigure();
                nf.setOriginalPdfPath(originalPdfPath);
                nf.setNoticeNumber(c.notice());
                nf.setFigureId(f.id());
                nf.setPage(f.page());
                nf.setKind(kind);
                nf.setInTable(c.inTable());
                nf.setX0(f.x0()); nf.setY0(f.y0()); nf.setX1(f.x1()); nf.setY1(f.y1());
                nf.setWidthPx(f.wPx()); nf.setHeightPx(f.hPx());
                nf.setOcrText(ocr);
                nf.setRotate(rotate);
                nf.setFilePath(root().relativize(png).toString().replace('\\', '/'));
                nf.setCleanFilePath(clean);
                out.add(nf);
            }
            log.info("Figures: {} of {} placements captured from {} in {} ms.", out.size(), figs.size(), pdf.getName(),
                    System.currentTimeMillis() - start);
        } catch (Exception e) {
            log.error("Figures: capture failed for {} - the notices are not affected.", pdf.getName(), e);
        }
        return out;
    }

    // ------------------------------------------------------------- files

    /** A stable folder per PDF: the same file uploaded twice lands in the same place. */
    static String pdfKey(File pdf) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(Files.readAllBytes(pdf.toPath()));
        StringBuilder sb = new StringBuilder();
        for (byte b : md.digest()) sb.append(String.format("%02x", b));
        return sb.substring(0, 16);
    }

    private boolean crop(File pdf, Placement f, Path png) {
        double k = dpi / 72.0;
        int x = (int) (f.x0() * k), y = (int) (f.y0() * k);
        int w = Math.max(1, (int) ((f.x1() - f.x0()) * k)), h = Math.max(1, (int) ((f.y1() - f.y0()) * k));
        String base = png.toString().substring(0, png.toString().length() - 4);
        run(List.of(pdftoppm, "-r", String.valueOf(dpi), "-png", "-singlefile", "-f", String.valueOf(f.page()),
                "-l", String.valueOf(f.page()), "-x", String.valueOf(x), "-y", String.valueOf(y),
                "-W", String.valueOf(w), "-H", String.valueOf(h), pdf.getAbsolutePath(), base), 120);
        return Files.exists(png);
    }

    private static String run(List<String> cmd, long timeoutSeconds) {
        try {
            Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
            byte[] out = p.getInputStream().readAllBytes();
            if (!p.waitFor(timeoutSeconds, TimeUnit.SECONDS)) { p.destroyForcibly(); return null; }
            return new String(out, StandardCharsets.UTF_8);
        } catch (Exception e) {
            log.warn("Figures: {} failed: {}", cmd.get(0), e.getMessage());
            return null;
        }
    }

    // ------------------------------------------------------------- reading

    private PpOcr rapid() {
        if (rapid == null && !rapidFailed) {
            synchronized (this) {
                if (rapid == null && !rapidFailed) {
                    try {
                        rapid = PpOcr.open(Path.of(modelsDir));
                    } catch (Throwable t) {           // models missing, ONNX Runtime unavailable
                        rapidFailed = true;
                        log.warn("Figures: PP-OCR could not load ({}); reading figures with Tesseract.", t.toString());
                    }
                }
            }
        }
        return rapid;
    }

    /** A figure's text in reading order, with the PP-OCR lines it came from
     *  (null when Tesseract read it). */
    record Reading(String text, List<PpOcr.Line> lines) {}

    /** PP-OCR, else Tesseract (--psm 11, as figures.ocr). */
    Reading read(Path png) {
        PpOcr eng = rapid();
        if (eng != null) {
            try {
                List<PpOcr.Line> lines = eng.read(ImageIO.read(png.toFile()));
                return new Reading(readingOrder(lines), lines);
            } catch (Throwable t) {
                log.warn("Figures: PP-OCR failed on {} ({}); using Tesseract.", png.getFileName(), t.toString());
            }
        }
        String s = run(List.of(tesseract, png.toString(), "-", "-l", "eng", "--psm", "11"), 120);
        return new Reading(s == null ? "" : s.replaceAll("\\s+", " ").strip(), null);
    }

    /** = figure_ocr_eval.reading_order: lines whose vertical centres are within
     *  half the median line height form one row, read left to right. */
    static String readingOrder(List<PpOcr.Line> lines) {
        if (lines.isEmpty()) return "";
        List<Integer> hs = new ArrayList<>();
        for (PpOcr.Line l : lines) hs.add(l.y1() - l.y0());
        Collections.sort(hs);
        double half = Math.max(1, hs.get(hs.size() / 2)) / 2.0;
        List<PpOcr.Line> sorted = new ArrayList<>(lines);
        sorted.sort(Comparator.comparingDouble(l -> (l.y0() + l.y1()) / 2.0));
        List<List<PpOcr.Line>> rows = new ArrayList<>();
        List<PpOcr.Line> cur = new ArrayList<>();
        double yc = 0;
        for (PpOcr.Line l : sorted) {
            double c = (l.y0() + l.y1()) / 2.0;
            if (!cur.isEmpty() && c - yc > half) { rows.add(cur); cur = new ArrayList<>(); }
            if (cur.isEmpty()) yc = c;
            cur.add(l);
        }
        rows.add(cur);
        StringBuilder sb = new StringBuilder();
        for (List<PpOcr.Line> row : rows) {
            row.sort(Comparator.comparingInt(PpOcr.Line::x0));
            if (sb.length() > 0) sb.append('\n');
            for (int i = 0; i < row.size(); i++) sb.append(i == 0 ? "" : " ").append(row.get(i).text());
        }
        return sb.toString();
    }

    private static final Pattern WORD4 = Pattern.compile("[A-Za-z]{4,}");

    /** words of >= 4 letters on lines PP-OCR reads with confidence >= 0.8 */
    static int confidentWords(List<PpOcr.Line> lines) {
        int n = 0;
        for (PpOcr.Line l : lines)
            if (l.score() >= 0.8) { Matcher m = WORD4.matcher(l.text()); while (m.find()) n++; }
        return n;
    }

    private List<PpOcr.Line> lines(BufferedImage img) {
        PpOcr eng = rapid();
        if (eng == null) return List.of();
        try {
            return eng.read(img);
        } catch (Throwable t) {
            return List.of();
        }
    }

    /** A map scanned upside down (2025 No 163 p61): read both ways up and turn
     *  only when the turned reading clearly wins (= figures.upside_down). The
     *  upright reading is the one the text came from, when PP-OCR made it. */
    boolean upsideDown(BufferedImage img, List<PpOcr.Line> upright) {
        int up = confidentWords(upright != null ? upright : lines(img));
        int down = confidentWords(lines(rotate180(img)));
        return down >= 3 && down > 2 * up;
    }

    // ------------------------------------------------------------- cleanup (= figures.clean)

    static double[] gray(BufferedImage img) {
        int w = img.getWidth(), h = img.getHeight();
        double[] g = new double[w * h];
        for (int y = 0; y < h; y++)
            for (int x = 0; x < w; x++) {
                int rgb = img.getRGB(x, y);
                // PIL "L": L = R * 299/1000 + G * 587/1000 + B * 114/1000
                g[y * w + x] = ((rgb >> 16 & 255) * 299 + (rgb >> 8 & 255) * 587 + (rgb & 255) * 114) / 1000;
            }
        return g;
    }

    static double percentile(double[] a, double q) {
        double[] s = a.clone();
        Arrays.sort(s);
        double pos = q / 100.0 * (s.length - 1);
        int lo = (int) Math.floor(pos), hi = (int) Math.ceil(pos);
        return s[lo] + (s[hi] - s[lo]) * (pos - lo);
    }

    /** how far the background is from white (brown / grey paper): 0 = white */
    static double paperTone(BufferedImage img) {
        return 255 - percentile(gray(img), 90);
    }

    /** separable Gaussian blur (sigma = radius, as PIL's GaussianBlur) */
    static double[] blur(double[] src, int w, int h, double sigma) {
        int r = Math.max(1, (int) Math.ceil(sigma * 3));
        double[] k = new double[2 * r + 1];
        double sum = 0;
        for (int i = -r; i <= r; i++) { k[i + r] = Math.exp(-(i * i) / (2 * sigma * sigma)); sum += k[i + r]; }
        for (int i = 0; i < k.length; i++) k[i] /= sum;
        double[] tmp = new double[w * h], out = new double[w * h];
        for (int y = 0; y < h; y++)
            for (int x = 0; x < w; x++) {
                double v = 0;
                for (int i = -r; i <= r; i++) v += k[i + r] * src[y * w + Math.min(w - 1, Math.max(0, x + i))];
                tmp[y * w + x] = v;
            }
        for (int y = 0; y < h; y++)
            for (int x = 0; x < w; x++) {
                double v = 0;
                for (int i = -r; i <= r; i++) v += k[i + r] * tmp[Math.min(h - 1, Math.max(0, y + i)) * w + x];
                out[y * w + x] = v;
            }
        return out;
    }

    /**
     * Local cleanup, no generative step: divide by a blurred copy (removes paper
     * tone, stains, uneven light), stretch contrast, sharpen; small scans 2x.
     * Every output pixel is a monotone function of its neighbourhood.
     */
    static BufferedImage clean(BufferedImage img, int rotate) {
        int w = img.getWidth(), h = img.getHeight();
        double[] g = gray(img);
        double[] bg = blur(g, w, h, Math.max(8, w / 60));
        double[] flat = new double[g.length];
        for (int i = 0; i < g.length; i++) flat[i] = Math.min(255, Math.max(0, g[i] / Math.max(bg[i], 1) * 255));
        double lo = percentile(flat, 1), hi = percentile(flat, 70);
        BufferedImage out = new BufferedImage(w, h, BufferedImage.TYPE_BYTE_GRAY);
        for (int y = 0; y < h; y++)
            for (int x = 0; x < w; x++) {
                int v = (int) Math.min(255, Math.max(0, (flat[y * w + x] - lo) / Math.max(1, hi - lo) * 255));
                out.getRaster().setSample(x, y, 0, v);
            }
        if (w < 1600) {
            BufferedImage big = new BufferedImage(w * 2, h * 2, BufferedImage.TYPE_BYTE_GRAY);
            java.awt.Graphics2D gr = big.createGraphics();
            gr.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BICUBIC);
            gr.drawImage(out, 0, 0, w * 2, h * 2, null);
            gr.dispose();
            out = big;
        }
        out = unsharp(out, 1.5, 1.2, 2);
        return rotate == 180 ? rotate180(out) : out;
    }

    /** unsharp mask (PIL UnsharpMask radius 1.5, percent 120, threshold 2) */
    static BufferedImage unsharp(BufferedImage img, double radius, double amount, int threshold) {
        int w = img.getWidth(), h = img.getHeight();
        double[] g = new double[w * h];
        for (int y = 0; y < h; y++) for (int x = 0; x < w; x++) g[y * w + x] = img.getRaster().getSample(x, y, 0);
        double[] b = blur(g, w, h, radius);
        BufferedImage out = new BufferedImage(w, h, BufferedImage.TYPE_BYTE_GRAY);
        for (int y = 0; y < h; y++)
            for (int x = 0; x < w; x++) {
                double d = g[y * w + x] - b[y * w + x];
                double v = Math.abs(d) >= threshold ? g[y * w + x] + d * amount : g[y * w + x];
                out.getRaster().setSample(x, y, 0, (int) Math.min(255, Math.max(0, Math.round(v))));
            }
        return out;
    }

    static BufferedImage rotate180(BufferedImage img) {
        int w = img.getWidth(), h = img.getHeight();
        BufferedImage out = new BufferedImage(w, h, img.getType() == 0 ? BufferedImage.TYPE_INT_RGB : img.getType());
        for (int y = 0; y < h; y++) for (int x = 0; x < w; x++) out.setRGB(w - 1 - x, h - 1 - y, img.getRGB(x, y));
        return out;
    }
}
