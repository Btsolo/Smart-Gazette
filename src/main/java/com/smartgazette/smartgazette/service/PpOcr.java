package com.smartgazette.smartgazette.service;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;

import java.awt.image.BufferedImage;
import java.nio.FloatBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * PP-OCRv6 text reading on ONNX Runtime - a Java port of RapidOCR 3.9's
 * pipeline (rapidocr/main.py, ch_ppocr_det, ch_ppocr_rec) for figures
 * (docs/specs/figures.md). RapidOCR read map titles, chart labels and table
 * images far better than Tesseract (tools/figure_ocr_eval.py); the Java
 * wrapper library tried first dropped every space, so the same models are run
 * here directly.
 *
 * Steps, as RapidOCR: keep the image within 30..2000 px; pad very wide strips
 * vertically; detect text regions (DB: probability map > 0.3, dilated,
 * regions -> minimum-area rectangles, mean probability >= 0.5, grown by
 * area * 1.6 / perimeter); cut each region out; recognise it (height 48,
 * CTC decode with the model's own character list + space); drop lines below
 * 0.5 confidence. Two simplifications: no angle classifier (figure text is
 * upright; an upside-down map is turned by FigureService), and regions come
 * from connected components instead of OpenCV contours.
 */
public final class PpOcr implements AutoCloseable {

    /** A line of text: its box in the image (pixels) and mean character confidence. */
    public record Line(int x0, int y0, int x1, int y1, String text, double score) {}

    private static final String DET = "PP-OCRv6_det_small.onnx", REC = "PP-OCRv6_rec_small.onnx",
            KEYS = "PP-OCRv6_rec_small.keys.txt";

    private final OrtEnvironment env;
    private final OrtSession det, rec;
    private final String[] chars;        // 0 = CTC blank, then the model's characters, last = space

    private PpOcr(OrtEnvironment env, OrtSession det, OrtSession rec, String[] chars) {
        this.env = env; this.det = det; this.rec = rec; this.chars = chars;
    }

    /** Loads the two models from {@code dir} (tools/fetch_ocr_models.py). */
    public static PpOcr open(Path dir) throws OrtException {
        if (!Files.isRegularFile(dir.resolve(DET)) || !Files.isRegularFile(dir.resolve(REC)))
            throw new IllegalStateException("PP-OCR models not found in " + dir.toAbsolutePath());
        OrtEnvironment env = OrtEnvironment.getEnvironment();
        OrtSession.SessionOptions o = new OrtSession.SessionOptions();
        OrtSession det = env.createSession(dir.resolve(DET).toString(), o);
        OrtSession rec = env.createSession(dir.resolve(REC).toString(), o);
        // the character list as a UTF-8 file (tools/fetch_ocr_models.py): the copy stored in the
        // model comes through ONNX Runtime's JNI as "modified UTF-8", which breaks its 4-byte
        // characters and loses 540 entries - the space among them
        Path keysFile = dir.resolve(KEYS);
        if (!Files.isRegularFile(keysFile))
            throw new IllegalStateException("character list not found: " + keysFile.toAbsolutePath());
        String list;
        try {
            list = Files.readString(keysFile, java.nio.charset.StandardCharsets.UTF_8);
        } catch (java.io.IOException e) {
            throw new IllegalStateException("cannot read " + keysFile, e);
        }
        String[] keys = list.split("\n", -1);
        int n = keys.length;
        while (n > 0 && keys[n - 1].isEmpty()) n--;
        String[] chars = new String[n + 2];
        chars[0] = "";
        for (int i = 0; i < n; i++) chars[i + 1] = keys[i].replace("\r", "");
        chars[n + 1] = " ";
        return new PpOcr(env, det, rec, chars);
    }

    @Override
    public void close() throws OrtException {
        det.close();
        rec.close();
    }

    // ------------------------------------------------------------- image as 3 float planes (B, G, R)

    /** h x w image, channels in BGR order as OpenCV / RapidOCR hold them */
    static final class Img {
        final int w, h;
        final float[][] c;   // c[0] = B, c[1] = G, c[2] = R

        Img(int w, int h) { this.w = w; this.h = h; c = new float[][]{new float[w * h], new float[w * h], new float[w * h]}; }

        static Img of(BufferedImage b) {
            Img m = new Img(b.getWidth(), b.getHeight());
            for (int y = 0; y < m.h; y++)
                for (int x = 0; x < m.w; x++) {
                    int rgb = b.getRGB(x, y), i = y * m.w + x;
                    m.c[2][i] = rgb >> 16 & 255; m.c[1][i] = rgb >> 8 & 255; m.c[0][i] = rgb & 255;
                }
            return m;
        }

        /** cv2.resize INTER_LINEAR (half-pixel centres) */
        Img resize(int nw, int nh) {
            Img o = new Img(nw, nh);
            double sx = (double) w / nw, sy = (double) h / nh;
            for (int y = 0; y < nh; y++) {
                double fy = (y + 0.5) * sy - 0.5;
                int y0 = (int) Math.floor(fy); double dy = fy - y0;
                int ya = clamp(y0, h), yb = clamp(y0 + 1, h);
                for (int x = 0; x < nw; x++) {
                    double fx = (x + 0.5) * sx - 0.5;
                    int x0 = (int) Math.floor(fx); double dx = fx - x0;
                    int xa = clamp(x0, w), xb = clamp(x0 + 1, w);
                    for (int k = 0; k < 3; k++) {
                        float[] s = c[k];
                        double v = (s[ya * w + xa] * (1 - dx) + s[ya * w + xb] * dx) * (1 - dy)
                                + (s[yb * w + xa] * (1 - dx) + s[yb * w + xb] * dx) * dy;
                        o.c[k][y * nw + x] = (float) v;
                    }
                }
            }
            return o;
        }

        /** black rows above and below (RapidOCR add_round_letterbox) */
        Img padVertical(int pad) {
            Img o = new Img(w, h + 2 * pad);
            for (int k = 0; k < 3; k++) System.arraycopy(c[k], 0, o.c[k], pad * w, w * h);
            return o;
        }

        /** bilinear sample, border replicated */
        float sample(int k, double fx, double fy) {
            int x0 = (int) Math.floor(fx), y0 = (int) Math.floor(fy);
            double dx = fx - x0, dy = fy - y0;
            int xa = clamp(x0, w), xb = clamp(x0 + 1, w), ya = clamp(y0, h), yb = clamp(y0 + 1, h);
            float[] s = c[k];
            return (float) ((s[ya * w + xa] * (1 - dx) + s[ya * w + xb] * dx) * (1 - dy)
                    + (s[yb * w + xa] * (1 - dx) + s[yb * w + xb] * dx) * dy);
        }
    }

    private static int clamp(int v, int n) { return v < 0 ? 0 : v >= n ? n - 1 : v; }

    private static int round32(double v) { return (int) (Math.round(v / 32.0) * 32); }

    // ------------------------------------------------------------- the pipeline

    public List<Line> read(BufferedImage image) throws OrtException {
        Img img = Img.of(image);
        int oriW = img.w, oriH = img.h;
        // keep within 30..2000 px (resize_image_within_bounds)
        double ratioW = 1, ratioH = 1;
        if (Math.max(img.w, img.h) > 2000) {
            double r = 2000.0 / Math.max(img.w, img.h);
            int nw = round32((int) (img.w * r)), nh = round32((int) (img.h * r));
            ratioW = (double) img.w / nw; ratioH = (double) img.h / nh;
            img = img.resize(nw, nh);
        }
        if (Math.min(img.w, img.h) < 30) {
            double r = 30.0 / Math.min(img.w, img.h);
            int nw = round32((int) (img.w * r)), nh = round32((int) (img.h * r));
            ratioW = (double) img.w / nw * ratioW; ratioH = (double) img.h / nh * ratioH;
            img = img.resize(nw, nh);
        }
        // pad very wide strips (apply_vertical_padding: min_height 30, width_height_ratio 8)
        int top = 0;
        if (img.h <= 30 || (double) img.w / img.h > 8) {
            int newH = Math.max((int) (img.w / 8.0), 30) * 2;
            top = Math.abs(newH - img.h) / 2;
            img = img.padVertical(top);
        }
        List<double[][]> boxes = detect(img);
        List<Line> out = new ArrayList<>();
        if (boxes.isEmpty()) return out;
        List<Img> crops = new ArrayList<>();
        for (double[][] b : boxes) crops.add(crop(img, b));
        String[] texts = new String[crops.size()];
        double[] scores = new double[crops.size()];
        recognise(crops, texts, scores);
        for (int i = 0; i < boxes.size(); i++) {
            if (texts[i].strip().isEmpty() || scores[i] < 0.5) continue;
            int x0 = Integer.MAX_VALUE, y0 = Integer.MAX_VALUE, x1 = 0, y1 = 0;
            for (double[] p : boxes.get(i)) {
                double x = Math.min(oriW, Math.max(0, p[0] * ratioW)), y = Math.min(oriH, Math.max(0, (p[1] - top) * ratioH));
                x0 = Math.min(x0, (int) x); y0 = Math.min(y0, (int) y);
                x1 = Math.max(x1, (int) x); y1 = Math.max(y1, (int) y);
            }
            out.add(new Line(x0, y0, x1, y1, texts[i], scores[i]));
        }
        return out;
    }

    // ------------------------------------------------------------- detection (DB)

    private List<double[][]> detect(Img img) throws OrtException {
        // DetPreProcess: limit_type "min", 736
        double ratio = Math.min(img.w, img.h) < 736 ? 736.0 / Math.min(img.w, img.h) : 1.0;
        int rw = round32((int) (img.w * ratio)), rh = round32((int) (img.h * ratio));
        if (rw <= 0 || rh <= 0) return List.of();
        Img r = img.resize(rw, rh);
        float[] input = new float[3 * rw * rh];
        for (int k = 0; k < 3; k++)
            for (int i = 0; i < rw * rh; i++) input[k * rw * rh + i] = (r.c[k][i] / 255f - 0.5f) / 0.5f;
        float[] prob;
        try (OnnxTensor t = OnnxTensor.createTensor(env, FloatBuffer.wrap(input), new long[]{1, 3, rh, rw});
             OrtSession.Result res = det.run(Map.of(det.getInputNames().iterator().next(), t))) {
            prob = flat((float[][][][]) res.get(0).getValue(), rw, rh);
        }
        return boxesFromMap(prob, rw, rh, img.w, img.h);
    }

    private static float[] flat(float[][][][] v, int w, int h) {
        float[] p = new float[w * h];
        for (int y = 0; y < h; y++) System.arraycopy(v[0][0][y], 0, p, y * w, w);
        return p;
    }

    /** DBPostProcess: thresh 0.3, dilation 2x2, box_thresh 0.5, unclip 1.6, min_size 3 */
    static List<double[][]> boxesFromMap(float[] prob, int w, int h, int destW, int destH) {
        boolean[] seg = new boolean[w * h];
        for (int i = 0; i < seg.length; i++) seg[i] = prob[i] > 0.3f;
        // cv2.dilate with a 2x2 kernel (anchor at its centre): a pixel is set when
        // it or its left / upper / upper-left neighbour is set
        boolean[] mask = new boolean[w * h];
        for (int y = 0; y < h; y++)
            for (int x = 0; x < w; x++)
                mask[y * w + x] = seg[y * w + x] || (x > 0 && seg[y * w + x - 1])
                        || (y > 0 && seg[(y - 1) * w + x]) || (x > 0 && y > 0 && seg[(y - 1) * w + x - 1]);
        int[] label = new int[w * h];
        List<double[][]> boxes = new ArrayList<>();
        int found = 0;
        int[] stack = new int[w * h];
        for (int s = 0; s < w * h && found < 1000; s++) {
            if (!mask[s] || label[s] != 0) continue;
            found++;
            // one region (8-connected): collect its pixels
            List<int[]> pts = new ArrayList<>();
            int sp = 0;
            stack[sp++] = s;
            label[s] = found;
            while (sp > 0) {
                int p = stack[--sp], px = p % w, py = p / w;
                pts.add(new int[]{px, py});
                for (int dy = -1; dy <= 1; dy++)
                    for (int dx = -1; dx <= 1; dx++) {
                        int nx = px + dx, ny = py + dy;
                        if (nx < 0 || ny < 0 || nx >= w || ny >= h) continue;
                        int q = ny * w + nx;
                        if (mask[q] && label[q] == 0) { label[q] = found; stack[sp++] = q; }
                    }
            }
            double[] rect = minAreaRect(hull(pts));          // cx, cy, rw, rh, angle (radians)
            if (Math.min(rect[2], rect[3]) < 3) continue;
            double[][] box = corners(rect);
            if (scoreFast(prob, w, h, box) < 0.5) continue;
            double d = rect[2] * rect[3] * 1.6 / (2 * (rect[2] + rect[3]));
            double[] grown = {rect[0], rect[1], rect[2] + 2 * d, rect[3] + 2 * d, rect[4]};
            if (Math.min(grown[2], grown[3]) < 5) continue;
            double[][] b = corners(grown);
            for (double[] p : b) {
                p[0] = Math.min(destW, Math.max(0, Math.round(p[0] / w * destW)));
                p[1] = Math.min(destH, Math.max(0, Math.round(p[1] / h * destH)));
            }
            b = clockwise(b);
            for (double[] p : b) {
                p[0] = (int) Math.min(Math.max(p[0], 0), destW - 1);
                p[1] = (int) Math.min(Math.max(p[1], 0), destH - 1);
            }
            if ((int) dist(b[0], b[1]) <= 3 || (int) dist(b[0], b[3]) <= 3) continue;
            boxes.add(b);
        }
        // sorted_boxes: by top-left y, a new line when y grows by >= 10, then by x
        boxes.sort(Comparator.comparingDouble(b -> b[0][1]));
        int[] line = new int[boxes.size()];
        for (int i = 1; i < boxes.size(); i++)
            line[i] = line[i - 1] + (boxes.get(i)[0][1] - boxes.get(i - 1)[0][1] >= 10 ? 1 : 0);
        Integer[] order = new Integer[boxes.size()];
        for (int i = 0; i < order.length; i++) order[i] = i;
        Arrays.sort(order, Comparator.<Integer>comparingInt(i -> line[i]).thenComparingDouble(i -> boxes.get(i)[0][0]));
        List<double[][]> sorted = new ArrayList<>();
        for (Integer i : order) sorted.add(boxes.get(i));
        return sorted;
    }

    static double dist(double[] a, double[] b) { return Math.hypot(a[0] - b[0], a[1] - b[1]); }

    /** convex hull (monotone chain) */
    static List<double[]> hull(List<int[]> pts) {
        List<int[]> p = new ArrayList<>(pts);
        p.sort((a, b) -> a[0] != b[0] ? Integer.compare(a[0], b[0]) : Integer.compare(a[1], b[1]));
        int n = p.size();
        if (n < 3) {
            List<double[]> r = new ArrayList<>();
            for (int[] q : p) r.add(new double[]{q[0], q[1]});
            return r;
        }
        int[][] hl = new int[2 * n][];
        int k = 0;
        for (int[] q : p) {
            while (k >= 2 && cross(hl[k - 2], hl[k - 1], q) <= 0) k--;
            hl[k++] = q;
        }
        for (int i = n - 2, t = k + 1; i >= 0; i--) {
            int[] q = p.get(i);
            while (k >= t && cross(hl[k - 2], hl[k - 1], q) <= 0) k--;
            hl[k++] = q;
        }
        List<double[]> r = new ArrayList<>();
        for (int i = 0; i < k - 1; i++) r.add(new double[]{hl[i][0], hl[i][1]});
        return r;
    }

    private static long cross(int[] o, int[] a, int[] b) {
        return (long) (a[0] - o[0]) * (b[1] - o[1]) - (long) (a[1] - o[1]) * (b[0] - o[0]);
    }

    /** minimum-area rectangle of a convex polygon (each edge direction tried): cx, cy, w, h, angle */
    static double[] minAreaRect(List<double[]> hull) {
        if (hull.size() == 1) return new double[]{hull.get(0)[0], hull.get(0)[1], 0, 0, 0};
        double best = Double.MAX_VALUE;
        double[] r = null;
        int n = hull.size();
        for (int i = 0; i < n; i++) {
            double[] a = hull.get(i), b = hull.get((i + 1) % n);
            double ang = Math.atan2(b[1] - a[1], b[0] - a[0]);
            double cs = Math.cos(ang), sn = Math.sin(ang);
            double minU = Double.MAX_VALUE, maxU = -Double.MAX_VALUE, minV = Double.MAX_VALUE, maxV = -Double.MAX_VALUE;
            for (double[] p : hull) {
                double u = p[0] * cs + p[1] * sn, v = -p[0] * sn + p[1] * cs;
                minU = Math.min(minU, u); maxU = Math.max(maxU, u); minV = Math.min(minV, v); maxV = Math.max(maxV, v);
            }
            double area = (maxU - minU) * (maxV - minV);
            if (area < best) {
                best = area;
                double cu = (minU + maxU) / 2, cv = (minV + maxV) / 2;
                r = new double[]{cu * cs - cv * sn, cu * sn + cv * cs, maxU - minU, maxV - minV, ang};
            }
        }
        return r;
    }

    /** the 4 corners of a rectangle, ordered as get_mini_boxes: top-left, top-right, bottom-right, bottom-left */
    static double[][] corners(double[] r) {
        double cs = Math.cos(r[4]), sn = Math.sin(r[4]), hw = r[2] / 2, hh = r[3] / 2;
        double[][] p = new double[4][];
        int k = 0;
        for (double[] s : new double[][]{{-1, -1}, {1, -1}, {1, 1}, {-1, 1}})
            p[k++] = new double[]{r[0] + s[0] * hw * cs - s[1] * hh * sn, r[1] + s[0] * hw * sn + s[1] * hh * cs};
        Arrays.sort(p, Comparator.comparingDouble(q -> q[0]));
        double[] i1, i2, i3, i4;
        if (p[1][1] > p[0][1]) { i1 = p[0]; i4 = p[1]; } else { i1 = p[1]; i4 = p[0]; }
        if (p[3][1] > p[2][1]) { i2 = p[2]; i3 = p[3]; } else { i2 = p[3]; i3 = p[2]; }
        return new double[][]{i1, i2, i3, i4};
    }

    /** order_points_clockwise: tl, tr, br, bl */
    static double[][] clockwise(double[][] pts) {
        double[][] x = pts.clone();
        Arrays.sort(x, Comparator.comparingDouble(q -> q[0]));
        double[][] left = {x[0], x[1]}, right = {x[2], x[3]};
        Arrays.sort(left, Comparator.comparingDouble(q -> q[1]));
        Arrays.sort(right, Comparator.comparingDouble(q -> q[1]));
        return new double[][]{left[0].clone(), right[0].clone(), right[1].clone(), left[1].clone()};
    }

    /** box_score_fast: mean probability inside the box polygon */
    static double scoreFast(float[] prob, int w, int h, double[][] box) {
        int xmin = clamp((int) Math.floor(Arrays.stream(box).mapToDouble(p -> p[0]).min().orElse(0)), w);
        int xmax = clamp((int) Math.ceil(Arrays.stream(box).mapToDouble(p -> p[0]).max().orElse(0)), w);
        int ymin = clamp((int) Math.floor(Arrays.stream(box).mapToDouble(p -> p[1]).min().orElse(0)), h);
        int ymax = clamp((int) Math.ceil(Arrays.stream(box).mapToDouble(p -> p[1]).max().orElse(0)), h);
        int[][] poly = new int[4][];
        for (int i = 0; i < 4; i++) poly[i] = new int[]{(int) box[i][0], (int) box[i][1]};
        double sum = 0;
        int n = 0;
        for (int y = ymin; y <= ymax; y++)
            for (int x = xmin; x <= xmax; x++)
                if (inside(poly, x, y)) { sum += prob[y * w + x]; n++; }
        return n == 0 ? 0 : sum / n;
    }

    /** pixel (x, y) inside or on the edge of a convex quadrilateral */
    private static boolean inside(int[][] q, int x, int y) {
        boolean pos = false, neg = false;
        for (int i = 0; i < 4; i++) {
            int[] a = q[i], b = q[(i + 1) % 4];
            long c = (long) (b[0] - a[0]) * (y - a[1]) - (long) (b[1] - a[1]) * (x - a[0]);
            if (c > 0) pos = true;
            if (c < 0) neg = true;
            if (pos && neg) return false;
        }
        return true;
    }

    /** get_rotate_crop_image: the box cut out upright; a tall cut is turned 90 degrees */
    static Img crop(Img img, double[][] p) {
        int cw = (int) Math.max(dist(p[0], p[1]), dist(p[2], p[3]));
        int ch = (int) Math.max(dist(p[0], p[3]), dist(p[1], p[2]));
        cw = Math.max(cw, 1); ch = Math.max(ch, 1);
        Img o = new Img(cw, ch);
        for (int v = 0; v < ch; v++)
            for (int u = 0; u < cw; u++) {
                double a = (u + 0.5) / cw, b = (v + 0.5) / ch;
                // bilinear map of the quadrilateral (exact for the rectangles DB returns)
                double fx = (1 - a) * (1 - b) * p[0][0] + a * (1 - b) * p[1][0] + a * b * p[2][0] + (1 - a) * b * p[3][0] - 0.5;
                double fy = (1 - a) * (1 - b) * p[0][1] + a * (1 - b) * p[1][1] + a * b * p[2][1] + (1 - a) * b * p[3][1] - 0.5;
                for (int k = 0; k < 3; k++) o.c[k][v * cw + u] = img.sample(k, fx, fy);
            }
        if ((double) ch / cw >= 1.5) {                     // np.rot90: counter-clockwise
            Img r = new Img(ch, cw);
            for (int y = 0; y < cw; y++)
                for (int x = 0; x < ch; x++)
                    for (int k = 0; k < 3; k++) r.c[k][y * ch + x] = o.c[k][x * cw + (cw - 1 - y)];
            return r;
        }
        return o;
    }

    // ------------------------------------------------------------- recognition (CTC)

    private void recognise(List<Img> crops, String[] texts, double[] scores) throws OrtException {
        int n = crops.size();
        Integer[] idx = new Integer[n];
        for (int i = 0; i < n; i++) idx[i] = i;
        Arrays.sort(idx, Comparator.comparingDouble(i -> (double) crops.get(i).w / crops.get(i).h));
        final int H = 48;
        for (int beg = 0; beg < n; beg += 6) {
            int end = Math.min(n, beg + 6);
            double maxRatio = 320.0 / 48;
            for (int i = beg; i < end; i++) maxRatio = Math.max(maxRatio, (double) crops.get(idx[i]).w / crops.get(idx[i]).h);
            int W = (int) (H * maxRatio);
            float[] batch = new float[(end - beg) * 3 * H * W];
            for (int i = beg; i < end; i++) {
                Img c = crops.get(idx[i]);
                double ratio = (double) c.w / c.h;
                int rw = Math.ceil(H * ratio) > W ? W : (int) Math.ceil(H * ratio);
                Img r = c.resize(Math.max(1, rw), H);
                int off = (i - beg) * 3 * H * W;
                for (int k = 0; k < 3; k++)
                    for (int y = 0; y < H; y++)
                        for (int x = 0; x < r.w; x++)
                            batch[off + k * H * W + y * W + x] = (r.c[k][y * r.w + x] / 255f - 0.5f) / 0.5f;
            }
            try (OnnxTensor t = OnnxTensor.createTensor(env, FloatBuffer.wrap(batch), new long[]{end - beg, 3, H, W});
                 OrtSession.Result res = rec.run(Map.of(rec.getInputNames().iterator().next(), t))) {
                float[][][] pred = (float[][][]) res.get(0).getValue();
                // the list must name every class the model outputs, or characters are silently lost
                if (pred.length > 0 && pred[0].length > 0 && pred[0][0].length != chars.length)
                    throw new IllegalStateException("character list has " + chars.length + " classes, the model "
                            + pred[0][0].length + " - re-run tools/fetch_ocr_models.py");
                for (int i = beg; i < end; i++) {
                    float[][] seq = pred[i - beg];
                    StringBuilder sb = new StringBuilder();
                    double conf = 0;
                    int kept = 0, prev = -1;
                    for (float[] step : seq) {
                        int best = 0;
                        for (int c = 1; c < step.length; c++) if (step[c] > step[best]) best = c;
                        if (best != 0 && best != prev) {
                            sb.append(chars[best]);
                            conf += step[best];
                            kept++;
                        }
                        prev = best;
                    }
                    texts[idx[i]] = sb.toString();
                    scores[idx[i]] = kept == 0 ? 0 : conf / kept;
                }
            }
        }
    }
}
