package com.smartgazette.smartgazette.service;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** PP-OCR detection geometry (= RapidOCR DBPostProcess). CI-safe: no models. */
class PpOcrTest {

    /** a probability map with text-like blobs (value 0.9) on a 0 background */
    private static float[] map(int w, int h, int[]... rects) {
        float[] p = new float[w * h];
        for (int[] r : rects)
            for (int y = r[1]; y < r[3]; y++)
                for (int x = r[0]; x < r[2]; x++) p[y * w + x] = 0.9f;
        return p;
    }

    @Test
    void oneTextLineGivesOneGrownBox() {
        // a 60 x 10 line at (20, 30): grown by area * 1.6 / perimeter = 600 * 1.6 / 140 ~ 6.9 px a side
        List<double[][]> boxes = PpOcr.boxesFromMap(map(200, 100, new int[]{20, 30, 80, 40}), 200, 100, 200, 100);
        assertEquals(1, boxes.size());
        double[][] b = boxes.get(0);
        assertTrue(b[0][0] < 20 && b[0][0] > 8, "left " + b[0][0]);
        assertTrue(b[1][0] > 80 && b[1][0] < 92, "right " + b[1][0]);
        assertTrue(b[0][1] < 30 && b[3][1] > 40, "top/bottom " + b[0][1] + " " + b[3][1]);
    }

    @Test
    void linesAreSortedTopToBottomThenLeftToRight() {
        float[] p = map(300, 200, new int[]{150, 20, 250, 32}, new int[]{20, 22, 120, 34}, new int[]{20, 100, 120, 112});
        List<double[][]> boxes = PpOcr.boxesFromMap(p, 300, 200, 300, 200);
        assertEquals(3, boxes.size());
        assertTrue(boxes.get(0)[0][0] < boxes.get(1)[0][0], "same line: left first");
        assertTrue(boxes.get(2)[0][1] > 80, "the lower line last");
    }

    @Test
    void specksAndWeakRegionsAreDropped() {
        float[] p = map(100, 100, new int[]{10, 10, 12, 12});      // 2 x 2 speck: shorter side < 3
        for (int i = 0; i < 40 * 10; i++) p[50 * 100 + 40 + (i % 40) + (i / 40) * 100] = 0.35f;   // above 0.3, mean < 0.5
        assertTrue(PpOcr.boxesFromMap(p, 100, 100, 100, 100).isEmpty());
    }

    @Test
    void boxesScaleBackToTheImage() {
        // the map is half the image size: boxes come back in image pixels
        List<double[][]> boxes = PpOcr.boxesFromMap(map(100, 50, new int[]{10, 15, 60, 25}), 100, 50, 200, 100);
        assertEquals(1, boxes.size());
        assertTrue(boxes.get(0)[1][0] > 120, "right edge in image pixels " + boxes.get(0)[1][0]);
    }
}
