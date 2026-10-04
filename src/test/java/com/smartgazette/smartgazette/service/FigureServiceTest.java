package com.smartgazette.smartgazette.service;

import org.junit.jupiter.api.Test;

import java.awt.image.BufferedImage;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Figure capture helpers (docs/specs/figures.md). CI-safe: no OCR engine, no PDF. */
class FigureServiceTest {

    @Test
    void ocrLinesAreReadRowByRow() {
        // 2023 No 154 p37: "Bonus" sits a few pixels lower than its amounts but is in their row
        List<PpOcr.Line> lines = List.of(
                new PpOcr.Line(300, 10, 400, 30, "25,911,449", 0.99),
                new PpOcr.Line(10, 12, 120, 33, "Agency Income", 0.99),
                new PpOcr.Line(10, 52, 60, 72, "Bonus", 0.99),
                new PpOcr.Line(300, 50, 400, 70, "3,224,208", 0.99));
        assertEquals("Agency Income 25,911,449\nBonus 3,224,208", FigureService.readingOrder(lines));
    }

    @Test
    void placementsAreReadFromTheExtractorList() throws Exception {
        List<FigureService.Placement> p = FigureService.readPlacements(
                "[{\"id\":\"1.1\",\"page\":1,\"x0\":211.72,\"y0\":73,\"x1\":417.16,\"y1\":263.56,\"src\":\"source-5\",\"px\":[613,569],\"pageHeight\":842}]");
        assertEquals(1, p.size());
        assertEquals("1.1", p.get(0).id());
        assertEquals(613, p.get(0).wPx());
        assertEquals(569, p.get(0).hPx());
    }

    @Test
    void noticeTextKeyedByItsNumber() {
        Map<String, String> n = FigureService.noticeTexts("cover\nGAZETTE NOTICE NO. 12 first\nGAZETTE NOTICE NO. 13 second");
        assertEquals("GAZETTE NOTICE NO. 12 first\n", n.get("12"));
        assertTrue(n.get("13").endsWith("second"));
    }

    @Test
    void cleanupWhitensBrownPaperAndKeepsTheLines() {
        // brown paper (grey 170) with a dark line (40) across it
        BufferedImage img = new BufferedImage(200, 100, BufferedImage.TYPE_INT_RGB);
        for (int y = 0; y < 100; y++)
            for (int x = 0; x < 200; x++) {
                int v = (y >= 48 && y <= 51) ? 40 : 170;
                img.setRGB(x, y, (v << 16) | (v << 8) | v);
            }
        assertTrue(FigureService.paperTone(img) > 25);
        BufferedImage out = FigureService.clean(img, 0);
        assertEquals(400, out.getWidth());               // small scans 2x
        int paper = out.getRaster().getSample(20, 20, 0), line = out.getRaster().getSample(20, 99, 0);
        assertTrue(paper > 240, "paper " + paper);
        assertTrue(line < 100, "line " + line);
    }

    @Test
    void upsideDownCopyIsTurned() {
        BufferedImage img = new BufferedImage(3, 2, BufferedImage.TYPE_INT_RGB);
        img.setRGB(0, 0, 0xFFFFFF);
        assertEquals(0xFFFFFF, FigureService.rotate180(img).getRGB(2, 1) & 0xFFFFFF);
    }
}
