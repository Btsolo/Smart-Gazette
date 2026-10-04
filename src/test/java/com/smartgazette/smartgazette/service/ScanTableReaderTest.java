package com.smartgazette.smartgazette.service;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Regression cases for ScanTableReader (= tools/scan_tables.py), the same as
 * in tools/audit.py. CI-safe: synthetic word boxes, rules and image.
 */
class ScanTableReaderTest {

    private static final String HEAD = "level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext\n";

    private static String word(int line, int x, int y, String text, int w) {
        return "5\t1\t1\t1\t" + line + "\t1\t" + x + "\t" + y + "\t" + w + "\t20\t90\t" + text + "\n";
    }

    private static String word(int line, int x, int y, String text) {
        return word(line, x, y, text, 12 * text.length());
    }

    @Test
    void ruledRowsJoinWrappedCellsAndDropRuleCharacters() {
        List<double[]> h = new ArrayList<>(), v = new ArrayList<>();
        for (double y : new double[]{100, 140, 220, 260}) h.add(new double[]{y, 50, 650});
        for (double x : new double[]{50, 250, 450, 650}) v.add(new double[]{x, 100, 260});
        ScanTableReader.Rules rules = new ScanTableReader.Rules(h, v, 2400, 3400);
        String tsv = HEAD + word(1, 60, 110, "Parcel") + word(1, 260, 110, "Owner") + word(1, 460, 110, "Area")
                + word(2, 200, 150, "1319]", 45) + word(2, 240, 150, "|") + word(2, 260, 150, "Jane") + word(2, 320, 150, "Doe")
                + word(2, 460, 150, "0.10")
                + word(3, 260, 185, "and") + word(3, 310, 185, "John") + word(3, 370, 185, "Roe")
                + word(4, 60, 230, "1320") + word(4, 260, 230, "Jim") + word(4, 320, 230, "Doe") + word(4, 460, 230, "0.20");
        assertEquals("Parcel | Owner | Area\n1319 | Jane Doe and John Roe | 0.10\n1320 | Jim Doe | 0.20",
                ScanTableReader.pageText(ScanTableReader.readWords(tsv), rules));
    }

    @Test
    void noCellsOutsideTables() {
        String tsv = HEAD + word(1, 60, 110, "hereof,") + word(1, 160, 110, "|") + word(1, 180, 110, "shall") + word(1, 260, 110, "issue")
                + word(2, 60, 150, "NOTICE") + word(2, 160, 150, "NO.") + word(2, 220, 150, "|") + word(2, 240, 150, "2057")
                + word(3, 60, 190, "district") + word(3, 160, 190, "of") + word(3, 200, 190, "|") + word(3, 220, 190, "Kakamega");
        assertEquals("hereof, I shall issue\nNOTICE NO. |2057\ndistrict of Kakamega",
                ScanTableReader.pageText(ScanTableReader.readWords(tsv), null));
    }

    @Test
    void rulingLinesFoundInAnImage() {
        int[][] g = new int[400][600];
        for (int[] row : g) java.util.Arrays.fill(row, 255);
        for (int y : new int[]{50, 150, 250}) for (int x = 50; x < 550; x++) g[y][x] = 0;
        for (int x : new int[]{50, 300, 550}) for (int y = 50; y <= 250; y++) g[y][x] = 0;
        ScanTableReader.Rules r = ScanTableReader.imageRules(g, 1, 150);
        assertEquals(3, r.h().size());
        assertEquals(3, r.v().size());
    }
}
