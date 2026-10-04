package com.smartgazette.smartgazette.service;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Regression cases for TableExtractor (= tools/table_extract.py). CI-safe:
 * no corpus needed.
 */
class TableExtractorTest {

    @Test
    void emptyCellKeepsItsColumn() {
        // a ruled table's empty cell reaches the extractor as "a | | c" (fix 7b):
        // the cleaner squashes "a |  | c"
        List<TableExtractor.Table> t = TableExtractor.tables(
                "GAZETTE NOTICE NO. 5\nParcel | Owner | Area\nX/1 | | 0.10\nX/2 | Jane Doe | 0.20\n");
        assertEquals(List.of("Parcel", "Owner", "Area"), t.get(0).columns());
        assertEquals(List.of("X/1", "", "0.10"), t.get(0).rows().get(0));
        assertEquals(List.of("X/2", "Jane Doe", "0.20"), t.get(0).rows().get(1));
    }

    @Test
    void labelRowWithTrailingBarStaysInTheTable() {
        // a ruled row merged across the table (date / venue) arrives as "label |"
        List<TableExtractor.Table> t = TableExtractor.tables("GAZETTE NOTICE NO. 6\nParcel No. | Owner | Area\n--- | --- | ---\n"
                + "Tuesday, 1st July, 2025 at the Chief's Office |\nX/1 | Jane Doe | 0.10\n| John Roe | 0.20\n");
        assertEquals(1, t.size());
        assertEquals(List.of("Parcel No.", "Owner", "Area"), t.get(0).columns());
        assertEquals(List.of("Tuesday, 1st July, 2025 at the Chief's Office", "", ""), t.get(0).rows().get(0));
        assertEquals(List.of("", "John Roe", "0.20"), t.get(0).rows().get(2));
    }
}
