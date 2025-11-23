package com.smartgazette.smartgazette.service;

import com.smartgazette.smartgazette.model.Gazette;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

@Service
public class ExcelExportService {

    public ByteArrayInputStream generateExcelReport(List<Gazette> gazettes) {
        try (Workbook workbook = new XSSFWorkbook(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {

            Sheet sheet = workbook.createSheet("Gazettes");

            // --- NEW: Extended Headers with ALL Info ---
            String[] headers = {
                    "ID", "Title", "Category", "Status", "Significance",
                    "Notice #", "Gazette Date", "Gazette Vol", "Signatory",
                    "Summary", "X-Summary", "Actionable Info",
                    "Article (Full)", "Original Content",
                    "Views", "Thumbs Up", "Thumbs Down",
                    "Created At", "Last Updated"
            };

            // Create Header Row with Bold Font
            Row headerRow = sheet.createRow(0);
            CellStyle headerStyle = workbook.createCellStyle();
            Font font = workbook.createFont();
            font.setBold(true);
            headerStyle.setFont(font);

            for (int col = 0; col < headers.length; col++) {
                Cell cell = headerRow.createCell(col);
                cell.setCellValue(headers[col]);
                cell.setCellStyle(headerStyle);
            }

            // Populate Data
            int rowIdx = 1;
            for (Gazette g : gazettes) {
                Row row = sheet.createRow(rowIdx++);

                row.createCell(0).setCellValue(g.getId() != null ? g.getId() : 0);
                row.createCell(1).setCellValue(safeStr(g.getTitle()));
                row.createCell(2).setCellValue(safeStr(g.getCategory()));
                row.createCell(3).setCellValue(g.getStatus() != null ? g.getStatus().toString() : "");
                row.createCell(4).setCellValue(g.getSignificanceRating());

                row.createCell(5).setCellValue(safeStr(g.getNoticeNumber()));
                row.createCell(6).setCellValue(g.getGazetteDate() != null ? g.getGazetteDate().toString() : "");
                row.createCell(7).setCellValue(safeStr(g.getGazetteVolume()));
                row.createCell(8).setCellValue(safeStr(g.getSignatory()));

                // Content Fields (Truncated to 32k chars to prevent Excel crash)
                row.createCell(9).setCellValue(truncate(g.getSummary()));
                row.createCell(10).setCellValue(truncate(g.getXSummary()));
                row.createCell(11).setCellValue(truncate(g.getActionableInfo()));
                row.createCell(12).setCellValue(truncate(g.getArticle()));
                row.createCell(13).setCellValue(truncate(g.getContent()));

                // Metrics
                row.createCell(14).setCellValue(g.getViewCount());
                row.createCell(15).setCellValue(g.getThumbsUp());
                row.createCell(16).setCellValue(g.getThumbsDown());

                // Timestamps
                row.createCell(17).setCellValue(g.getSystemPublishedAt() != null ? g.getSystemPublishedAt().toString() : "");
                row.createCell(18).setCellValue(g.getLastUpdatedAt() != null ? g.getLastUpdatedAt().toString() : "");
            }

            workbook.write(out);
            return new ByteArrayInputStream(out.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException("Failed to generate Excel file: " + e.getMessage(), e);
        }
    }

    // Helper to handle nulls
    private String safeStr(String s) {
        return s != null ? s : "";
    }

    // Helper to prevent Excel cell overflow (max 32,767 chars)
    private String truncate(String s) {
        if (s == null) return "";
        if (s.length() > 32000) return s.substring(0, 32000) + "...[TRUNCATED]";
        return s;
    }
}