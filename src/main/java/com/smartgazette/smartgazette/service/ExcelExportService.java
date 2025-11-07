package com.smartgazette.smartgazette.service;

import com.smartgazette.smartgazette.model.Gazette;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

@Service
public class ExcelExportService {

    /**
     * Generates an Excel report from a list of Gazette notices.
     * PER FIX T014: This method now EXCLUDES the 'article' and 'content' fields
     * to prevent errors from exceeding the 32,767 character cell limit.
     */
    public ByteArrayInputStream generateExcelReport(List<Gazette> gazettes) {
        // We use try-with-resources to ensure the workbook is closed properly
        try (Workbook workbook = new XSSFWorkbook(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {

            // Create a new sheet in the workbook
            Sheet sheet = workbook.createSheet("Gazettes");

            // --- FIX: Define ONLY exportable fields (no article, content) ---
            String[] headers = {
                    "ID", "Title", "Summary", "Category", "Status",
                    "Notice Number", "Gazette Date", "Gazette Number",
                    "Actionable Info", "Published Date", "Signatory"
            };

            // Create a header row
            Row headerRow = sheet.createRow(0);
            for (int col = 0; col < headers.length; col++) {
                Cell cell = headerRow.createCell(col);
                cell.setCellValue(headers[col]);
            }

            // --- FIX: Populate data rows (EXCLUDING article and content) ---
            int rowIdx = 1;
            for (Gazette gazette : gazettes) {
                Row row = sheet.createRow(rowIdx++);

                row.createCell(0).setCellValue(gazette.getId() != null ? gazette.getId() : 0L);
                row.createCell(1).setCellValue(gazette.getTitle() != null ? gazette.getTitle() : "");
                row.createCell(2).setCellValue(gazette.getSummary() != null ? gazette.getSummary() : "");
                row.createCell(3).setCellValue(gazette.getCategory() != null ? gazette.getCategory() : "");
                row.createCell(4).setCellValue(gazette.getStatus() != null ? gazette.getStatus().toString() : "");
                row.createCell(5).setCellValue(gazette.getNoticeNumber() != null ? gazette.getNoticeNumber() : "");
                row.createCell(6).setCellValue(gazette.getGazetteDate() != null ? gazette.getGazetteDate().toString() : "");
                row.createCell(7).setCellValue(gazette.getGazetteNumber() != null ? gazette.getGazetteNumber() : "");

                // --- FIX: Add truncation safety for Actionable Info ---
                String actionableInfo = gazette.getActionableInfo();
                if (actionableInfo != null && actionableInfo.length() > 32000) {
                    actionableInfo = actionableInfo.substring(0, 31997) + "...";
                }
                row.createCell(8).setCellValue(actionableInfo != null ? actionableInfo : "");

                row.createCell(9).setCellValue(gazette.getPublishedDate() != null ? gazette.getPublishedDate().toString() : "");
                row.createCell(10).setCellValue(gazette.getSignatory() != null ? gazette.getSignatory() : "");
            }

            // Auto-size columns for readability
            for (int i = 0; i < headers.length; i++) {
                sheet.autoSizeColumn(i);
            }

            // Write the workbook to an in-memory output stream
            workbook.write(out);

            // Return an InputStream from the in-memory byte array
            return new ByteArrayInputStream(out.toByteArray());
        } catch (IOException e) {
            // Updated error message for clarity
            throw new RuntimeException("Failed to generate Excel file: " + e.getMessage(), e);
        }
    }
}