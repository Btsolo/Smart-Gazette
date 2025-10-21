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

    public ByteArrayInputStream generateExcelReport(List<Gazette> gazettes) {
        // We use try-with-resources to ensure the workbook is closed properly
        try (Workbook workbook = new XSSFWorkbook(); ByteArrayOutputStream out = new ByteArrayOutputStream()) {

            // Create a new sheet in the workbook
            Sheet sheet = workbook.createSheet("Gazettes");

            // Create a header row
            Row headerRow = sheet.createRow(0);
            String[] headers = {
                    "ID", "Notice Number", "Title", "Category", "Official Date", "Signatory",
                    "Summary", "X (Twitter) Summary", "Full Article", "Actionable Info"
            };
            for (int col = 0; col < headers.length; col++) {
                Cell cell = headerRow.createCell(col);
                cell.setCellValue(headers[col]);
            }

            // Populate the data rows
            int rowIdx = 1;
            for (Gazette gazette : gazettes) {
                Row row = sheet.createRow(rowIdx++);
                row.createCell(0).setCellValue(gazette.getId());
                row.createCell(1).setCellValue(gazette.getNoticeNumber());
                row.createCell(2).setCellValue(gazette.getTitle());
                row.createCell(3).setCellValue(gazette.getCategory());
                row.createCell(4).setCellValue(gazette.getPublishedDate().toString());
                row.createCell(5).setCellValue(gazette.getSignatory());
                row.createCell(6).setCellValue(gazette.getSummary());
                row.createCell(7).setCellValue(gazette.getXSummary());
                row.createCell(8).setCellValue(gazette.getArticle());
                row.createCell(9).setCellValue(gazette.getActionableInfo());
            }

            // Write the workbook to an in-memory output stream
            workbook.write(out);

            // Return an InputStream from the in-memory byte array
            return new ByteArrayInputStream(out.toByteArray());
        } catch (IOException e) {
            throw new RuntimeException("Failed to generate Excel file: " + e.getMessage());
        }
    }
}