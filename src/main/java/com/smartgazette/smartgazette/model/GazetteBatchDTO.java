package com.smartgazette.smartgazette.model;

import java.time.LocalDate;

// A DTO (Data Transfer Object) to hold the results of our custom query
public class GazetteBatchDTO {
    private String originalPdfPath;
    private LocalDate gazetteDate;
    private String gazetteNumber;
    private long noticeCount;
    private long failedCount;

    public GazetteBatchDTO(String originalPdfPath, LocalDate gazetteDate, String gazetteNumber, long noticeCount, long failedCount) {
        this.originalPdfPath = originalPdfPath;
        this.gazetteDate = gazetteDate;
        this.gazetteNumber = gazetteNumber;
        this.noticeCount = noticeCount;
        this.failedCount = failedCount;
    }

    // Getters
    public String getOriginalPdfPath() { return originalPdfPath; }
    public LocalDate getGazetteDate() { return gazetteDate; }
    public String getGazetteNumber() { return gazetteNumber; }
    public long getNoticeCount() { return noticeCount; }
    public long getFailedCount() { return failedCount; }
}