package com.smartgazette.smartgazette.model;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;

import java.time.LocalDateTime;

/**
 * One image a gazette prints (docs/specs/figures.md, Stage B): a map, chart,
 * table image, prescribed image, party symbol, coat of arms, stamp, logo...
 *
 * Every image is kept, whatever its kind - a stamp or seal can carry
 * authentication. It is tied to its notice the way notices are tied to their
 * PDF: by {@code originalPdfPath} + {@code noticeNumber} (null for the cover).
 * The original crop is always kept; {@code cleanFilePath} is a cleaned copy of
 * a faded / brown-paper scan, shown beside it, never instead of it.
 */
@Entity
@Table(name = "notice_figure", indexes = {
        @Index(name = "idx_figure_notice", columnList = "originalPdfPath, noticeNumber")})
public class NoticeFigure {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String originalPdfPath;
    private String noticeNumber;

    /** "p.k": the k-th image on page p, as the marker [[FIGURE:p.k]] in the notice text. */
    private String figureId;
    private int page;
    private String kind;
    private boolean inTable;

    /** Place on the page, in PDF points from the top-left corner. */
    private double x0, y0, x1, y1;

    /** Size of the embedded image in pixels. */
    private Integer widthPx, heightPx;

    @Column(columnDefinition = "TEXT")
    private String ocrText;

    /** Degrees the cleaned copy is turned (180 for a map scanned upside down). */
    private int rotate;

    /** Paths relative to the figures directory. */
    private String filePath;
    private String cleanFilePath;

    @CreationTimestamp
    private LocalDateTime createdAt;

    /** The kinds a reader looks at with the article; the rest are listed as "other images". */
    public boolean isMeaningful() {
        return switch (kind == null ? "" : kind) {
            case "map", "chart", "table_image", "prescribed_image", "form", "photo", "page_scan", "table_symbol" -> true;
            default -> false;
        };
    }

    /** Kind as words for the page ("table_image" -> "Table image"). */
    public String getKindLabel() {
        if (kind == null || kind.isEmpty()) return "";
        String s = kind.replace('_', ' ');
        return Character.toUpperCase(s.charAt(0)) + s.substring(1);
    }

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getOriginalPdfPath() { return originalPdfPath; }
    public void setOriginalPdfPath(String originalPdfPath) { this.originalPdfPath = originalPdfPath; }
    public String getNoticeNumber() { return noticeNumber; }
    public void setNoticeNumber(String noticeNumber) { this.noticeNumber = noticeNumber; }
    public String getFigureId() { return figureId; }
    public void setFigureId(String figureId) { this.figureId = figureId; }
    public int getPage() { return page; }
    public void setPage(int page) { this.page = page; }
    public String getKind() { return kind; }
    public void setKind(String kind) { this.kind = kind; }
    public boolean isInTable() { return inTable; }
    public void setInTable(boolean inTable) { this.inTable = inTable; }
    public double getX0() { return x0; }
    public void setX0(double x0) { this.x0 = x0; }
    public double getY0() { return y0; }
    public void setY0(double y0) { this.y0 = y0; }
    public double getX1() { return x1; }
    public void setX1(double x1) { this.x1 = x1; }
    public double getY1() { return y1; }
    public void setY1(double y1) { this.y1 = y1; }
    public Integer getWidthPx() { return widthPx; }
    public void setWidthPx(Integer widthPx) { this.widthPx = widthPx; }
    public Integer getHeightPx() { return heightPx; }
    public void setHeightPx(Integer heightPx) { this.heightPx = heightPx; }
    public String getOcrText() { return ocrText; }
    public void setOcrText(String ocrText) { this.ocrText = ocrText; }
    public int getRotate() { return rotate; }
    public void setRotate(int rotate) { this.rotate = rotate; }
    public String getFilePath() { return filePath; }
    public void setFilePath(String filePath) { this.filePath = filePath; }
    public String getCleanFilePath() { return cleanFilePath; }
    public void setCleanFilePath(String cleanFilePath) { this.cleanFilePath = cleanFilePath; }
    public LocalDateTime getCreatedAt() { return createdAt; }
}
