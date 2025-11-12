package com.smartgazette.smartgazette.model;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Column;

@Entity
@Table(name = "gazette")
public class Gazette {

    // --- Fields ---
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String title;
    private String category;
    private String link;
    private String noticeNumber;
    private String signatory;
    private Integer sourceOrder;
    private String gazetteVolume;
    private String gazetteNumber;
    private LocalDate gazetteDate;


    @Column(columnDefinition = "TEXT")
    private String content;

    @Column(columnDefinition = "TEXT")
    private String summary;


    @Column(length = 280)
    private String xSummary;

    @Column(columnDefinition = "TEXT")
    private String article;

    @Column(columnDefinition = "TEXT")
    private String actionableInfo;

    @Column(name = "published_date")
    private LocalDate publishedDate;

    @CreationTimestamp
    @Column(name = "system_published_at", updatable = false)
    private LocalDateTime systemPublishedAt;

    @UpdateTimestamp
    @Column(name = "last_updated_at")
    private LocalDateTime lastUpdatedAt;

    @Enumerated(EnumType.STRING) // This tells JPA to store the status as a readable string ("SUCCESS", "FAILED")
    private ProcessingStatus status;

    @Column(name = "original_pdf_path")
    private String originalPdfPath;

    // --- (METRIC COLLECTION) ---
    @Column(nullable = false, columnDefinition = "integer default 0")
    private int thumbsUp = 0;

    @Column(nullable = false, columnDefinition = "integer default 0")
    private int thumbsDown = 0;


    @Column(nullable = false, columnDefinition = "integer default 0")
    private int viewCount = 0;

    // --- Constructors ---

    // No-argument constructor (required by JPA)
    public Gazette() {
    }

    public Gazette(String title, String category, String link, String noticeNumber, String signatory, String content, String summary, String xSummary, String article, String actionableInfo, LocalDate publishedDate) {
        this.title = title;
        this.category = category;
        this.link = link;
        this.noticeNumber = noticeNumber;
        this.signatory = signatory;
        this.content = content;
        this.summary = summary;
        this.xSummary = xSummary;
        this.article = article;
        this.actionableInfo = actionableInfo;
        this.publishedDate = publishedDate;
    }


    // --- Getters and Setters ---

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public String getNoticeNumber() {
        return noticeNumber;
    }

    public void setNoticeNumber(String noticeNumber) {
        this.noticeNumber = noticeNumber;
    }

    public String getSignatory() {
        return signatory;
    }



    public void setSignatory(String signatory) {
        this.signatory = signatory;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }


    public String getXSummary() {
        return xSummary;
    }

    public void setXSummary(String xSummary) {
        this.xSummary = xSummary;
    }

    public String getArticle() {
        return article;
    }

    public void setArticle(String article) {
        this.article = article;
    }

    public String getActionableInfo() {
        return actionableInfo;
    }

    public void setActionableInfo(String actionableInfo) {
        this.actionableInfo = actionableInfo;
    }

    public LocalDate getPublishedDate() {
        return publishedDate;
    }

    public void setPublishedDate(LocalDate publishedDate) {
        this.publishedDate = publishedDate;
    }

    public LocalDateTime getSystemPublishedAt() {
        return systemPublishedAt;
    }

    public void setSystemPublishedAt(LocalDateTime systemPublishedAt) {
        this.systemPublishedAt = systemPublishedAt;
    }

    public LocalDateTime getLastUpdatedAt() {
        return lastUpdatedAt;
    }

    public void setLastUpdatedAt(LocalDateTime lastUpdatedAt) {
        this.lastUpdatedAt = lastUpdatedAt;
    }

    public Integer getSourceOrder() {
        return sourceOrder;
    }

    public void setSourceOrder(Integer sourceOrder) {
        this.sourceOrder = sourceOrder;
    }

    public ProcessingStatus getStatus() {
        return status;
    }

    public void setStatus(ProcessingStatus status) {
        this.status = status;
    }

    public String getGazetteVolume() {
        return gazetteVolume;
    }

    public void setGazetteVolume(String gazetteVolume) {
        this.gazetteVolume = gazetteVolume;
    }

    public String getGazetteNumber() {
        return gazetteNumber;
    }

    public void setGazetteNumber(String gazetteNumber) {
        this.gazetteNumber = gazetteNumber;
    }

    public LocalDate getGazetteDate() {
        return gazetteDate;
    }

    public void setGazetteDate(LocalDate gazetteDate) {
        this.gazetteDate = gazetteDate;
    }

    public int getThumbsUp() {
        return thumbsUp;
    }

    public void setThumbsUp(int thumbsUp) {
        this.thumbsUp = thumbsUp;
    }

    public int getThumbsDown() {
        return thumbsDown;
    }

    public void setThumbsDown(int thumbsDown) {
        this.thumbsDown = thumbsDown;
    }

    public String getOriginalPdfPath() {
        return originalPdfPath;
    }

    public void setOriginalPdfPath(String originalPdfPath) {
        this.originalPdfPath = originalPdfPath;
    }

    public int getViewCount() { return viewCount; }

    public void setViewCount(int viewCount) { this.viewCount = viewCount; }
}
