package com.smartgazette.smartgazette.model;

import jakarta.persistence.*;

import java.time.LocalDateTime;

/**
 * A sign that reference data may be out of date (docs/specs/reference-watch.md):
 * e.g. a notice cites a section our copy of the law does not have. Raised by
 * ReferenceWatch from what the system reads; it never changes reference data
 * itself - it says what to do. The same flag seen again only updates its count.
 */
@Entity
@Table(name = "reference_flag", uniqueConstraints = @UniqueConstraint(columnNames = {"dataSet", "refKey", "watcher", "detail"}))
public class ReferenceFlag {

    public enum Status { OPEN, DONE, DISMISSED }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** "law", "geography", ... */
    private String dataSet;
    /** e.g. "water_act" */
    private String refKey;
    /** e.g. "law.missing_section" */
    private String watcher;
    /** e.g. "s.158" */
    private String detail;

    @Column(columnDefinition = "TEXT")
    private String message;
    @Column(columnDefinition = "TEXT")
    private String action;
    @Column(columnDefinition = "TEXT")
    private String evidence;
    /** where it was first seen: PDF path + notice number */
    private String source;

    private int seenCount;
    private LocalDateTime firstSeen;
    private LocalDateTime lastSeen;

    @Enumerated(EnumType.STRING)
    private Status status = Status.OPEN;
    private LocalDateTime closedAt;

    public Long getId() { return id; }
    public String getDataSet() { return dataSet; }
    public void setDataSet(String dataSet) { this.dataSet = dataSet; }
    public String getRefKey() { return refKey; }
    public void setRefKey(String refKey) { this.refKey = refKey; }
    public String getWatcher() { return watcher; }
    public void setWatcher(String watcher) { this.watcher = watcher; }
    public String getDetail() { return detail; }
    public void setDetail(String detail) { this.detail = detail; }
    public String getMessage() { return message; }
    public void setMessage(String message) { this.message = message; }
    public String getAction() { return action; }
    public void setAction(String action) { this.action = action; }
    public String getEvidence() { return evidence; }
    public void setEvidence(String evidence) { this.evidence = evidence; }
    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }
    public int getSeenCount() { return seenCount; }
    public void setSeenCount(int seenCount) { this.seenCount = seenCount; }
    public LocalDateTime getFirstSeen() { return firstSeen; }
    public void setFirstSeen(LocalDateTime firstSeen) { this.firstSeen = firstSeen; }
    public LocalDateTime getLastSeen() { return lastSeen; }
    public void setLastSeen(LocalDateTime lastSeen) { this.lastSeen = lastSeen; }
    public Status getStatus() { return status; }
    public void setStatus(Status status) { this.status = status; }
    public LocalDateTime getClosedAt() { return closedAt; }
    public void setClosedAt(LocalDateTime closedAt) { this.closedAt = closedAt; }
}
