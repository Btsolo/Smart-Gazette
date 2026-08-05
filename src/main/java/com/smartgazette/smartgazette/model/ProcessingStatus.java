package com.smartgazette.smartgazette.model;

public enum ProcessingStatus {
    SUCCESS, // For successfully processed articles
    PARTIAL, // Extraction succeeded, generation pending/failed — extractedDataJson is retryable
    FAILED   // Failed before extraction completed — nothing structured to retry from
}