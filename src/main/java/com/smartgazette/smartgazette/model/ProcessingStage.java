package com.smartgazette.smartgazette.model;

public enum ProcessingStage {
    TRIAGED,      // category known, nothing extracted yet
    EXTRACTED,    // structured fields extracted, no article yet
    GENERATED     // article written — fully complete
}
