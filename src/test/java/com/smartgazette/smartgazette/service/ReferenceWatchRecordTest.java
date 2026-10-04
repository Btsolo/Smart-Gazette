package com.smartgazette.smartgazette.service;

import com.smartgazette.smartgazette.model.ReferenceFlag;
import com.smartgazette.smartgazette.repository.ReferenceFlagRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Flags are stored once, counted when seen again, and logged once. Needs the database. */
@SpringBootTest(properties = {"scraper.enabled=false", "retry.enabled=false"})
class ReferenceWatchRecordTest {

    @Autowired
    private ReferenceWatch watch;
    @Autowired
    private ReferenceFlagRepository repo;

    private static final String NOTICE = "IN EXERCISE of the powers under section 72 of the Water Act, as amended by the Water (Amendment) Act, 2099.";

    @AfterEach
    void clean() {
        repo.findByDataSetAndRefKeyAndWatcherAndDetail("law", "water_act", "law.amended_after_copy", "Water (Amendment) Act, 2099")
                .ifPresent(repo::delete);
    }

    @Test
    void aFlagIsStoredOnceAndCounted() {
        watch.watch(List.of(NOTICE), "test.pdf");
        watch.watch(List.of(NOTICE), "test.pdf");
        ReferenceFlag f = repo.findByDataSetAndRefKeyAndWatcherAndDetail("law", "water_act", "law.amended_after_copy",
                "Water (Amendment) Act, 2099").orElseThrow();
        assertEquals(2, f.getSeenCount());
        assertEquals(ReferenceFlag.Status.OPEN, f.getStatus());
        assertEquals("test.pdf #1", f.getSource());
    }
}
