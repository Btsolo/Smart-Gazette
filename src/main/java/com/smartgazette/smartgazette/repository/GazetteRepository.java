package com.smartgazette.smartgazette.repository;

import com.smartgazette.smartgazette.model.Gazette;
import org.springframework.data.jpa.repository.JpaRepository;
import com.smartgazette.smartgazette.model.ProcessingStatus;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import java.util.List;


@Repository
public interface GazetteRepository extends JpaRepository<Gazette, Long> {

    // --- THIS IS THE FIX ---
    // As per T014 Report, we must sort by the gazette publication date first (newest first),
    // and THEN by the source order within that gazette.
    //
    // RENAMED the method to be simple, forcing Spring to use the @Query
    // and NOT parse the method name.
    @Query("SELECT g FROM Gazette g ORDER BY g.gazetteDate DESC, g.sourceOrder ASC, g.id ASC")
    List<Gazette> findAllWithCorrectSorting();
    // --- END OF FIX ---


    // NEW METHOD: Find all articles with a specific status, ordered correctly.
    // --- FIX: This query MUST match the main sorting order and has a simple name. ---
    @Query("SELECT g FROM Gazette g WHERE g.status = ?1 ORDER BY g.gazetteDate DESC, g.sourceOrder ASC, g.id ASC")
    List<Gazette> findAllWithStatusAndCorrectSorting(ProcessingStatus status);
}