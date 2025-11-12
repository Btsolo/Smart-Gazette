package com.smartgazette.smartgazette.repository;

import com.smartgazette.smartgazette.model.Gazette;
import org.springframework.data.jpa.repository.JpaRepository;
import com.smartgazette.smartgazette.model.ProcessingStatus;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;


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
    @Query("SELECT g FROM Gazette g WHERE g.status = 'SUCCESS' ORDER BY g.gazetteDate DESC, g.sourceOrder ASC, g.id ASC")
    Page<Gazette> findAllSuccessfulWithCorrectSorting(Pageable pageable);

    // NEW METHOD for Phase 2.7
    List<Gazette> findAllByGazetteDateAndOriginalPdfPathIsNull(LocalDate gazetteDate);

    // !!! ADD THIS NEW METHOD FOR THE RETRY FEATURE !!!
    @Query("SELECT g FROM Gazette g WHERE g.status = 'FAILED' ORDER BY g.gazetteDate DESC, g.sourceOrder ASC, g.id ASC")
    List<Gazette> findAllFailedWithCorrectSorting();

    // !!! ADD THIS NEW METHOD FOR THE SCRAPER !!!
    List<Gazette> findAllByGazetteNumber(String gazetteNumber);

    // !!! ADD THIS NEW, MORE PRECISE METHOD FOR THE SCRAPER !!!
    Optional<Gazette> findFirstByGazetteNumberAndGazetteDate(String gazetteNumber, LocalDate gazetteDate);

}