package com.smartgazette.smartgazette.repository;

import com.smartgazette.smartgazette.model.Gazette;
import com.smartgazette.smartgazette.model.ProcessingStatus;
import com.smartgazette.smartgazette.model.GazetteBatchDTO; // <-- NEW
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.stereotype.Repository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import jakarta.transaction.Transactional;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;


@Repository
public interface GazetteRepository extends JpaRepository<Gazette, Long> {

    // Used by the Admin Content page when not paginated
    @Query("SELECT g FROM Gazette g ORDER BY g.gazetteDate DESC, g.sourceOrder ASC, g.id ASC")
    List<Gazette> findAllWithCorrectSorting();

    // Used by retry feature to find all FAILED notices
    @Query("SELECT g FROM Gazette g WHERE g.status = 'FAILED' ORDER BY g.gazetteDate DESC, g.sourceOrder ASC, g.id ASC")
    List<Gazette> findAllFailedWithCorrectSorting(); // <-- FIXES YOUR ERROR

    // --- PAGINATION METHODS ---
    @Query("SELECT g FROM Gazette g WHERE g.status = 'SUCCESS' ORDER BY g.gazetteDate DESC, g.sourceOrder ASC, g.id ASC")
    Page<Gazette> findAllSuccessfulWithCorrectSorting(Pageable pageable);

    @Query("SELECT g FROM Gazette g WHERE g.category = ?1 AND g.status = 'SUCCESS' ORDER BY g.gazetteDate DESC, g.sourceOrder ASC, g.id ASC")
    Page<Gazette> findAllSuccessfulByCategory(String category, Pageable pageable);

    // --- SCRAPING & DUPLICATE CHECK METHODS ---
    List<Gazette> findAllByGazetteNumber(String gazetteNumber);
    Optional<Gazette> findFirstByGazetteNumberAndGazetteDate(String gazetteNumber, LocalDate gazetteDate);

    // --- BATCH MANAGEMENT METHODS (RESOLVES DTO ERRORS) ---
    @Query("SELECT new com.smartgazette.smartgazette.model.GazetteBatchDTO(" +
            "g.originalPdfPath, g.gazetteDate, g.gazetteNumber, COUNT(g), " +
            "SUM(CASE WHEN g.status = 'FAILED' THEN 1 ELSE 0 END)) " +
            "FROM Gazette g WHERE g.originalPdfPath IS NOT NULL " +
            "GROUP BY g.originalPdfPath, g.gazetteDate, g.gazetteNumber " +
            "ORDER BY g.gazetteDate DESC, g.gazetteNumber DESC")
    List<GazetteBatchDTO> findGazetteBatches();

    @Transactional
    @Modifying
    void deleteAllByOriginalPdfPath(String originalPdfPath);
}