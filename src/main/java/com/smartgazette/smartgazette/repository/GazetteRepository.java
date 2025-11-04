package com.smartgazette.smartgazette.repository;

import com.smartgazette.smartgazette.model.Gazette;
import org.springframework.data.jpa.repository.JpaRepository;
import com.smartgazette.smartgazette.model.ProcessingStatus;
import org.springframework.stereotype.Repository;
import java.util.List;


@Repository
public interface GazetteRepository extends JpaRepository<Gazette, Long> {
    List<Gazette> findAllByOrderBySourceOrderAsc();

    // NEW METHOD: Find all articles with a specific status, ordered correctly.
    List<Gazette> findAllByStatusOrderBySourceOrderAsc(ProcessingStatus status);
}
