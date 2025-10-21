package com.smartgazette.smartgazette.repository;

import com.smartgazette.smartgazette.model.Gazette;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import java.util.List;

@Repository
public interface GazetteRepository extends JpaRepository<Gazette, Long> {
    List<Gazette> findAllByOrderBySourceOrderAsc();
}
