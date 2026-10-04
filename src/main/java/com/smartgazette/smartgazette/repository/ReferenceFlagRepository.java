package com.smartgazette.smartgazette.repository;

import com.smartgazette.smartgazette.model.ReferenceFlag;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface ReferenceFlagRepository extends JpaRepository<ReferenceFlag, Long> {

    Optional<ReferenceFlag> findByDataSetAndRefKeyAndWatcherAndDetail(String dataSet, String refKey, String watcher, String detail);

    List<ReferenceFlag> findByStatusOrderByFirstSeenAsc(ReferenceFlag.Status status);
}
