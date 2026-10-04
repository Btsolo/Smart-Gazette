package com.smartgazette.smartgazette.repository;

import com.smartgazette.smartgazette.model.NoticeFigure;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface NoticeFigureRepository extends JpaRepository<NoticeFigure, Long> {

    List<NoticeFigure> findByOriginalPdfPathAndNoticeNumberOrderByPageAscIdAsc(String originalPdfPath, String noticeNumber);

    boolean existsByOriginalPdfPath(String originalPdfPath);

    @Transactional
    @Modifying
    void deleteAllByOriginalPdfPath(String originalPdfPath);
}
