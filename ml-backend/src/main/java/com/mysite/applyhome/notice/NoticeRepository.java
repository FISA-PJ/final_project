package com.mysite.applyhome.notice;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDate;
import java.util.List;

public interface NoticeRepository extends JpaRepository<Notice, Long>, JpaSpecificationExecutor<Notice> {
    // 공고번호로 공고 찾기
    Notice findByNoticeNumber(String noticeNumber);
    
    // 제목으로 공고 검색
    List<Notice> findByNoticeTitleContaining(String keyword);
    
    // 지역으로 공고 검색
    List<Notice> findByLocationContaining(String location);
    
    // 공고 상태로 검색
    List<Notice> findByNoticeStatus(NoticeStatus status);
    
    // 접수 기간 내 공고 검색
    @Query("SELECT n FROM Notice n WHERE n.postDate <= :date AND n.applicationEndDate >= :date")
    List<Notice> findActiveNotices(@Param("date") LocalDate date);
    
    // 게시일 기준으로 공고 검색
    List<Notice> findByPostDateBetween(LocalDate startDate, LocalDate endDate);
    
    // 정정 공고 여부로 검색
    List<Notice> findByIsCorrection(Boolean isCorrection);
    
    // ID 목록으로 공고 검색 (페이징 처리)
    Page<Notice> findByIdIn(List<Long> ids, Pageable pageable);

    @Query("SELECT n FROM Notice n WHERE n.noticeTitle LIKE %:keyword%")
    Page<Notice> findByNoticeTitleContaining(@Param("keyword") String keyword, Pageable pageable);
}
