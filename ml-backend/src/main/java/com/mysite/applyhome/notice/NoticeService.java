package com.mysite.applyhome.notice;

import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;

@RequiredArgsConstructor
@Service
public class NoticeService {
    private final NoticeRepository noticeRepository;
    private final SupplyTypeRepository supplyTypeRepository;

    // 모든 공고 목록 조회 (페이징 처리)
    public Page<Notice> getList(int page) {
        Pageable pageable = PageRequest.of(page, 100, Sort.by("postDate").descending());
        return this.noticeRepository.findAll(pageable);
    }

    // 공고 상세 조회
    public Notice getNotice(Long id) {
        return this.noticeRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("해당 공고가 없습니다."));
    }

    // 공고번호로 공고 조회
    public Notice getNoticeByNumber(String noticeNumber) {
        return this.noticeRepository.findByNoticeNumber(noticeNumber);
    }

    // 제목으로 공고 검색
    public List<Notice> searchByTitle(String keyword) {
        return this.noticeRepository.findByNoticeTitleContaining(keyword);
    }

    // 지역으로 공고 검색
    public List<Notice> searchByLocation(String location) {
        return this.noticeRepository.findByLocationContaining(location);
    }

    // 접수중인 공고 조회
    public List<Notice> getActiveNotices() {
        return this.noticeRepository.findActiveNotices(LocalDate.now());
    }

    // 공고 상태로 검색
    public List<Notice> getNoticesByStatus(NoticeStatus status) {
        return this.noticeRepository.findByNoticeStatus(status);
    }

    // 게시일 기준으로 공고 검색
    public List<Notice> getNoticesByDateRange(LocalDate startDate, LocalDate endDate) {
        return this.noticeRepository.findByPostDateBetween(startDate, endDate);
    }

    // 사용자 유형에 따른 공고 조회
    public Page<Notice> getNoticesByPrimeType(String primeType, int page) {
        Pageable pageable = PageRequest.of(page, 10, Sort.by("postDate").descending());
        
        List<Long> noticeIds;
        if ("특별공급유형 아님".equals(primeType)) {
            noticeIds = supplyTypeRepository.findNoticeIdsForNonSpecialSupply();
        } else {
            noticeIds = supplyTypeRepository.findNoticeIdsByPrimeType(primeType);
        }
        
        if (noticeIds.isEmpty()) {
            return Page.empty(pageable);
        }
        
        return noticeRepository.findByIdIn(noticeIds, pageable);
    }
}