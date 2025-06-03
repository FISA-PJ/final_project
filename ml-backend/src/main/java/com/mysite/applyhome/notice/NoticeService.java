package com.mysite.applyhome.notice;

import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Service;
import jakarta.persistence.criteria.Subquery;
import jakarta.persistence.criteria.Root;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

@RequiredArgsConstructor
@Service
@Transactional(readOnly = true)
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

    public Page<Notice> getFilteredNotices(int page, String region, String area, String price, String moveInDate) {
        Pageable pageable = PageRequest.of(page, 10);
        Specification<Notice> spec = Specification.where(null);

        // 지역 필터링
        if (region != null && !region.isEmpty() && !region.equals("전체")) {
            spec = spec.and((root, query, cb) -> cb.like(root.get("location"), "%" + region + "%"));
        }

        // 전용면적 필터링
        if (area != null && !area.isEmpty() && !area.equals("전체")) {
            spec = spec.and((root, query, cb) -> {
                Subquery<Long> subquery = query.subquery(Long.class);
                Root<HouseType> houseTypeRoot = subquery.from(HouseType.class);
                
                switch (area) {
                    case "under60":
                        subquery.select(houseTypeRoot.get("noticeId"))
                               .where(cb.and(
                                   cb.equal(houseTypeRoot.get("noticeId"), root.get("id")),
                                   cb.lessThan(houseTypeRoot.get("exclusiveArea"), new BigDecimal("60.00"))
                               ));
                        break;
                    case "60to84":
                        subquery.select(houseTypeRoot.get("noticeId"))
                               .where(cb.and(
                                   cb.equal(houseTypeRoot.get("noticeId"), root.get("id")),
                                   cb.greaterThanOrEqualTo(houseTypeRoot.get("exclusiveArea"), new BigDecimal("60.00")),
                                   cb.lessThan(houseTypeRoot.get("exclusiveArea"), new BigDecimal("85.00"))
                               ));
                        break;
                    case "85to100":
                        subquery.select(houseTypeRoot.get("noticeId"))
                               .where(cb.and(
                                   cb.equal(houseTypeRoot.get("noticeId"), root.get("id")),
                                   cb.greaterThanOrEqualTo(houseTypeRoot.get("exclusiveArea"), new BigDecimal("85.00")),
                                   cb.lessThan(houseTypeRoot.get("exclusiveArea"), new BigDecimal("100.00"))
                               ));
                        break;
                    case "over100":
                        subquery.select(houseTypeRoot.get("noticeId"))
                               .where(cb.and(
                                   cb.equal(houseTypeRoot.get("noticeId"), root.get("id")),
                                   cb.greaterThanOrEqualTo(houseTypeRoot.get("exclusiveArea"), new BigDecimal("100.00"))
                               ));
                        break;
                }
                return cb.exists(subquery);
            });
        }

        // 분양가 필터링
        if (price != null && !price.isEmpty() && !price.equals("전체")) {
            spec = spec.and((root, query, cb) -> {
                Subquery<Long> subquery = query.subquery(Long.class);
                Root<HouseType> houseTypeRoot = subquery.from(HouseType.class);
                
                switch (price) {
                    case "under5":
                        subquery.select(houseTypeRoot.get("noticeId"))
                               .where(cb.and(
                                   cb.equal(houseTypeRoot.get("noticeId"), root.get("id")),
                                   cb.lessThan(houseTypeRoot.get("avgPrice"), new BigDecimal("500000000"))
                               ));
                        break;
                    case "5to7":
                        subquery.select(houseTypeRoot.get("noticeId"))
                               .where(cb.and(
                                   cb.equal(houseTypeRoot.get("noticeId"), root.get("id")),
                                   cb.greaterThanOrEqualTo(houseTypeRoot.get("avgPrice"), new BigDecimal("500000000")),
                                   cb.lessThan(houseTypeRoot.get("avgPrice"), new BigDecimal("700000000"))
                               ));
                        break;
                    case "7to10":
                        subquery.select(houseTypeRoot.get("noticeId"))
                               .where(cb.and(
                                   cb.equal(houseTypeRoot.get("noticeId"), root.get("id")),
                                   cb.greaterThanOrEqualTo(houseTypeRoot.get("avgPrice"), new BigDecimal("700000000")),
                                   cb.lessThan(houseTypeRoot.get("avgPrice"), new BigDecimal("1000000000"))
                               ));
                        break;
                    case "over10":
                        subquery.select(houseTypeRoot.get("noticeId"))
                               .where(cb.and(
                                   cb.equal(houseTypeRoot.get("noticeId"), root.get("id")),
                                   cb.greaterThanOrEqualTo(houseTypeRoot.get("avgPrice"), new BigDecimal("1000000000"))
                               ));
                        break;
                }
                return cb.exists(subquery);
            });
        }

        // 입주예정 필터링
        if (moveInDate != null && !moveInDate.isEmpty() && !moveInDate.equals("전체")) {
            spec = spec.and((root, query, cb) -> {
                if (moveInDate.equals("2028년 이후")) {
                    return cb.and(
                        cb.not(cb.like(root.get("moveInDate"), "%2025%")),
                        cb.not(cb.like(root.get("moveInDate"), "%2026%")),
                        cb.not(cb.like(root.get("moveInDate"), "%2027%"))
                    );
                } else {
                    return cb.like(root.get("moveInDate"), "%" + moveInDate + "%");
                }
            });
        }

        return noticeRepository.findAll(spec, pageable);
    }

    public Page<Notice> searchNotices(String keyword, Pageable pageable) {
        return noticeRepository.findByNoticeTitleContaining(keyword, pageable);
    }
}