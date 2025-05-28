package com.mysite.applyhome.notice;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import java.util.List;

public interface SupplyTypeRepository extends JpaRepository<SupplyType, Long> {
    @Query("SELECT DISTINCT st.noticeId FROM SupplyType st WHERE st.supplyType LIKE %:primeType%")
    List<Long> findNoticeIdsByPrimeType(@Param("primeType") String primeType);

    @Query("SELECT DISTINCT st.noticeId FROM SupplyType st WHERE st.supplyType LIKE '%일반%' OR st.supplyType LIKE '%무순위%'")
    List<Long> findNoticeIdsForNonSpecialSupply();
} 