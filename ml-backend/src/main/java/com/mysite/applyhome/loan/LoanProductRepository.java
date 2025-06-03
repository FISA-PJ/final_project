package com.mysite.applyhome.loan;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface LoanProductRepository extends JpaRepository<LoanProduct, Long>, JpaSpecificationExecutor<LoanProduct> {
    List<LoanProduct> findByLoanType(String loanType);
    
    @Query("SELECT l FROM LoanProduct l WHERE l.loanType = :loanType AND " +
           "(:income IS NULL OR (l.incomeMin <= :income AND l.incomeMax >= :income)) AND " +
           "(:age IS NULL OR (l.targetAgeMin <= :age AND l.targetAgeMax >= :age))")
    List<LoanProduct> findMatchingLoans(@Param("loanType") String loanType,
                                      @Param("income") Double income,
                                      @Param("age") Double age);
} 