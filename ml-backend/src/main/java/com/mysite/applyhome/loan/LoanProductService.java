package com.mysite.applyhome.loan;

import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.domain.Specification;
import java.util.List;

@Service
public class LoanProductService {
    @Autowired
    private LoanProductRepository loanProductRepository;

    public List<LoanProduct> getLoansByType(String loanType) {
        return loanProductRepository.findByLoanType(loanType);
    }

    public List<LoanProduct> getMatchingLoans(String loanType, Double income, Double age) {
        return loanProductRepository.findMatchingLoans(loanType, income, age);
    }

    public Page<LoanProduct> getFilteredLoans(int page, String bank, String interestRate, String loanLimit, String loanPeriod) {
        Pageable pageable = PageRequest.of(page, 10);
        Specification<LoanProduct> spec = Specification.where(null);

        // 은행 필터링
        if (bank != null && !bank.isEmpty() && !bank.equals("전체")) {
            spec = spec.and((root, query, cb) -> {
                switch (bank) {
                    case "kb":
                        return cb.like(root.get("bankName"), "%국민은행%");
                    case "sh":
                        return cb.like(root.get("bankName"), "%신한은행%");
                    case "wr":
                        return cb.like(root.get("bankName"), "%우리은행%");
                    case "hn":
                        return cb.like(root.get("bankName"), "%하나은행%");
                    case "other":
                        return cb.not(cb.or(
                            cb.like(root.get("bankName"), "%국민은행%"),
                            cb.like(root.get("bankName"), "%신한은행%"),
                            cb.like(root.get("bankName"), "%우리은행%"),
                            cb.like(root.get("bankName"), "%하나은행%")
                        ));
                    default:
                        return null;
                }
            });
        }

        // 금리 범위 필터링
        if (interestRate != null && !interestRate.isEmpty() && !interestRate.equals("전체")) {
            spec = spec.and((root, query, cb) -> {
                switch (interestRate) {
                    case "under3":
                        return cb.lessThan(root.get("rateMax"), 3.0);
                    case "3to4":
                        return cb.and(
                            cb.lessThanOrEqualTo(root.get("rateMin"), 4.0),
                            cb.greaterThanOrEqualTo(root.get("rateMax"), 3.0)
                        );
                    case "4to5":
                        return cb.and(
                            cb.lessThanOrEqualTo(root.get("rateMin"), 5.0),
                            cb.greaterThanOrEqualTo(root.get("rateMax"), 4.0)
                        );
                    case "over5":
                        return cb.greaterThan(root.get("rateMin"), 5.0);
                    default:
                        return null;
                }
            });
        }

        // 대출한도 필터링
        if (loanLimit != null && !loanLimit.isEmpty() && !loanLimit.equals("전체")) {
            spec = spec.and((root, query, cb) -> {
                switch (loanLimit) {
                    case "under2":
                        return cb.lessThan(root.get("loanLimit"), 200000000L);
                    case "2to3":
                        return cb.and(
                            cb.greaterThanOrEqualTo(root.get("loanLimit"), 200000000L),
                            cb.lessThan(root.get("loanLimit"), 300000000L)
                        );
                    case "3to5":
                        return cb.and(
                            cb.greaterThanOrEqualTo(root.get("loanLimit"), 300000000L),
                            cb.lessThan(root.get("loanLimit"), 500000000L)
                        );
                    case "over5":
                        return cb.greaterThanOrEqualTo(root.get("loanLimit"), 500000000L);
                    default:
                        return null;
                }
            });
        }

        // 대출기간 필터링
        if (loanPeriod != null && !loanPeriod.isEmpty() && !loanPeriod.equals("전체")) {
            spec = spec.and((root, query, cb) -> {
                switch (loanPeriod) {
                    case "under10":
                        return cb.lessThan(root.get("loanTerm"), 120); // 10년 = 120개월
                    case "10to20":
                        return cb.and(
                            cb.greaterThanOrEqualTo(root.get("loanTerm"), 120),
                            cb.lessThan(root.get("loanTerm"), 240)
                        );
                    case "20to30":
                        return cb.and(
                            cb.greaterThanOrEqualTo(root.get("loanTerm"), 240),
                            cb.lessThanOrEqualTo(root.get("loanTerm"), 360)
                        );
                    case "over30":
                        return cb.greaterThan(root.get("loanTerm"), 360);
                    default:
                        return null;
                }
            });
        }

        return loanProductRepository.findAll(spec, pageable);
    }
} 