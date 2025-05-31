package com.mysite.applyhome.loan;

import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
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
} 