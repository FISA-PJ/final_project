package com.mysite.applyhome.loan;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import java.util.List;

@Controller
@RequestMapping("/loan")
public class LoanProductController {
    @Autowired
    private LoanProductService loanProductService;

    @GetMapping("/list")
    public String list(Model model) {
        List<LoanProduct> mortgageLoans = loanProductService.getLoansByType("주택담보");
        model.addAttribute("mortgageLoans", mortgageLoans);
        return "loan_page";
    }

    @GetMapping("/api/products")
    @ResponseBody
    public List<LoanProduct> getProductsByType(@RequestParam String loanType) {
        return loanProductService.getLoansByType(loanType);
    }

    @GetMapping("/api/filter")
    @ResponseBody
    public ResponseEntity<Page<LoanProduct>> getFilteredLoans(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(required = false) String bank,
            @RequestParam(required = false) String interestRate,
            @RequestParam(required = false) String loanLimit,
            @RequestParam(required = false) String loanPeriod) {
        return ResponseEntity.ok(loanProductService.getFilteredLoans(page, bank, interestRate, loanLimit, loanPeriod));
    }
} 