package com.mysite.applyhome.loan;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;
import org.springframework.beans.factory.annotation.Autowired;
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
} 