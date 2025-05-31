package com.mysite.applyhome.loan;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;

@Entity
@Getter
@Setter
@Table(name = "housing_loan_products")
public class LoanProduct {
    @Id
    @Column(name = "loan_id")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long loanId;

    @Column(name = "name", nullable = false, length = 200)
    private String name;

    @Column(name = "loan_type", nullable = false, length = 50)
    private String loanType;

    @Column(name = "bank_name", nullable = false, length = 100)
    private String bankName;

    @Column(name = "target_group", length = 50)
    private String targetGroup;

    @Column(name = "loan_term")
    private Integer loanTerm;

    @Column(name = "loan_limit")
    private Long loanLimit;

    @Column(name = "target_age_min")
    private Double targetAgeMin;

    @Column(name = "target_age_max")
    private Double targetAgeMax;

    @Column(name = "income_min")
    private Double incomeMin;

    @Column(name = "income_max")
    private Double incomeMax;

    @Column(name = "house_owned_limit")
    private Boolean houseOwnedLimit;

    @Column(name = "first_home_only")
    private Boolean firstHomeOnly;

    @Column(name = "rate_min")
    private Double rateMin;

    @Column(name = "rate_max")
    private Double rateMax;

    @Column(name = "repayment_method", length = 50)
    private String repaymentMethod;
} 