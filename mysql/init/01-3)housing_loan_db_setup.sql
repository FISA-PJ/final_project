-- 데이터베이스 생성 및 선택
CREATE DATABASE IF NOT EXISTS housing_loan;

USE housing_loan;

-- 주택대출 테이블 생성
CREATE TABLE IF NOT EXISTS housing_loan_products (
    fin_prdt_cd VARCHAR(20) PRIMARY KEY,      -- 금융상품 코드
    kor_co_nm VARCHAR(100),                   -- 금융회사명
    loan_type VARCHAR(50),                    -- 대출종류
    fin_prdt_nm VARCHAR(200),                 -- 금융상품명
    mrtg_type_nm VARCHAR(50),                 -- 담보유형명
    lend_rate_type_nm VARCHAR(50),            -- 금리유형
    rpay_type_nm VARCHAR(50),                 -- 상환방식
    lend_rate_min DECIMAL(5,2),               -- 최저금리
    lend_rate_max DECIMAL(5,2),               -- 최고금리
    lend_rate_avg DECIMAL(5,2)                -- 평균금리
);