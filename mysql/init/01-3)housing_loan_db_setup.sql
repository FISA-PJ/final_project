-- 데이터베이스 생성 및 선택
CREATE DATABASE IF NOT EXISTS housing_loan;

USE housing_loan;

-- 수정된 주택대출 상품 테이블 생성
CREATE TABLE IF NOT EXISTS housing_loan_products (
    loan_id INT PRIMARY KEY, -- 대출 상품 ID
    name VARCHAR(200), -- 상품명
    loan_type VARCHAR(50), -- 대출유형
    bank_name VARCHAR(100), -- 금융사 이름
    target_group VARCHAR(50), -- 대상 그룹 (일반 대상자, 신혼부부 등)
    loan_term INT, -- 대출기간 (월 단위)
    loan_limit BIGINT, -- 최대 대출 한도 (원 단위로 변경)
    target_age_min DECIMAL(3,1), -- 최소 연령 조건 (소수점 허용)
    target_age_max DECIMAL(3,1), -- 최대 연령 조건 (소수점 허용)
    income_min DECIMAL(8,1), -- 최소 연소득 조건 (만원 단위, 소수점 허용)
    income_max DECIMAL(8,1), -- 최대 연소득 조건 (만원 단위, 소수점 허용)
    house_owned_limit BOOLEAN, -- 무주택자만 가능 여부
    first_home_only BOOLEAN, -- 생애최초만 가능 여부
    rate_min DECIMAL(4,1), -- 최저 금리 (소수점 첫째자리까지)
    rate_max DECIMAL(4,1), -- 최고 금리 (소수점 첫째자리까지)
    repayment_method VARCHAR(50) -- 상환 방식
);