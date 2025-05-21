-- 데이터베이스 생성 및 선택
CREATE DATABASE IF NOT EXISTS notice_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE notice_db;

-- 공고 메인 테이블 (정정 횟수 필드 제거)
CREATE TABLE IF NOT EXISTS notices (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    
    -- 기본 공고 정보
    notice_number VARCHAR(50) NOT NULL UNIQUE,
    notice_title VARCHAR(500) NOT NULL,
    post_date DATE NOT NULL,
    application_start_date DATE NOT NULL,
    application_end_date DATE NOT NULL,
    location VARCHAR(255),
    
    -- 상태 관리
    notice_status ENUM('접수중', '접수마감') NOT NULL,
    
    -- 정정공고 여부만 유지
    is_correction BOOLEAN DEFAULT FALSE,
    
    -- 시스템 필드
    created_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- 인덱스
    INDEX idx_notice_number (notice_number),
    INDEX idx_post_date (post_date),
    INDEX idx_status (notice_status),
    INDEX idx_application_period (application_start_date, application_end_date),
    
    -- 제약조건
    CONSTRAINT chk_application_dates CHECK (application_end_date >= application_start_date)
);