-- 데이터베이스 생성 및 선택
CREATE DATABASE IF NOT EXISTS notice_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE notice_db;

-- DROP TABLES notices_history;
-- DROP TABLES notices;

-- 공고 메인 테이블
CREATE TABLE IF NOT EXISTS notices (
    id BIGINT PRIMARY KEY AUTO_INCREMENT,
    
    -- 기본 공고 정보
    notice_number VARCHAR(50) NOT NULL UNIQUE,
    notice_title VARCHAR(500) NOT NULL,
    post_date DATE NOT NULL,
    application_start_date DATE NOT NULL,
    application_end_date DATE NOT NULL,
    location VARCHAR(255),
    
    -- 상태 관리 (컬럼명을 notice_status로 변경)
    notice_status ENUM('접수중', '접수마감') NOT NULL,
    
    -- 정정공고 관리
    is_correction BOOLEAN DEFAULT FALSE,
    correction_count INTEGER DEFAULT 0,
    
    -- 시스템 필드
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- 인덱스
    INDEX idx_notice_number (notice_number),
    INDEX idx_post_date (post_date),
    INDEX idx_status (notice_status),
    INDEX idx_application_period (application_start_date, application_end_date),
    
    -- 제약조건
    CONSTRAINT chk_application_dates CHECK (application_end_date >= application_start_date)
);

-- 공고 히스토리 테이블 생성
CREATE TABLE notice_history (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    notice_id BIGINT,
    notice_number VARCHAR(50),
    notice_title VARCHAR(500),
    post_date DATE,
    application_start_date DATE,
    application_end_date DATE,
    location VARCHAR(255),
    version_number INT DEFAULT 1,
    is_correction BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (notice_id) REFERENCES notices(id)
);
