-- 문자셋 설정
SET NAMES utf8mb4;
SET CHARACTER SET utf8mb4;
SET collation_connection = utf8mb4_0900_ai_ci;

-- 데이터베이스 생성 및 선택
CREATE DATABASE IF NOT EXISTS notice_db CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

USE notice_db;

CREATE TABLE notices (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    notice_number VARCHAR(50) NOT NULL UNIQUE,
    notice_title  VARCHAR(500) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
    post_date DATE NOT NULL,
    application_end_date DATE,
    document_start_date DATE,
    document_end_date DATE,
    contract_start_date DATE,
    contract_end_date DATE,
    winning_date DATE,
    move_in_date VARCHAR(20),
    location TEXT,
    is_correction BOOLEAN DEFAULT FALSE,
    notice_status ENUM('접수중', '접수마감', '결과발표') NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE supply_types (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    notice_id BIGINT NOT NULL,
    supply_type VARCHAR(100) NOT NULL,
    FOREIGN KEY (notice_id) REFERENCES notices(id) ON DELETE CASCADE
);

CREATE TABLE house_types (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    notice_id BIGINT NOT NULL,
    house_type VARCHAR(50) NOT NULL,
    exclusive_area DECIMAL(10,2),
    unit_count INT,
    avg_price DECIMAL(15,2),
    FOREIGN KEY (notice_id) REFERENCES notices(id) ON DELETE CASCADE
);