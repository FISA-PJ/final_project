USE notice_db;

-- 문자셋 설정
SET NAMES utf8mb4;
SET CHARACTER SET utf8mb4;

-- 기존 프로시저 삭제
DROP PROCEDURE IF EXISTS InsertNewNotice;
DROP PROCEDURE IF EXISTS ProcessCorrectionNoticeWithHistory;

-- 신규 공고 삽입 프로시저
DELIMITER //
CREATE PROCEDURE InsertNewNotice(
    IN p_notice_number VARCHAR(50),
    IN p_notice_title VARCHAR(500),
    IN p_post_date DATE,
    IN p_application_start_date DATE,
    IN p_application_end_date DATE,
    IN p_location VARCHAR(255),
    IN p_is_correction VARCHAR(100)
)
BEGIN
    DECLARE v_new_status ENUM('접수중', '접수마감');
    
    -- 상태 계산
    SET v_new_status = CASE 
        WHEN CURDATE() BETWEEN p_application_start_date AND p_application_end_date THEN '접수중'
        ELSE '접수마감'
    END;
    
    -- 중복 체크 후 삽입
    INSERT IGNORE INTO notices (
        notice_number, notice_title, post_date,
        application_start_date, application_end_date, location,
        notice_status, is_correction, correction_count
    ) VALUES (
        p_notice_number, p_notice_title, p_post_date,
        p_application_start_date, p_application_end_date, p_location,
        v_new_status, FALSE, 0
    );
END //
DELIMITER ;

-- 정정공고 처리 프로시저 (히스토리 테이블 버전)
DELIMITER //
CREATE PROCEDURE ProcessCorrectionNoticeWithHistory(
    IN p_notice_number VARCHAR(50),
    IN p_notice_title VARCHAR(500),
    IN p_post_date DATE,
    IN p_application_start_date DATE,
    IN p_application_end_date DATE,
    IN p_location VARCHAR(255)
)
BEGIN
    DECLARE v_existing_id BIGINT DEFAULT NULL;    -- 기존 공고 ID 저장
    DECLARE v_base_title VARCHAR(500);            -- 정정공고 키워드 제거한 기본 제목
    DECLARE v_new_status ENUM('접수중', '접수마감'); -- 계산될 공고 상태
    DECLARE v_max_version INT DEFAULT 1;          -- 최대 버전 번호
    
    -- 제목 정제
    SET v_base_title = TRIM(BOTH ' ' FROM 
        REPLACE(
            REPLACE(p_notice_title, '(정정공고)', ''),
            '[정정공고]', ''
        )
    );
    
    -- 현재 날짜 기준으로 접수중/접수마감 상태 결정
    SET v_new_status = CASE 
        WHEN CURDATE() BETWEEN p_application_start_date AND p_application_end_date THEN '접수중'
        ELSE '접수마감'
    END;
    
    -- 정제된 제목을 포함하는 기존 공고 검색 (최근 30일 이내)
    SELECT id INTO v_existing_id
    FROM notices
    WHERE notice_title LIKE CONCAT('%', v_base_title, '%')
      AND post_date >= DATE_SUB(p_post_date, INTERVAL 30 DAY)
    ORDER BY post_date DESC
    LIMIT 1;
    
    IF v_existing_id IS NOT NULL THEN
        -- 1) 기존 공고의 현재 정보를 히스토리 테이블에 저장
        INSERT INTO notice_history (
            notice_id, notice_number, notice_title, post_date,
            application_start_date, application_end_date, location,
            version_number, is_correction
        )
        SELECT 
            id, notice_number, notice_title, post_date,
            application_start_date, application_end_date, location,
            1, is_correction
        FROM notices
        WHERE id = v_existing_id;
        
        -- 2) 다음 버전 번호 계산
        SELECT COALESCE(MAX(version_number), 0) + 1 INTO v_max_version
        FROM notice_history
        WHERE notice_id = v_existing_id;
        
        -- 3) 정정 내용을 히스토리에 새 버전으로 추가
        INSERT INTO notice_history (
            notice_id, notice_number, notice_title, post_date,
            application_start_date, application_end_date, location,
            version_number, is_correction
        ) VALUES (
            v_existing_id, p_notice_number, p_notice_title, p_post_date,
            p_application_start_date, p_application_end_date, p_location,
            v_max_version, TRUE
        );
        
        -- 4) 원본 공고 테이블 업데이트
        UPDATE notices
        SET 
            notice_title = p_notice_title,
            post_date = p_post_date,
            application_start_date = p_application_start_date,
            application_end_date = p_application_end_date,
            location = p_location,
            notice_status = v_new_status,
            is_correction = TRUE,
            correction_count = v_max_version - 1,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = v_existing_id;
        
    ELSE
        -- 1) notices 테이블에 신규 공고로 등록
        INSERT INTO notices (
            notice_number, notice_title, post_date,
            application_start_date, application_end_date, location,
            notice_status, is_correction
        ) VALUES (
            p_notice_number, p_notice_title, p_post_date,
            p_application_start_date, p_application_end_date, p_location,
            v_new_status, TRUE
        );
        
        -- 2) 히스토리 테이블에도 버전 1로 추가
        INSERT INTO notice_history (
            notice_id, notice_number, notice_title, post_date,
            application_start_date, application_end_date, location,
            version_number, is_correction
        ) VALUES (
            LAST_INSERT_ID(), p_notice_number, p_notice_title, p_post_date,
            p_application_start_date, p_application_end_date, p_location,
            1, TRUE
        );
    END IF;
    
END //
DELIMITER ;