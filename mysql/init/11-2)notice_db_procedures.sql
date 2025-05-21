USE notice_db;

-- 문자셋 설정
SET NAMES utf8mb4;
SET CHARACTER SET utf8mb4;

-- 기존 프로시저 삭제
DROP PROCEDURE IF EXISTS InsertNewNotice;
DROP PROCEDURE IF EXISTS ProcessCorrectionNoticeWithHistory;

-- 신규 공고 삽입 프로시저 (created_at 파라미터 추가)
DELIMITER //
CREATE PROCEDURE InsertNewNotice(
    IN p_notice_number VARCHAR(50),
    IN p_notice_title VARCHAR(500),
    IN p_post_date DATE,
    IN p_application_start_date DATE,
    IN p_application_end_date DATE,
    IN p_location VARCHAR(255),
    IN p_is_correction BOOLEAN,
    IN p_job_execution_time TIMESTAMP
)
BEGIN
    DECLARE v_new_status ENUM('접수중', '접수마감');
    
    -- 상태 계산
    SET v_new_status = CASE 
        WHEN CURDATE() BETWEEN p_application_start_date AND p_application_end_date THEN '접수중'
        ELSE '접수마감'
    END;
    
    -- 중복 체크 후 삽입 (created_at에 작업 실행 시간 삽입)
    INSERT IGNORE INTO notices (
        notice_number, notice_title, post_date,
        application_start_date, application_end_date, location,
        notice_status, is_correction, created_at
    ) VALUES (
        p_notice_number, p_notice_title, p_post_date,
        p_application_start_date, p_application_end_date, p_location,
        v_new_status, p_is_correction, p_job_execution_time
    );
END //
DELIMITER ;

-- 수정된 정정공고 처리 프로시저 (created_at 파라미터 추가)
DELIMITER //
CREATE PROCEDURE ProcessCorrectionNotice(
    IN p_notice_number VARCHAR(50),
    IN p_notice_title VARCHAR(500),
    IN p_post_date DATE,
    IN p_application_start_date DATE,
    IN p_application_end_date DATE,
    IN p_location VARCHAR(255),
    IN p_job_execution_time TIMESTAMP
)
BEGIN
    DECLARE v_existing_id BIGINT DEFAULT NULL;    -- 기존 공고 ID 저장
    DECLARE v_base_title VARCHAR(500);            -- 정정공고 키워드 제거한 기본 제목
    DECLARE v_new_status ENUM('접수중', '접수마감'); -- 계산될 공고 상태
    
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
        -- 기존 공고가 있는 경우, 업데이트
        UPDATE notices
        SET 
            notice_title = p_notice_title,
            post_date = p_post_date,
            application_start_date = p_application_start_date,
            application_end_date = p_application_end_date,
            location = p_location,
            notice_status = v_new_status,
            is_correction = TRUE,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = v_existing_id;
        
    ELSE
        -- 기존 공고가 없는 경우, 신규 공고로 등록 (created_at에 작업 실행 시간 삽입)
        INSERT INTO notices (
            notice_number, notice_title, post_date,
            application_start_date, application_end_date, location,
            notice_status, is_correction, created_at
        ) VALUES (
            p_notice_number, p_notice_title, p_post_date,
            p_application_start_date, p_application_end_date, p_location,
            v_new_status, TRUE, p_job_execution_time
        );
    END IF;
    
END //
DELIMITER ;