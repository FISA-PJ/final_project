USE notice_db;

-- 문자셋 설정
SET NAMES utf8mb4;
SET CHARACTER SET utf8mb4;

-- 기존 프로시저 삭제
DROP PROCEDURE IF EXISTS UpsertNotice;
DROP PROCEDURE IF EXISTS UpdateAllNoticeStatuses;

DELIMITER //

-- 공고 상태 업데이트 프로시저
CREATE PROCEDURE UpdateAllNoticeStatuses()
BEGIN
    UPDATE notices
    SET notice_status = 
        CASE
            WHEN winning_date IS NOT NULL AND winning_date <= CURDATE() THEN '결과발표'
            WHEN application_end_date IS NOT NULL AND application_end_date < CURDATE() THEN '접수마감'
            ELSE '접수중'
        END,
    updated_at = CURRENT_TIMESTAMP;
END //

-- 공고 등록/수정 프로시저
CREATE PROCEDURE UpsertNotice(
    IN p_notice_number VARCHAR(50),
    IN p_notice_title VARCHAR(255),
    IN p_post_date DATE,
    IN p_application_end_date DATE,
    IN p_document_start_date DATE,
    IN p_document_end_date DATE,
    IN p_contract_start_date DATE,
    IN p_contract_end_date DATE,
    IN p_winning_date DATE,
    IN p_move_in_date VARCHAR(20),
    IN p_location TEXT,
    IN p_is_correction BOOLEAN,
    IN p_supply_types JSON,
    IN p_house_types JSON
)
BEGIN
    DECLARE v_original_notice_id BIGINT;
    DECLARE v_new_notice_id BIGINT;
    DECLARE v_error_message TEXT;
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        -- 에러 발생 시 롤백
        ROLLBACK;
        GET DIAGNOSTICS CONDITION 1
            v_error_message = MESSAGE_TEXT;
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = v_error_message;
    END;
    
    -- 트랜잭션 시작
    START TRANSACTION;
    
    -- 정정공고인 경우 이전 공고 찾기
    IF p_is_correction THEN
        -- 정정/변경/수정 문구 제거한 제목으로 이전 공고 찾기
        SET @clean_title = REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(p_notice_title, '[정정]', ''),
                            '[변경]', ''
                        ),
                        '[수정]', ''
                    ),
                    '[재공고]', ''
                ),
                '[취소]', ''
            ),
            '[연기]', ''
        );
        
        -- 같은 날짜의 유사한 제목을 가진 공고 찾기
        SELECT id INTO v_original_notice_id
        FROM notices
        WHERE DATE(post_date) = DATE(p_post_date)
        AND is_correction = FALSE
        AND notice_title LIKE CONCAT('%', @clean_title, '%')
        ORDER BY id DESC
        LIMIT 1;
        
        -- 이전 공고 관련 데이터 삭제
        IF v_original_notice_id IS NOT NULL THEN
            DELETE FROM house_types WHERE notice_id = v_original_notice_id;
            DELETE FROM supply_types WHERE notice_id = v_original_notice_id;
            DELETE FROM notices WHERE id = v_original_notice_id;
        END IF;
    END IF;
    
    -- 새 공고 등록
    INSERT INTO notices (
        notice_number, notice_title, post_date, application_end_date,
        document_start_date, document_end_date, contract_start_date, contract_end_date,
        winning_date, move_in_date, location, is_correction, notice_status,
        created_at, updated_at
    ) VALUES (
        p_notice_number, p_notice_title, p_post_date, p_application_end_date,
        p_document_start_date, p_document_end_date, p_contract_start_date, p_contract_end_date,
        p_winning_date, p_move_in_date, p_location, p_is_correction,
        CASE
            WHEN p_winning_date IS NOT NULL AND p_winning_date <= CURDATE() THEN '결과발표'
            WHEN p_application_end_date IS NOT NULL AND p_application_end_date < CURDATE() THEN '접수마감'
            ELSE '접수중'
        END,
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    );
    
    SET v_new_notice_id = LAST_INSERT_ID();
    
    -- 공급유형 등록
    IF p_supply_types IS NOT NULL AND JSON_LENGTH(p_supply_types) > 0 THEN
        INSERT INTO supply_types (notice_id, supply_type, created_at, updated_at)
        SELECT 
            v_new_notice_id,
            JSON_UNQUOTE(JSON_EXTRACT(p_supply_types, CONCAT('$[', jt.seq, ']'))),
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM JSON_TABLE(
            CONCAT('[', REPEAT('0,', JSON_LENGTH(p_supply_types)-1), '0', ']'),
            '$[*]' COLUMNS (seq INT PATH '$')
        ) AS jt;
    END IF;
    
    -- 주택형 정보 등록
    IF p_house_types IS NOT NULL AND JSON_LENGTH(p_house_types) > 0 THEN
        INSERT INTO house_types (
            notice_id, house_type, exclusive_area, unit_count, avg_price,
            created_at, updated_at
        )
        SELECT 
            v_new_notice_id,
            JSON_UNQUOTE(JSON_EXTRACT(house_type_obj, '$.house_type')),
            JSON_EXTRACT(house_type_obj, '$.exclusive_area') + 0.0,
            JSON_EXTRACT(house_type_obj, '$.unit_count') + 0,
            JSON_EXTRACT(house_type_obj, '$.avg_price') + 0.0,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM JSON_TABLE(
            p_house_types,
            '$[*]' COLUMNS (house_type_obj JSON PATH '$')
        ) AS jt;
    END IF;
    
    -- 트랜잭션 커밋
    COMMIT;
    
END //

DELIMITER ;