USE notice_db;

-- 기존 프로시저 삭제
DROP PROCEDURE IF EXISTS UpsertNotice;

DELIMITER //

CREATE PROCEDURE UpsertNotice(
    IN p_notice_number VARCHAR(50) CHARACTER SET utf8mb4,
    IN p_notice_title VARCHAR(500) CHARACTER SET utf8mb4,
    IN p_post_date DATE,
    IN p_application_end_date DATE,
    IN p_document_start_date DATE,
    IN p_document_end_date DATE,
    IN p_contract_start_date DATE,
    IN p_contract_end_date DATE,
    IN p_winning_date DATE,
    IN p_move_in_date VARCHAR(20) CHARACTER SET utf8mb4,
    IN p_location TEXT CHARACTER SET utf8mb4,
    IN p_is_correction BOOLEAN,
    IN p_supply_types JSON,
    IN p_house_types JSON
)
main_block: BEGIN
    -- 변수 선언
    DECLARE v_original_notice_id BIGINT DEFAULT NULL;
    DECLARE v_new_notice_id BIGINT DEFAULT NULL;
    DECLARE v_clean_title TEXT CHARACTER SET utf8mb4 DEFAULT '';
    DECLARE v_notice_status VARCHAR(20) CHARACTER SET utf8mb4;
    DECLARE v_existing_notice_id BIGINT DEFAULT NULL;
    DECLARE v_result_message VARCHAR(255) CHARACTER SET utf8mb4;

    -- 간소화된 예외처리 핸들러
    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        ROLLBACK;
        RESIGNAL;  -- 원래 에러를 그대로 다시 발생시킴
    END;

    -- 트랜잭션 시작
    START TRANSACTION;

    -- 필수 파라미터 검증
    IF p_notice_number IS NULL OR TRIM(p_notice_number) = '' THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'notice_number is required';
    END IF;

    IF p_notice_title IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'notice_title is required';
    END IF;

    IF p_post_date IS NULL THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'post_date is required';
    END IF;

    -- 날짜 유효성 검증
    IF p_application_end_date IS NOT NULL AND p_application_end_date < p_post_date THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'application_end_date cannot be earlier than post_date';
    END IF;

    IF p_document_start_date IS NOT NULL AND p_document_end_date IS NOT NULL 
       AND p_document_end_date < p_document_start_date THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'document_end_date cannot be earlier than document_start_date';
    END IF;

    IF p_contract_start_date IS NOT NULL AND p_contract_end_date IS NOT NULL 
       AND p_contract_end_date < p_contract_start_date THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'contract_end_date cannot be earlier than contract_start_date';
    END IF;

    -- 문자열 길이 검증
    IF CHAR_LENGTH(p_notice_number) > 50 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'notice_number is too long (max 50 characters)';
    END IF;

    IF CHAR_LENGTH(p_notice_title) > 500 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'notice_title is too long (max 500 characters)';
    END IF;

    IF p_move_in_date IS NOT NULL AND CHAR_LENGTH(p_move_in_date) > 20 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'move_in_date is too long (max 20 characters)';
    END IF;

    -- 기존 공고 번호 확인
    SELECT id INTO v_existing_notice_id
    FROM notices
    WHERE notice_number = p_notice_number
    LIMIT 1;

    -- 정정공고 처리
    IF p_is_correction THEN
        SET v_clean_title = p_notice_title;

        -- 제목에서 정정 관련 키워드 제거
        SET v_clean_title = REPLACE(v_clean_title, '[정정공고]', '');
        SET v_clean_title = REPLACE(v_clean_title, '[변경]', '');
        SET v_clean_title = REPLACE(v_clean_title, '[수정]', '');
        SET v_clean_title = REPLACE(v_clean_title, '[재공고]', '');
        SET v_clean_title = REPLACE(v_clean_title, '[취소]', '');
        SET v_clean_title = REPLACE(v_clean_title, '[연기]', '');
        SET v_clean_title = TRIM(v_clean_title);

        -- 기존 공고 검색
        SELECT id INTO v_original_notice_id
        FROM notices
        WHERE DATE(post_date) = DATE(p_post_date)
          AND is_correction = FALSE
          AND notice_title LIKE CONCAT('%', v_clean_title, '%')
        ORDER BY id DESC
        LIMIT 1;

        -- 기존 공고 삭제
        IF v_original_notice_id IS NOT NULL THEN
            DELETE FROM house_types WHERE notice_id = v_original_notice_id;
            DELETE FROM supply_types WHERE notice_id = v_original_notice_id;
            DELETE FROM notices WHERE id = v_original_notice_id;
        END IF;
    END IF;

    -- JSON 유효성 검사
    IF p_supply_types IS NOT NULL THEN
        IF NOT JSON_VALID(p_supply_types) THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Invalid supply_types JSON format';
        END IF;
        IF JSON_TYPE(p_supply_types) != 'ARRAY' THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'supply_types must be a JSON array';
        END IF;
    END IF;

    IF p_house_types IS NOT NULL THEN
        IF NOT JSON_VALID(p_house_types) THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Invalid house_types JSON format';
        END IF;
        IF JSON_TYPE(p_house_types) != 'ARRAY' THEN
            SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'house_types must be a JSON array';
        END IF;
    END IF;

    -- 공고 상태 계산
    SET v_notice_status = 
        CASE
            WHEN p_winning_date IS NOT NULL AND p_winning_date <= CURDATE() THEN '결과발표'
            WHEN p_application_end_date IS NOT NULL AND p_application_end_date < CURDATE() THEN '접수마감'
            ELSE '접수중'
        END;

    -- 기존 공고가 있는 경우 업데이트
    IF v_existing_notice_id IS NOT NULL THEN
        -- 기존 공고 업데이트
        UPDATE notices
        SET notice_title = p_notice_title,
            post_date = p_post_date,
            application_end_date = p_application_end_date,
            document_start_date = p_document_start_date,
            document_end_date = p_document_end_date,
            contract_start_date = p_contract_start_date,
            contract_end_date = p_contract_end_date,
            winning_date = p_winning_date,
            move_in_date = p_move_in_date,
            location = p_location,
            is_correction = p_is_correction,
            notice_status = v_notice_status,
            updated_at = CURRENT_TIMESTAMP
        WHERE id = v_existing_notice_id;

        SET v_new_notice_id = v_existing_notice_id;

        -- 기존 공급유형과 주택형 삭제
        DELETE FROM supply_types WHERE notice_id = v_existing_notice_id;
        DELETE FROM house_types WHERE notice_id = v_existing_notice_id;
    ELSE
        -- 새로운 공고 등록
        INSERT INTO notices (
            notice_number, notice_title, post_date, application_end_date,
            document_start_date, document_end_date, contract_start_date, contract_end_date,
            winning_date, move_in_date, location, is_correction, notice_status,
            created_at, updated_at
        ) VALUES (
            p_notice_number, p_notice_title, p_post_date, p_application_end_date,
            p_document_start_date, p_document_end_date, p_contract_start_date, p_contract_end_date,
            p_winning_date, p_move_in_date, p_location, p_is_correction,
            v_notice_status,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        );

        SET v_new_notice_id = LAST_INSERT_ID();
    END IF;

    -- 공급유형 저장
    IF p_supply_types IS NOT NULL AND JSON_LENGTH(p_supply_types) > 0 THEN
        INSERT INTO supply_types (notice_id, supply_type)
        SELECT 
            v_new_notice_id,
            JSON_UNQUOTE(value)
        FROM JSON_TABLE(
            p_supply_types,
            '$[*]' COLUMNS (value JSON PATH '$')
        ) AS jt;
    END IF;

    -- 주택형 저장
    IF p_house_types IS NOT NULL AND JSON_LENGTH(p_house_types) > 0 THEN
        INSERT INTO house_types (
            notice_id, house_type, exclusive_area, unit_count, avg_price
        )
        SELECT 
            v_new_notice_id,
            JSON_UNQUOTE(JSON_EXTRACT(house_type_obj, '$.house_type')),
            CAST(JSON_UNQUOTE(JSON_EXTRACT(house_type_obj, '$.exclusive_area')) AS DECIMAL(10,2)),
            CAST(JSON_UNQUOTE(JSON_EXTRACT(house_type_obj, '$.unit_count')) AS UNSIGNED),
            CAST(JSON_UNQUOTE(JSON_EXTRACT(house_type_obj, '$.avg_price')) AS DECIMAL(15,2))
        FROM JSON_TABLE(
            p_house_types,
            '$[*]' COLUMNS (house_type_obj JSON PATH '$')
        ) AS jt;
    END IF;

    -- 커밋
    COMMIT;
    
    -- -- 결과 반환
    -- SELECT 
    --     v_new_notice_id as notice_id,
    --     CONCAT('SUCCESS: Notice ', IF(v_existing_notice_id IS NOT NULL, 'updated', 'registered'), ' with ID ', v_new_notice_id) as result_message;
END;
//

DELIMITER ;

-- UpdateAllNoticeStatuses 프로시저 생성
DROP PROCEDURE IF EXISTS UpdateAllNoticeStatuses;

DELIMITER //

CREATE PROCEDURE UpdateAllNoticeStatuses()
BEGIN
    -- 공고 상태를 일괄적으로 다음 규칙에 따라 업데이트
    UPDATE notices
    SET notice_status = 
        CASE
            WHEN winning_date IS NOT NULL AND winning_date <= CURDATE() THEN '결과발표'
            WHEN application_end_date IS NOT NULL AND application_end_date < CURDATE() THEN '접수마감'
            ELSE '접수중'
        END,
        updated_at = CURRENT_TIMESTAMP;
END;
//

DELIMITER ;