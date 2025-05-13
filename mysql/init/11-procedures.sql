-- 청약 자격 조건 평가를 위한 최적화된 프로시저

USE app_db;

-- 문자셋 설정
SET NAMES utf8mb4;
SET CHARACTER SET utf8mb4;

-- 기존 프로시저 삭제
DROP PROCEDURE IF EXISTS bulk_evaluate_eligibility_optimized;
DROP PROCEDURE IF EXISTS evaluate_eligibility_optimized;

-- 배치 처리 프로시저 구현
DELIMITER //

CREATE PROCEDURE bulk_evaluate_eligibility_optimized()
BEGIN
    DECLARE done INT DEFAULT FALSE;
    DECLARE batch_size INT DEFAULT 1000;
    DECLARE offset_val INT DEFAULT 0;
    DECLARE total_count INT;
    
    -- 총 대상자 수를 먼저 확인
    SELECT COUNT(*) INTO total_count FROM Personal_Profiles;
    
    -- 진행 시작 로그
    INSERT INTO Processing_Log (log_message, log_timestamp)
    VALUES (CONCAT('Starting bulk eligibility evaluation for ', total_count, ' profiles'), NOW());
    
    -- 임시 테이블 생성 (각 유형별 자격 요건을 미리 계산)
    DROP TEMPORARY TABLE IF EXISTS temp_savings_valid;
    CREATE TEMPORARY TABLE temp_savings_valid (
        resident_registration_number VARCHAR(13) PRIMARY KEY,
        is_valid BOOLEAN
    );
    
    -- 청약 저축 요건을 미리 계산
    INSERT INTO temp_savings_valid
    SELECT 
        resident_registration_number,
        TRUE
    FROM Personal_Subscription_Savings_Info
    WHERE subscription_savings_period_months >= 6
    AND number_of_payments >= 6
    GROUP BY resident_registration_number;
    
    -- 진행 로그
    INSERT INTO Processing_Log (log_message, log_timestamp)
    VALUES ('Created temporary table for valid savings', NOW());
    
    -- 배치 처리 시작
    WHILE offset_val < total_count DO
        -- 대상자를 배치 단위로 처리
        INSERT INTO Housing_Subscription_Eligibility (
            resident_registration_number, 
            eligibility_prime_type, 
            eligibility_sub_type, 
            last_assessment_date
        )
        SELECT 
            pp.resident_registration_number,
            -- 1차 유형 분류 로직
            CASE
                WHEN (IFNULL(pmi.marriage_duration, 0) <= 84 OR (pmi.marriage_type = '이혼' AND IFNULL(hci.children_age_1_to_6_count, 0) >= 1))
                    AND hi.is_household_without_housing = TRUE 
                    AND hai.real_estate_amount <= 215500000
                    AND hai.car_value <= 38030000
                    AND tsv.is_valid = TRUE
                    AND (
                        CASE
                            WHEN hmi.is_dual_income = TRUE THEN
                                CASE
                                    WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 14410624
                                    WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 17156176
                                    WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 18062096
                                    WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 19466172
                                    WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 20870248
                                    ELSE hmi.monthly_avg_income_amount <= 22274324
                                END
                            ELSE
                                CASE
                                    WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 9366906
                                    WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 11151514
                                    WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 11740362
                                    WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 12653012
                                    WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 13565661
                                    ELSE hmi.monthly_avg_income_amount <= 14478311
                                END
                        END
                    )
                THEN '신혼부부'
                WHEN IFNULL(haa.ancestor_age_above_65, 0) >= 1
                    AND IFNULL(haa.ancestor_support_duration, 0) >= 36
                    AND hi.is_household_without_housing = TRUE
                    AND hai.real_estate_amount <= 215500000
                    AND hai.car_value <= 38030000
                    AND tsv.is_valid = TRUE
                    AND (
                        CASE
                            WHEN hmi.is_dual_income = TRUE THEN
                                CASE
                                    WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 14410624
                                    WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 17156176
                                    WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 18062096
                                    WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 19466172
                                    WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 20870248
                                    ELSE hmi.monthly_avg_income_amount <= 22274324
                                END
                            ELSE
                                CASE
                                    WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 8646374
                                    WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 10293706
                                    WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 10837258
                                    WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 11679703
                                    WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 12522149
                                    ELSE hmi.monthly_avg_income_amount <= 13364594
                                END
                        END
                    )
                THEN '노부모부양'
                WHEN (IFNULL(hci.children_age_1_to_6_count, 0) + IFNULL(hci.children_age_7_to_19_count, 0)) >= 2
                    AND hi.is_household_without_housing = TRUE
                    AND hai.real_estate_amount <= 215500000
                    AND hai.car_value <= 38030000
                    AND tsv.is_valid = TRUE
                    AND (
                        CASE
                            WHEN hmi.is_dual_income = TRUE THEN
                                CASE
                                    WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 14410624
                                    WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 17156176
                                    WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 18062096
                                    WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 19466172
                                    WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 20870248
                                    ELSE hmi.monthly_avg_income_amount <= 22274324
                                END
                            ELSE
                                CASE
                                    WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 8646374
                                    WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 10293706
                                    WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 10837258
                                    WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 11679703
                                    WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 12522149
                                    ELSE hmi.monthly_avg_income_amount <= 13364594
                                END
                        END
                    )
                THEN '다자녀'
                ELSE '특별공급유형 아님'
            END AS eligibility_prime_type,
            
            -- 2차 유형 로직
            CASE
                -- 신혼부부 케이스
                WHEN (IFNULL(pmi.marriage_duration, 0) <= 84 OR (pmi.marriage_type = '이혼' AND IFNULL(hci.children_age_1_to_6_count, 0) >= 1))
                    AND hi.is_household_without_housing = TRUE
                    AND hai.real_estate_amount <= 215500000
                    AND hai.car_value <= 38030000
                    AND tsv.is_valid = TRUE
                THEN 
                    CASE 
                        WHEN hmi.is_dual_income = FALSE AND 
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 7205312
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 8578088
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 9031048
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 9733086
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 10435124
                               ELSE hmi.monthly_avg_income_amount <= 11137162
                           END
                           OR
                           hmi.is_dual_income = TRUE AND 
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 8646374
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 10293706
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 10837258
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 11679703
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 12522149
                               ELSE hmi.monthly_avg_income_amount <= 13364594
                           END
                       THEN '우선공급'
                       WHEN hmi.is_dual_income = FALSE AND
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 5764250
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 6862470
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 7224838
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 7786469
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 8348099
                               ELSE hmi.monthly_avg_income_amount <= 8909730
                           END
                           OR
                           hmi.is_dual_income = TRUE AND
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 7205312
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 8578088
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 9031048
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 9733086
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 10435124
                               ELSE hmi.monthly_avg_income_amount <= 11137162
                           END
                       THEN '우선공급(배점)'
                       ELSE '추첨공급'
                    END
                -- 노부모부양 케이스
                WHEN IFNULL(haa.ancestor_age_above_65, 0) >= 1
                    AND IFNULL(haa.ancestor_support_duration, 0) >= 36
                    AND hi.is_household_without_housing = TRUE
                    AND hai.real_estate_amount <= 215500000
                    AND hai.car_value <= 38030000
                    AND tsv.is_valid = TRUE
                THEN
                    CASE
                        WHEN hmi.is_dual_income = FALSE AND
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 8646374
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 10293706
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 10837258
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 11679703
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 12522149
                               ELSE hmi.monthly_avg_income_amount <= 13364594
                           END
                           OR
                           hmi.is_dual_income = TRUE AND
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 9366906
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 11151514
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 11740362
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 12653012
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 13565661
                               ELSE hmi.monthly_avg_income_amount <= 14478311
                           END
                        THEN '우선공급'
                        ELSE '추첨공급'
                    END
                -- 다자녀 케이스
                WHEN (IFNULL(hci.children_age_1_to_6_count, 0) + IFNULL(hci.children_age_7_to_19_count, 0)) >= 2
                    AND hi.is_household_without_housing = TRUE
                    AND hai.real_estate_amount <= 215500000
                    AND hai.car_value <= 38030000
                    AND tsv.is_valid = TRUE
                THEN
                    CASE
                        WHEN hmi.is_dual_income = FALSE AND
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 8646374
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 10293706
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 10837258
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 11679703
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 12522149
                               ELSE hmi.monthly_avg_income_amount <= 13364594
                           END
                           OR
                           hmi.is_dual_income = TRUE AND
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 9366906
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 11151514
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 11740362
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 12653012
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 13565661
                               ELSE hmi.monthly_avg_income_amount <= 14478311
                           END
                        THEN '우선공급'
                        ELSE '추첨공급'
                    END
                ELSE ''
            END AS eligibility_sub_type,
            CURRENT_TIMESTAMP AS last_assessment_date
        FROM Personal_Profiles AS pp
        LEFT JOIN Household_Info AS hi ON pp.resident_registration_number = hi.resident_registration_number
        LEFT JOIN Household_Monthly_Income_Info AS hmi ON hi.household_info_id = hmi.household_info_id
        LEFT JOIN Household_Asset_Info AS hai ON hi.household_info_id = hai.household_info_id
        LEFT JOIN Household_Children_Info AS hci ON hi.household_info_id = hci.household_info_id
        LEFT JOIN Household_Ancestor_Info AS haa ON hi.household_info_id = haa.household_info_id
        LEFT JOIN Personal_Marriage_Info AS pmi ON pp.resident_registration_number = pmi.resident_registration_number
        LEFT JOIN temp_savings_valid tsv ON pp.resident_registration_number = tsv.resident_registration_number
        LIMIT batch_size OFFSET offset_val
        ON DUPLICATE KEY UPDATE 
            eligibility_prime_type = VALUES(eligibility_prime_type),
            eligibility_sub_type = VALUES(eligibility_sub_type),
            last_assessment_date = CURRENT_TIMESTAMP;
            
        -- 오프셋 증가
        SET offset_val = offset_val + batch_size;
        
        -- 진행 상황 기록
        INSERT INTO Processing_Log (log_message, log_timestamp)
        VALUES (CONCAT('Processed batch: ', offset_val, ' of ', total_count), NOW());
        
        -- 각 배치 처리 후 잠시 지연시켜 DB 부하 분산 (선택사항)
        DO SLEEP(0.1);
    END WHILE;
    
    -- 임시 테이블 삭제
    DROP TEMPORARY TABLE IF EXISTS temp_savings_valid;
    
    -- 완료 로그
    INSERT INTO Processing_Log (log_message, log_timestamp)
    VALUES ('Completed eligibility evaluation', NOW());
END //

DELIMITER ;

-- 청약 자격 평가 프로시저 구현(개인)
DELIMITER //

CREATE PROCEDURE evaluate_eligibility_optimized(IN p_rrn VARCHAR(13))
BEGIN
    tmp_block: BEGIN
    -- 프로시저 시작 시간 기록용 변수
    DECLARE start_time TIMESTAMP;
    -- 저축 유효성 변수
    DECLARE v_valid_savings BOOLEAN DEFAULT FALSE;
    DECLARE v_prime_type VARCHAR(50) DEFAULT "특별공급유형 아님";
    DECLARE v_sub_type VARCHAR(50) DEFAULT '';

    SET start_time = NOW();

    -- 시작 로그 저장
    INSERT INTO Processing_Log (log_message, log_timestamp)
    VALUES (CONCAT('Starting individual eligibility evaluation for RRN: ', p_rrn), start_time);

    -- 청약 저축 유효성 한 번에 확인
    SELECT COUNT(*) > 0 INTO v_valid_savings
    FROM Personal_Subscription_Savings_Info
    WHERE resident_registration_number = p_rrn
      AND subscription_savings_period_months >= 6
      AND number_of_payments >= 6
    LIMIT 1;
    
    -- 저축 조건 확인 로그
    INSERT INTO Processing_Log (log_message, log_timestamp)
    VALUES (CONCAT('Savings validity check for RRN ', p_rrn, ': ', IF(v_valid_savings, 'Valid', 'Invalid')), NOW());
    
    -- 저축 조건이 충족되지 않으면 일찍 반환
    IF v_valid_savings = FALSE THEN
        INSERT INTO Housing_Subscription_Eligibility (
            resident_registration_number, 
            eligibility_prime_type, 
            eligibility_sub_type, 
            last_assessment_date
        )
        VALUES (
            p_rrn,
            '특별공급유형 아님',
            '',
            CURRENT_TIMESTAMP
        )
        ON DUPLICATE KEY UPDATE 
            eligibility_prime_type = '특별공급유형 아님',
            eligibility_sub_type = '',
            last_assessment_date = CURRENT_TIMESTAMP;
            
        -- 조기 종료 로그
        INSERT INTO Processing_Log (log_message, log_timestamp)
        VALUES (CONCAT('Early return for RRN ', p_rrn, ': Savings condition not met'), NOW());
            
        -- 일찍 종료
        -- RETURN;
        LEAVE tmp_block;
    END IF;
    
    SELECT 
        -- 1차 유형 분류 로직
        CASE
            WHEN (IFNULL(pmi.marriage_duration, 0) <= 84 OR (pmi.marriage_type = '이혼' AND IFNULL(hci.children_age_1_to_6_count, 0) >= 1))
                AND hi.is_household_without_housing = TRUE 
                AND hai.real_estate_amount <= 215500000
                AND hai.car_value <= 38030000
                AND v_valid_savings = TRUE
                AND (
                    CASE
                        WHEN hmi.is_dual_income = TRUE THEN
                            CASE
                                WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 14410624
                                WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 17156176
                                WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 18062096
                                WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 19466172
                                WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 20870248
                                ELSE hmi.monthly_avg_income_amount <= 22274324
                            END
                        ELSE
                            CASE
                                WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 9366906
                                WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 11151514
                                WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 11740362
                                WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 12653012
                                WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 13565661
                                ELSE hmi.monthly_avg_income_amount <= 14478311
                            END
                    END
                )
            THEN '신혼부부'
            WHEN IFNULL(haa.ancestor_age_above_65, 0) >= 1
                AND IFNULL(haa.ancestor_support_duration, 0) >= 36
                AND hi.is_household_without_housing = TRUE
                AND hai.real_estate_amount <= 215500000
                AND hai.car_value <= 38030000
                AND v_valid_savings = TRUE
                AND (
                    CASE
                        WHEN hmi.is_dual_income = TRUE THEN
                            CASE
                                WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 14410624
                                WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 17156176
                                WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 18062096
                                WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 19466172
                                WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 20870248
                                ELSE hmi.monthly_avg_income_amount <= 22274324
                            END
                        ELSE
                            CASE
                                WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 8646374
                                WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 10293706
                                WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 10837258
                                WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 11679703
                                WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 12522149
                                ELSE hmi.monthly_avg_income_amount <= 13364594
                            END
                    END
                )
            THEN '노부모부양'
            WHEN (IFNULL(hci.children_age_1_to_6_count, 0) + IFNULL(hci.children_age_7_to_19_count, 0)) >= 2
                AND hi.is_household_without_housing = TRUE
                AND hai.real_estate_amount <= 215500000
                AND hai.car_value <= 38030000
                AND v_valid_savings = TRUE
                AND (
                    CASE
                        WHEN hmi.is_dual_income = TRUE THEN
                            CASE
                                WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 14410624
                                WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 17156176
                                WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 18062096
                                WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 19466172
                                WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 20870248
                                ELSE hmi.monthly_avg_income_amount <= 22274324
                            END
                        ELSE
                            CASE
                                WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 8646374
                                WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 10293706
                                WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 10837258
                                WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 11679703
                                WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 12522149
                                ELSE hmi.monthly_avg_income_amount <= 13364594
                            END
                    END
                )
            THEN '다자녀'
            ELSE '특별공급유형 아님'
        END,
        
        -- 2차 유형 로직
        CASE
            -- 신혼부부 케이스
            WHEN (IFNULL(pmi.marriage_duration, 0) <= 84 OR (pmi.marriage_type = '이혼' AND IFNULL(hci.children_age_1_to_6_count, 0) >= 1))
                AND hi.is_household_without_housing = TRUE
                AND hai.real_estate_amount <= 215500000
                AND hai.car_value <= 38030000
                AND v_valid_savings = TRUE
            THEN 
                CASE 
                    WHEN hmi.is_dual_income = FALSE AND 
                       CASE
                           WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 7205312
                           WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 8578088
                           WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 9031048
                           WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 9733086
                           WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 10435124
                           ELSE hmi.monthly_avg_income_amount <= 11137162
                       END
                       OR
                       hmi.is_dual_income = TRUE AND 
                       CASE
                           WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 8646374
                           WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 10293706
                           WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 10837258
                           WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 11679703
                           WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 12522149
                           ELSE hmi.monthly_avg_income_amount <= 13364594
                       END
                   THEN '우선공급'
                   WHEN hmi.is_dual_income = FALSE AND
                       CASE
                           WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 5764250
                           WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 6862470
                           WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 7224838
                           WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 7786469
                           WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 8348099
                           ELSE hmi.monthly_avg_income_amount <= 8909730
                       END
                       OR
                       hmi.is_dual_income = TRUE AND
                       CASE
                           WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 7205312
                           WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 8578088
                           WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 9031048
                           WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 9733086
                           WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 10435124
                           ELSE hmi.monthly_avg_income_amount <= 11137162
                       END
                   THEN '우선공급(배점)'
                   ELSE '추첨공급'
                END
            -- 노부모부양 케이스
            WHEN IFNULL(haa.ancestor_age_above_65, 0) >= 1
                AND IFNULL(haa.ancestor_support_duration, 0) >= 36
                AND hi.is_household_without_housing = TRUE
                AND hai.real_estate_amount <= 215500000
                AND hai.car_value <= 38030000
                AND v_valid_savings = TRUE
            THEN
                CASE
                    WHEN hmi.is_dual_income = FALSE AND
                       CASE
                           WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 8646374
                           WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 10293706
                           WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 10837258
                           WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 11679703
                           WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 12522149
                           ELSE hmi.monthly_avg_income_amount <= 13364594
                       END
                       OR
                       hmi.is_dual_income = TRUE AND
                       CASE
                           WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 9366906
                           WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 11151514
                           WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 11740362
                           WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 12653012
                           WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 13565661
                           ELSE hmi.monthly_avg_income_amount <= 14478311
                       END
                    THEN '우선공급'
                    ELSE '추첨공급'
                END
            -- 다자녀 케이스
            WHEN (IFNULL(hci.children_age_1_to_6_count, 0) + IFNULL(hci.children_age_7_to_19_count, 0)) >= 2
                AND hi.is_household_without_housing = TRUE
                AND hai.real_estate_amount <= 215500000
                AND hai.car_value <= 38030000
                AND v_valid_savings = TRUE
            THEN
                CASE
                    WHEN hmi.is_dual_income = FALSE AND
                       CASE
                           WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 8646374
                           WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 10293706
                           WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 10837258
                           WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 11679703
                           WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 12522149
                           ELSE hmi.monthly_avg_income_amount <= 13364594
                       END
                       OR
                       hmi.is_dual_income = TRUE AND
                       CASE
                           WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 9366906
                           WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 11151514
                           WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 11740362
                           WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 12653012
                           WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 13565661
                           ELSE hmi.monthly_avg_income_amount <= 14478311
                       END
                    THEN '우선공급'
                    ELSE '추첨공급'
                END
            ELSE ''
        END
    INTO 
        v_prime_type, 
        v_sub_type
    FROM Personal_Profiles AS pp
    LEFT JOIN Household_Info AS hi ON pp.resident_registration_number = hi.resident_registration_number
    LEFT JOIN Household_Monthly_Income_Info AS hmi ON hi.household_info_id = hmi.household_info_id
    LEFT JOIN Household_Asset_Info AS hai ON hi.household_info_id = hai.household_info_id
    LEFT JOIN Household_Children_Info AS hci ON hi.household_info_id = hci.household_info_id
    LEFT JOIN Household_Ancestor_Info AS haa ON hi.household_info_id = haa.household_info_id
    LEFT JOIN Personal_Marriage_Info AS pmi ON pp.resident_registration_number = pmi.resident_registration_number
    WHERE pp.resident_registration_number = p_rrn
    LIMIT 1;
    
    -- 결과 저장
    INSERT INTO Housing_Subscription_Eligibility (
        resident_registration_number, 
        eligibility_prime_type, 
        eligibility_sub_type, 
        last_assessment_date
    )
    VALUES (
        p_rrn,
        v_prime_type,
        v_sub_type,
        CURRENT_TIMESTAMP
    )
    ON DUPLICATE KEY UPDATE 
        eligibility_prime_type = VALUES(eligibility_prime_type),
        eligibility_sub_type = VALUES(eligibility_sub_type),
        last_assessment_date = CURRENT_TIMESTAMP;
        
    -- 완료 로그 및 실행 시간 측정
    INSERT INTO Processing_Log (log_message, log_timestamp)
    VALUES (CONCAT('Completed individual eligibility evaluation for RRN: ', p_rrn, 
                   ', Prime Type: ', v_prime_type,
                   ', Sub Type: ', v_sub_type,
                   ', Duration: ', TIMESTAMPDIFF(MICROSECOND, start_time, NOW()) / 1000000, ' seconds'), 
           NOW());
    END tmp_block;
END //

DELIMITER ;