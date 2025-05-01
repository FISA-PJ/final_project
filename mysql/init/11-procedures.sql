-- procedures.sql
-- 청약 자격 조건 평가를 위한 프로시저

USE app_db;

-- 문자셋 설정
SET NAMES utf8mb4;
SET CHARACTER SET utf8mb4;

-- 기존 프로시저 삭제
DROP PROCEDURE IF EXISTS bulk_evaluate_eligibility;
DROP PROCEDURE IF EXISTS evaluate_eligibility;

-- 대량 자격 평가 프로시저
DELIMITER //
CREATE PROCEDURE bulk_evaluate_eligibility()
BEGIN
    -- 개별 자격 평가 로직을 대량 처리용으로 최적화
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
                AND EXISTS (
                    SELECT 1 FROM Personal_Subscription_Savings_Info AS pssi2
                    WHERE pssi2.resident_registration_number = pp.resident_registration_number 
                    AND pssi2.subscription_savings_period_months >= 6
                    AND pssi2.number_of_payments >= 6
                )
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
                AND EXISTS (
                    SELECT 1 FROM Personal_Subscription_Savings_Info AS pssi2
                    WHERE pssi2.resident_registration_number = pp.resident_registration_number 
                    AND pssi2.subscription_savings_period_months >= 6
                    AND pssi2.number_of_payments >= 6
                )
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
                AND EXISTS (
                    SELECT 1 FROM Personal_Subscription_Savings_Info AS pssi2
                    WHERE pssi2.resident_registration_number = pp.resident_registration_number 
                    AND pssi2.subscription_savings_period_months >= 6
                    AND pssi2.number_of_payments >= 6
                )
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
            -- 구체적인 2차 유형 케이스 로직
            WHEN (IFNULL(pmi.marriage_duration, 0) <= 84 OR (pmi.marriage_type = '이혼' AND IFNULL(hci.children_age_1_to_6_count, 0) >= 1))
                AND hi.is_household_without_housing = TRUE
                AND hai.real_estate_amount <= 215500000
                AND hai.car_value <= 38030000
                AND EXISTS (
                    SELECT 1 FROM Personal_Subscription_Savings_Info AS pssi2
                    WHERE pssi2.resident_registration_number = pp.resident_registration_number 
                    AND pssi2.subscription_savings_period_months >= 6
                    AND pssi2.number_of_payments >= 6
                )
            THEN 
                CASE 
                    WHEN (hmi.is_dual_income = FALSE AND (
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 7205312
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 8578088
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 9031048
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 9733086
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 10435124
                               ELSE hmi.monthly_avg_income_amount <= 11137162
                           END
                       ))
                       OR
                       (hmi.is_dual_income = TRUE AND (
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 8646374
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 10293706
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 10837258
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 11679703
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 12522149
                               ELSE hmi.monthly_avg_income_amount <= 13364594
                           END
                       ))
                   THEN '우선공급'
                   WHEN (hmi.is_dual_income = FALSE AND (
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 5764250
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 6862470
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 7224838
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 7786469
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 8348099
                               ELSE hmi.monthly_avg_income_amount <= 8909730
                           END
                       ))
                       OR
                       (hmi.is_dual_income = TRUE AND (
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 7205312
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 8578088
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 9031048
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 9733086
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 10435124
                               ELSE hmi.monthly_avg_income_amount <= 11137162
                           END
                       ))
                   THEN '우선공급(배점)'
                   ELSE '추첨공급'
                END
            WHEN IFNULL(haa.ancestor_age_above_65, 0) >= 1
                AND IFNULL(haa.ancestor_support_duration, 0) >= 36
                AND hi.is_household_without_housing = TRUE
                AND hai.real_estate_amount <= 215500000
                AND hai.car_value <= 38030000
                AND EXISTS (
                    SELECT 1 FROM Personal_Subscription_Savings_Info AS pssi2
                    WHERE pssi2.resident_registration_number = pp.resident_registration_number 
                    AND pssi2.subscription_savings_period_months >= 6
                    AND pssi2.number_of_payments >= 6
                )
            THEN
                CASE
                    WHEN (hmi.is_dual_income = FALSE AND (
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 8646374
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 10293706
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 10837258
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 11679703
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 12522149
                               ELSE hmi.monthly_avg_income_amount <= 13364594
                           END
                       ))
                       OR
                       (hmi.is_dual_income = TRUE AND (
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 9366906
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 11151514
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 11740362
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 12653012
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 13565661
                               ELSE hmi.monthly_avg_income_amount <= 14478311
                           END
                       ))
                    THEN '우선공급'
                    ELSE '추첨공급'
                END
            WHEN (IFNULL(hci.children_age_1_to_6_count, 0) + IFNULL(hci.children_age_7_to_19_count, 0)) >= 2
                AND hi.is_household_without_housing = TRUE
                AND hai.real_estate_amount <= 215500000
                AND hai.car_value <= 38030000
                AND EXISTS (
                    SELECT 1 FROM Personal_Subscription_Savings_Info AS pssi2
                    WHERE pssi2.resident_registration_number = pp.resident_registration_number 
                    AND pssi2.subscription_savings_period_months >= 6
                    AND pssi2.number_of_payments >= 6
                )
            THEN
                CASE
                    WHEN (hmi.is_dual_income = FALSE AND (
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 8646374
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 10293706
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 10837258
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 11679703
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 12522149
                               ELSE hmi.monthly_avg_income_amount <= 13364594
                           END
                       ))
                       OR
                       (hmi.is_dual_income = TRUE AND (
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 9366906
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 11151514
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 11740362
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 12653012
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 13565661
                               ELSE hmi.monthly_avg_income_amount <= 14478311
                           END
                       ))
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
    LEFT JOIN Personal_Subscription_Savings_Info AS pssi ON pp.resident_registration_number = pssi.resident_registration_number
    LEFT JOIN Personal_Marriage_Info AS pmi ON pp.resident_registration_number = pmi.resident_registration_number
    ON DUPLICATE KEY UPDATE 
        eligibility_prime_type = VALUES(eligibility_prime_type),
        eligibility_sub_type = VALUES(eligibility_sub_type),
        last_assessment_date = CURRENT_TIMESTAMP;
END //
DELIMITER ;

-- 개별 자격 평가 프로시저
DELIMITER //
CREATE PROCEDURE evaluate_eligibility(IN p_rrn VARCHAR(13))
BEGIN
    -- 개별 자격 평가 로직을 대량 처리용으로 최적화
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
                AND EXISTS (
                    SELECT 1 FROM Personal_Subscription_Savings_Info AS pssi2
                    WHERE pssi2.resident_registration_number = pp.resident_registration_number 
                    AND pssi2.subscription_savings_period_months >= 6
                    AND pssi2.number_of_payments >= 6
                )
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
                AND EXISTS (
                    SELECT 1 FROM Personal_Subscription_Savings_Info AS pssi2
                    WHERE pssi2.resident_registration_number = pp.resident_registration_number 
                    AND pssi2.subscription_savings_period_months >= 6
                    AND pssi2.number_of_payments >= 6
                )
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
                AND EXISTS (
                    SELECT 1 FROM Personal_Subscription_Savings_Info AS pssi2
                    WHERE pssi2.resident_registration_number = pp.resident_registration_number 
                    AND pssi2.subscription_savings_period_months >= 6
                    AND pssi2.number_of_payments >= 6
                )
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
            -- 구체적인 2차 유형 케이스 로직
            WHEN (IFNULL(pmi.marriage_duration, 0) <= 84 OR (pmi.marriage_type = '이혼' AND IFNULL(hci.children_age_1_to_6_count, 0) >= 1))
                AND hi.is_household_without_housing = TRUE
                AND hai.real_estate_amount <= 215500000
                AND hai.car_value <= 38030000
                AND EXISTS (
                    SELECT 1 FROM Personal_Subscription_Savings_Info AS pssi2
                    WHERE pssi2.resident_registration_number = pp.resident_registration_number 
                    AND pssi2.subscription_savings_period_months >= 6
                    AND pssi2.number_of_payments >= 6
                )
            THEN 
                CASE 
                    WHEN (hmi.is_dual_income = FALSE AND (
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 7205312
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 8578088
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 9031048
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 9733086
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 10435124
                               ELSE hmi.monthly_avg_income_amount <= 11137162
                           END
                       ))
                       OR
                       (hmi.is_dual_income = TRUE AND (
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 8646374
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 10293706
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 10837258
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 11679703
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 12522149
                               ELSE hmi.monthly_avg_income_amount <= 13364594
                           END
                       ))
                   THEN '우선공급'
                   WHEN (hmi.is_dual_income = FALSE AND (
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 5764250
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 6862470
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 7224838
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 7786469
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 8348099
                               ELSE hmi.monthly_avg_income_amount <= 8909730
                           END
                       ))
                       OR
                       (hmi.is_dual_income = TRUE AND (
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 7205312
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 8578088
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 9031048
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 9733086
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 10435124
                               ELSE hmi.monthly_avg_income_amount <= 11137162
                           END
                       ))
                   THEN '우선공급(배점)'
                   ELSE '추첨공급'
                END
            WHEN IFNULL(haa.ancestor_age_above_65, 0) >= 1
                AND IFNULL(haa.ancestor_support_duration, 0) >= 36
                AND hi.is_household_without_housing = TRUE
                AND hai.real_estate_amount <= 215500000
                AND hai.car_value <= 38030000
                AND EXISTS (
                    SELECT 1 FROM Personal_Subscription_Savings_Info AS pssi2
                    WHERE pssi2.resident_registration_number = pp.resident_registration_number 
                    AND pssi2.subscription_savings_period_months >= 6
                    AND pssi2.number_of_payments >= 6
                )
            THEN
                CASE
                    WHEN (hmi.is_dual_income = FALSE AND (
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 8646374
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 10293706
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 10837258
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 11679703
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 12522149
                               ELSE hmi.monthly_avg_income_amount <= 13364594
                           END
                       ))
                       OR
                       (hmi.is_dual_income = TRUE AND (
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 9366906
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 11151514
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 11740362
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 12653012
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 13565661
                               ELSE hmi.monthly_avg_income_amount <= 14478311
                           END
                       ))
                    THEN '우선공급'
                    ELSE '추첨공급'
                END
            WHEN (IFNULL(hci.children_age_1_to_6_count, 0) + IFNULL(hci.children_age_7_to_19_count, 0)) >= 2
                AND hi.is_household_without_housing = TRUE
                AND hai.real_estate_amount <= 215500000
                AND hai.car_value <= 38030000
                AND EXISTS (
                    SELECT 1 FROM Personal_Subscription_Savings_Info AS pssi2
                    WHERE pssi2.resident_registration_number = pp.resident_registration_number 
                    AND pssi2.subscription_savings_period_months >= 6
                    AND pssi2.number_of_payments >= 6
                )
            THEN
                CASE
                    WHEN (hmi.is_dual_income = FALSE AND (
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 8646374
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 10293706
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 10837258
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 11679703
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 12522149
                               ELSE hmi.monthly_avg_income_amount <= 13364594
                           END
                       ))
                       OR
                       (hmi.is_dual_income = TRUE AND (
                           CASE
                               WHEN hi.household_member_count <= 3 THEN hmi.monthly_avg_income_amount <= 9366906
                               WHEN hi.household_member_count = 4 THEN hmi.monthly_avg_income_amount <= 11151514
                               WHEN hi.household_member_count = 5 THEN hmi.monthly_avg_income_amount <= 11740362
                               WHEN hi.household_member_count = 6 THEN hmi.monthly_avg_income_amount <= 12653012
                               WHEN hi.household_member_count = 7 THEN hmi.monthly_avg_income_amount <= 13565661
                               ELSE hmi.monthly_avg_income_amount <= 14478311
                           END
                       ))
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
    LEFT JOIN Personal_Subscription_Savings_Info AS pssi ON pp.resident_registration_number = pssi.resident_registration_number
    LEFT JOIN Personal_Marriage_Info AS pmi ON pp.resident_registration_number = pmi.resident_registration_number
    ON DUPLICATE KEY UPDATE 
        eligibility_prime_type = VALUES(eligibility_prime_type),
        eligibility_sub_type = VALUES(eligibility_sub_type),
        last_assessment_date = CURRENT_TIMESTAMP;
END //
DELIMITER ;