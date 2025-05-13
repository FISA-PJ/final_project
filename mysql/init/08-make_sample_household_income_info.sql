START TRANSACTION;

USE app_db;

-- 세대 월소득 정보 데이터 생성
INSERT INTO Household_Monthly_Income_Info (
  household_info_id,
  is_dual_income,
  monthly_avg_income_amount
)
SELECT 
  hi.household_info_id,
  -- 맞벌이 여부 (혼인 상태인 경우 50% 확률로 맞벌이)
  IF(RAND() < 0.5 AND pp.marriage_status = TRUE, TRUE, FALSE) AS is_dual_income,
  -- 월 평균 소득액 (200만원 ~ 1000만원)
  CASE
    -- 맞벌이 여부와 혼인 상태에 따라 소득 범위 조정
    WHEN IF(RAND() < 0.5 AND pp.marriage_status = TRUE, TRUE, FALSE) = TRUE THEN 
      -- 맞벌이일 경우: 300만~1000만 원 (중위 소득 근처에 몰리도록 조정)
      FLOOR(3000000 + (RAND() * RAND() * 7000000))  -- 중위 소득(약 500만~600만) 근처에 데이터 몰림
    ELSE
      -- 맞벌이가 아닐 경우: 200만~600만 원
      FLOOR(2000000 + (RAND() * RAND() * 4000000))  -- 중위 소득(약 300만~400만) 근처에 데이터 몰림
  END +
  -- 가구원 수에 따라 소득 약간 증가 (가구원 1명당 약 50만 원 추가 가능성)
  (CASE WHEN hi.household_member_count > 2 THEN FLOOR(RAND() * 500000 * (hi.household_member_count - 2)) ELSE 0 END) AS monthly_avg_income_amount
FROM Household_Info AS hi
JOIN Personal_Profiles AS pp ON hi.resident_registration_number = pp.resident_registration_number;

-- 세대 월소득 정보 데이터 생성 후 확인 쿼리
SELECT 'Household_Monthly_Income_Info 데이터 생성 완료' AS table_name, COUNT(*) AS record_count FROM Household_Monthly_Income_Info;
COMMIT;