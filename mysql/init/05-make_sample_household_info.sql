START TRANSACTION;

USE app_db;

-- 세대 구성 정보 데이터 생성
INSERT INTO Household_Info (
  resident_registration_number,
  household_member_count,
  household_childeren_count,
  household_ancestor_count,
  is_household_without_housing
)
SELECT 
  pp.resident_registration_number,
  -- 가구원 수: 본인(1명) + 배우자(혼인 상태일 경우 1명 추가) + 자녀 수 + 직계존속 수
  1 + 
  (CASE WHEN pp.marriage_status = TRUE THEN 1 ELSE 0 END) +
  child_count +
  ancestor_count AS household_member_count,
  -- 자녀 수: 30% 확률로 자녀가 있으며, 있을 경우 1~3명
  child_count,
  -- 직계존속 수: 15% 확률로 직계존속이 있으며, 있을 경우 1~2명
  ancestor_count,
  -- 무주택세대구성원 여부: 70% 확률로 무주택
  IF(RAND() < 0.7, TRUE, FALSE) AS is_household_without_housing
FROM (
  SELECT 
    resident_registration_number,
    marriage_status,
    (CASE WHEN RAND() < 0.3 THEN FLOOR(1 + RAND() * 3) ELSE 0 END) AS child_count,
    (CASE WHEN RAND() < 0.15 THEN FLOOR(1 + RAND() * 2) ELSE 0 END) AS ancestor_count
  FROM Personal_Profiles
) AS pp;

-- 세대 구성 정보 데이터 생성 후 확인 쿼리
SELECT
  'Household_Info 데이터 생성 완료' AS table_name,
  COUNT(*) AS record_count
FROM
  Household_Info;

COMMIT;