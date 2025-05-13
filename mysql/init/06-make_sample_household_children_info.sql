START TRANSACTION;

USE app_db;

-- 세대 자녀 정보 데이터 생성
INSERT INTO Household_Children_Info (
  household_info_id,
  children_age_7_to_19_count,
  children_age_1_to_6_count
)
SELECT 
  hi.household_info_id,
  -- 7세 초과 19세 이하 자녀 수: 총 자녀 수의 일부 (랜덤 비율로 분배)
  FLOOR(hi.household_childeren_count * RAND()) AS children_age_7_to_19_count,
  -- 6세 이하 자녀 수: 총 자녀 수에서 나머지
  hi.household_childeren_count - FLOOR(hi.household_childeren_count * RAND()) AS children_age_1_to_6_count
FROM Household_Info AS hi
WHERE hi.household_childeren_count > 0;

-- 세대 자녀 정보 데이터 생성 후 확인 쿼리
SELECT 
  'Household_Children_Info 데이터 생성 완료' AS table_name,
  COUNT(*) AS record_count
FROM
  Household_Children_Info;

COMMIT;