START TRANSACTION;

USE app_db;

-- 세대 직계존속 부양 정보 데이터 생성
INSERT INTO Household_Ancestor_Info (
  household_info_id,
  ancestor_age_above_65,
  ancestor_age_lessthan_65,
  ancestor_support_duration
)
SELECT 
  hi.household_info_id,
  -- 65세 이상 직계존속 수: 총 직계존속 수의 일부 (랜덤 비율로 분배)
  FLOOR(hi.household_ancestor_count * RAND()) AS ancestor_age_above_65,
  -- 65세 미만 직계존속 수: 총 직계존속 수에서 나머지
  hi.household_ancestor_count - FLOOR(hi.household_ancestor_count * RAND()) AS ancestor_age_lessthan_65,
  -- 부양 기간 (1~120개월)
  FLOOR(1 + RAND() * 120) AS ancestor_support_duration
FROM Household_Info AS hi
WHERE hi.household_ancestor_count > 0;

-- 세대 직계존속 부양 정보 생성 후 확인 쿼리
SELECT 'Household_Ancestor_Info 데이터 생성 완료' AS table_name, COUNT(*) AS record_count FROM Household_Ancestor_Info;

COMMIT;