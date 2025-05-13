START TRANSACTION;

USE app_db;

-- 세대 자산 정보 데이터 생성
INSERT INTO Household_Asset_Info (
  household_info_id,
  real_estate_amount,
  car_value
)
SELECT 
  hi.household_info_id,
  -- 부동산 가액 (무주택인 경우 0원, 주택 보유의 경우 1억~10억)
  CASE 
    WHEN hi.is_household_without_housing = TRUE THEN 0
    ELSE 100000000 + FLOOR(RAND() * 900000000)
  END AS real_estate_amount,
  -- 차량 가액 (0원 ~ 5000만원)
  CASE 
    WHEN RAND() < 0.8 THEN FLOOR(RAND() * 50000000)
    ELSE 0
  END AS car_value
FROM Household_Info AS hi;

-- 세대 자산 정보 생성 후 확인 쿼리
SELECT 'Household_Asset_Info 데이터 생성 완료' AS table_name, COUNT(*) AS record_count FROM Household_Asset_Info;

COMMIT;