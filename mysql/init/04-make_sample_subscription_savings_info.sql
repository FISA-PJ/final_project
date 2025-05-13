START TRANSACTION;

USE app_db;

-- 인덱스 비활성화 (기본 키 제외)
ALTER TABLE Personal_Subscription_Savings_Info DISABLE KEYS;

-- 개인 청약 저축 정보 데이터 생성
INSERT INTO Personal_Subscription_Savings_Info (
  resident_registration_number,
  subscription_savings_type,
  subscription_savings_period_months,
  monthly_deposit_amount,
  number_of_payments
)
SELECT 
  resident_registration_number,
  -- 청약 저축 유형 랜덤 선택 (1부터 4까지의 값만 반환하도록 명확히 보장)
  ELT(GREATEST(1, LEAST(4, FLOOR(1 + RAND() * 4))), '주택청약종합저축', '청약저축', '청약예금', '청약부금') AS subscription_savings_type,
  -- 가입 기간 (0~120개월)
  FLOOR(RAND() * 121) AS subscription_savings_period_months,
  -- 월 납입금액 (5만원 ~ 50만원, 5만원 단위)
  FLOOR(1 + RAND() * 10) * 50000 AS monthly_deposit_amount,
  -- 납입 횟수 (0~120회)
  FLOOR(RAND() * 121) AS number_of_payments
FROM Personal_Profiles
WHERE RAND() < 0.7; -- 70%의 사람만 청약 저축 가입

-- 인덱스 재활성화
ALTER TABLE Personal_Subscription_Savings_Info ENABLE KEYS;

-- 개인 청약 저축 정보 데이터 생성 후 확인 쿼리
SELECT
  'Personal_Subscription_Savings_Info 데이터 생성 완료' AS table_name,
  COUNT(*) AS record_count
FROM
  Personal_Subscription_Savings_Info;

COMMIT;