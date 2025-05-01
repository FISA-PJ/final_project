START TRANSACTION;

USE app_db;

-- 개인 혼인 정보 데이터 생성
INSERT INTO Personal_Marriage_Info (
  resident_registration_number,
  marriage_duration,
  marriage_type
)
SELECT 
  resident_registration_number,
  -- 혼인 중인 경우 혼인 기간 및 유형 설정
  CASE 
    WHEN marriage_status = TRUE THEN 
      FLOOR(1 + RAND() * 240) -- 1개월 ~ 20년
    ELSE 
      0
  END AS marriage_duration,
  CASE 
    WHEN marriage_status = TRUE THEN 
      CASE 
        WHEN RAND() < 0.8 THEN 'married'
        WHEN RAND() < 0.5 THEN 'divorced'
        ELSE 'widowed'
      END
    ELSE 
      NULL
  END AS marriage_type
FROM Personal_Profiles;

-- 개인 혼인 정보 데이터 생성 후 확인 쿼리
SELECT
  'Personal_Marriage_Info 데이터 생성 완료' AS table_name,
  COUNT(*) AS record_count
FROM
  Personal_Marriage_Info;

COMMIT;