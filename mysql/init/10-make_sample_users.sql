START TRANSACTION;

USE app_db;

-- 현재 날짜 설정 (테스트 데이터 생성 기준일)
SET @current_date = CURDATE();

-- 사용자 데이터 생성 (500명)
-- 먼저 랜덤하게 500명 선택
CREATE TEMPORARY TABLE selected_persons AS
SELECT resident_registration_number
FROM Personal_Profiles
ORDER BY RAND()
LIMIT 500;

-- 사용자 데이터 생성
INSERT INTO Users (
  user_login_id,
  user_password_hash,
  resident_registration_number,
  user_type,
  user_registration_date,
  user_last_login_date
)
SELECT 
  -- 사용자 아이디 (이메일 형식)
  CONCAT(
    LOWER(
      SUBSTRING(
        REPLACE(pp.personal_name, ' ', ''),
        1,
        IF(LENGTH(pp.personal_name) > 3, 3, LENGTH(pp.personal_name))
      )
    ),
    SUBSTRING(pp.resident_registration_number, 1, 6),
    '@',
    ELT(1 + FLOOR(RAND() * 5), 'gmail.com', 'naver.com', 'daum.net', 'kakao.com', 'outlook.com')
  ) AS user_login_id,
  -- 비밀번호 해시 (현실에서는 실제 암호화 알고리즘 사용 필요)
  CONCAT('$2a$10$', SUBSTRING(MD5(RAND()), 1, 53)) AS user_password_hash,
  sp.resident_registration_number,
  -- 사용자 유형 (admin 1%, reviewer 2%, personal 97%)
  CASE 
    WHEN RAND() < 0.01 THEN 'admin'
    WHEN RAND() < 0.02 THEN 'reviewer'
    ELSE 'personal'
  END AS user_type,
  -- 가입일자 (최근 1년 내)
  DATE_SUB(@current_date, INTERVAL FLOOR(RAND() * 365) DAY) AS user_registration_date,
  -- 최근 로그인 일자 (가입일 이후)
  CASE 
    WHEN RAND() < 0.8 THEN DATE_SUB(@current_date, INTERVAL FLOOR(RAND() * 30) DAY)
    ELSE NULL
  END AS user_last_login_date
FROM selected_persons sp
JOIN Personal_Profiles pp ON sp.resident_registration_number = pp.resident_registration_number;

-- 사용자 데이터 생성 후 확인 쿼리
SELECT 'Users 데이터 생성 완료' AS table_name, COUNT(*) AS record_count FROM Users;

-- 임시 테이블 삭제
DROP TEMPORARY TABLE IF EXISTS selected_persons;

COMMIT;