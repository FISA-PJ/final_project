START TRANSACTION;

USE app_db;

-- 현재 날짜 설정 (테스트 데이터 생성 기준일)
SET @current_date = CURDATE();

-- 실행 시간 단축을 위해 임시 테이블 생성
DROP TEMPORARY TABLE IF EXISTS temp_personal_profiles;
CREATE TEMPORARY TABLE temp_personal_profiles (
  personal_id INT NOT NULL AUTO_INCREMENT,
  personal_name VARCHAR(50) NOT NULL,
  personal_birth_date DATE NOT NULL,
  resident_registration_number VARCHAR(13) NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  marriage_status BOOLEAN NOT NULL,
  PRIMARY KEY (personal_id)
);

-- 먼저 성씨 데이터 생성
DROP TEMPORARY TABLE IF EXISTS temp_surnames;
CREATE TEMPORARY TABLE temp_surnames (
  surname VARCHAR(10)
) ENGINE=InnoDB;

-- 성씨 데이터 삽입 (레이블 루프 대신 프로시저 사용)
DELIMITER $$

CREATE PROCEDURE insert_surnames()
BEGIN
  SET @korean_surnames = '김,이,박,최,정,강,조,윤,장,임,한,오,서,신,권,황,안,송,류,전,홍,고,문,양,손,배,조,백,허,유,남,심,노,정,하,곽,성,차,주,우,구,신,임,전,민,유,진,지,엄,채,원,천,방,공,현,함,변,염,여,추,도,소,석,선,설,마,길,위,표,명,기,반,왕,금,옥,육,인,맹,제,모,탁,국,여,진,상,변,성,라,왕,하,방,송,인,간,감';
  
  SET @current_surname = @korean_surnames;

  SET @loop_count = 0;
  
  WHILE LENGTH(@current_surname) > 0 AND @loop_count < 200 DO
    SET @loop_count = @loop_count + 1;
    SET @pos = INSTR(@current_surname, ',');
    
    IF @pos = 0 THEN
      -- 마지막 성씨
      INSERT INTO temp_surnames VALUES (@current_surname);
      SET @current_surname = '';
    ELSE
      -- 성씨 추출 및 저장
      INSERT INTO temp_surnames VALUES (LEFT(@current_surname, @pos - 1));
      SET @current_surname = SUBSTRING(@current_surname, @pos + 1);
    END IF;
  END WHILE;
END$$

CREATE PROCEDURE generate_personal_profiles()
BEGIN
  DECLARE i INT DEFAULT 0;
  DECLARE surname VARCHAR(10);
  DECLARE firstname VARCHAR(10);
  DECLARE birth_date DATE;
  DECLARE gender CHAR(1);
  DECLARE birth_year CHAR(2);
  DECLARE birth_month CHAR(2);
  DECLARE birth_day CHAR(2);
  DECLARE gender_digit CHAR(1);
  DECLARE random_digits CHAR(6);
  DECLARE rrn VARCHAR(13);
  DECLARE created_datetime DATETIME;
  DECLARE marriage_status BOOLEAN;
  
  -- 먼저 성씨 데이터 생성
  CALL insert_surnames();
  
  WHILE i < 10000 DO
    -- 이름 생성
    SELECT surname INTO @surname FROM temp_surnames ORDER BY RAND() LIMIT 1;
    IF @surname IS NULL OR @surname = '' THEN
      SET surname = '김';
    ELSE
      SET surname = @surname;
    END IF;
    
    -- 이름 패턴 (1글자 또는 2글자)
    IF RAND() < 0.3 THEN
      -- 1글자 이름 (성 + 1글자)
      SET firstname = ELT(FLOOR(1 + RAND() * 20), '준', '민', '서', '지', '현', '우', '진', '수', '영', '원', '호', '윤', '재', '은', '도', '주', '혜', '성', '경', '석');
    ELSE
      -- 2글자 이름 (성 + 2글자)
      SET firstname = CONCAT(
        ELT(FLOOR(1 + RAND() * 20), '민', '서', '지', '현', '우', '준', '진', '수', '영', '원', '호', '윤', '재', '은', '도', '주', '혜', '성', '경'),
        ELT(FLOOR(1 + RAND() * 20), '준', '민', '서', '지', '현', '우', '진', '수', '영', '원', '호', '윤', '재', '은', '도', '주', '혜', '성', '경')
      );
    END IF;
    
    IF firstname IS NULL OR firstname = '' THEN
      SET firstname = '민준';
    END IF;
    
    -- 생년월일 생성 (1960년 ~ 2006년)
    SET birth_year = LPAD(FLOOR(60 + RAND() * 47), 2, '0');
    SET birth_month = LPAD(FLOOR(1 + RAND() * 12), 2, '0');
    
    -- 월별 일수 계산
    SET birth_day = CASE birth_month
      WHEN '02' THEN LPAD(FLOOR(1 + RAND() * 28), 2, '0')
      WHEN '04' THEN LPAD(FLOOR(1 + RAND() * 30), 2, '0')
      WHEN '06' THEN LPAD(FLOOR(1 + RAND() * 30), 2, '0')
      WHEN '09' THEN LPAD(FLOOR(1 + RAND() * 30), 2, '0')
      WHEN '11' THEN LPAD(FLOOR(1 + RAND() * 30), 2, '0')
      ELSE LPAD(FLOOR(1 + RAND() * 31), 2, '0')
    END;
    
    SET birth_date = STR_TO_DATE(CONCAT('19', birth_year, '-', birth_month, '-', birth_day), '%Y-%m-%d');
    
    -- 성별 랜덤 설정 (남성: 1,3,5,7,9 / 여성: 0,2,4,6,8)
    SET gender = IF(RAND() < 0.5, 'M', 'F');
    
    -- 주민등록번호 생성
    -- 1900년대 출생: 남성 1, 여성 2
    -- 2000년대 출생: 남성 3, 여성 4
    IF birth_year >= '00' AND birth_year <= '06' THEN
      -- 2000년대 출생
      SET gender_digit = IF(gender = 'M', '3', '4');
    ELSE
      -- 1900년대 출생
      SET gender_digit = IF(gender = 'M', '1', '2');
    END IF;
    
    -- 주민등록번호 조합 (앞 6자리 + 뒷 7자리)
    SET random_digits = LPAD(FLOOR(RAND() * 1000000), 6, '0');
    SET rrn = CONCAT(birth_year, birth_month, birth_day, gender_digit, random_digits);
    -- 중복 체크 (임시 테이블 내에서 확인)
    SET @retry_count = 0;
    WHILE EXISTS (SELECT 1 FROM temp_personal_profiles WHERE resident_registration_number = rrn) DO
      SET @retry_count = @retry_count + 1;
      SET random_digits = LPAD(FLOOR(RAND() * 1000000), 6, '0');
      SET rrn = CONCAT(birth_year, birth_month, birth_day, gender_digit, random_digits);
    END WHILE;
    IF @retry_count >= 100 THEN
    SET rrn = CONCAT(birth_year, birth_month, birth_day, gender_digit, '999999'); -- 임시 값으로 대체
    END IF;

    -- 생성/수정 일시 (1~3년 전)
    SET created_datetime = DATE_SUB(@current_date, INTERVAL FLOOR(365 + RAND() * 730) DAY);
    
    -- 혼인 상태 (연령대에 따라 확률 조정)
    -- 20세 미만: 대부분 미혼
    -- 20~29세: 30% 기혼
    -- 30~39세: 70% 기혼
    -- 40세 이상: 90% 기혼
    SET marriage_status = FALSE;
    IF TIMESTAMPDIFF(YEAR, birth_date, @current_date) < 20 THEN
      SET marriage_status = FALSE;
    ELSEIF TIMESTAMPDIFF(YEAR, birth_date, @current_date) < 30 THEN
      SET marriage_status = IF(RAND() < 0.3, TRUE, FALSE);
    ELSEIF TIMESTAMPDIFF(YEAR, birth_date, @current_date) < 40 THEN
      SET marriage_status = IF(RAND() < 0.7, TRUE, FALSE);
    ELSE
      SET marriage_status = IF(RAND() < 0.9, TRUE, FALSE);
    END IF;
    
    -- 임시 테이블에 데이터 삽입
    INSERT INTO temp_personal_profiles (
      personal_name, 
      personal_birth_date, 
      resident_registration_number, 
      created_at, 
      updated_at, 
      marriage_status
    ) VALUES (
      CONCAT(surname, firstname),
      birth_date,
      rrn,
      created_datetime,
      created_datetime,
      marriage_status
    );
    
    SET i = i + 1;
    
    -- 200개 단위로 실제 테이블에 이관 및 임시 테이블 비우기
    IF i % 200 = 0 THEN
      INSERT IGNORE INTO Personal_Profiles (
        personal_name, 
        personal_birth_date, 
        resident_registration_number, 
        created_at, 
        updated_at, 
        marriage_status
      ) 
      SELECT 
        personal_name, 
        personal_birth_date, 
        resident_registration_number, 
        created_at, 
        updated_at, 
        marriage_status 
      FROM temp_personal_profiles;
      
      -- 로그 기록
      SELECT CONCAT('데이터 처리 진행 중: ', i, ' 레코드 완료') AS progress;
      
      -- 임시 테이블 비우기
      TRUNCATE TABLE temp_personal_profiles;
      
      -- 중간 커밋
      COMMIT;
    END IF;
  END WHILE;
  
  -- 남은 데이터 처리
  IF EXISTS (SELECT 1 FROM temp_personal_profiles) THEN
    INSERT IGNORE INTO Personal_Profiles (
      personal_name, 
      personal_birth_date, 
      resident_registration_number, 
      created_at, 
      updated_at, 
      marriage_status
    ) 
    SELECT 
      personal_name, 
      personal_birth_date, 
      resident_registration_number, 
      created_at, 
      updated_at, 
      marriage_status 
    FROM temp_personal_profiles;
    
    TRUNCATE TABLE temp_personal_profiles;
  END IF;
END$$

DELIMITER ;

-- 데이터 생성 프로시저 호출
SET autocommit = 0;
CALL generate_personal_profiles();

-- 프로시저 삭제
DROP PROCEDURE IF EXISTS insert_surnames;
DROP PROCEDURE IF EXISTS generate_personal_profiles;

-- 개인 프로필 데이터 생성 후 확인 쿼리
SELECT
  'Personal_Profiles 데이터 생성 완료' AS table_name, 
  COUNT(*) AS record_count 
FROM 
  Personal_Profiles;

-- 임시 테이블 삭제
DROP TEMPORARY TABLE IF EXISTS temp_surnames;
DROP TEMPORARY TABLE IF EXISTS temp_personal_profiles;

COMMIT;