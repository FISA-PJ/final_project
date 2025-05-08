-- 데이터베이스 생성 및 선택
CREATE DATABASE IF NOT EXISTS app_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE app_db;

-- 개인정보 테이블 (모든 국민 정보가 있는 테이블이므로 먼저 생성)
CREATE TABLE Personal_Profiles (
  personal_id          INT AUTO_INCREMENT   NOT NULL COMMENT '개인 고유 식별자',
  personal_name        VARCHAR(50)          NOT NULL COMMENT '개인 이름',
  personal_birth_date  DATE                 NOT NULL COMMENT '개인 생년월일',
  resident_registration_number VARCHAR(13) NOT NULL COMMENT '주민등록번호',
  created_at           DATETIME           NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '생성일시',
  updated_at           DATETIME           NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '수정일시',
  marriage_status      BOOLEAN            NOT NULL DEFAULT FALSE COMMENT '개인 혼인 여부',
  PRIMARY KEY (personal_id),
  UNIQUE INDEX idx_resident_registration_number (resident_registration_number)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '개인정보';

-- 사용자 테이블
CREATE TABLE Users (
  user_id                INT AUTO_INCREMENT                     NOT NULL COMMENT '사용자 고유 식별자',
  user_login_id          VARCHAR(50)                            NOT NULL COMMENT '사용자 아이디(로그인용)',
  user_password_hash     VARCHAR(255)                           NOT NULL COMMENT '사용자 비밀번호',
  resident_registration_number VARCHAR(13)                      NOT NULL COMMENT '주민등록번호',
  user_type              ENUM('admin', 'personal', 'reviewer')  NOT NULL DEFAULT 'personal' COMMENT '사용자 유형',
  user_registration_date DATETIME                               NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '가입일자',
  user_last_login_date   DATETIME                               NULL     COMMENT '최근 로그인 일자',
  PRIMARY KEY (user_id),
  UNIQUE INDEX idx_user_login_id (user_login_id),
  UNIQUE INDEX idx_resident_registration_number (resident_registration_number),
  CONSTRAINT FK_Personal_Profiles_TO_Users
    FOREIGN KEY (resident_registration_number)
    REFERENCES Personal_Profiles (resident_registration_number)
    ON DELETE RESTRICT
    ON UPDATE CASCADE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '사용자';

-- 청약 자격 조건 저장 테이블
CREATE TABLE IF NOT EXISTS Housing_Subscription_Eligibility (
  eligibility_id                  INT AUTO_INCREMENT NOT NULL                  COMMENT '자격 정보 고유 식별자',
  resident_registration_number    VARCHAR(13)        NOT NULL                  COMMENT '주민등록번호',
  eligibility_prime_type          VARCHAR(20)        NOT NULL                  COMMENT '청약 자격 1차 유형',
  eligibility_sub_type            VARCHAR(20)        NOT NULL                  COMMENT '청약 자격 2차 유형',
  last_assessment_date            DATETIME           DEFAULT CURRENT_TIMESTAMP COMMENT '최근 평가 일시',
  PRIMARY KEY (eligibility_id),
  INDEX idx_eligibility_rrn (resident_registration_number),
  CONSTRAINT FK_Personal_Profiles_TO_Housing_Subscription_Eligibility
    FOREIGN KEY (resident_registration_number)
    REFERENCES Personal_Profiles (resident_registration_number)
    ON DELETE CASCADE
    ON UPDATE CASCADE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '청약 자격 조건';

-- 포인트 점수 상세 정보 테이블 (기존 테이블들에 추가)
CREATE TABLE IF NOT EXISTS Point_Score_Details (
  point_score_id INT AUTO_INCREMENT PRIMARY KEY,
  eligibility_id INT NOT NULL,
  residence_period_point INT UNSIGNED DEFAULT 0 COMMENT '거주기간 가점',
  saving_period_point INT UNSIGNED DEFAULT 0 COMMENT '저축기간 가점',
  children_count_point INT UNSIGNED DEFAULT 0 COMMENT '자녀 수 가점',
  no_housing_period_point INT UNSIGNED DEFAULT 0 COMMENT '무주택기간 가점',
  CONSTRAINT FK_Housing_Subscription_Eligibility_TO_Point_Score_Details
    FOREIGN KEY (eligibility_id)
    REFERENCES Housing_Subscription_Eligibility (eligibility_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '가점 상세 정보';

-- 개인 혼인 정보 테이블
CREATE TABLE Personal_Marriage_Info (
  marriage_info_id  INT AUTO_INCREMENT                     NOT NULL COMMENT '혼인 정보 고유 식별자',
  resident_registration_number VARCHAR(13)                 NOT NULL COMMENT '주민등록번호',
  marriage_duration INT UNSIGNED                           NULL     DEFAULT 0 COMMENT '혼인 기간(개월)',
  marriage_type     ENUM('married', 'divorced', 'widowed') NULL     COMMENT '혼인 유형(기혼, 이혼, 사별)',
  PRIMARY KEY (marriage_info_id),
  CONSTRAINT FK_Personal_Profiles_TO_Personal_Marriage_Info
    FOREIGN KEY (resident_registration_number)
    REFERENCES Personal_Profiles (resident_registration_number)
    ON DELETE CASCADE
    ON UPDATE CASCADE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '개인 혼인 정보';

-- 개인 청약 저축 정보 테이블
CREATE TABLE Personal_Subscription_Savings_Info (
  personal_subscription_savings_info_id  INT AUTO_INCREMENT                       NOT NULL COMMENT '개인 청약 저축 정보 고유 식별자',
  resident_registration_number           VARCHAR(13)                              NOT NULL COMMENT '주민등록번호',
  subscription_savings_type              VARCHAR(50)                              NOT NULL COMMENT '청약 저축 유형',
  subscription_savings_period_months     INT UNSIGNED                             NULL     DEFAULT 0 COMMENT '가입기간(월)',
  monthly_deposit_amount                 DECIMAL(10, 0) UNSIGNED                  NULL     DEFAULT 0 COMMENT '월 납입금',
  number_of_payments                     INT UNSIGNED                             NULL     DEFAULT 0 COMMENT '약정납입일 준수 납입횟수',
  PRIMARY KEY (personal_subscription_savings_info_id),
  CONSTRAINT FK_Personal_Profiles_TO_Personal_Subscription_Savings_Info
    FOREIGN KEY (resident_registration_number)
    REFERENCES Personal_Profiles (resident_registration_number)
    ON DELETE CASCADE
    ON UPDATE CASCADE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '개인 청약 저축 정보';

-- 세대 구성 정보 테이블
CREATE TABLE Household_Info (
  household_info_id             INT AUTO_INCREMENT NOT NULL COMMENT '세대 구성 정보 고유 식별자',
  resident_registration_number  VARCHAR(13)        NOT NULL COMMENT '주민등록번호',
  household_member_count        INT UNSIGNED       NULL     DEFAULT 1 COMMENT '가구원 수(자기 포함)',
  household_childeren_count     INT UNSIGNED       NULL     DEFAULT 0 COMMENT '자녀 부양 여부',
  household_ancestor_count      INT UNSIGNED       NULL     DEFAULT 0 COMMENT '직계존속 부양 여부',
  is_household_without_housing  BOOLEAN            NULL     DEFAULT TRUE COMMENT '무주택세대구성원 여부',
  PRIMARY KEY (household_info_id),
  CONSTRAINT FK_Personal_Profiles_TO_Household_Info
    FOREIGN KEY (resident_registration_number)
    REFERENCES Personal_Profiles (resident_registration_number)
    ON DELETE CASCADE
    ON UPDATE CASCADE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '세대 구성 정보';

-- 세대 자녀 정보 테이블
CREATE TABLE Household_Children_Info (
  household_children_info_id  INT AUTO_INCREMENT NOT NULL COMMENT '세대 자녀 정보 고유 식별자',
  household_info_id           INT                NOT NULL COMMENT '세대 구성 정보 테이블 고유 식별자',
  children_age_7_to_19_count  INT UNSIGNED       NULL     DEFAULT 0 COMMENT '미성년(만 6세 초과 및 만 19세 이하) 자녀 수',
  children_age_1_to_6_count   INT UNSIGNED       NULL     DEFAULT 0 COMMENT '영유아(만 6세 이하) 자녀 수',
  PRIMARY KEY (household_children_info_id),
  CONSTRAINT FK_Household_Info_TO_Household_Children_Info
    FOREIGN KEY (household_info_id)
    REFERENCES Household_Info (household_info_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '세대 자녀 정보';

-- 세대 직계존속 부양 정보 테이블
CREATE TABLE Household_Ancestor_Info (
  household_ancestor_info_id INT AUTO_INCREMENT NOT NULL COMMENT '세대 직계존속 부양 정보 고유 식별자',
  household_info_id          INT                NOT NULL COMMENT '세대 구성 정보 테이블 고유 식별자',
  ancestor_age_above_65      INT UNSIGNED       NULL     DEFAULT 0 COMMENT '만 65세 이상 사람 수',
  ancestor_age_lessthan_65   INT UNSIGNED       NULL     DEFAULT 0 COMMENT '만 65세 미만 사람 수',
  ancestor_support_duration  INT UNSIGNED       NULL     DEFAULT 0 COMMENT '부양 기간(월)',
  PRIMARY KEY (household_ancestor_info_id),
  CONSTRAINT FK_Household_Info_TO_Household_Ancestor_Info
    FOREIGN KEY (household_info_id)
    REFERENCES Household_Info (household_info_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '세대 직계존속 부양 정보';

-- 세대 월소득 정보 테이블
CREATE TABLE Household_Monthly_Income_Info (
  household_monthly_income_info_id INT AUTO_INCREMENT NOT NULL COMMENT '세대 월소득 정보 고유 식별자',
  household_info_id                INT                NOT NULL COMMENT '세대 구성 정보 테이블 고유 식별자',
  is_dual_income                   BOOLEAN            NULL     DEFAULT FALSE COMMENT '본인 및 배우자 모두 소득 여부',
  monthly_avg_income_amount        DECIMAL(12, 0)     NULL     DEFAULT 0 COMMENT '세대 월 평균소득액(합)',
  PRIMARY KEY (household_monthly_income_info_id),
  CONSTRAINT FK_Household_Info_TO_Household_Monthly_Income_Info
    FOREIGN KEY (household_info_id)
    REFERENCES Household_Info (household_info_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '세대 월소득 정보';

-- 세대 자산 정보 테이블
CREATE TABLE Household_Asset_Info (
  household_asset_info INT AUTO_INCREMENT NOT NULL COMMENT '세대 자산 정보 고유 식별자',
  household_info_id    INT                NOT NULL COMMENT '세대 구성 정보 테이블 고유 식별자',
  real_estate_amount   DECIMAL(15, 0)     NULL     DEFAULT 0 COMMENT '부동산(토지+건축물)',
  car_value            DECIMAL(15, 0)     NULL     DEFAULT 0 COMMENT '자동차가액(세대내 차량최고가액)',
  PRIMARY KEY (household_asset_info),
  CONSTRAINT FK_Household_Info_TO_Household_Asset_Info
    FOREIGN KEY (household_info_id)
    REFERENCES Household_Info (household_info_id)
    ON DELETE CASCADE
    ON UPDATE CASCADE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '세대 자산 정보';

-- 로그 테이블
CREATE TABLE IF NOT EXISTS Processing_Log (
  log_id INT AUTO_INCREMENT PRIMARY KEY,
  log_message VARCHAR(255) NOT NULL,
  log_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci COMMENT '처리 로그';