-- triggers.sql
-- 청약 자격 조건 평가를 위한 트리거

USE app_db;

-- 문자셋 설정
SET NAMES utf8mb4;
SET CHARACTER SET utf8mb4;

-- 기존 트리거 삭제
DROP TRIGGER IF EXISTS after_user_insert;
DROP TRIGGER IF EXISTS after_profile_update;
DROP TRIGGER IF EXISTS after_profile_insert;
DROP TRIGGER IF EXISTS after_savings_update;
DROP TRIGGER IF EXISTS after_savings_insert;
DROP TRIGGER IF EXISTS after_household_update;
DROP TRIGGER IF EXISTS after_household_insert;
DROP TRIGGER IF EXISTS after_monthly_income_update;
DROP TRIGGER IF EXISTS after_monthly_income_insert;
DROP TRIGGER IF EXISTS after_asset_update;
DROP TRIGGER IF EXISTS after_asset_insert;
DROP TRIGGER IF EXISTS after_children_update;
DROP TRIGGER IF EXISTS after_children_insert;
DROP TRIGGER IF EXISTS after_ancestor_update;
DROP TRIGGER IF EXISTS after_ancestor_insert;

-- 트리거 정의
DELIMITER //

-- 사용자 가입 시 자격 평가 트리거
CREATE TRIGGER after_user_insert
AFTER INSERT ON Users
FOR EACH ROW
BEGIN
  CALL evaluate_eligibility(NEW.resident_registration_number);
END //

-- 개인 프로필 업데이트 시 자격 재평가 트리거
CREATE TRIGGER after_profile_update
AFTER UPDATE ON Personal_Profiles
FOR EACH ROW
BEGIN
  CALL evaluate_eligibility(NEW.resident_registration_number);
END //

-- 개인 프로필 삽입 시 자격 평가 트리거
CREATE TRIGGER after_profile_insert
AFTER INSERT ON Personal_Profiles
FOR EACH ROW
BEGIN
  CALL evaluate_eligibility(NEW.resident_registration_number);
END //

-- 청약 저축 정보 변경 시 자격 재평가 트리거
CREATE TRIGGER after_savings_update
AFTER UPDATE ON Personal_Subscription_Savings_Info
FOR EACH ROW
BEGIN
  CALL evaluate_eligibility(NEW.resident_registration_number);
END //

-- 청약 저축 정보 삽입 시 자격 평가 트리거
CREATE TRIGGER after_savings_insert
AFTER INSERT ON Personal_Subscription_Savings_Info
FOR EACH ROW
BEGIN
  CALL evaluate_eligibility(NEW.resident_registration_number);
END //

-- 세대 정보 변경 시 자격 재평가 트리거
CREATE TRIGGER after_household_update
AFTER UPDATE ON Household_Info
FOR EACH ROW
BEGIN
  CALL evaluate_eligibility(NEW.resident_registration_number);
END //

-- 세대 정보 삽입 시 자격 평가 트리거
CREATE TRIGGER after_household_insert
AFTER INSERT ON Household_Info
FOR EACH ROW
BEGIN
  CALL evaluate_eligibility(NEW.resident_registration_number);
END //

-- 세대 월소득 정보 변경 시 자격 재평가 트리거
CREATE TRIGGER after_monthly_income_update
AFTER UPDATE ON Household_Monthly_Income_Info
FOR EACH ROW
BEGIN
  DECLARE rrn VARCHAR(13);
  SELECT resident_registration_number INTO rrn
  FROM Household_Info
  WHERE household_info_id = NEW.household_info_id;
  CALL evaluate_eligibility(rrn);
END //

-- 세대 월소득 정보 삽입 시 자격 평가 트리거
CREATE TRIGGER after_monthly_income_insert
AFTER INSERT ON Household_Monthly_Income_Info
FOR EACH ROW
BEGIN
  DECLARE rrn VARCHAR(13);
  SELECT resident_registration_number INTO rrn
  FROM Household_Info
  WHERE household_info_id = NEW.household_info_id;
  CALL evaluate_eligibility(rrn);
END //

-- 세대 자산 정보 변경 시 자격 재평가 트리거
CREATE TRIGGER after_asset_update
AFTER UPDATE ON Household_Asset_Info
FOR EACH ROW
BEGIN
  DECLARE rrn VARCHAR(13);
  SELECT resident_registration_number INTO rrn
  FROM Household_Info
  WHERE household_info_id = NEW.household_info_id;
  CALL evaluate_eligibility(rrn);
END //

-- 세대 자산 정보 삽입 시 자격 평가 트리거
CREATE TRIGGER after_asset_insert
AFTER INSERT ON Household_Asset_Info
FOR EACH ROW
BEGIN
  DECLARE rrn VARCHAR(13);
  SELECT resident_registration_number INTO rrn
  FROM Household_Info
  WHERE household_info_id = NEW.household_info_id;
  CALL evaluate_eligibility(rrn);
END //

-- 세대 자녀 정보 변경 시 자격 재평가 트리거
CREATE TRIGGER after_children_update
AFTER UPDATE ON Household_Children_Info
FOR EACH ROW
BEGIN
  DECLARE rrn VARCHAR(13);
  SELECT resident_registration_number INTO rrn
  FROM Household_Info
  WHERE household_info_id = NEW.household_info_id;
  CALL evaluate_eligibility(rrn);
END //

-- 세대 자녀 정보 삽입 시 자격 평가 트리거
CREATE TRIGGER after_children_insert
AFTER INSERT ON Household_Children_Info
FOR EACH ROW
BEGIN
  DECLARE rrn VARCHAR(13);
  SELECT resident_registration_number INTO rrn
  FROM Household_Info
  WHERE household_info_id = NEW.household_info_id;
  CALL evaluate_eligibility(rrn);
END //

-- 세대 직계존속 정보 변경 시 자격 재평가 트리거
CREATE TRIGGER after_ancestor_update
AFTER UPDATE ON Household_Ancestor_Info
FOR EACH ROW
BEGIN
  DECLARE rrn VARCHAR(13);
  SELECT resident_registration_number INTO rrn
  FROM Household_Info
  WHERE household_info_id = NEW.household_info_id;
  CALL evaluate_eligibility(rrn);
END //

-- 세대 직계존속 정보 삽입 시 자격 평가 트리거
CREATE TRIGGER after_ancestor_insert
AFTER INSERT ON Household_Ancestor_Info
FOR EACH ROW
BEGIN
  DECLARE rrn VARCHAR(13);
  SELECT resident_registration_number INTO rrn
  FROM Household_Info
  WHERE household_info_id = NEW.household_info_id;
  CALL evaluate_eligibility(rrn);
END //

DELIMITER ;