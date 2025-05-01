START TRANSACTION;

USE app_db;

-- 데이터 생성 후 자격 평가 수행
CALL bulk_evaluate_eligibility();

-- 데이터 검증 쿼리
SELECT 'Personal_Profiles' AS table_name, COUNT(*) AS record_count FROM Personal_Profiles UNION ALL
SELECT 'Users', COUNT(*) FROM Users UNION ALL
SELECT 'Personal_Marriage_Info', COUNT(*) FROM Personal_Marriage_Info UNION ALL
SELECT 'Personal_Subscription_Savings_Info', COUNT(*) FROM Personal_Subscription_Savings_Info UNION ALL
SELECT 'Household_Info', COUNT(*) FROM Household_Info UNION ALL
SELECT 'Household_Children_Info', COUNT(*) FROM Household_Children_Info UNION ALL
SELECT 'Household_Ancestor_Info', COUNT(*) FROM Household_Ancestor_Info UNION ALL
SELECT 'Household_Monthly_Income_Info', COUNT(*) FROM Household_Monthly_Income_Info UNION ALL
SELECT 'Household_Asset_Info', COUNT(*) FROM Household_Asset_Info;

SELECT 
  (SELECT COUNT(*) FROM Personal_Profiles) AS personal_profiles_count,
  (SELECT COUNT(*) FROM Users) AS users_count,
  (SELECT COUNT(*) FROM Household_Info) AS household_info_count;

COMMIT;