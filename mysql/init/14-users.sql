-- root 계정 원격 접속 제거 (있는 경우)
DROP USER IF EXISTS 'root'@'%';

-- root는 localhost에서만 접속 가능하도록 설정
ALTER USER 'root'@'localhost' IDENTIFIED WITH caching_sha2_password BY '1dlxoals!';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' WITH GRANT OPTION;

-- 원격 접속용 관리자 계정 생성 및 권한 설정
CREATE USER IF NOT EXISTS 'admin'@'%' IDENTIFIED WITH caching_sha2_password BY 'admin_password';
GRANT ALL PRIVILEGES ON *.* TO 'admin'@'%' WITH GRANT OPTION;

-- 애플리케이션 사용자 생성 및 권한 설정
CREATE USER IF NOT EXISTS 'app_user'@'%' IDENTIFIED WITH caching_sha2_password BY 'app_user_password';
GRANT ALL PRIVILEGES ON `app_db`.* TO 'app_user'@'%';

-- 읽기 전용 사용자 생성 (보고용)
CREATE USER IF NOT EXISTS 'report_user'@'%' IDENTIFIED WITH caching_sha2_password BY 'report_password';
GRANT SELECT ON `app_db`.* TO 'report_user'@'%';

-- 권한 적용
FLUSH PRIVILEGES;