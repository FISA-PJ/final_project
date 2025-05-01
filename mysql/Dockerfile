# MySQL 공식 이미지를 기반으로 사용
FROM mysql:8.0.36

# MySQL 실행 시 사용할 명령어 설정
CMD ["--character-set-server=utf8mb4", \
    "--collation-server=utf8mb4_unicode_ci", \
    "--skip-character-set-client-handshake", \
    "--default-authentication-plugin=caching_sha2_password"]