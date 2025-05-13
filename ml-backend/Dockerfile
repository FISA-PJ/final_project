# 1단계: Gradle 빌드
FROM gradle:8.4-jdk21 AS build

# Gradle 캐시 사용을 위한 설정
ENV GRADLE_USER_HOME=/home/gradle/.gradle

# 작업 디렉토리 설정
WORKDIR /app

# 필요한 파일 복사
COPY build.gradle settings.gradle ./
COPY src ./src

# 의존성 미리 다운 (캐싱 최적화)
RUN gradle build -x test --no-daemon

# 2단계: 실행 전용 JDK 환경
FROM openjdk:21-jdk-slim

WORKDIR /app

# build/libs 아래 생성된 jar 복사 (이름은 실제 jar에 맞게 수정)
COPY --from=build /app/build/libs/*.jar app.jar

# 실행
ENTRYPOINT ["java", "-jar", "app.jar"]