from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
import time
import logging

def init_driver(headers, remote_url="http://airflow-selenium:4444/wd/hub", max_retries=3):
    """Selenium 드라이버 초기화 (원격 연결) - 향상된 안정성과 재시도 로직"""
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            chrome_opts = Options()
            chrome_opts.add_argument("--headless")
            chrome_opts.add_argument("--disable-gpu")
            chrome_opts.add_argument("--no-sandbox")  # 컨테이너 환경에서 중요
            chrome_opts.add_argument("--disable-dev-shm-usage")  # 메모리 문제 방지
            chrome_opts.add_argument(f"user-agent={headers['User-Agent']}")

            # 기본 Capabilities 설정
            capabilities = webdriver.DesiredCapabilities.CHROME.copy()
            capabilities['browserName'] = 'chrome'
            
            # 원격 드라이버 생성
            driver = webdriver.Remote(
                command_executor=remote_url,
                options=chrome_opts,
                desired_capabilities=capabilities
            )

            # 타임아웃 설정 (WebDriver 객체에 대해 설정)
            driver.set_page_load_timeout(30)  # 페이지 로드 타임아웃
            driver.set_script_timeout(30)     # 스크립트 실행 타임아웃
            driver.implicitly_wait(10)        # 암시적 대기 시간

            # WebDriverWait을 사용하여 명시적 대기 시간 설정
            wait = WebDriverWait(driver, 30)  # 30초 대기
            logging.info("Selenium 드라이버 초기화 성공")
            
            return driver, wait
            
        except Exception as e:
            retry_count += 1
            logging.warning(f"Selenium 연결 실패 ({retry_count}/{max_retries}): {e}")
            
            if retry_count >= max_retries:
                logging.error(f"최대 재시도 횟수 도달. 마지막 오류: {e}")
                raise
            
            # 재시도 전 대기
            time.sleep(5)