from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
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
            
            # 타임아웃 설정을 늘립니다
            capabilities = DesiredCapabilities.CHROME.copy()
            capabilities['pageLoadStrategy'] = 'normal'  # 페이지가 완전히 로드될 때까지 기다림
            
            logging.info(f"Selenium 연결 시도 {retry_count + 1}/{max_retries}: {remote_url}")
            
            driver = webdriver.Remote(
                command_executor=remote_url,
                options=chrome_opts,
                desired_capabilities=capabilities,
            )
            
            # 타임아웃 시간을 30초로 늘림
            wait = WebDriverWait(driver, 30)
            
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