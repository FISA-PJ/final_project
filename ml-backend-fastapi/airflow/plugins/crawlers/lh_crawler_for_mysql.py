# crawlers/lh_crawler.py - LH 크롤러 모듈
import time
import logging
from datetime import datetime
import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException

import os
import csv
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional

from plugins.utils.web_helpers import init_driver

# 로거 설정
logger = logging.getLogger(__name__)

# 설정 클래스 추가
@dataclass
class CrawlerConfig:
    """크롤러 설정 클래스"""
    def __init__(self):
        self.navigation_wait_time = 2
        self.search_wait_time = 3
        self.detail_page_wait_time = 2

        # 셀렉터 정의
        self.search_button_id = "btnSah"
        self.notice_links_selector = "a.wrtancInfoBtn"

def collect_lh_notices(list_url, headers, target_date=None) -> List[Dict]:
    """
    LH 공고문 크롤링 (순수 크롤링만, DB 연결 없음)
    
    Args:
        list_url: 공고 목록 URL
        headers: HTTP 헤더
        target_date: 조회 대상 날짜 (기본값: 오늘)
    
    Returns:
        List[Dict]: 크롤링된 공고 데이터 리스트
    """
    logger.info("LH 공고문 크롤링 시작")
    
    config = CrawlerConfig()
    
    notices_data = []
    
    driver = None
    session = None
    
    try:
        # 세션 초기화
        session = requests.Session()
        session.headers.update(headers)
        logger.info("🌐 HTTP 세션 초기화 완료")
        
        # 웹 드라이버 초기화
        logger.info("🚀 웹 드라이버 초기화 중...")
        driver, wait = init_driver(headers=headers)
        logger.info("✅ 웹 드라이버 초기화 완료")

        # 공고 목록 페이지 접속
        logger.info(f"📄 공고 목록 페이지 접속: {list_url}")
        start_time = time.time()
        driver.get(list_url)
        load_time = time.time() - start_time
        time.sleep(config.navigation_wait_time)
        logger.info(f"✅ 페이지 로드 완료 ({load_time:.2f}초)")

        # 날짜 설정
        if target_date is None:
            target_date = datetime.today().date()
        logger.info(f"📅 조회 날짜 설정: {target_date}")
        
        # 검색 조건 설정
        logger.info("🔧 검색 조건 설정 중...")
        select_elements = {
            "유형": (By.ID, "srchTypeAisTpCd", "05"),
            "고시 유형": (By.ID, "aisTpCdData05", ""),
            "공급주체": (By.ID, "cnpCd", ""),
            "공급상태": (By.ID, "panSs", "")
        }
        
        for name, (by, selector, value) in select_elements.items():
            try:
                select_element = wait.until(EC.presence_of_element_located((by, selector)))
                Select(select_element).select_by_value(value)
                logger.info(f"✓ {name} 설정 완료: {value}")
            except Exception as e:
                logger.warning(f"⚠️ {name} 설정 실패: {e}")
        
        # 날짜 필터링
        date_str = target_date.strftime("%Y-%m-%d")
        for date_field in ["startDt", "endDt"]:
            try:
                date_input = driver.find_element(By.ID, date_field)
                driver.execute_script(
                    "arguments[0].removeAttribute('readonly'); arguments[0].value = arguments[1];",
                    date_input, date_str
                )
                logger.info(f"공고 검색 날짜 설정 완료: {date_field} = {date_str}")
            except Exception as e:
                logger.warning(f"공고 검색 날짜 설정 실패: {date_field}, 오류: {e}")
        
        # 검색 실행
        logger.info("🔍 검색 실행 중...")
        try:
            # 검색 버튼 찾기 - 여러 방법 시도
            search_button = None
            search_selectors = [
                (By.ID, config.search_button_id),
                (By.CSS_SELECTOR, f"#{config.search_button_id}"),
                (By.XPATH, f"//button[@id='{config.search_button_id}']"),
                (By.XPATH, "//button[contains(@class, 'search') or contains(text(), '검색')]")
            ]
            for selector_type, selector in search_selectors:
                try:
                    search_button = wait.until(EC.element_to_be_clickable((selector_type, selector)))
                    logger.info(f"✓ 검색 버튼 발견: {selector_type} - {selector}")
                    break
                except:
                    continue
            if not search_button:
                raise Exception("검색 버튼을 찾을 수 없습니다")

            # 검색 버튼 클릭 전 스크롤
            try:
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", search_button)
                time.sleep(0.5)
            except:
                pass

            # 검색 실행
            logger.info("🔄 검색 버튼 클릭 중...")
            search_button.click()

            # 검색 결과 로딩 대기
            time.sleep(config.search_wait_time)
            
            # 검색 결과 확인
            try:
                # 결과 테이블이나 오류 메시지 확인
                wait.until(lambda driver: (
                    driver.find_elements(By.CSS_SELECTOR, config.notice_links_selector) or
                    driver.find_elements(By.CSS_SELECTOR, ".noData") or
                    "검색된 내용이 없습니다" in driver.page_source
                ))
                logger.info("✅ 검색 실행 완료")    
            except:
                logger.warning("⚠️ 검색 결과 확인 실패 - 계속 진행")
            
        except Exception as e:
            logger.error(f"❌ 검색 실행 실패: {e}")
            
            # 디버깅 정보 수집
            try:
                logger.info("🔍 디버깅 정보 수집 중...")
                logger.info(f"현재 URL: {driver.current_url}")
                logger.info(f"페이지 제목: {driver.title}")
                
                # 페이지 소스에서 검색 버튼 찾기
                if config.search_button_id in driver.page_source:
                    logger.info(f"✓ 페이지 소스에 '{config.search_button_id}' 존재")
                else:
                    logger.info(f"❌ 페이지 소스에 '{config.search_button_id}' 없음")
                
                # 모든 버튼 찾기
                buttons = driver.find_elements(By.TAG_NAME, "button")
                logger.info(f"페이지 내 버튼 수: {len(buttons)}개")
                for i, button in enumerate(buttons[:5]):  # 처음 5개만
                    try:
                        button_id = button.get_attribute('id')
                        button_text = button.text.strip()
                        button_class = button.get_attribute('class')
                        logger.info(f"버튼[{i}]: id='{button_id}', text='{button_text}', class='{button_class}'")
                    except:
                        pass
                
            except Exception as debug_error:
                logger.info(f"디버깅 정보 수집 실패: {debug_error}")
            
            raise
        
        # === 공고 목록 추출 ===
        logger.info("📋 공고 목록 추출 중...")
        row_links = driver.find_elements(By.CSS_SELECTOR, config.notice_links_selector)
        if not row_links:
            logger.info("📭 오늘의 공고가 없습니다.")
            return notices_data
        
        logger.info(f"📊 발견된 공고 수: {len(row_links)}개")
        logger.info("-" * 50)
        
        # 각 공고 처리
        for idx in range(len(row_links)):
            try:
                # 매번 새로 요소 가져오기
                row_links = driver.find_elements(By.CSS_SELECTOR, config.notice_links_selector)
                if idx >= len(row_links):
                    logger.warning(f"⚠️인덱스 초과: {idx} >= {len(row_links)}")
                    break
                
                link = row_links[idx]
                # 공고 코드 추출
                wrtan_no = link.get_attribute("data-id1")
                logger.info(f"📄 [{idx+1}/{len(row_links)}] 공고 처리 시작: {wrtan_no}")
                
                # 상세 페이지로 이동
                logger.info(f" → 상세 페이지 접속 중...")
                logger.info(f"URL: {link.get_attribute('href')}")
                link.click()
                time.sleep(config.detail_page_wait_time)
                
                # 공고 정보 추출
                logger.info(f" → 공고 정보 추출 중...")
                notice_data = extract_notice_data(driver, wrtan_no, target_date)
                if notice_data:
                    notices_data.append(notice_data)
                    logger.info(f"✅ 공고 처리 완료: {notice_data.get('notice_number')}, {notice_data.get('notice_title')}")
                else:
                    logger.warning(f"⚠️ 공고 정보 추출 실패")
                
                # 목록으로 돌아가기
                try:
                    logger.info(f"← 목록으로 돌아가는 중...")
                    
                    # 뒤로가기
                    driver.back()
                    wait.until(EC.element_to_be_clickable((By.ID, config.search_button_id)))
                    time.sleep(config.navigation_wait_time)
                    logger.info("✅ 브라우저 뒤로가기 성공")

                except Exception as e:
                    logger.warning(f"⚠️ 목록 복귀 실패: {e}")
                    
            except Exception as e:
                logger.error(f"❌ 공고 처리 중 오류 (인덱스 {idx}): {e}")
                try:
                    driver.back()
                    time.sleep(config.navigation_wait_time)
                except:
                    pass
        
        logger.info("-" * 50)
        logger.info(f"🎉 크롤링 완료: {len(notices_data)}개 공고 수집")
        logger.info("=" * 60)
        return notices_data
    
    except Exception as e:
        logger.error(f"❌ 크롤링 프로세스 오류: {str(e)}", exc_info=True)
        raise

    finally:
        # 리소스 정리
        logger.info("🧹 리소스 정리 중...")
        
        if session:
            session.close()
            logger.info("✓ HTTP 세션 종료")
        
        if driver:
            try:
                driver.quit()
                logger.info("✓ 웹드라이버 종료 완료")
            except Exception as e:
                logger.error(f"❌ 웹드라이버 종료 중 오류: {e}")
        
        logger.info("🏁 크롤링 세션 완전 종료")

def extract_notice_data(driver, wrtan_no: str, target_date: datetime.date) -> Optional[Dict]:
    """개별 공고에서 데이터 추출"""
    # 공고 상세 페이지에서 데이터 추출
    logger.debug(f"📄 공고 데이터 추출 시작: {wrtan_no}")

    try:
        # === 기본 페이지 상태 확인 ===
        try:
            current_url = driver.current_url
            logger.debug(f"현재 페이지 URL: {current_url}")
            
            # 페이지 로드 확인
            if "error" in current_url.lower() or "404" in current_url:
                logger.warning(f"⚠️ 잘못된 페이지 접근: {current_url}")
                return None
                
        except Exception as e:
            logger.debug(f"페이지 상태 확인 오류: {e}")

        # === 공고일 추출 ===
        logger.debug(f"📅 공고일 추출 중...")
        try:
            pub_date_text = driver.find_element(By.XPATH, "//li[strong[text()='공고일']]").text
            pub_date = pub_date_text.replace("공고일", "").strip().replace(".", "")
            if len(pub_date) == 8:
                formatted_pub_date = f"{pub_date[:4]}-{pub_date[4:6]}-{pub_date[6:8]}"
            else:
                formatted_pub_date = target_date.strftime("%Y-%m-%d")
        except Exception as e:
            pub_date = target_date.strftime("%Y%m%d")
            formatted_pub_date = target_date.strftime("%Y-%m-%d")
            logger.warning(f"공고일 추출 실패 ({wrtan_no}): {e}")
        
        # === 공고명 추출 ===
        try:
             # 가능한 셀렉터들
            title_selectors = [
                "h1", "h2", "h3",                          # 헤딩 태그
                ".bbsV_subject", ".bbs_subject",           # 제목 클래스
                ".title", ".subject",                      # 일반적인 제목 클래스
                "#title", "#subject"                       # ID 기반
            ]

            title = None
            for selector in title_selectors:
                title_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for title_element in title_elements:
                    text = title_element.text.strip()
                    if text and len(text) > 10:
                        title = text
                        logger.info(f"✓ 공고명: {title}")
                        break
                if title:
                    break
        except Exception as e:
            title = f"공고 {wrtan_no}"
            logger.warning(f"❌ 제목 추출 실패 ({wrtan_no}): {e}")
        
        # 주소 정보 추출
        address = extract_address_from_content(driver)
        
        # 공급일정 추출 (접수기간/당첨자발표일/계약기간)
        supply_schedule = extract_supply_schedule(driver)
        
        # 공고 데이터 구성
        notice_data = {
            'notice_number': wrtan_no,                                      # 공고코드
            'notice_title': title,                                          # 공고명
            'post_date': formatted_pub_date,                                # 공고일자
            'application_start_date': supply_schedule.get('start_date'),    # 접수기간(시작일)
            'application_end_date': supply_schedule.get('end_date'),        # 접수기간(종료일)
            'location': address,                                            # 소재지
            'is_correction': detect_correction_notice(title)                # 정정공고 감지 추가
        }
        
        logger.info(f"✅ 공고 세부 정보 추출 완료: {wrtan_no}")
        return notice_data
        
    except Exception as e:
        logger.error(f"❌ 공고 세부 정보 추출 실패 ({wrtan_no}): {e}")
        return None

def extract_address_from_content(driver) -> Optional[str]:
    """웹페이지에서 공고 소재지 추출"""
    try:
        content_items = driver.find_elements(By.CSS_SELECTOR, "li.w100")
        address_keywords = ["동", "구", "로", "시", "군", "읍", "면"]
        
        for item in content_items:
            try:
                item_text = item.text.strip()
                if any(keyword in item_text for keyword in address_keywords):
                    try:
                        strong_text = item.find_element(By.TAG_NAME, "strong").text.strip()
                        address = item_text.replace(strong_text, "").strip()
                        if address.startswith('"') and address.endswith('"'):
                            address = address[1:-1].strip()
                    except:
                        address = None
                    
                    if address and len(address) > 2:
                        logger.info(f"✓ 소재지 발견: {address}")
                        return address
            except Exception as e:
                logger.info(f"❌ 개별 소재지 추출 오류: {e}")
                continue
    except Exception as e:
        logger.info(f"❌ 소재지 추출 전체 오류: {e}")
        logger.info(f"⚠️ 소재지 추출 실패로 소재지는 '없음'으로 설정")
        address = '없음'

    return address

def extract_supply_schedule(driver) -> Optional[str]:
    """공급일정 정보 추출"""
    logger.info(f"🔍 공급일정 검색 중...")
    
    try:
        schedule_data = {}
        
        # === ID로 직접 접근 (가장 효율적) ===
        logger.info(f"ID로 직접 접근 시도...")
        
        id_mappings = {
            '접수기간 ': 'sta_acpDt',
            '당첨자발표일 ': 'sta_pzwrDt',
            '계약기간 ': 'sta_ctrtDt'  
        }
        
        for key, element_id in id_mappings.items():
            try:
                element = driver.find_element(By.ID, element_id)
                text = element.text.strip()
                if text:
                    # 접수기간은 시작일과 종료일로 분리
                    if key == '접수기간 ':
                        contract_dates = parse_contract_period(text)
                        if contract_dates:
                            schedule_data['start_date'] = contract_dates['start_date']
                            schedule_data['end_date'] = contract_dates['end_date']
                            logger.info(f"✓ ID '{element_id}' 에서 접수기간 분리: {contract_dates}")
                        else:
                            schedule_data[key] = text
                            logger.info(f"✓ ID '{element_id}' 에서 {key}: {text}")
                    else:
                        schedule_data[key] = text
                        logger.info(f"✓ ID '{element_id}' 에서 {key}: {text}")
                # text 값이 없으면 '없음'을 반환
                else:
                    schedule_data[key] = "없음"
                    logger.info(f"⚠️ ID '{element_id}' 에서 {key}는 '없음'으로 저장함.")
            except NoSuchElementException:
                logger.info(f"❌ ID '{element_id}' 요소 없음")
                # elememt_id가 없으면 '없음'으로 저장
                schedule_data[key] = "없음"
                logger.info(f"⚠️ ID '{element_id}' 에서 {key}는 '없음'으로 저장함.")
            except Exception as e:
                logger.info(f"⚠️ ID '{element_id}' 접근 오류: {e}")
        
        if schedule_data:
            logger.info(f"✅ ID 접근으로 공급일정 추출 성공 ({len(schedule_data)}개 항목)")
            return schedule_data
        
    except Exception as e:
        logger.info(f"❌ 공급일정 추출 전체 오류: {e}")
        return None

def classify_notices_by_location(notices_data: List[Dict], csv_file_path: str) -> Tuple[List[Dict], List[Dict]]:
    """
    소재지 없는 공고는 CSV에 저장
    DB용과 CSV용 데이터를 분리하여 반환
    
    Args:
        notices_data: 크롤링된 공고 데이터
        csv_file_path: CSV 파일 저장 경로
    
    Returns:
        Tuple[List[Dict], List[Dict]]: (DB용 공고, CSV용 공고)
    """
    db_notices = []
    csv_notices = []
    
    # 주소 유무에 따라 데이터 분리
    for notice in notices_data:
        if notice.get('location') != '없음' and notice.get('location'):  # 주소가 있으면 DB용
            db_notices.append(notice)
        else:                                                           # 주소가 없으면 CSV에 저장
            csv_notices.append(notice)
    
    # CSV 저장
    if csv_notices:
        # 디렉토리 생성
        os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)
        
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as file:
            fieldnames = ['notice_number', 'notice_title', 'post_date', 
                         'application_start_date', 'application_end_date', 'location', 'is_correction']
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            
            for notice in csv_notices:
                writer.writerow({
                    'notice_number': notice['notice_number'],
                    'notice_title': notice['notice_title'],
                    'post_date': notice['post_date'],
                    'application_start_date': notice.get('application_start_date', ''),
                    'application_end_date': notice.get('application_end_date', ''),
                    'location': notice.get('location', ''),
                    'is_correction': notice.get('is_correction', False)
                })
        
        logger.info(f"CSV 저장 완료: {len(csv_notices)}개 공고 -> {csv_file_path}")
    
    return db_notices, csv_notices

def detect_correction_notice(title: str) -> bool:
    """제목에서 정정공고 여부 판별"""
    if not title:
        logger.info("❓ 공고명 없음")
        return False
    
    # 정정공고 키워드 목록
    correction_keywords = ['정정', '변경', '수정', '재공고', '추가', '취소', '연기']
    
    logger.info(f"🔍 공고 제목 분석: \"{title}\"")
    
    # 키워드 검색 (각 키워드마다 개별 로깅)
    detected_keywords = []
    for keyword in correction_keywords:
        if keyword in title:
            detected_keywords.append(keyword)
            logger.info(f"- '{keyword}' 키워드 발견")
    
    # 결과 로깅
    if detected_keywords:
        keywords_str = "', '".join(detected_keywords)
        logger.info(f"🔄 정정공고 감지됨 - 키워드: '{keywords_str}'")
        return True
    else:
        logger.info(f"✅ 일반 공고 (정정공고 아님)")
        return False

def parse_contract_period(text: str) -> Optional[dict]:
    """계약기간 텍스트를 시작일과 종료일로 분리"""
    import re
    from datetime import datetime
    
    if not text:
        logger.warning("❓ 계약기간 텍스트가 비어있음")
        return None
    
    try:
        logger.info(f"📅 계약기간 분석: \"{text}\"")
        
        # 패턴 정의 및 설명
        patterns = [
            (r'(\d{4}\.\d{2}\.\d{2})\s*~\s*(\d{4}\.\d{2}\.\d{2})', "연.월.일 형태"),
            (r'(\d{4}-\d{2}-\d{2})\s*~\s*(\d{4}-\d{2}-\d{2})', "연-월-일 형태"),
            (r'(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)\s*~\s*(\d{4}년\s*\d{1,2}월\s*\d{1,2}일)', "한글 형태"),
            (r'(\d{1,2}\.\d{1,2})\s*~\s*(\d{1,2}\.\d{1,2})', "월.일 형태")
        ]
        
        for i, (pattern, desc) in enumerate(patterns):
            match = re.search(pattern, text)
            if match:
                start_str = match.group(1).strip()
                end_str = match.group(2).strip()
                
                logger.debug(f"🔍 패턴 #{i+1} ({desc}) 매칭: '{start_str}' ~ '{end_str}'")
                
                # 날짜 표준화 (YYYY-MM-DD 형태로)
                start_date = normalize_date(start_str)
                end_date = normalize_date(end_str)
                
                if start_date and end_date:
                    result = {
                        'start_date': start_date,
                        'end_date': end_date,
                        'original_text': text,
                        'pattern_matched': desc
                    }
                    logger.info(f"✅ 계약기간 추출 성공: {start_date} ~ {end_date}")
                    return result
                else:
                    logger.warning(f"⚠️ 날짜 변환 실패: '{start_str}' ~ '{end_str}'")
        
        # 텍스트가 숫자나 날짜 형식을 포함하는지 확인 (디버깅용)
        has_numbers = bool(re.search(r'\d', text))
        has_tilde = '~' in text
        
        if has_numbers and has_tilde:
            logger.warning(f"❗ 날짜 형식 오류: \"{text}\" - 숫자와 물결표는 있으나 패턴 매칭 실패")
        elif has_numbers:
            logger.warning(f"❗ 날짜 형식 오류: \"{text}\" - 숫자는 있으나 물결표(~) 없음")
        else:
            logger.warning(f"❌ 계약기간 형식 아님: \"{text}\"")
        
        return None
        
    except Exception as e:
        logger.error(f"💥 계약기간 파싱 오류: {str(e)}, 입력: \"{text}\"")
        import traceback
        logger.debug(f"🔬 상세 오류: {traceback.format_exc()}")
        return None
        
    except Exception as e:
        logger.info(f"계약기간 파싱 오류: {e}, 텍스트: {text}")
        return None

def normalize_date(date_str: str) -> Optional[str]:
    """다양한 날짜 형식을 YYYY-MM-DD로 표준화"""
    from datetime import datetime, date
    
    if not date_str or not isinstance(date_str, str):
        logger.warning(f"⚠️ 유효하지 않은 날짜 입력: {date_str}")
        return None
    
    try:
        logger.debug(f"🗓️ 날짜 표준화: \"{date_str}\"")
        
        # 2025.12.31 형태
        if '.' in date_str and len(date_str.split('.')) == 3:
            parts = date_str.split('.')
            if len(parts[0]) == 4:  # 년도 포함
                result = f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
                if validate_date(result):
                    logger.debug(f"✅ 연.월.일 형식 변환 성공: \"{result}\"")
                    return result
                else:
                    logger.warning(f"⚠️ 유효하지 않은 날짜: \"{result}\"")
            else:  # 년도 없음 (현재 년도 사용)
                current_year = datetime.now().year
                result = f"{current_year}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
                if validate_date(result):
                    logger.debug(f"✅ 현재 연도 추가: \"{result}\"")
                    return result
                else:
                    logger.warning(f"⚠️ 유효하지 않은 날짜: \"{result}\"")
        
        # 2025-12-31 형태 (이미 표준화됨)
        elif '-' in date_str and len(date_str.split('-')) == 3:
            parts = date_str.split('-')
            result = f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
            if validate_date(result):
                logger.debug(f"✅ 표준 형식 확인: \"{result}\"")
                return result
            else:
                logger.warning(f"⚠️ 유효하지 않은 날짜: \"{result}\"")
        
        # 2025년 12월 31일 형태
        elif '년' in date_str and '월' in date_str and '일' in date_str:
            import re
            match = re.search(r'(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일', date_str)
            if match:
                year, month, day = match.groups()
                result = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                if validate_date(result):
                    logger.debug(f"✅ 한글 날짜 변환 성공: \"{result}\"")
                    return result
                else:
                    logger.warning(f"⚠️ 유효하지 않은 날짜: \"{result}\"")
            else:
                logger.warning(f"❌ 한글 날짜 패턴 불일치: \"{date_str}\"")
        
        # 12.31 형태 (년도 없음)
        elif '.' in date_str and len(date_str.split('.')) == 2:
            parts = date_str.split('.')
            current_year = datetime.now().year
            result = f"{current_year}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"
            if validate_date(result):
                logger.debug(f"✅ 월.일에 현재 연도 추가: \"{result}\"")
                return result
            else:
                logger.warning(f"⚠️ 유효하지 않은 날짜: \"{result}\"")
        
        logger.warning(f"❓ 인식할 수 없는 날짜 형식: \"{date_str}\"")
        return None
        
    except Exception as e:
        logger.error(f"💥 날짜 표준화 오류: {e}, 입력: \"{date_str}\"")
        return None

def validate_date(date_str: str) -> bool:
    """날짜 문자열의 유효성 검사 (YYYY-MM-DD 형식)"""
    from datetime import datetime
    try:
        if not date_str:
            return False
        
        # YYYY-MM-DD 형식인지 확인
        if not isinstance(date_str, str) or len(date_str.split('-')) != 3:
            return False
        
        # 실제 날짜로 변환 가능한지 확인
        year, month, day = map(int, date_str.split('-'))
        datetime(year, month, day)
        
        return True
    except ValueError:
        return False
    except Exception:
        return False