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
            "공급상태": (By.ID, "panSs", ""),
            "공고명 검색어" : (By.ID, "searchValue", "입주자")
        }
        
        for name, (by, selector, value) in select_elements.items():
            try:
                element = wait.until(EC.presence_of_element_located((by, selector)))
                tag_name = element.tag_name.lower()

                if tag_name == "select":
                    Select(element).select_by_value(value)
                elif tag_name == "input":
                    element.clear()
                    element.send_keys(value)
                else:
                    logger.warning(f"⚠️ {name}: 알 수 없는 요소 타입: {tag_name}")

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
                logger.info(f"📅 공고 검색 날짜 설정 완료: {date_field} = {date_str}")
            except Exception as e:
                logger.warning(f"📅 공고 검색 날짜 설정 실패: {date_field}, 오류: {e}")
        
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
                raise Exception("⚠️ 검색 버튼을 찾을 수 없습니다")

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
        
        # 각 공고 상세 내용 긁어오기
        for idx in range(len(row_links)):
            try:
                # 매번 새로 요소 가져오기
                row_links = driver.find_elements(By.CSS_SELECTOR, config.notice_links_selector)
                if idx >= len(row_links):
                    logger.warning(f"⚠️인덱스 초과: {idx} >= {len(row_links)}")
                    break
                
                link = row_links[idx]

                # 공고코드 추출
                wrtan_no = link.get_attribute("data-id1")
                logger.info(f"📄 [{idx+1}/{len(row_links)}] 공고 처리 시작: 공고번호 {wrtan_no}")
                
                # 상세 페이지로 이동
                logger.info(f"→ 상세 페이지 접속 중...")
                logger.info(f"URL: {link.get_attribute('href')}")
                link.click()
                time.sleep(config.detail_page_wait_time)
                
                # 공고 정보 추출 시작
                logger.info(f"→ 공고 세부 정보 추출 중...")
                notice_data = extract_notice_data(driver, wrtan_no, target_date)
                if notice_data:
                    notices_data.append(notice_data)
                    logger.info(f"✅ 공고 처리 완료: {notice_data.get('notice_number')}, {notice_data.get('notice_title')}")
                else:
                    logger.warning(f"⚠️ 공고 세부 정보 추출 실패")
                
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
    logger.debug(f"📄 공고번호 {wrtan_no} 상세내용 추출 시작:")

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

        # 공고 마감일 추출
        application_end_date = extract_application_end_date(driver)
        
        # 소재지 추출
        address = extract_address_from_content(driver)

        # 공급유형 구분 추출
        supply_type = extract_supply_type(driver)
        
        # 공급일정 추출 (당첨자발표일자/당첨자서류제출기간/계약체결기간)
        supply_schedule = extract_supply_schedule(driver)
        
        # 입주예정월 추출
        move_in_schedule = extract_move_in_schedule(driver)

        # 주택형 정보 추출
        house_types = extract_house_types(driver)
        
        # 공고 데이터 구성
        notice_data = {
            'notice_number': wrtan_no,                                                      # 공고코드
            'notice_title': title,                                                          # 공고명
            'post_date': formatted_pub_date,                                                # 공고일자
            'application_end_date': application_end_date,                                   # 공고 마감일
            'document_start_date': supply_schedule.get('document_start_date'),              # 당첨자 서류제출(시작일)
            'document_end_date': supply_schedule.get('document_end_date'),                  # 당첨자 서류제출(종료일)
            'contract_start_date' : supply_schedule.get('contract_start_date'),             # 계약체결기간(시작일)
            'contract_end_date' : supply_schedule.get('contract_end_date'),                 # 계약체결기간(종료일)
            'winning_date' : supply_schedule.get('당첨자 발표일자'),                          # 당첨자 발표일자
            'move_in_date' : move_in_schedule,                                              # 입주예정월
            'location': address,                                                            # 소재지
            'is_correction': detect_correction_notice(title),                               # 공고명에서 정정공고 키워드 여부
            'supply_type': supply_type,                                                     # 공급유형
            'house_types': house_types                                                      # 주택형 정보
        }
        
        logger.info(f"✅ 공고번호 {wrtan_no} 세부 정보 추출 완료")
        return notice_data
        
    except Exception as e:
        error_msg = f"❌ 공고번호 {wrtan_no} 공고 세부 정보 추출 실패: {str(e)}"
        logger.error(error_msg)
        
        # 실패 정보를 CSV 파일에 저장
        try:
            current_url = driver.current_url
        except:
            current_url = "URL 추출 실패"
            
        save_failed_notice_to_csv(
            wrtan_no, 
            target_date, 
            error_msg, 
            url=current_url
        )
        
        return None

def extract_application_end_date(driver) -> Optional[str]:
    """
    공고 마감일 정보 추출 (bbsV_data 클래스에서)
    
    Args:
        driver: Selenium WebDriver 인스턴스
        
    Returns:
        Optional[str]: 공고 마감일 정보 (예: "2024-03-15"). 실패 시 None 반환
    """
    logger.info("🔍 공고 마감일 검색 중...")
    
    try:
        # bbsV_data 클래스 내에서 마감일 정보 찾기
        bbs_data = driver.find_elements(By.CSS_SELECTOR, ".bbsV_data li")
        
        for item in bbs_data:
            try:
                text = item.text.strip()
                # "마감일2025.06.05" 또는 "마감일 : 2025.06.04" 형식 모두 처리
                if '마감일' in text:
                    import re
                    # 날짜 추출을 위한 정규식 패턴
                    date_match = re.search(r'(\d{4})[.\-](\d{1,2})[.\-](\d{1,2})', text)
                    if date_match:
                        year, month, day = date_match.groups()
                        formatted_date = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                        if validate_date(formatted_date):
                            logger.info(f"✓ 마감일 추출 성공: {formatted_date}")
                            return formatted_date
                        else:
                            logger.warning(f"⚠️ 유효하지 않은 날짜: {formatted_date}")
                    else:
                        logger.warning(f"⚠️ 날짜 형식 매칭 실패: {text}")
            except Exception as e:
                logger.debug(f"항목 처리 중 오류: {e}")
                continue

        logger.warning("⚠️ 공고 마감일을 찾을 수 없습니다")
        return None

    except Exception as e:
        logger.error(f"❌ 마감일 추출 중 오류 발생: {str(e)}")
        return None

def extract_move_in_schedule(driver) -> Optional[str]:
    """
    입주예정월 정보 추출
    
    Args:
        driver: Selenium WebDriver 인스턴스
        
    Returns:
        Optional[str]: 입주예정월 정보 (예: "2028년 01월"). 실패 시 None 반환
    """
    logger.info("🔍 입주예정월 검색 중...")
    
    try:
        # 방법 1: 리스트 항목에서 검색
        elements = driver.find_elements(By.CSS_SELECTOR, "ul.list_st1 li")
        for element in elements:
            text = element.text.strip()
            if '입주예정' in text:
                # 날짜 부분만 추출
                date_part = text.split(':')[-1].strip() if ':' in text else text
                logger.info(f"✅ 리스트에서 입주예정월 추출 성공: {date_part}")
                return date_part

        # 방법 2: 직접적인 텍스트 검색
        elements = driver.find_elements(By.XPATH, "//*[contains(text(), '입주예정')]")
        for element in elements:
            text = element.text.strip()
            if '입주예정' in text:
                # 부모 요소에서 전체 텍스트 가져오기
                try:
                    full_text = element.find_element(By.XPATH, "..").text.strip()
                except:
                    full_text = text
                
                # "입주예정월 : 2028년 01월" 형식에서 날짜 부분만 추출
                if ':' in full_text:
                    date_part = full_text.split(':')[-1].strip()
                    logger.info(f"✅ 텍스트 검색으로 입주예정월 추출 성공: {date_part}")
                    return date_part
                elif '입주예정' in full_text:
                    # "입주예정 2028년 01월" 형식 처리
                    import re
                    date_match = re.search(r'(\d{4}년\s*\d{1,2}월)', full_text)
                    if date_match:
                        date_part = date_match.group(1)
                        logger.info(f"✅ 정규식으로 입주예정월 추출 성공: {date_part}")
                        return date_part

        # 방법 3: class가 w100인 요소들 검색
        elements = driver.find_elements(By.CSS_SELECTOR, ".w100")
        for element in elements:
            text = element.text.strip()
            if '입주예정' in text:
                # 날짜 부분만 추출
                if ':' in text:
                    date_part = text.split(':')[-1].strip()
                    logger.info(f"✅ w100 클래스에서 입주예정월 추출 성공: {date_part}")
                    return date_part

        logger.warning("⚠️ 입주예정월 정보를 찾을 수 없습니다")
        return None

    except Exception as e:
        logger.error(f"❌ 입주예정월 추출 중 오류 발생: {str(e)}")
        return None

def extract_address_from_content(driver) -> Optional[str]:
    """
    공고 내용에서 소재지 정보 추출
    """
    try:
        logger.info("🔍 소재지 정보 추출 시작...")

        # .list_st1.li_w25 > li (li100 클래스 포함 여부와 무관하게 li 전체 탐색)
        location_elements = driver.find_elements(By.CSS_SELECTOR, ".list_st1.li_w25 > li")
        logger.info(f"✓ li 개수: {len(location_elements)}")

        for idx, element in enumerate(location_elements):
            text = element.text.strip()
            logger.info(f"✓ [{idx}] li 텍스트: {text}")

            # "소재지"가 포함된 요소 탐색
            if "소재지" in text:
                logger.info(f"✓ '소재지' 포함 li 발견: {text}")

                # 콜론(:)이 있으면 그 뒤, 없으면 '소재지' 뒤 텍스트 추출
                if ":" in text:
                    location = text.split(":", 1)[1].strip()
                else:
                    location = text.replace("소재지", "").strip()

                logger.info(f"✓ 소재지 정보 추출 성공: {location}")
                return location

        logger.warning("⚠️ 소재지 정보를 찾을 수 없습니다")
        return None

    except Exception as e:
        logger.error(f"❌ 소재지 정보 추출 중 오류 발생: {str(e)}")
        return None

def extract_supply_type(driver) -> Optional[str]:
    """공급일정 표에서 공급유형 구분 정보 추출"""
    logger.info("🔍 공급유형 구분 테이블 검색 시작...")
    
    try:
        # 1. 먼저 공급일정 섹션의 테이블 찾기
        tables = driver.find_elements(By.CSS_SELECTOR, "div.tbl_st table")
        if not tables:
            # 2. 실패시 모든 테이블 검색
            tables = driver.find_elements(By.CSS_SELECTOR, "table.tbl_st")
            logger.info("일반 테이블 검색으로 전환")
        
        logger.info(f"✓ 발견된 테이블 수: {len(tables)}개")
        
        for idx, table in enumerate(tables):
            try:
                # 테이블 헤더 확인
                headers = [th.text.strip() for th in table.find_elements(By.TAG_NAME, "th")]
                logger.info(f"테이블 #{idx+1} 헤더: {headers}")
                
                # 공급일정 테이블인지 확인 (구분, 신청일시, 신청방법 컬럼이 있는지)
                if any('구분' in h for h in headers) and any('신청' in h for h in headers):
                    logger.info(f"✓ 공급일정 테이블 발견! (테이블 #{idx+1})")
                    
                    # 첫 번째 열(구분) 데이터 추출
                    rows = table.find_elements(By.TAG_NAME, "tr")[1:]  # 헤더 제외
                    # 각 공급유형별 정보를 딕셔너리로 만들어 리스트에 추가
                    types = []
                    
                    for row in rows:
                        try:
                            cells = row.find_elements(By.TAG_NAME, "td")
                            if cells:
                                type_text = cells[0].text.strip()  # 첫 번째 열
                                if type_text and not any(skip in type_text.lower() for skip in ['합계', '계']):
                                    types.append(type_text)
                                    logger.info(f" - 유형: {type_text}")
                        except Exception as row_error:
                            logger.warning(f"⚠️ 행 처리 중 오류: {row_error}")
                            continue
                    
                    if types:
                        logger.info(f"✅ 공급유형 추출 성공! 총 {len(types)}개 유형")
                        return types
                    
            except Exception as table_error:
                logger.warning(f"⚠️ 테이블 #{idx+1} 처리 중 오류: {table_error}")
                continue
        
        logger.warning("⚠️ 공급유형을 찾을 수 없습니다")
        return None

    except Exception as e:
        logger.error(f"❌ 공급유형 추출 중 오류 발생: {str(e)}")
        return None

def extract_supply_schedule(driver) -> Optional[dict]:
    """공급일정 정보 추출"""
    logger.info(f"🔍 공급일정 검색 중...")
    
    try:
        schedule_data = {}
        
        # === ID로 직접 접근 (1차 시도) ===
        logger.info(f"ID로 직접 접근 시도...")
        
        id_mappings = {
            '당첨자 서류제출기간': 'sta_PzwrPprSbmDt',
            '당첨자 발표일자': 'sta_PzwrAncDt',
            '계약체결기간': 'sta_ctrtStDt'  # 공백 제거
        }
        
        for key, element_id in id_mappings.items():
            try:
                element = driver.find_element(By.ID, element_id)
                logger.info(f"✓ '{key}' - '{element_id}' 요소 찾음")
                text = element.text.strip()
                
                if text and text.lower() != '없음':
                    if key in ['당첨자 서류제출기간', '계약체결기간']:
                        dates = parse_contract_period(text)
                        if dates:
                            if key == '당첨자 서류제출기간':
                                schedule_data['document_start_date'] = dates['start_date']
                                schedule_data['document_end_date'] = dates['end_date']
                                logger.info(f"✓ '{key}' 기간 분리: {dates['start_date']} ~ {dates['end_date']}")
                            else:  # 계약체결기간
                                schedule_data['contract_start_date'] = dates['start_date']
                                schedule_data['contract_end_date'] = dates['end_date']
                                logger.info(f"✓ '{key}' 기간 분리: {dates['start_date']} ~ {dates['end_date']}")
                    else:  # 당첨자 발표일자
                        # 날짜 형식 변환
                        normalized_date = normalize_date(text)
                        if normalized_date:
                            schedule_data['당첨자 발표일자'] = normalized_date
                            logger.info(f"✓ '{key}' 날짜 변환: {normalized_date}")
                        else:
                            schedule_data[key] = text
                            logger.info(f"⚠️ '{key}' 날짜 변환 실패, 원본 저장: {text}")
                else:
                    schedule_data[key] = "없음"
                    logger.info(f"⚠️ '{key}' 값이 비어있거나 '없음'")
                    
            except NoSuchElementException:
                logger.info(f"❌ {key} ID '{element_id}' 요소 없음")
            except Exception as e:
                logger.info(f"⚠️ {key} 처리 중 오류: {e}")
        
        # === 테이블에서 검색 (2차 시도) ===
        if not all(key in schedule_data for key in ['document_start_date', 'contract_start_date', '당첨자 발표일자']):
            logger.info("📋 테이블에서 공급일정 검색 시도...")
            
            tables = driver.find_elements(By.CSS_SELECTOR, "table.tbl_st")
            for table in tables:
                try:
                    headers = [th.text.strip() for th in table.find_elements(By.TAG_NAME, "th")]
                    if any('일정' in h for h in headers):
                        rows = table.find_elements(By.TAG_NAME, "tr")
                        for row in rows:
                            cells = row.find_elements(By.TAG_NAME, "td")
                            if not cells:
                                continue
                                
                            row_text = row.text.strip()
                            
                            # 서류제출기간 확인
                            if '서류' in row_text and '제출' in row_text and 'document_start_date' not in schedule_data:
                                dates = parse_contract_period(cells[-1].text.strip())
                                if dates:
                                    schedule_data['document_start_date'] = dates['start_date']
                                    schedule_data['document_end_date'] = dates['end_date']
                                    logger.info(f"✓ 테이블에서 서류제출기간 발견: {dates['start_date']} ~ {dates['end_date']}")
                            
                            # 계약체결기간 확인
                            if '계약' in row_text and 'contract_start_date' not in schedule_data:
                                dates = parse_contract_period(cells[-1].text.strip())
                                if dates:
                                    schedule_data['contract_start_date'] = dates['start_date']
                                    schedule_data['contract_end_date'] = dates['end_date']
                                    logger.info(f"✓ 테이블에서 계약체결기간 발견: {dates['start_date']} ~ {dates['end_date']}")
                            
                            # 당첨자 발표 확인
                            if '당첨' in row_text and '발표' in row_text and '당첨자 발표일자' not in schedule_data:
                                date_text = cells[-1].text.strip()
                                normalized_date = normalize_date(date_text)
                                if normalized_date:
                                    schedule_data['당첨자 발표일자'] = normalized_date
                                    logger.info(f"✓ 테이블에서 당첨자 발표일 발견: {normalized_date}")
                
                except Exception as table_error:
                    logger.warning(f"⚠️ 테이블 처리 중 오류: {table_error}")
                    continue
        
        # 결과 확인 및 반환
        if schedule_data:
            logger.info(f"✅ 공급일정 추출 완료 ({len(schedule_data)}개 항목)")
            return schedule_data
        else:
            logger.warning("⚠️ 공급일정 정보를 찾을 수 없습니다")
            return None
        
    except Exception as e:
        logger.error(f"❌ 공급일정 추출 중 오류 발생: {str(e)}")
        return None

def extract_house_types(driver) -> List[Dict]:
    """주택형 정보 추출 (주택형, 전용면적, 세대수, 평균분양가)"""
    logger.info("🏠 주택형 정보 추출 시작...")
    
    house_types = []
    
    try:
        # 1. 먼저 주택형 섹션의 테이블 찾기
        tables = driver.find_elements(By.CSS_SELECTOR, "div.tbl_st table")
        if not tables:
            # 2. 실패시 모든 테이블 검색
            tables = driver.find_elements(By.CSS_SELECTOR, "table.tbl_st")
            logger.info("일반 테이블 검색으로 전환")
        
        logger.info(f"✓ 발견된 테이블 수: {len(tables)}개")
        
        for idx, table in enumerate(tables):
            try:
                # 테이블 헤더 확인
                headers = [th.text.strip() for th in table.find_elements(By.TAG_NAME, "th")]
                logger.info(f"테이블 #{idx+1} 헤더: {headers}")
                
                # 주택형 테이블인지 확인 (주택형/타입, 전용면적, 세대수 컬럼이 있는지)
                has_type = any(keyword in h for h in headers for keyword in ['주택형', '타입'])
                has_area = any(keyword in h for h in headers for keyword in ['전용면적', '전용'])
                has_count = any('세대수' in h for h in headers)
                has_price = any('분양가' in h for h in headers)
                
                if has_type and any([has_area, has_count, has_price]):
                    logger.info(f"✓ 주택형 테이블 발견! (테이블 #{idx+1})")
                    
                    # 컬럼 인덱스 찾기
                    area_idx = next((i for i, h in enumerate(headers) if any(keyword in h for keyword in ['전용면적', '전용'])), None) - 1
                    count_idx = next((i for i, h in enumerate(headers) if '금회공급 세대수' in h), None) - 1
                    price_idx = next((i for i, h in enumerate(headers) if '평균분양' in h), None) - 1
                    
                    logger.info(f"컬럼 인덱스 - 전용면적: {area_idx}, 금회공급 세대수: {count_idx}, 평균분양가격(원): {price_idx}")
                    
                    # 데이터 행 추출
                    rows = table.find_elements(By.TAG_NAME, "tr")[1:]  # 헤더 제외
                    logger.info(f"발견된 데이터 행 수: {len(rows)}개")
                    
                    for row_idx, row in enumerate(rows):
                        try:
                            # pc_red 클래스를 가진 요소에서 주택형 추출
                            house_type_element = row.find_element(By.CSS_SELECTOR, "th.pc_red")
                            house_type = house_type_element.text.strip() if house_type_element else None
                            
                            cells = row.find_elements(By.TAG_NAME, "td")
                            
                            # 데이터 추출
                            raw_data = {
                                'house_type': house_type,
                                'exclusive_area': cells[area_idx].text.strip() if area_idx is not None and area_idx < len(cells) else None,
                                'unit_count': cells[count_idx].text.strip() if count_idx is not None and count_idx < len(cells) else None,
                                'avg_price': cells[price_idx].text.strip() if price_idx is not None and price_idx < len(cells) else None
                            }
                            logger.info("=== 추출된 원본 데이터 ===")
                            logger.info(raw_data)
                            cells = row.find_elements(By.TAG_NAME, "td")
                            # 각 셀의 실제 값을 모두 로깅
                            logger.info(f"=== 행 #{row_idx+1} 전체 셀 데이터 ===")
                            for i, cell in enumerate(cells):
                                logger.info(f"셀[{i}]: '{cell.text.strip()}'")
                            
                            # pc_red 클래스를 가진 요소에서 주택형 추출
                            house_type_element = row.find_element(By.CSS_SELECTOR, "th.pc_red")
                            house_type = house_type_element.text.strip() if house_type_element else None
                            logger.info(f"주택형(pc_red): '{house_type}'")
                            
                            # 인덱스별 실제 데이터 로깅
                            logger.info("=== 인덱스별 추출 데이터 ===")
                            logger.info(f"전용면적({area_idx}): '{cells[area_idx].text.strip() if area_idx is not None and area_idx < len(cells) else 'None'}'")
                            logger.info(f"세대수({count_idx}): '{cells[count_idx].text.strip() if count_idx is not None and count_idx < len(cells) else 'None'}'")
                            logger.info(f"분양가격({price_idx}): '{cells[price_idx].text.strip() if price_idx is not None and price_idx < len(cells) else 'None'}'")
                            
                            # 셀 개수 확인
                            max_idx = max(filter(None, [area_idx, count_idx, price_idx]))
                            if len(cells) <= max_idx:
                                logger.warning(f"행 #{row_idx+1}: 셀 개수 부족 (필요: {max_idx+1}, 실제: {len(cells)})")
                                continue
                            
                            logger.info("=== 추출된 원본 데이터 ===")
                            logger.info(raw_data)
                            
                            # 데이터 정제
                            house_info = {
                                'house_type': raw_data['house_type'],
                                'exclusive_area': clean_numeric_string(raw_data['exclusive_area']),
                                'unit_count': clean_numeric_string(raw_data['unit_count']),
                                'avg_price': clean_numeric_string(raw_data['avg_price'])
                            }
                            logger.info("=== 정제된 데이터 ===")
                            logger.info(house_info)
                            
                            house_types.append(house_info)
                            logger.info(f"✓ 행 #{row_idx+1} 주택형 정보 추출: {house_info} 완료")
                            
                        except Exception as row_error:
                            logger.warning(f"⚠️ 행 #{row_idx+1} 처리 중 오류: {row_error}")
                            logger.exception(row_error)  # 상세 에러 로그 추가
                            continue
                else:
                    logger.debug(f"테이블 #{idx+1}는 주택형 테이블이 아님 (헤더 불일치)")
                    
            except Exception as table_error:
                logger.warning(f"⚠️ 테이블 #{idx+1} 처리 중 오류: {table_error}")
                continue
        
        if house_types:
            logger.info(f"✅ 총 {len(house_types)}개 주택형 정보 추출 완료")
        else:
            logger.warning("⚠️ 주택형 정보를 찾을 수 없습니다")
        
        return house_types
        
    except Exception as e:
        logger.error(f"❌ 주택형 정보 추출 중 오류 발생: {str(e)}")
        return []

def classify_notices_by_completeness(notices_data: List[Dict], csv_file_path: str) -> Tuple[List[Dict], List[Dict]]:
    """
    데이터 완성도에 따라 공고를 분류
    - 모든 필드가 있는 공고: DB 저장용
    - 하나라도 필드가 없는 공고: CSV 저장용
    
    Args:
        notices_data: 크롤링된 공고 데이터
        csv_file_path: CSV 파일 저장 경로
    
    Returns:
        Tuple[List[Dict], List[Dict]]: (DB용 공고, CSV용 공고)
    """
    db_notices = []
    csv_notices = []
    
    # 필수 필드 정의 (DB 테이블 스키마 기준)
    required_fields = {
        'base': [  # notices 테이블 필수 필드
            'notice_number',          # 공고번호
            'notice_title',           # 공고명
            'post_date',             # 공고일자
            'application_end_date',   # 공고 마감일
            'document_start_date',    # 당첨자 서류제출(시작일)
            'document_end_date',      # 당첨자 서류제출(종료일)
            'contract_start_date',    # 계약체결기간(시작일)
            'contract_end_date',      # 계약체결기간(종료일)
            'winning_date',           # 당첨자 발표일자
            'move_in_date',          # 입주예정월
            'location',              # 소재지
            'is_correction'          # 정정공고 여부
        ],
        'supply_type': [  # supply_types 테이블 필수 필드
            'supply_type'            # 공급유형
        ],
        'house_type': [  # house_types 테이블 필수 필드
            'house_type',            # 주택형
            'exclusive_area',        # 전용면적
            'unit_count',           # 세대수
            'avg_price'             # 평균분양가
        ]
    }
    
    # 필드 값 검증 로직 강화
    def is_valid_field(value):
        if value is None:
            return False
        if isinstance(value, str) and (value.strip() == '' or value.lower() == '없음'):
            return False
        return True

    # 각 공고 데이터 검사
    for notice in notices_data:
        is_complete = True
        missing_fields = []
        
        # 1. 기본 필드 검사
        for field in required_fields['base']:
            if not is_valid_field(notice.get(field)):
                is_complete = False
                missing_fields.append(field)
        
        # 2. 공급유형 검사
        if not notice.get('supply_type') or not isinstance(notice['supply_type'], (list, tuple)):
            is_complete = False
            missing_fields.append('supply_type')
        
        # 3. 주택형 정보 검사
        if notice.get('house_types'):
            for house_type in notice['house_types']:
                for field in required_fields['house_type']:
                    if not house_type.get(field) or house_type.get(field) == '없음' or house_type.get(field) == '' or house_type.get(field) is None:
                        is_complete = False
                        missing_fields.append(f'house_type.{field}')
        else:
            is_complete = False
            missing_fields.append('house_types')
        
        # 분류 및 로깅
        if is_complete:
            logger.info(f"✅ DB 저장 대상 공고: {notice.get('notice_number')} - 모든 필드 존재")
            db_notices.append(notice)
        else:
            logger.info(f"📝 CSV 저장 대상 공고: {notice.get('notice_number')} - 누락 필드: {', '.join(missing_fields)}")
            csv_notices.append(notice)
    
    # CSV 저장
    if csv_notices:
        # 디렉토리 생성
        os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)
        
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as file:
            # CSV 헤더 설정 (DB 테이블 스키마 기준)
            fieldnames = [
                # notices 테이블 필드
                'notice_number', 'notice_title', 'post_date', 'application_end_date',
                'document_start_date', 'document_end_date',
                'contract_start_date', 'contract_end_date',
                'winning_date', 'move_in_date', 'location', 'is_correction',
                # house_types 테이블 필드
                'house_type', 'exclusive_area', 'unit_count', 'avg_price',
                # supply_types 테이블 필드
                'supply_type'
            ]
            
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            
            # 각 공고와 주택형 정보를 개별 행으로 저장
            for notice in csv_notices:
                # 기본 공고 정보
                base_data = {
                    'notice_number': notice.get('notice_number', ''),
                    'notice_title': notice.get('notice_title', ''),
                    'post_date': notice.get('post_date', ''),
                    'application_end_date': notice.get('application_end_date', ''),
                    'document_start_date': notice.get('document_start_date', ''),
                    'document_end_date': notice.get('document_end_date', ''),
                    'contract_start_date': notice.get('contract_start_date', ''),
                    'contract_end_date': notice.get('contract_end_date', ''),
                    'winning_date': notice.get('winning_date', ''),
                    'move_in_date': notice.get('move_in_date', ''),
                    'location': notice.get('location', ''),
                    'is_correction': notice.get('is_correction', False),
                    'supply_type': str(notice.get('supply_type', []))
                }
                
                # 주택형 정보가 있는 경우
                if notice.get('house_types'):
                    for house_type in notice['house_types']:
                        row_data = base_data.copy()
                        row_data.update({
                            'house_type': house_type.get('house_type', ''),
                            'exclusive_area': house_type.get('exclusive_area', ''),
                            'unit_count': house_type.get('unit_count', ''),
                            'avg_price': house_type.get('avg_price', '')
                        })
                        writer.writerow(row_data)
                else:
                    # 주택형 정보가 없는 경우 기본 정보만 저장
                    writer.writerow(base_data)
        
        logger.info(f"📁 CSV 저장 완료: {len(csv_notices)}개 공고 -> {csv_file_path}")
        logger.info(f"📊 분류 결과 - DB 저장 대상: {len(db_notices)}개 공고, CSV 저장 대상: {len(csv_notices)}개 공고")
    
    return db_notices, csv_notices

def save_failed_notice_to_csv(wrtan_no, target_date, error_msg, url=None):
    """실패한 공고 정보를 CSV 파일에 저장"""
    # 기본 디렉토리 설정
    log_dir = "/opt/airflow/downloads/failed_notices"
    os.makedirs(log_dir, exist_ok=True)
    
    # target_date로 파일 이름 생성
    csv_file_path = f"{log_dir}/failed_notices_{target_date}.csv"
    
    # 실패 정보 준비
    failed_notice = {
        "notice_number": wrtan_no,
        "target_date": target_date.strftime("%Y-%m-%d") if hasattr(target_date, 'strftime') else str(target_date),
        "error_message": error_msg,
        "url": url or "URL 없음"
    }
    
    # CSV 파일에 추가
    try:
        # 필드 이름 정의
        fieldnames = ["notice_number", "target_date", "error_message", "url"]
        
        # 파일 존재 여부 확인
        file_exists = os.path.isfile(csv_file_path)
        
        # CSV 파일 열기 (추가 모드)
        with open(csv_file_path, 'a', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            # 파일이 새로 생성된 경우 헤더 작성
            if not file_exists:
                writer.writeheader()
            
            # 데이터 행 추가
            writer.writerow(failed_notice)
        
        logger.info(f"✅ 실패 공고 {wrtan_no} 정보가 CSV 파일에 저장되었습니다: {csv_file_path}")
        return csv_file_path
    except Exception as e:
        logger.error(f"⚠️ 실패 공고 저장 중 오류 발생: {e}")
        return None
    
def detect_correction_notice(title: str) -> bool:
    """제목에서 정정공고 여부 판별"""
    if not title:
        logger.info("❓ 공고명 없음")
        return False
    
    # 정정공고 키워드 목록
    correction_keywords = ['정정', '변경', '수정', '재공고', '취소', '연기']
    
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
    """기간 텍스트를 시작일과 종료일로 분리"""
    import re
    from datetime import datetime
    
    if not text:
        logger.warning("❓ 기간 텍스트가 비어있음")
        return None
    
    try:
        logger.info(f"📅 기간 분석: \"{text}\"")
        
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
                
                logger.debug(f"패턴 #{i+1} ({desc}) 매칭: '{start_str}' ~ '{end_str}'")
                
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
                    logger.info(f"✅ 기간 파싱 성공: {start_date} ~ {end_date}")
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
            logger.warning(f"❌ 기간 형식 아님: \"{text}\"")
        
        return None
        
    except Exception as e:
        logger.error(f"💥 기간 파싱 오류: {str(e)}, 입력: \"{text}\"")
        import traceback
        logger.debug(f"🔬 상세 오류: {traceback.format_exc()}")
        return None

def normalize_date(date_str: str) -> Optional[str]:
    """다양한 날짜 형식을 YYYY-MM-DD로 표준화"""
    if not date_str or not isinstance(date_str, str):
        logger.warning(f"⚠️ 유효하지 않은 날짜 입력: {date_str}")
        return None
    
    try:
        logger.debug(f"🗓️ 날짜 표준화: \"{date_str}\"")
        
        # 숫자만 추출
        import re
        numbers = re.findall(r'\d+', date_str)
        
        if len(numbers) >= 3:
            year = numbers[0] if len(numbers[0]) == 4 else None
            if year:
                month = numbers[1].zfill(2)
                day = numbers[2].zfill(2)
                
                result = f"{year}-{month}-{day}"
                if validate_date(result):
                    logger.debug(f"✅ 날짜 변환 성공: \"{result}\"")
                    return result
                else:
                    logger.warning(f"⚠️ 유효하지 않은 날짜: {result}")
            else:
                logger.warning(f"⚠️ 올바른 연도 형식이 아님: {numbers[0]}")
        else:
            logger.warning(f"⚠️ 충분한 날짜 구성요소가 없음: {date_str}")
        
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

def clean_numeric_string(value: str) -> Optional[str]:
    """
    숫자 문자열 정제 (콤마 제거, 단위 제거 등)
    
    Args:
        value: 정제할 문자열
        
    Returns:
        Optional[str]: 정제된 숫자 문자열
    """
    if not value:
        return None
        
    try:
        # 콤마 제거
        value = value.replace(',', '')
        
        # 숫자만 추출
        import re
        numbers = re.findall(r'\d+\.?\d*', value)
        if numbers:
            return numbers[0]
            
        return None
        
    except Exception:
        return None