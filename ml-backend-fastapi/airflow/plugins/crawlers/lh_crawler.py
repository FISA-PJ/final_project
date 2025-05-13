# -*- coding: utf-8 -*-
"""
LH 공고문 크롤러 - 점진적 개선 버전
기존 코드에 에러 처리, 로깅, 설정 관리 기능 추가
"""

import time
import logging
from datetime import datetime
import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
import os
import csv
from dataclasses import dataclass
from typing import List, Tuple, Dict, Optional

from plugins.utils.web_helpers import init_driver
from plugins.utils.file_helpers import sanitize_filename

# 로깅 설정
logger = logging.getLogger(__name__)

# 설정 클래스 추가
@dataclass
class CrawlerConfig:
    """크롤러 설정"""
    navigation_wait_time: float = 1.0
    search_wait_time: float = 3.0
    detail_page_wait_time: float = 2.0
    download_chunk_size: int = 8192
    max_retry_count: int = 3
    
    # 셀렉터 정의
    notice_links_selector: str = "a.wrtancInfoBtn"
    search_button_id: str = "btnSah"
    pdf_files_selector: str = "dd > ul.bbsV_link.file > li"

# LH 공고문 URL 수집 및 PDF 다운로드를 위한 함수 (개선된 버전)
def collect_lh_file_urls_and_pdf(base_url, list_url, download_url, download_dir, headers, target_date=None) -> List[Tuple[str, str, Dict[str, str]]]:
    """
    LH 공고문 URL 수집 및 PDF 다운로드 (개선된 버전)
    
    개선사항:
    - 로깅 추가
    - 설정값 관리
    - 에러 처리 강화
    - 메트릭 수집
    """
    logger.info("LH 공고문 크롤링 시작")
    
    # 설정 초기화
    config = CrawlerConfig()
    
    # 이미 다운로드된 파일 목록을 집합으로 저장하여 중복 다운로드 방지
    already_downloaded = set(os.listdir(download_dir))
    url_list = []       # 수집된 URL과 메타데이터를 저장할 리스트

    # 결과 요약을 위한 변수 추가
    downloaded_count = 0
    skipped_count = 0
    error_count = 0

    # 드라이버 종료 상태 추적
    driver = None
    session = None

    try:
        # 세션 재사용으로 연결 최적화
        session = requests.Session()
        session.headers.update(headers)     # 헤더 설정

        driver, wait = init_driver(headers=headers)     # Selenium 드라이버 초기화
        logger.info("웹 드라이버 초기화 완료")

        driver.get(list_url)             # 공고문 목록 페이지로 이동
        time.sleep(config.navigation_wait_time)                    # 페이지 로딩 대기 

        # target_date가 제공되지 않으면 현재 날짜 사용
        if target_date is None:
            target_date = datetime.today().date()
        logger.info(f"조회 날짜: {target_date}")

        # 유형 설정 (개선: 하드코딩 방지)
        # 드롭다운 메뉴에서 검색 조건 설정 (공고 유형 : 05, 나머지 조건은 전체)
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
                logger.debug(f"드롭다운 설정 완료: {name} = {value}")
            except Exception as e:
                logger.warning(f"드롭다운 설정 실패: {name}, 오류: {e}")

        # 날짜 필터링
        # 시작일(startDt)과 종료일(endDt)을 오늘 날짜로 설정
        date_str = target_date.strftime("%Y-%m-%d")
        for date_field in ["startDt", "endDt"]:
            try:
                date_input = driver.find_element(By.ID, date_field)
                driver.execute_script(
                    "arguments[0].removeAttribute('readonly'); arguments[0].value = arguments[1];",
                    date_input, date_str
                )
                logger.debug(f"날짜 설정 완료: {date_field} = {date_str}")
            except Exception as e:
                logger.warning(f"날짜 설정 실패: {date_field}, 오류: {e}")

        # 검색 버튼 클릭으로 필터 적용
        try:
            search_btn = wait.until(EC.element_to_be_clickable((By.ID, config.search_button_id)))
            search_btn.click()
            # 한번더 클릭 (페이지의 작동 방식에 따라 필요)
            wait.until(EC.element_to_be_clickable((By.ID, config.search_button_id))).click()
            # 검색결과 로딩 대기
            time.sleep(config.search_wait_time)
            logger.info("검색 실행 완료")
        except Exception as e:
            logger.error(f"검색 실행 실패: {e}")
            raise

        # 오늘의 공고 목록 확인
        # 공고문 목록에서 링크 요소 추출
        row_links = driver.find_elements(By.CSS_SELECTOR, config.notice_links_selector)
        if not row_links:
            logger.info("오늘의 공고가 없습니다.")
            return url_list         # 빈 리스트 반환

        logger.info(f"발견된 공고 수: {len(row_links)}개")

        # 각 페이지를 조회하며 공고문 다운로드
        # 각 공고 링크를 클릭하여 상세 페이지로 이동 후 공고문 파일 다운로드
        for idx in range(len(row_links)):
            try:
                # 매번 새로 요소 가져오기(페이지 상태 변화 대비)
                row_links = driver.find_elements(By.CSS_SELECTOR, config.notice_links_selector)
                if idx >= len(row_links):
                    logger.warning(f"인덱스 초과: {idx} >= {len(row_links)}")
                    break
                    
                link = row_links[idx]
                # 공고 번호 추출
                # 공고 번호는 data-id1 속성에서 가져옴
                wrtan_no = link.get_attribute("data-id1")
                logger.info(f"[{idx+1}] 공고 처리: {wrtan_no}")
                # 공고 링크 클릭하여 상세 페이지로 이동
                link.click()
                # 상세 페이지 로딩 대기
                time.sleep(config.detail_page_wait_time)

                # 공고일 추출 (개선: 에러 처리 강화)
                try:
                    pub_date_text = driver.find_element(By.XPATH, "//li[strong[text()='공고일']]").text
                    # 날짜 부분만 추출 및 정제 -> 공고일 텍스트에서 '공고일' 문자열 제거 후 날짜 포맷 변경
                    pub_date = pub_date_text.replace("공고일", "").strip().replace(".", "")
                    logger.debug(f"공고일 추출: {pub_date}")
                except Exception as e:
                    # 공고일 정보가 없을 경우 오늘 날짜로 설정
                    pub_date = target_date.strftime("%Y%m%d")
                    logger.warning(f"공고일 추출 실패, 기본값 사용: {pub_date}, 오류: {e}")

                # 공고문 리스트 추출
                try:
                    # 공고문 섹션에서 파일 리스트 추출
                    dl = driver.find_element(By.CSS_SELECTOR, "dl.col_red")
                    # 파일 리스트 추출
                    items = dl.find_elements(By.CSS_SELECTOR, config.pdf_files_selector)
                    logger.debug(f"공고문 파일 수: {len(items)}")
                except Exception as e:
                    logger.warning(f"공고문 섹션 없음, 건너뜀: {e}")
                    # 공고문 섹션이 없을 경우 이전 공고로 이동
                    driver.back()
                    time.sleep(config.navigation_wait_time)
                    continue

                # 공고문 파일 다운로드
                for li in items:
                    try:
                        a = li.find_element(By.TAG_NAME, "a")           # 공고문 파일 링크 요소 추출
                        filename = a.text.strip()                       # 파일명 추출 및 공백 제거
                        # 공고문 파일 링크에서 href 속성 추출
                        name_part, ext = os.path.splitext(filename)     # 파일명과 확장자 분리

                        # PDF 파일만 필터링
                        if '공고' not in filename or ext.lower() != '.pdf':     # 공고문이 아닌 경우 건너뜀
                            continue
                        short_name = sanitize_filename(name_part)           # 파일명 안전하게 처리
                        
                        # 파일 ID 추출 (개선: 예외 처리 추가)
                        href = a.get_attribute("href")
                        if not href or "'" not in href:
                            logger.warning(f"잘못된 href 속성: {href}")
                            continue
                            
                        file_id = href.split("'")[1]     # 파일 ID 추출
                        file_url = f"{download_url}?fileid={file_id}"       # 파일 다운로드 URL 생성
                        safe_filename = f"{wrtan_no}_{pub_date}_{short_name}{ext}"     # 안전한 파일명 생성
                        save_path = os.path.join(download_dir, safe_filename)       # 파일 저장 경로 설정

                        # 중복 다운로드 방지
                        if safe_filename in already_downloaded:
                            logger.info(f"이미 존재하는 파일 건너뜀: {safe_filename}")
                            skipped_count += 1  # 건너뛴 파일 수 증가
                        else:
                            # 다운로드 실행 (개선: 에러 처리 강화)
                            try:
                                with session.get(
                                    file_url,
                                    headers={"Referer": driver.current_url},
                                    stream=True,
                                    timeout=30  # 타임아웃 추가
                                ) as resp:
                                    resp.raise_for_status()
                                    with open(save_path, "wb") as fw:
                                        for chunk in resp.iter_content(chunk_size=config.download_chunk_size):
                                            if chunk:
                                                fw.write(chunk)
                                
                                # 파일 크기 확인
                                file_size = os.path.getsize(save_path)
                                if file_size == 0:
                                    os.remove(save_path)
                                    logger.error(f"다운로드된 파일이 비어있음: {save_path}")
                                    error_count += 1
                                    continue
                                
                                logger.info(f"다운로드 완료: {save_path} ({file_size} bytes)")
                                downloaded_count += 1
                                already_downloaded.add(safe_filename)  # 다운로드 완료된 파일명 추가
                            except Exception as e:
                                logger.error(f"다운로드 실패: {save_path}, 오류: {e}")
                                error_count += 1
                                continue

                        # 공고문 URL과 메타데이터 저장
                        url_list.append((
                            file_url,
                            safe_filename,
                            {
                                "wrtan_no": wrtan_no,
                                "pub_date": pub_date,
                                "filename": filename
                            }
                        ))
                    except Exception as e:
                        logger.error(f"파일 처리 중 오류: {e}")
                        error_count += 1
                        continue

                # 공고문 다운로드 후 상세 페이지에서 목록으로 돌아가기
                driver.back()
                # 검색 버튼 대기
                wait.until(EC.element_to_be_clickable((By.ID, config.search_button_id)))
                # 검색결과 로딩 대기
                time.sleep(config.navigation_wait_time)
            except Exception as e:
                logger.error(f"공고 처리 중 오류 (인덱스 {idx}): {e}")
                error_count += 1
                # 오류 발생 시 목록으로 돌아가기 시도
                try:
                    driver.back()
                    time.sleep(config.navigation_wait_time)
                except:
                    pass

        # 다운로드 결과 요약 (개선: 상세 통계)
        total_files = downloaded_count + skipped_count + error_count
        success_rate = (downloaded_count / total_files * 100) if total_files > 0 else 0
        
        logger.info("=== 크롤링 완료 통계 ===")
        logger.info(f"전체 파일: {total_files}")
        logger.info(f"새로 다운로드: {downloaded_count}")
        logger.info(f"이미 존재: {skipped_count}")
        logger.info(f"오류 발생: {error_count}")
        logger.info(f"성공률: {success_rate:.1f}%")

        return url_list

    except Exception as e:
        logger.error(f"전체 크롤링 프로세스 오류: {str(e)}", exc_info=True)
        raise
    finally:
        # 리소스 정리 (개선: 명시적 정리)
        if session:
            session.close()
            logger.debug("HTTP 세션 종료")
            
        if driver is not None:
            try:
                logger.info("웹드라이버 종료 시도")
                driver.quit()
                logger.info("웹드라이버 종료 완료")
            except Exception as e:
                logger.error(f"웹드라이버 종료 중 오류: {e}")


# LH 공고문 URL 수집 및 주소 정보 추출 (개선된 버전)
def collect_lh_notices_with_address(base_url, list_url, download_url, download_dir, headers, target_date=None) -> List[Tuple[str, str, Dict[str, str]]]:
    """
    LH 공고문 URL 수집 및 주소 정보 추출 (개선된 버전)
    
    개선사항:
    - 로깅 추가
    - CSV 처리 개선
    - 에러 처리 강화
    """
    logger.info("LH 공고문 주소 정보 크롤링 시작")
    
    # 설정 초기화
    config = CrawlerConfig()
    
    # 주소 정보를 저장할 파일 경로
    address_file = os.path.join(download_dir, "address_notices.csv")
    no_address_file = os.path.join(download_dir, "no_address_notices.csv")
    url_list = []       # 수집된 URL과 메타데이터를 저장할 리스트

    # 결과 요약을 위한 변수 추가
    address_found_count = 0
    no_address_count = 0
    error_count = 0

    # 드라이버 종료 상태 추적
    driver = None
    session = None

    # CSV 파일 생성 또는 추가 모드로 열기 - 파일이 없는 경우에만 헤더 추가
    file_exists_address = os.path.isfile(address_file)
    file_exists_no_address = os.path.isfile(no_address_file)
    
    # 주소가 있는 공고용 파일이 없으면 새로 생성하고 헤더 추가
    if not file_exists_address:
        try:
            with open(address_file, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['공고일', '공고번호', '공고명', '주소'])
            logger.info(f"주소 있는 공고 CSV 파일 생성: {address_file}")
        except Exception as e:
            logger.error(f"CSV 파일 생성 실패: {address_file}, 오류: {e}")
            raise

    # 주소가 없는 공고용 파일이 없으면 새로 생성하고 헤더 추가
    if not file_exists_no_address:
        try:
            with open(no_address_file, 'w', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow(['공고일', '공고번호', '공고명'])
            logger.info(f"주소 없는 공고 CSV 파일 생성: {no_address_file}")
        except Exception as e:
            logger.error(f"CSV 파일 생성 실패: {no_address_file}, 오류: {e}")
            raise

    try:
        # 세션 재사용으로 연결 최적화
        session = requests.Session()
        session.headers.update(headers)     # 헤더 설정

        driver, wait = init_driver(headers=headers)     # Selenium 드라이버 초기화
        logger.info("웹 드라이버 초기화 완료")

        driver.get(list_url)             # 공고문 목록 페이지로 이동
        time.sleep(config.navigation_wait_time)                    # 페이지 로딩 대기 

        # target_date가 제공되지 않으면 현재 날짜 사용
        if target_date is None:
            target_date = datetime.today().date()
        logger.info(f"조회 날짜: {target_date}")

        # 유형 설정 (개선: 로깅 추가)
        # 드롭다운 메뉴에서 검색 조건 설정 (공고 유형 : 05, 나머지 조건은 전체)
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
                logger.debug(f"드롭다운 설정 완료: {name} = {value}")
            except Exception as e:
                logger.warning(f"드롭다운 설정 실패: {name}, 오류: {e}")

        # 날짜 필터링
        # 시작일(startDt)과 종료일(endDt)을 오늘 날짜로 설정
        date_str = target_date.strftime("%Y-%m-%d")
        for date_field in ["startDt", "endDt"]:
            try:
                date_input = driver.find_element(By.ID, date_field)
                driver.execute_script(
                    "arguments[0].removeAttribute('readonly'); arguments[0].value = arguments[1];",
                    date_input, date_str
                )
                logger.debug(f"날짜 설정 완료: {date_field} = {date_str}")
            except Exception as e:
                logger.warning(f"날짜 설정 실패: {date_field}, 오류: {e}")

        # 검색 버튼 클릭으로 필터 적용
        try:
            search_btn = wait.until(EC.element_to_be_clickable((By.ID, config.search_button_id)))
            search_btn.click()
            # 한번더 클릭 (페이지의 작동 방식에 따라 필요)
            wait.until(EC.element_to_be_clickable((By.ID, config.search_button_id))).click()
            # 검색결과 로딩 대기
            time.sleep(config.search_wait_time)
            logger.info("검색 실행 완료")
        except Exception as e:
            logger.error(f"검색 실행 실패: {e}")
            raise

        # 오늘의 공고 목록 확인
        # 공고문 목록에서 링크 요소 추출
        row_links = driver.find_elements(By.CSS_SELECTOR, config.notice_links_selector)
        if not row_links:
            logger.info("오늘의 공고가 없습니다.")
            return url_list         # 빈 리스트 반환

        logger.info(f"발견된 공고 수: {len(row_links)}개")

        # 각 페이지를 조회하며 주소 정보 추출
        for idx in range(len(row_links)):
            try:
                # 매번 새로 요소 가져오기(페이지 상태 변화 대비)
                row_links = driver.find_elements(By.CSS_SELECTOR, config.notice_links_selector)
                if idx >= len(row_links):
                    logger.warning(f"인덱스 초과: {idx} >= {len(row_links)}")
                    break
                    
                link = row_links[idx]
                # 공고 번호 추출
                # 공고 번호는 data-id1 속성에서 가져옴
                wrtan_no = link.get_attribute("data-id1")
                logger.info(f"[{idx+1}] 공고 처리: {wrtan_no}")
                # 공고 링크 클릭하여 상세 페이지로 이동
                link.click()
                # 상세 페이지 로딩 대기
                time.sleep(config.detail_page_wait_time)

                # 공고일 추출 (개선: 에러 처리 강화)
                try:
                    pub_date_text = driver.find_element(By.XPATH, "//li[strong[text()='공고일']]").text
                    # 날짜 부분만 추출 및 정제 -> 공고일 텍스트에서 '공고일' 문자열 제거 후 날짜 포맷 변경
                    pub_date = pub_date_text.replace("공고일", "").strip().replace(".", "")
                    logger.debug(f"공고일 추출: {pub_date}")
                except Exception as e:
                    # 공고일 정보가 없을 경우 오늘 날짜로 설정
                    pub_date = target_date.strftime("%Y%m%d")
                    logger.warning(f"공고일 추출 실패, 기본값 사용: {pub_date}, 오류: {e}")
                
                # 공고 제목 추출 (개선: 에러 처리 강화)
                try:
                    title_element = driver.find_element(By.CSS_SELECTOR, "h2.bbsV_subject")
                    title = title_element.text.strip()
                    logger.debug(f"제목 추출: {title}")
                except Exception as e:
                    title = f"공고 {wrtan_no}"
                    logger.warning(f"제목 추출 실패, 기본값 사용: {title}, 오류: {e}")
                
                # 공고문 파일명 추출 (개선: 에러 처리 강화)
                try:
                    # 공고문 섹션에서 파일 리스트 추출
                    dl = driver.find_element(By.CSS_SELECTOR, "dl.col_red")
                    # 파일 리스트 추출
                    items = dl.find_elements(By.CSS_SELECTOR, config.pdf_files_selector)
                    
                    # PDF 파일명 추출 (첫 번째 PDF 파일 사용)
                    name_part = ""
                    for li in items:
                        try:
                            a = li.find_element(By.TAG_NAME, "a")
                            filename = a.text.strip()
                            _, ext = os.path.splitext(filename)
                            if '공고' in filename and ext.lower() == '.pdf':
                                name_part = os.path.splitext(filename)[0]
                                logger.debug(f"PDF 파일명 추출: {name_part}")
                                break
                        except Exception as e:
                            logger.debug(f"개별 파일 처리 중 오류: {e}")
                            continue
                    
                    if not name_part:
                        name_part = title
                        logger.debug(f"PDF 파일명을 찾을 수 없어 제목 사용: {name_part}")
                except Exception as e:
                    name_part = title
                    logger.warning(f"공고문 섹션 없음, 제목 사용: {e}")
                
                # 주소 정보 추출 시도 (개선: 더 정확한 주소 패턴 인식)
                address = None
                try:
                    # 공고 내용에서 주소 정보 찾기
                    content_items = driver.find_elements(By.CSS_SELECTOR, "li.w100")
                    logger.debug(f"컨텐츠 아이템 수: {len(content_items)}")
                    
                    # 주소 키워드 패턴 개선
                    address_keywords = ["동", "구", "로", "시", "군", "읍", "면"]
                    
                    for item in content_items:
                        try:
                            item_text = item.text.strip()
                            # 주소 패턴 확인 (예시: "장원시 진해구 해원로 45(석동,진해석동 우림필유)")
                            if any(keyword in item_text for keyword in address_keywords):
                                # <strong> 태그 이후의 텍스트가 주소인 경우
                                try:
                                    strong_text = item.find_element(By.TAG_NAME, "strong").text.strip()
                                    address = item_text.replace(strong_text, "").strip()
                                    if address.startswith('"') and address.endswith('"'):
                                        address = address[1:-1].strip()
                                except:
                                    address = item_text
                                
                                if address:
                                    logger.info(f"주소 발견: {address}")
                                    break
                        except Exception as e:
                            logger.debug(f"개별 컨텐츠 아이템 처리 중 오류: {e}")
                            continue
                    
                    # CSV 파일에 저장 (개선: 에러 처리 추가)
                    if address:
                        try:
                            with open(address_file, 'a', newline='', encoding='utf-8') as file:
                                writer = csv.writer(file)
                                writer.writerow([pub_date, wrtan_no, name_part, address])
                            address_found_count += 1
                            logger.debug(f"주소 정보 CSV 저장 완료: {wrtan_no}")
                        except Exception as e:
                            logger.error(f"주소 정보 CSV 저장 실패: {e}")
                            error_count += 1
                    else:
                        logger.info("주소 정보를 찾을 수 없습니다.")
                        try:
                            with open(no_address_file, 'a', newline='', encoding='utf-8') as file:
                                writer = csv.writer(file)
                                writer.writerow([pub_date, wrtan_no, name_part])
                            no_address_count += 1
                            logger.debug(f"주소 없는 공고 CSV 저장 완료: {wrtan_no}")
                        except Exception as e:
                            logger.error(f"주소 없는 공고 CSV 저장 실패: {e}")
                            error_count += 1
                    
                    # 메타데이터와 함께 URL 정보 저장
                    url_list.append((
                        driver.current_url,
                        f"{wrtan_no}_{pub_date}",
                        {
                            "wrtan_no": wrtan_no,
                            "pub_date": pub_date,
                            "name_part": name_part,
                            "address": address,
                            "title": title
                        }
                    ))
                    
                except Exception as e:
                    logger.error(f"주소 추출 중 오류: {str(e)}")
                    # 오류 발생 시에도 주소 없는 공고로 간주하고 CSV에 저장
                    try:
                        with open(no_address_file, 'a', newline='', encoding='utf-8') as file:
                            writer = csv.writer(file)
                            writer.writerow([pub_date, wrtan_no, name_part])
                        no_address_count += 1
                    except Exception as csv_e:
                        logger.error(f"CSV 저장 중 오류: {csv_e}")
                    error_count += 1

                # 상세 페이지에서 목록으로 돌아가기
                try:
                    driver.back()
                    # 검색 버튼 대기
                    wait.until(EC.element_to_be_clickable((By.ID, config.search_button_id)))
                    # 검색결과 로딩 대기
                    time.sleep(config.navigation_wait_time)
                except Exception as e:
                    logger.warning(f"목록으로 돌아가기 실패: {e}")
            except Exception as e:
                logger.error(f"공고 처리 중 오류 (인덱스 {idx}): {e}")
                error_count += 1
                # 오류 발생 시 목록으로 돌아가기 시도
                try:
                    driver.back()
                    time.sleep(config.navigation_wait_time)
                except:
                    pass

        # 결과 요약 (개선: 상세 통계)
        total_notices = len(row_links)
        success_rate = ((address_found_count + no_address_count) / total_notices * 100) if total_notices > 0 else 0
        
        logger.info("=== 주소 수집 완료 통계 ===")
        logger.info(f"전체 공고: {total_notices}")
        logger.info(f"주소 정보 있음: {address_found_count} (파일: {address_file})")
        logger.info(f"주소 정보 없음: {no_address_count} (파일: {no_address_file})")
        logger.info(f"오류 발생: {error_count}")
        logger.info(f"성공률: {success_rate:.1f}%")

        return url_list

    except Exception as e:
        logger.error(f"전체 주소 수집 프로세스 오류: {str(e)}", exc_info=True)
        raise
    finally:
        # 리소스 정리 (개선: 명시적 정리)
        if session:
            session.close()
            logger.debug("HTTP 세션 종료")
            
        if driver is not None:
            try:
                logger.info("웹드라이버 종료 시도")
                driver.quit()
                logger.info("웹드라이버 종료 완료")
            except Exception as e:
                logger.error(f"웹드라이버 종료 중 오류: {e}")


# 사용 예시 및 테스트 코드
if __name__ == "__main__":
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 테스트 실행
    base_url = "https://apply.lh.or.kr"
    list_url = f"{base_url}/lhapply/apply/wt/wrtanc/selectWrtancList.do?viewType=srch"
    download_url = f"{base_url}/lhapply/lhFile.do"
    download_dir = "/opt/airflow/downloads"
    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        # PDF 다운로드 테스트
        logger.info("PDF 다운로드 테스트 시작")
        result1 = collect_lh_file_urls_and_pdf(base_url, list_url, download_url, download_dir, headers)
        logger.info(f"PDF 다운로드 결과: {len(result1)}개")
        
        # 주소 수집 테스트
        logger.info("주소 수집 테스트 시작")
        result2 = collect_lh_notices_with_address(base_url, list_url, download_url, download_dir, headers)
        logger.info(f"주소 수집 결과: {len(result2)}개")
    except Exception as e:
        logger.error(f"테스트 실행 중 오류: {e}", exc_info=True)