import time
from datetime import datetime, timedelta
import requests
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
import os

from plugins.utils.web_helpers import init_driver
from plugins.utils.file_helpers import sanitize_filename

from typing import List, Tuple, Dict

# LH 공고문 URL 수집
# Selenium을 이용하여 LH 공고문 사이트에서 오늘의 공고문 URL 및 PDF 파일 다운로드
def collect_lh_file_urls(base_url, list_url, download_url, download_dir, headers, target_date=None) -> List[Tuple[str, str, Dict[str, str]]]:
    # 이미 다운로드된 파일 목록을 집합으로 저장하여 중복 다운로드 방지
    already_downloaded = set(os.listdir(download_dir))
    url_list = []       # 수집된 URL과 메타데이터를 저장할 리스트

    # 결과 요약을 위한 변수 추가
    downloaded_count = 0
    skipped_count = 0

    # 드라이버 종료 상태 추적
    driver = None

    try:
        # 세션 재사용으로 연결 최적화
        session = requests.Session()
        session.headers.update(headers)     # 헤더 설정

        driver, wait = init_driver(headers=headers)     # Selenium 드라이버 초기화

        driver.get(list_url)             # 공고문 목록 페이지로 이동
        time.sleep(1)                    # 페이지 로딩 대기 

        # target_date가 제공되지 않으면 현재 날짜 사용
        if target_date is None:
            target_date = datetime.today().date()
        print(f"🔍 조회 날짜: {target_date}")

        # 유형 설정
        # 드롭다운 메뉴에서 검색 조건 설정 (공고 유형 : 05, 나머지 조건은 전체)
        select_elements = {
                "유형": (By.ID, "srchTypeAisTpCd", "05"),
                "고시 유형": (By.ID, "aisTpCdData05", ""),
                "공급주체": (By.ID, "cnpCd", ""),
                "공급상태": (By.ID, "panSs", "")
            }
        
        for name, (by, selector, value) in select_elements.items():
                select_element = wait.until(EC.presence_of_element_located((by, selector)))
                Select(select_element).select_by_value(value)

        # 날짜 필터링
        # 시작일(startDt)과 종료일(endDt)을 오늘 날짜로 설정
        date_str = target_date.strftime("%Y-%m-%d")
        for date_field in ["startDt", "endDt"]:
            date_input = driver.find_element(By.ID, date_field)
            driver.execute_script(
                "arguments[0].removeAttribute('readonly'); arguments[0].value = arguments[1];",
                date_input, date_str
            )

        # 검색 버튼 클릭으로 필터 적용
        search_btn = wait.until(EC.element_to_be_clickable((By.ID, "btnSah")))
        search_btn.click()
        # 한번더 클릭 (페이지의 작동 방식에 따라 필요)
        wait.until(EC.element_to_be_clickable((By.ID, "btnSah"))).click()
        # 검색결과 로딩 대기
        time.sleep(3)

        # 오늘의 공고 목록 확인
        # 공고문 목록에서 링크 요소 추출
        row_links = driver.find_elements(By.CSS_SELECTOR, "a.wrtancInfoBtn")
        if not row_links:
            print("❌ 오늘의 공고가 없습니다. 종료합니다.")
            return url_list         # 빈 리스트 반환

        print(f"📋 오늘의 공고 수: {len(row_links)}개")

        # 각 페이지를 조회하며 공고문 다운로드
        # 각 공고 링크를 클릭하여 상세 페이지로 이동 후 공고문 파일 다운로드
        for idx in range(len(row_links)):
            # 매번 새로 요소 가져오기(페이지 상태 변화 대비)
            row_links = driver.find_elements(By.CSS_SELECTOR, "a.wrtancInfoBtn")
            link = row_links[idx]
            # 공고 번호 추출
            # 공고 번호는 data-id1 속성에서 가져옴
            wrtan_no = link.get_attribute("data-id1")
            print(f"\n[{idx+1}] 공고 클릭: {wrtan_no}")
            # 공고 링크 클릭하여 상세 페이지로 이동
            link.click()
            # 상세 페이지 로딩 대기
            time.sleep(2)

            # 공고일 추출
            try:
                pub_date_text = driver.find_element(By.XPATH, "//li[strong[text()='공고일']]").text
                # 날짜 부분만 추출 및 정제 -> 공고일 텍스트에서 '공고일' 문자열 제거 후 날짜 포맷 변경
                pub_date = pub_date_text.replace("공고일", "").strip().replace(".", "")
            except:
                # 공고일 정보가 없을 경우 오늘 날짜로 설정
                pub_date = target_date.strftime("%Y%m%d")

            # 공고문 리스트 추출
            try:
                # 공고문 섹션에서 파일 리스트 추출
                dl = driver.find_element(By.CSS_SELECTOR, "dl.col_red")
                # 파일 리스트 추출
                items = dl.find_elements(By.CSS_SELECTOR, "dd > ul.bbsV_link.file > li")
            except:
                print("⚠️ 공고문 섹션 없음, 건너뜁니다.")
                # 공고문 섹션이 없을 경우 이전 공고로 이동
                driver.back()
                time.sleep(1)
                continue

            print(f" 📄 공고문 파일 수: {len(items)}")

            # 공고문 파일 다운로드
            for li in items:
                a = li.find_element(By.TAG_NAME, "a")           # 공고문 파일 링크 요소 추출
                filename = a.text.strip()                       # 파일명 추출 및 공백 제거
                # 공고문 파일 링크에서 href 속성 추출
                name_part, ext = os.path.splitext(filename)     # 파일명과 확장자 분리

                # PDF 파일만 필터링
                if '공고' not in filename or ext.lower() != '.pdf':     # 공고문이 아닌 경우 건너뜀
                    continue
                short_name = sanitize_filename(name_part)           # 파일명 안전하게 처리
                file_id = a.get_attribute("href").split("'")[1]     # 파일 ID 추출
                file_url = f"{download_url}?fileid={file_id}"       # 파일 다운로드 URL 생성
                safe_filename = f"{wrtan_no}_{pub_date}_{short_name}{ext}"     # 안전한 파일명 생성
                save_path = os.path.join(download_dir, safe_filename)       # 파일 저장 경로 설정

                # 중복 다운로드 방지
                if safe_filename in already_downloaded:
                    print(f"⏩ 이미 존재 다운로드건너뜀: {safe_filename}")
                    skipped_count += 1  # 건너뛴 파일 수 증가
                else:
                    with session.get(
                        file_url,
                        headers={"Referer": driver.current_url},
                        stream=True
                    ) as resp:
                        resp.raise_for_status()
                        with open(save_path, "wb") as fw:
                            for chunk in resp.iter_content(chunk_size=8192):
                                if chunk:
                                    fw.write(chunk)
                    print(f"✅ 다운로드 완료 → {save_path}")
                    downloaded_count += 1
                    already_downloaded.add(safe_filename)  # 다운로드 완료된 파일명 추가

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

            # 공고문 다운로드 후 상세 페이지에서 목록으로 돌아가기
            driver.back()
            # 검색 버튼 대기
            wait.until(EC.element_to_be_clickable((By.ID, "btnSah")))
            # 검색결과 로딩 대기
            time.sleep(1)

        # 다운로드 결과 요약
        if downloaded_count > 0:
            print(f"✅ 공고문 다운로드 완료: 새로 다운로드 {downloaded_count}건, 이미 존재하여 건너뛴 파일 {skipped_count}건")
        else:
            if skipped_count > 0:
                print(f"ℹ️ 새로 다운로드한 파일 없음 (이미 존재하는 파일 {skipped_count}건)")
            else:
                print("ℹ️ 다운로드 가능한 공고문 파일이 없습니다.")

        return url_list

    except Exception as e:
        print(f"❌ 공고문 수집 중 오류 발생: {str(e)}")
        raise
    finally:
        # 드라이버가 초기화되었고 아직 유효한 경우에만 종료
        if driver is not None:
            try:
                print("finally 블록에서 웹드라이버 종료 시도")
                driver.quit()
                print("finally 블록에서 웹드라이버 종료 완료")
            except Exception as e:
                print(f"finally 블록에서 웹드라이버 종료 시도 중 오류 발생: {e}")
                pass