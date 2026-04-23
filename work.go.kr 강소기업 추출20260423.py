import requests
from bs4 import BeautifulSoup
import pandas as pd
import urllib3
import time
import math
import re
import csv
import os
import random

from selenium import webdriver

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─────────────────────────────────────────────
# 설정값 (필요 시 여기서만 수정)
# ─────────────────────────────────────────────
OUTPUT_FILE = "워크넷_강소기업_완성본.csv"
RESULT_CNT = "100"           # 페이지당 수집 건수 (50→100으로 요청 수 절반 감소)
CSRF_REFRESH_INTERVAL = 30   # 몇 개 1차업종마다 CSRF 토큰 갱신할지
DELAY_MIN = 0.3              # 요청 간 최소 딜레이(초)
DELAY_MAX = 0.6              # 요청 간 최대 딜레이(초)
MAX_RETRIES = 3              # HTTP 요청 실패 시 최대 재시도 횟수

FIELDNAMES = [
    "1차_분류전체", "2차_분류전체", "기업명",
    "1차_업종", "2차_업종", "규모", "근로자수", "소재지", "관심기업"
]

MAIN_URL = "https://www.work.go.kr/jobyoung/smallGiants/corpInfoSrchList.do?coGbCd=small"
POST_URL = "https://www.work.go.kr/jobyoung/smallGiants/corpInfoSrchListPost.do"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": MAIN_URL,
    "X-Requested-With": "XMLHttpRequest",
}

CATEGORIES = {
    "100": "일자리 친화",
    "200": "기술력 우수",
    "300": "재무건전성",
    "400": "글로벌역량",
    "500": "지역선도기업",
    "600": "사회적가치",
    "700": "신청 강소기업",
}


# ─────────────────────────────────────────────
# 유틸리티
# ─────────────────────────────────────────────

def extract_csrf_from_html(html: str) -> str:
    """이미 로드된 HTML에서 CSRF 토큰 추출 (별도 HTTP 요청 없음)"""
    soup = BeautifulSoup(html, "html.parser")
    meta = soup.find("meta", {"name": "_csrf"})
    if not meta:
        raise ValueError("CSRF 토큰을 찾을 수 없습니다. 사이트 구조가 변경되었을 수 있습니다.")
    return meta["content"]


def refresh_csrf_via_session(session: requests.Session) -> str:
    """
    장시간 수집 중 CSRF 토큰 갱신용.
    이미 세션에 Selenium 쿠키가 이식되어 있으므로 GET 요청이 정상 작동합니다.
    """
    res = session.get(MAIN_URL, verify=False, timeout=30)
    return extract_csrf_from_html(res.text)


def post_with_retry(
    session: requests.Session,
    url: str,
    data: dict,
    headers: dict,
) -> requests.Response:
    """지수 백오프(exponential backoff) 방식으로 POST 요청 재시도"""
    for attempt in range(MAX_RETRIES):
        try:
            res = session.post(url, data=data, headers=headers, timeout=30)
            res.raise_for_status()
            return res
        except Exception as e:
            if attempt == MAX_RETRIES - 1:
                raise  # 마지막 시도까지 실패하면 예외를 상위로 전달
            wait = 2 ** attempt  # 1초 → 2초 → 4초
            print(f"   ⚠️  재시도 {attempt + 1}/{MAX_RETRIES} ({wait}초 대기): {e}")
            time.sleep(wait)


def save_batch(batch: list):
    """배치(1차 업종 단위)를 CSV에 즉시 append 저장 — 중간 유실 방지"""
    if not batch:
        return
    file_exists = os.path.isfile(OUTPUT_FILE)
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerows(batch)


def get_completed_ind1_names() -> set:
    """
    이미 저장된 CSV에서 완료된 1차_업종 목록을 읽어 반환.
    잡 재실행 시 완료된 업종은 건너뛰어 이어서 수집 가능.
    """
    if not os.path.isfile(OUTPUT_FILE):
        return set()
    try:
        df = pd.read_csv(OUTPUT_FILE, encoding="utf-8-sig", usecols=["1차_업종"])
        return set(df["1차_업종"].dropna().unique())
    except Exception:
        return set()


# ─────────────────────────────────────────────
# 1단계: Selenium으로 업종 트리 추출
# ─────────────────────────────────────────────

def get_industry_mapping():
    """
    웹페이지 JS 함수 실행으로 1차/2차 업종 카테고리 구조 추출.
    Selenium 쿠키와 CSRF 토큰도 함께 반환해 requests 세션에 이식합니다.
    -> 별도 GET 요청 없이 수집 시작 가능 (타임아웃 방지 핵심)
    Returns: (mapping: dict, selenium_cookies: list, csrf_token: str)
    """
    print("🤖 1단계: 웹 브라우저로 1차/2차 업종 카테고리 구조를 파악합니다...")

    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")               # GitHub Actions headless 필수
    options.add_argument("--window-size=1920,1080")     # 렌더링 안정성
    options.add_argument("--disable-extensions")
    options.add_argument("--proxy-server=direct://")
    options.add_argument("--proxy-bypass-list=*")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    driver = webdriver.Chrome(options=options)
    mapping = {}
    selenium_cookies = []
    csrf_token = ""

    try:
        driver.get(MAIN_URL)
        time.sleep(3)

        # 팝업 alert 처리
        try:
            driver.switch_to.alert.accept()
        except Exception:
            pass

        # ── 핵심: 페이지 로드 직후 쿠키와 CSRF 토큰 추출 ──
        # requests 세션에 이식하면 별도 GET 요청 없이 수집 가능
        selenium_cookies = driver.get_cookies()
        csrf_token = extract_csrf_from_html(driver.page_source)
        print(f"🔑 Selenium에서 쿠키 {len(selenium_cookies)}개 및 CSRF 토큰 추출 완료")

        soup = BeautifulSoup(driver.page_source, "html.parser")
        ind1_items = []

        for btn in soup.select("li[id^='indTpCd1_'] button"):
            onclick = btn.get("onclick", "")
            match = re.search(r"fnIndTpCd1\('([^']+)','([^']+)'", onclick)
            if match and match.group(1):
                ind1_items.append((match.group(1), match.group(2)))

        for val1, name1 in ind1_items:
            mapping[val1] = {"name": name1, "sub": []}
            driver.execute_script(f"fnIndTpCd1('{val1}', '{name1}');")
            time.sleep(0.4)

            soup2 = BeautifulSoup(driver.page_source, "html.parser")
            for btn in soup2.select("#subIndTpList li button"):
                onclick2 = btn.get("onclick", "")
                match2 = re.search(r"fnIndTpCd2\('([^']+)','([^']+)'", onclick2)
                if match2 and match2.group(1):
                    mapping[val1]["sub"].append({
                        "code": match2.group(1),
                        "name": match2.group(2),
                    })

        print(f"✅ 카테고리 파악 완료! 총 {len(mapping)}개 1차 업종 구조를 읽었습니다.\n")

    except Exception as e:
        print(f"❌ 업종 매핑 중 오류 발생: {e}")
    finally:
        driver.quit()

    return mapping, selenium_cookies, csrf_token


# ─────────────────────────────────────────────
# 2단계: requests 세션으로 데이터 수집
# ─────────────────────────────────────────────

def scrape_worknet_optimized():
    industry_mapping, selenium_cookies, csrf_token = get_industry_mapping()
    if not industry_mapping:
        print("업종 구조를 가져오지 못해 종료합니다.")
        return
    if not csrf_token:
        print("CSRF 토큰을 가져오지 못해 종료합니다.")
        return

    # 이미 완료된 1차 업종은 건너뜀 (Resume 기능)
    completed_ind1 = get_completed_ind1_names()
    if completed_ind1:
        print(f"⏭️  이미 완료된 1차 업종 {len(completed_ind1)}개를 건너뜁니다. (Resume 모드)\n")

    session = requests.Session()
    session.verify = False

    # ── 핵심: Selenium 쿠키를 requests 세션에 이식 ──
    # 동일한 서버 세션을 공유하므로 별도 GET 요청 없이 즉시 POST 가능
    for cookie in selenium_cookies:
        session.cookies.set(cookie["name"], cookie["value"], domain=cookie.get("domain", ""))
    print(f"🍪 Selenium 쿠키 {len(selenium_cookies)}개를 requests 세션에 이식 완료")
    print("🚀 2단계: 순차 수집을 시작합니다.\n")

    total_saved = 0  # 누적 저장 건수 (메모리 사용량 최소화)

    for code, cat_name in CATEGORIES.items():
        print(f"\n▶ [{cat_name}] 카테고리 수집 중...")

        for idx, (ind1_code, ind1_info) in enumerate(industry_mapping.items()):

            # ── CSRF 토큰 주기적 갱신 (장시간 수집 세션 만료 방지) ──
            # 쿠키가 이식된 세션이므로 GET 요청이 정상 작동합니다
            if idx > 0 and idx % CSRF_REFRESH_INTERVAL == 0:
                try:
                    csrf_token = refresh_csrf_via_session(session)
                    print(f"   🔄 CSRF 토큰 갱신 완료 (idx={idx})")
                except Exception as e:
                    print(f"   ⚠️  CSRF 토큰 갱신 실패, 기존 토큰 유지: {e}")

            # ── Resume: 이미 완료된 업종 건너뜀 ──
            if ind1_info["name"] in completed_ind1:
                print(f"   ⏭️  [{ind1_info['name']}] 이미 완료, 건너뜀")
                continue

            batch = []  # 1차 업종 단위 배치 버퍼

            try:
                # 1차 업종에 데이터가 있는지 먼저 확인 (불필요한 2차 루프 방지)
                check_1st = {
                    "pageIndex": "1",
                    "coGbCd": "small",
                    "smlgntCoClcd1": code,
                    "superIndTpCd": ind1_code,
                    "indTpCd": ind1_code,
                    "resultCnt": "10",
                    "_csrf": csrf_token,
                }
                res1 = post_with_retry(session, POST_URL, check_1st, HEADERS)
                soup1 = BeautifulSoup(res1.text, "html.parser")
                cnt_tag = soup1.select_one("p.count strong.font-orange")

                if not cnt_tag or int(cnt_tag.get_text(strip=True).replace(",", "")) == 0:
                    continue  # 이 1차 업종엔 데이터 없음, 다음으로

                # 2차 업종별 수집
                for ind2 in ind1_info["sub"]:
                    ind_tp_cd_combined = ind1_code + ind2["code"]

                    check_2nd = {
                        "pageIndex": "1",
                        "coGbCd": "small",
                        "smlgntCoClcd1": code,
                        "superIndTpCd": ind1_code,
                        "subIndTpCd": ind2["code"],
                        "indTpCd": ind_tp_cd_combined,
                        "resultCnt": RESULT_CNT,
                        "_csrf": csrf_token,
                    }
                    res2 = post_with_retry(session, POST_URL, check_2nd, HEADERS)
                    soup2 = BeautifulSoup(res2.text, "html.parser")
                    cnt2_tag = soup2.select_one("p.count strong.font-orange")

                    if not cnt2_tag:
                        continue
                    total2 = int(cnt2_tag.get_text(strip=True).replace(",", ""))
                    if total2 == 0:
                        continue

                    total_pages = math.ceil(total2 / int(RESULT_CNT))

                    for page in range(1, total_pages + 1):
                        payload = check_2nd.copy()
                        payload["pageIndex"] = str(page)
                        payload["sortField"] = "busiNm"
                        payload["sortOrderBy"] = "ASC"

                        res3 = post_with_retry(session, POST_URL, payload, HEADERS)
                        sub_soup = BeautifulSoup(res3.text, "html.parser")
                        rows = sub_soup.select("table.board-list > tbody > tr")

                        for row in rows:
                            cols = row.find_all("td")
                            if len(cols) < 5:
                                continue

                            a_tag = cols[0].select_one("a")
                            corp_name = a_tag.get_text(strip=True) if a_tag else ""

                            info_td = cols[2]
                            ems = info_td.select("em")

                            raw_emp = ems[1].get_text(strip=True) if len(ems) > 1 else ""
                            clean_emp = re.sub(r"[^0-9]", "", raw_emp)
                            emp_count = int(clean_emp) if clean_emp else ""

                            batch.append({
                                "1차_분류전체": cat_name,
                                "2차_분류전체": cols[1].get_text(strip=True),
                                "기업명": corp_name,
                                "1차_업종": ind1_info["name"],
                                "2차_업종": ind2["name"],
                                "규모": ems[0].get_text(strip=True) if ems else "",
                                "근로자수": emp_count,
                                "소재지": cols[3].get_text(strip=True),
                                "관심기업": (
                                    cols[4].select_one("p").get_text(strip=True).replace("건", "")
                                    if cols[4].select_one("p") else ""
                                ),
                            })

                        # 랜덤 딜레이 — IP 차단 방지 (0.3~0.6초)
                        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

                # ── 1차 업종 완료: 배치를 파일에 즉시 저장 ──
                save_batch(batch)
                total_saved += len(batch)
                print(f"   ㄴ [{ind1_info['name']}] 완료 → {len(batch)}건 저장 (누적: {total_saved}건)")

            except Exception as e:
                # 배치에 뭔가 쌓인 게 있으면 부분 저장
                if batch:
                    save_batch(batch)
                    total_saved += len(batch)
                    print(f"   ⚠️  오류 전 [{ind1_info['name']}] 부분 저장: {len(batch)}건")
                print(f"   ❌ [{ind1_info['name']}] 수집 중 에러 발생: {e}")


# ─────────────────────────────────────────────
# 실행 진입점
# ─────────────────────────────────────────────

if __name__ == "__main__":
    start_time = time.time()

    scrape_worknet_optimized()

    # 최종 중복 제거
    if os.path.isfile(OUTPUT_FILE):
        df = pd.read_csv(OUTPUT_FILE, encoding="utf-8-sig")
        before = len(df)
        df = df.drop_duplicates(subset=["기업명", "소재지"], keep="first")
        df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

        elapsed = time.time() - start_time
        mins, secs = divmod(int(elapsed), 60)

        print(f"\n🎉 수집 완료! {before}건 수집 → 중복 제거 후 {len(df)}건 저장.")
        print(f"⏱️  총 소요 시간: {mins}분 {secs}초")
    else:
        print("\n⚠️  저장된 파일이 없습니다. 수집 결과를 확인해주세요.")
