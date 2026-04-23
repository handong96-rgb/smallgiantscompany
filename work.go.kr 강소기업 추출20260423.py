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
# 💡 pytz 대신 파이썬 기본 내장 모듈만 사용합니다.
from datetime import datetime, timezone, timedelta

from selenium import webdriver

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─────────────────────────────────────────────
# 설정값 (필요 시 여기서만 수정)
# ─────────────────────────────────────────────
OUTPUT_FILE = "워크넷_강소기업_완성본.csv"
RESULT_CNT = "100"           
CSRF_REFRESH_INTERVAL = 30   
DELAY_MIN = 0.3              
DELAY_MAX = 0.6              
MAX_RETRIES = 3              

# '데이터 추출일' 열 추가
FIELDNAMES = [
    "1차_분류전체", "2차_분류전체", "기업명",
    "1차_업종", "2차_업종", "규모", "근로자수", "소재지", "관심기업", "데이터 추출일"
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

def get_kst_now():
    """한국 시간(KST)을 문자열로 반환 (외부 라이브러리 불필요)"""
    # UTC 기준 시간에 9시간을 더해 KST를 만듭니다.
    KST = timezone(timedelta(hours=9))
    return datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')

def extract_csrf_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    meta = soup.find("meta", {"name": "_csrf"})
    if not meta:
        raise ValueError("CSRF 토큰을 찾을 수 없습니다.")
    return meta["content"]

def refresh_csrf_via_session(session: requests.Session) -> str:
    res = session.get(MAIN_URL, verify=False, timeout=30)
    return extract_csrf_from_html(res.text)

def post_with_retry(session, url, data, headers):
    for attempt in range(MAX_RETRIES):
        try:
            res = session.post(url, data=data, headers=headers, timeout=30)
            res.raise_for_status()
            return res
        except Exception as e:
            if attempt == MAX_RETRIES - 1: raise
            time.sleep(2 ** attempt)

def save_batch(batch: list):
    if not batch: return
    file_exists = os.path.isfile(OUTPUT_FILE)
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerows(batch)

# ─────────────────────────────────────────────
# 1단계: Selenium으로 업종 트리 추출
# ─────────────────────────────────────────────

def get_industry_mapping():
    print("🤖 1단계: 웹 브라우저로 업종 구조 및 세션 정보 파악 중...")
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=" + HEADERS["User-Agent"])

    driver = webdriver.Chrome(options=options)
    mapping = {}
    selenium_cookies = []
    csrf_token = ""

    try:
        driver.get(MAIN_URL)
        time.sleep(3)
        selenium_cookies = driver.get_cookies()
        csrf_token = extract_csrf_from_html(driver.page_source)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        ind1_items = []
        for btn in soup.select("li[id^='indTpCd1_'] button"):
            onclick = btn.get("onclick", "")
            match = re.search(r"fnIndTpCd1\('([^']+)','([^']+)'", onclick)
            if match: ind1_items.append((match.group(1), match.group(2)))

        for val1, name1 in ind1_items:
            mapping[val1] = {"name": name1, "sub": []}
            driver.execute_script(f"fnIndTpCd1('{val1}', '{name1}');")
            time.sleep(0.4)
            soup2 = BeautifulSoup(driver.page_source, "html.parser")
            for btn in soup2.select("#subIndTpList li button"):
                onclick2 = btn.get("onclick", "")
                match2 = re.search(r"fnIndTpCd2\('([^']+)','([^']+)'", onclick2)
                if match2: mapping[val1]["sub"].append({"code": match2.group(1), "name": match2.group(2)})
        print("✅ 업종 구조 파악 완료.\n")
    finally:
        driver.quit()
    return mapping, selenium_cookies, csrf_token

# ─────────────────────────────────────────────
# 2단계: 데이터 수집
# ─────────────────────────────────────────────

def scrape_worknet_optimized():
    industry_mapping, selenium_cookies, csrf_token = get_industry_mapping()
    if not industry_mapping: return

    session = requests.Session()
    for cookie in selenium_cookies:
        session.cookies.set(cookie["name"], cookie["value"], domain=cookie.get("domain", ""))

    total_saved = 0
    # 💡 내장 모듈로 구한 한국 시간 고정
    extraction_date = get_kst_now()

    for code, cat_name in CATEGORIES.items():
        print(f"▶ [{cat_name}] 수집 중...")
        for idx, (ind1_code, ind1_info) in enumerate(industry_mapping.items()):
            if idx > 0 and idx % CSRF_REFRESH_INTERVAL == 0:
                csrf_token = refresh_csrf_via_session(session)

            batch = []
            try:
                check_1st = {"pageIndex": "1", "coGbCd": "small", "smlgntCoClcd1": code, "superIndTpCd": ind1_code, "indTpCd": ind1_code, "resultCnt": "10", "_csrf": csrf_token}
                res1 = post_with_retry(session, POST_URL, check_1st, HEADERS)
                soup1 = BeautifulSoup(res1.text, "html.parser")
                cnt_tag = soup1.select_one("p.count strong.font-orange")
                if not cnt_tag or int(cnt_tag.get_text(strip=True).replace(",", "")) == 0: continue

                for ind2 in ind1_info["sub"]:
                    check_2nd = {"pageIndex": "1", "coGbCd": "small", "smlgntCoClcd1": code, "superIndTpCd": ind1_code, "subIndTpCd": ind2["code"], "indTpCd": ind1_code + ind2["code"], "resultCnt": RESULT_CNT, "_csrf": csrf_token}
                    res2 = post_with_retry(session, POST_URL, check_2nd, HEADERS)
                    soup2 = BeautifulSoup(res2.text, "html.parser")
                    total_pages = math.ceil(int(soup2.select_one("p.count strong.font-orange").get_text(strip=True).replace(",", "")) / int(RESULT_CNT))

                    for page in range(1, total_pages + 1):
                        payload = check_2nd.copy()
                        payload["pageIndex"] = str(page)
                        res3 = post_with_retry(session, POST_URL, payload, HEADERS)
                        rows = BeautifulSoup(res3.text, "html.parser").select("table.board-list > tbody > tr")

                        for row in rows:
                            cols = row.find_all("td")
                            if len(cols) < 5: continue
                            ems = cols[2].select("em")
                            clean_emp = re.sub(r"[^0-9]", "", ems[1].get_text(strip=True)) if len(ems) > 1 else ""
                            
                            batch.append({
                                "1차_분류전체": cat_name,
                                "2차_분류전체": cols[1].get_text(strip=True),
                                "기업명": cols[0].get_text(strip=True),
                                "1차_업종": ind1_info["name"],
                                "2차_업종": ind2["name"],
                                "규모": ems[0].get_text(strip=True) if ems else "",
                                "근로자수": int(clean_emp) if clean_emp else "",
                                "소재지": cols[3].get_text(strip=True),
                                "관심기업": cols[4].select_one("p").get_text(strip=True).replace("건", "") if cols[4].select_one("p") else "",
                                "데이터 추출일": extraction_date
                            })
                        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

                save_batch(batch)
                total_saved += len(batch)
                print(f"   ㄴ [{ind1_info['name']}] 완료 (누적: {total_saved}건)")
            except Exception as e:
                print(f"   ❌ 에러: {e}")

if __name__ == "__main__":
    start_time = time.time()
    
    if os.path.isfile(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
    
    scrape_worknet_optimized()

    if os.path.isfile(OUTPUT_FILE):
        df = pd.read_csv(OUTPUT_FILE, encoding="utf-8-sig")
        df.drop_duplicates(subset=["기업명", "소재지"], keep="first").to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")
        mins, secs = divmod(int(time.time() - start_time), 60)
        print(f"\n🎉 수집 완료! 총 {len(df)}건 저장. (소요시간: {mins}분 {secs}초)")
