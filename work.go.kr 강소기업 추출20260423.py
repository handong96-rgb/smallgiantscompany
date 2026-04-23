import requests
from bs4 import BeautifulSoup
import pandas as pd
import urllib3
import time
import math
import re

# 💡 동적 2차업종 추출을 위해 Selenium 추가
from selenium import webdriver

# 보안 경고 무시
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_csrf_token(session, url):
    """메인 페이지에서 실시간 보안 토큰(CSRF) 확보"""
    res = session.get(url, verify=False)
    soup = BeautifulSoup(res.text, 'html.parser')
    return soup.find("meta", {"name": "_csrf"})["content"]

def get_industry_mapping():
    """웹페이지에 내장된 자바스크립트 함수를 직접 실행하여 1차/2차 업종 매핑을 빠르고 안전하게 추출합니다."""
    print("🤖 1단계: 웹 브라우저를 통해 1차/2차 업종 카테고리 구조를 파악합니다...")
    
    options = webdriver.ChromeOptions()
    options.add_argument('--headless') 
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    driver = webdriver.Chrome(options=options)
    
    mapping = {}
    try:
        driver.get("https://www.work.go.kr/jobyoung/smallGiants/corpInfoSrchList.do?coGbCd=small")
        time.sleep(3) # 사이트 완전 로드 대기
        
        try:
            alert = driver.switch_to.alert
            alert.accept()
        except:
            pass
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        ind1_items = []
        
        for btn in soup.select("li[id^='indTpCd1_'] button"):
            onclick = btn.get('onclick', '')
            match = re.search(r"fnIndTpCd1\('([^']+)','([^']+)'", onclick)
            if match and match.group(1):
                ind1_items.append((match.group(1), match.group(2)))
        
        for val1, name1 in ind1_items:
            mapping[val1] = {"name": name1, "sub": []}
            
            driver.execute_script(f"fnIndTpCd1('{val1}', '{name1}');")
            time.sleep(0.4) 
            
            soup2 = BeautifulSoup(driver.page_source, 'html.parser')
            for btn in soup2.select("#subIndTpList li button"):
                onclick2 = btn.get('onclick', '')
                match2 = re.search(r"fnIndTpCd2\('([^']+)','([^']+)'", onclick2)
                if match2 and match2.group(1):
                    mapping[val1]["sub"].append({
                        "code": match2.group(1),
                        "name": match2.group(2)
                    })
                    
        print(f"✅ 카테고리 파악 완료! 총 {len(mapping)}개의 1차 업종 구조를 읽었습니다.\n")
    except Exception as e:
        print(f"❌ 업종 매핑 중 오류 발생: {e}")
    finally:
        driver.quit()
        
    return mapping

def scrape_worknet_optimized():
    industry_mapping = get_industry_mapping()
    if not industry_mapping:
        print("업종 구조를 가져오지 못해 종료합니다.")
        return []

    # 💡 깃허브 최적화: 쓰레드를 나누지 않고 하나의 세션(연결망)을 끝까지 유지하여 통신 속도를 높입니다.
    session = requests.Session()
    session.verify = False

    main_url = "https://www.work.go.kr/jobyoung/smallGiants/corpInfoSrchList.do?coGbCd=small"
    post_url = "https://www.work.go.kr/jobyoung/smallGiants/corpInfoSrchListPost.do"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": main_url,
        "X-Requested-With": "XMLHttpRequest"
    }

    csrf_token = get_csrf_token(session, main_url)

    categories = {
        "100": "일자리 친화", "200": "기술력 우수", "300": "재무건전성",
        "400": "글로벌역량", "500": "지역선도기업", "600": "사회적가치", "700": "신청 강소기업"
    }
    
    final_data = []

    print("🚀 2단계: 깃허브 액션(Single-Thread) 최적화 모드로 순차 수집을 시작합니다.")

    for code, cat_name in categories.items():
        print(f"\n▶ [{cat_name}] 카테고리 수집 중...")
        
        for ind1_code, ind1_info in industry_mapping.items():
            
            check_1st = {
                "pageIndex": "1", "coGbCd": "small",
                "smlgntCoClcd1": code, 
                "superIndTpCd": ind1_code,
                "indTpCd": ind1_code, 
                "resultCnt": "10", "_csrf": csrf_token
            }
            try:
                res1 = session.post(post_url, data=check_1st, headers=headers)
                soup1 = BeautifulSoup(res1.text, 'html.parser')
                cnt_tag = soup1.select_one('p.count strong.font-orange')
                
                if not cnt_tag or int(cnt_tag.get_text(strip=True).replace(',', '')) == 0:
                    continue 

                for ind2 in ind1_info["sub"]:
                    ind_tp_cd_combined = ind1_code + ind2['code']
                    
                    check_2nd = {
                        "pageIndex": "1", "coGbCd": "small",
                        "smlgntCoClcd1": code, 
                        "superIndTpCd": ind1_code, 
                        "subIndTpCd": ind2['code'],
                        "indTpCd": ind_tp_cd_combined,
                        "resultCnt": "50", "_csrf": csrf_token
                    }
                    res2 = session.post(post_url, data=check_2nd, headers=headers)
                    soup2 = BeautifulSoup(res2.text, 'html.parser')
                    cnt2_tag = soup2.select_one('p.count strong.font-orange')
                    
                    if not cnt2_tag: continue
                    total2 = int(cnt2_tag.get_text(strip=True).replace(',', ''))
                    if total2 == 0: continue
                    
                    total_pages = math.ceil(total2 / 50)
                    
                    for page in range(1, total_pages + 1):
                        payload = check_2nd.copy()
                        payload["pageIndex"] = str(page)
                        payload["sortField"] = "busiNm"
                        payload["sortOrderBy"] = "ASC"

                        res3 = session.post(post_url, data=payload, headers=headers)
                        sub_soup = BeautifulSoup(res3.text, 'html.parser')
                        rows = sub_soup.select('table.board-list > tbody > tr')

                        for row in rows:
                            cols = row.find_all('td')
                            if len(cols) < 5: continue
                            
                            a_tag = cols[0].select_one('a')
                            corp_name = a_tag.get_text(strip=True) if a_tag else ""
                            
                            info_td = cols[2]
                            ems = info_td.select('em')
                            
                            raw_emp = ems[1].get_text(strip=True) if len(ems) > 1 else ""
                            clean_emp = re.sub(r'[^0-9]', '', raw_emp) 
                            emp_count = int(clean_emp) if clean_emp else "" 
                            
                            final_data.append({
                                "1차_분류전체": cat_name,
                                "2차_분류전체": cols[1].get_text(strip=True),
                                "기업명": corp_name,
                                "1차_업종": ind1_info['name'],   
                                "2차_업종": ind2['name'],        
                                "규모": ems[0].get_text(strip=True) if len(ems) > 0 else "",
                                "근로자수": emp_count,           
                                "소재지": cols[3].get_text(strip=True),
                                "관심기업": cols[4].select_one('p').get_text(strip=True).replace('건', '') if cols[4].select_one('p') else ""
                            })
                        
                        time.sleep(0.05) # 깃허브 환경에 맞게 딜레이 최소화
                        
                # 💡 깃허브 액션 로그창이 터지지 않도록 1차 업종이 끝날 때마다만 출력합니다.
                print(f"   ㄴ [{ind1_info['name']}] 하위 수집 완료 (현재 누적: {len(final_data)}건)")
                
            except Exception as e:
                print(f"   ❌ [{ind1_info['name']}] 수집 중 에러 발생: {e}")
                pass
                
    return final_data

if __name__ == "__main__":
    start_time = time.time()
    
    results = scrape_worknet_optimized()
    
    if results:
        df = pd.DataFrame(results)
        df = df.drop_duplicates(subset=['기업명', '소재지'], keep='first')
        df.to_csv("워크넷_강소기업_완성본.csv", index=False, encoding="utf-8-sig")
        
        end_time = time.time()
        elapsed_time = end_time - start_time
        mins = int(elapsed_time // 60)
        secs = int(elapsed_time % 60)
        
        print(f"\n🎉 깃허브 환경 수집 완료! 총 {len(df)}건 확보.")
        print(f"⏱️ 총 소요 시간: {mins}분 {secs}초")
