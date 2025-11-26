import re
from selenium.webdriver.common.by import By
import undetected_chromedriver as uc
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webdriver import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from httpcore import TimeoutException
from datetime import datetime

"""
kipris_extractor에 사용되는 기본 유틸 함수들
"""
def clean(s: str) -> str | None:
    if not s or not s.strip():
        return None
    return " ".join(s.split()).strip()

def parse_name_and_id(text: str):
    # "이름\n(1234567890)" 또는 "이름(123...)" 형태 처리
    t = clean(text)
    m = re.search(r"\(([^)]+)\)\s*$", t)
    if m:
        sid = clean(m.group(1))
        name = clean(t[:m.start()])
        return name, sid
    return t, ""

def text_without_em(td_el) -> str:
    td_text = clean(td_el.get_attribute("innerText") or td_el.text)
    try:
        em = td_el.find_element(By.CSS_SELECTOR, "em.th")
        em_text = clean(em.get_attribute("innerText") or em.text)
        if em_text and td_text.startswith(em_text):
            return clean(td_text[len(em_text):])
    except Exception:
        pass
    return td_text

# 자바스크립트로 클릭하는 함수
def js_click(driver:WebDriver, el:WebElement):
    driver.execute_script("arguments[0].click();", el)

# 제목 정규화
def normalize_title(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"\s+", "", s)                    # 모든 공백 제거
    s = re.sub(r"[^0-9A-Za-z가-힣/]", "", s)      # 특수문자 제거(한글/영문/숫자/슬래시)
    return s.lower()

# 제목 포함 여부
def title_contains(norm_title: str, *keywords: str) -> bool:
    return any(k in norm_title for k in [normalize_title(k) for k in keywords])

# 각 상세정보 section에서 title을 찾아 반환
def get_section_title(section: WebElement) -> str | None:
    """section 내부에서 h4 또는 h5 제목을 찾아 정규화 후 반환"""
    try:
        elem = section.find_element(By.CSS_SELECTOR, ".title-box h4.title, .title-box h5.title")
        title = (elem.text or "").strip()
        return normalize_title(title) if title else None
    except Exception:
        return None

"""
웹 브라우저 조작 유틸 함수들
"""
# kipris 접속 함수
def open_browser(category: str) -> WebDriver:
    opts = uc.ChromeOptions()
    # opts.add_argument("--headless=new")

    """
    # 우분투 환경
    opts.add_argument("--no-sandbox")  # root 환경 / Docker 환경에서 필수
    opts.add_argument("--disable-dev-shm-usage")  # /dev/shm 공간 부족 방지
    opts.add_argument("--disable-gpu")  # GPU 없는 서버에서 필수
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--remote-debugging-port=9222")  # 충돌 방지
    opts.add_argument("--single-process")  # 일부 환경에서 안정성 증가
    opts.add_argument("--disable-infobars")
    opts.add_argument("--disable-extensions")
    """

    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-popup-blocking")

    driver = uc.Chrome(options=opts)
    driver.get(f"https://www.kipris.or.kr/khome/search/searchResult.do?tab={category}")

    return driver

# 회사명으로 특허검색 (특허,실용신안)
def search_by_ap(driver:WebDriver,btn:str, comp_name:str):
    try:
        # 지식재산정보 상세검색창 선택
        modal_search_detail = driver.find_element(By.ID, "modalSearchDetail")

        # 지식재산정보 상세검색창에서 실용신안 버튼 클릭
        label = modal_search_detail.find_element(By.CSS_SELECTOR, f'label[for="{btn}"]')
        label.click()

        search_box = modal_search_detail.find_element(By.ID, "sd01_g04_text_01")
        driver.execute_script("arguments[0].value = arguments[1];", search_box, comp_name)

        # 검색하기 버튼 클릭
        button = driver.find_element(By.CSS_SELECTOR, "button.btn-search[data-lang-id='adsr.search']")
        button.click()
    except Exception as e:
        raise

# 출원번호로 정렬하는 함수
def sort_by_application_an(driver:WebDriver):
    try:
        # select태그 옵션을 자바스크립트로 변경
        driver.execute_script("""
        const sel = document.getElementById('sortCondition01');
        const options = sel.options;
        for (let i = 0; i < options.length; i++) {
            if (options[i].text === '출원번호') {  // 여기서 원하는 텍스트 지정
                sel.selectedIndex = i;
                sel.dispatchEvent(new Event('change'));
                break;
            }
        }
        """)

        # 함수 실행 
        driver.execute_script("optionSearch();")
    except Exception as e:
        raise

# 건수를 구하는 함수
def get_total_num(driver:WebDriver, category:str) -> int:
    wait = WebDriverWait(driver, 10)

    """
    특허, 실용신안 : patent
    디자인 : design
    상표 : trademark
    심판 : judgement
    기타문헌 : etc
    """
    try:
        wait.until(lambda d: d.find_element(By.ID, f'{category}TotalCount').text.strip().isdigit())
        nums = int(driver.find_element(By.ID, f'{category}TotalCount').text)
    except Exception as e:
        raise

    return nums

# 다음 페이지로 이동
def go_next_page(driver: WebDriver):
    try:
        wait = WebDriverWait(driver, 10)
        old_card = driver.find_element(By.CSS_SELECTOR, "article,result-item")
        btn_next = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, '.btn-navi.next'))
        )
        btn_next.click()

        wait.until(EC.staleness_of(old_card))

        wait.until(
            EC.presence_of_element_located((By.ID, 'resultSection'))
        )
        wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.link.under'))
        )
    except Exception as e:
        raise

# 검색 결과가 있으면 결과 리스트를 반환하는 함수
def has_result(driver:WebDriver) -> tuple[bool,list]:
    wait = WebDriverWait(driver, 10)
    try:
        result_section = wait.until(
            EC.presence_of_element_located((By.ID, 'resultSection'))
        )
        wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "article.result-item"))
        )
        result_cards = result_section.find_elements(By.CSS_SELECTOR, "article.result-item")
        if result_cards:
            return True, result_cards
        else:
            return False, []
    except Exception:
        raise

# 결과 리스트에서 하나의 결과를 클릭해세 상세 페이지를 여는 함수
def open_card(driver:WebDriver, card:WebElement):
    try:
        wait = WebDriverWait(driver, 10)
        wait.until(EC.element_to_be_clickable(card))
        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", card)
        btn_link = card.find_element(By.CSS_SELECTOR, "button.link.under")

        try:
            btn_link.click()
        except Exception:
            try:
                js_click(driver, btn_link)
            except Exception as e1:
                print("open_card e1 : ", e1)
                return
        # 상세결과 창이 열릴때까지 대기
        wait = WebDriverWait(driver, 10)
        wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#mainResultDetail .tab-section-01"))
        )
    except Exception as e:
        print("e : ", e)
        raise

class DataInsertError(Exception):
    pass

class DuplicateError(Exception):
    pass