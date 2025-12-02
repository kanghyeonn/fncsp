import re
import time
from typing import Tuple, List
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc

class WebDriverManger:

    @staticmethod
    def create_driver(category: str) -> WebDriver:
        opts = uc.ChromeOptions()

        # 헤드리스 모드
        opts.add_argument("--headless=new")

        # 우분투/Docker 환경 대응
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--disable-software-rasterizer")
        opts.add_argument("--remote-debugging-port=9222")
        opts.add_argument("--single-process")

        # 기타 옵션
        opts.add_argument("--start-maximized")
        opts.add_argument("--disable-popup-blocking")
        opts.add_argument("--disable-infobars")
        opts.add_argument("--disable-extensions")

        # User-Agent 설정 (봇 감지 회피)
        opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

        try:
            driver = uc.Chrome(options=opts)
            driver.implicitly_wait(10)

            # KIPRIS 사이트 접속
            url = f"https://www.kipris.or.kr/khome/search/searchResult.do?tab={category}"
            driver.get(url)
            time.sleep(2)  # 페이지 로딩 대기

            print(f"WebDriver 생성 완료: {category}")
            return driver

        except Exception as e:
            print(f"WebDriver 생성 실패: {e}")
            raise


def search_by_ap(
        driver: WebDriver,
        search_id: str,
        search_value: str
) -> None:
    try:
        # 상세검색 모달 찾기
        modal = driver.find_element(By.ID, "modalSearchDetail")

        # 검색 타입 선택 (라디오 버튼)
        if search_id.startswith("sd01_ck"):
            label = modal.find_element(By.CSS_SELECTOR, f'label[for="{search_id}"]')
            label.click()
            time.sleep(0.5)

            # 검색어 입력 (출원인 필드)
            search_box = modal.find_element(By.ID, "sd01_g04_text_01")
        else:
            # 직접 검색 필드 ID 사용
            search_box = modal.find_element(By.ID, search_id)

        # JavaScript로 값 설정 (일반 입력보다 안정적)
        driver.execute_script(
            "arguments[0].value = arguments[1];",
            search_box,
            search_value
        )

        # 검색 버튼 클릭
        search_btn = driver.find_element(
            By.CSS_SELECTOR,
            "button.btn-search[data-lang-id='adsr.search']"
        )
        search_btn.click()

        # 결과 로딩 대기
        time.sleep(2)

        print(f"검색 완료: {search_value}")

    except Exception as e:
        print(f"검색 실패: {e}")
        raise

def sort_by_application_an(driver: WebDriver) -> None:
    try:
        # JavaScript로 정렬 옵션 변경
        driver.execute_script("""
            const sel = document.getElementById('sortCondition01');
            if (!sel) return;

            const options = sel.options;
            for (let i = 0; i < options.length; i++) {
                if (options[i].text === '출원번호') {
                    sel.selectedIndex = i;
                    sel.dispatchEvent(new Event('change'));
                    break;
                }
            }
        """)

        # 정렬 함수 실행
        driver.execute_script("optionSearch();")

        # 정렬 완료 대기
        time.sleep(1)

        print("출원번호 순 정렬 완료")

    except Exception as e:
        print(f"정렬 실패: {e}")
        raise

def get_total_num(driver: WebDriver, category: str) -> int:
    try:
        wait = WebDriverWait(driver, 10)

        # 개수가 숫자로 표시될 때까지 대기
        wait.until(
            lambda d: d.find_element(
                By.ID,
                f'{category}TotalCount'
            ).text.strip().replace(',', '').isdigit()
        )

        # 개수 추출
        count_element = driver.find_element(By.ID, f'{category}TotalCount')
        count_text = count_element.text.strip().replace(',', '')
        total = int(count_text)

        print(f"검색 결과: {total:,}건")
        return total

    except Exception as e:
        print(f"개수 조회 실패: {e}")
        return 0

def has_result(driver: WebDriver) -> Tuple[bool, List[WebElement]]:
    try:
        wait = WebDriverWait(driver, 10)

        # 결과 섹션 로딩 대기
        result_section = wait.until(
            EC.presence_of_element_located((By.ID, 'resultSection'))
        )

        # 카드 요소 대기
        wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "article.result-item"))
        )

        # 모든 카드 수집
        cards = result_section.find_elements(By.CSS_SELECTOR, "article.result-item")

        if cards:
            print(f"결과 카드 {len(cards)}개 발견")
            return True, cards
        else:
            print("결과 없음")
            return False, []

    except Exception as e:
        print(f"결과 확인 실패: {e}")
        return False, []

def open_card(driver: WebDriver, card: WebElement) -> None:
    try:
        wait = WebDriverWait(driver, 10)

        # 카드가 클릭 가능할 때까지 대기
        wait.until(EC.element_to_be_clickable(card))

        # 카드를 화면 중앙으로 스크롤
        driver.execute_script(
            "arguments[0].scrollIntoView({block: 'center'});",
            card
        )
        time.sleep(0.5)

        # 링크 버튼 찾기
        btn_link = card.find_element(By.CSS_SELECTOR, "button.link.under")

        # 클릭 시도 (일반 클릭 -> JavaScript 클릭)
        try:
            btn_link.click()
        except Exception:
            driver.execute_script("arguments[0].click();", btn_link)

        # 상세 페이지 로딩 대기
        wait.until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "#mainResultDetail .tab-section-01")
            )
        )

        time.sleep(0.5)

    except Exception as e:
        print(f"카드 열기 실패: {e}")
        raise

def go_next_page(driver: WebDriver) -> None:
    try:
        wait = WebDriverWait(driver, 10)

        # 현재 카드 참조 (stale 체크용)
        old_card = driver.find_element(By.CSS_SELECTOR, "article.result-item")

        # 다음 버튼 찾기 및 클릭
        btn_next = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, '.btn-navi.next'))
        )
        btn_next.click()

        # 페이지 전환 대기 (이전 카드가 stale 될 때까지)
        wait.until(EC.staleness_of(old_card))

        # 새 페이지 로딩 대기
        wait.until(
            EC.presence_of_element_located((By.ID, 'resultSection'))
        )
        wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'button.link.under'))
        )

        time.sleep(0.5)
        print("✓ 다음 페이지 이동 완료")

    except Exception as e:
        print(f"✗ 페이지 이동 실패: {e}")
        raise

def clean(text: str) -> str:
    if not text or not text.strip():
        return None

    # 공백 정규화
    cleaned = " ".join(text.split()).strip()
    return cleaned if cleaned else None

def normalize_title(text) -> str:
    if not text:
        return ""

    # 공백 제거
    normalized = re.sub(r"\s+", "", text)

    # 특수문자 제거 (한글/영문/숫자/슬래시만 유지)
    normalized = re.sub(r"[^0-9A-Za-z가-힣/]", "", normalized)

    return normalized.lower()

def title_contains(title: str, *keywords: str) -> bool:
    normalized = normalize_title(title)
    return any(
        normalize_title(keyword) in normalized
        for keyword in keywords
    )

def text_without_em(element: WebElement) -> str:
    text = clean(element.get_attribute("innerText") or element.text)

    try:
        em = element.find_element(By.CSS_SELECTOR, "em.th")
        em_text = clean(em.get_attribute("innerText") or em.text)

        if em_text and text and text.startswith(em_text):
            text = clean(text[len(em_text):])
    except Exception:
        pass

    return text

def parse_name_and_id(text: str) -> Tuple[str, str]:
    text = clean(text)

    # 괄호 안의 ID 추출
    match = re.search(r"\(([^)]+)\)\s*$", text)

    if match:
        sid = clean(match.group(1))
        name = clean(text[:match.start()])
        return name, sid

    return text, ""


def get_section_title(section: WebElement) -> str:
    try:
        elem = section.find_element(
            By.CSS_SELECTOR,
            ".title-box h4.title, .title-box h5.title"
        )
        title = (elem.text or "").strip()
        return normalize_title(title) if title else None
    except Exception:
        return None

def js_click(driver: WebDriver, element: WebElement) -> None:
    driver.execute_script("arguments[0].click();", element)