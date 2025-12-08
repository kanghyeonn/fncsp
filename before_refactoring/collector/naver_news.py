import time
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import urllib.parse
import re
from before_refactoring.db.es import get_es_conn, insert_naver_news
import functools
from datetime import datetime, timedelta

try:
    from before_refactoring.collector.alter import send_naver_alert
except ImportError:
    print("collector.alter 모듈을 찾을 수 없습니다.")

DATA_TYPE = "naver_news"
PERIOD = 365


# -----------------------------------------------------
# 날짜 변환 함수
# -----------------------------------------------------
def format_date(date: str) -> str:
    s = date.strip()
    # '오전'/'오후' -> 'AM'/'PM' 변환
    s = s.replace("오전", "AM").replace("오후", "PM")
    # 경우에 따라 날짜 뒤에 '.'이 있거나 없을 수 있으니 둘 다 시도
    formats = ["%Y.%m.%d. %p %I:%M", "%Y.%m.%d %p %I:%M"]
    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    # 실패하면 명확한 에러 메시지
    raise ValueError(f"지원하지 않는 형식입니다: {date!r}")


# -----------------------------------------------------
# backoff 함수
# -----------------------------------------------------
def backoff_with_db_logging(max_retries=5, base_delay=1, data_type=None):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = base_delay

            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)

                except Exception as e:
                    error_msg = f"[{func.__name__}] attempt={attempt}, error={str(e)}"

                    conn = get_connection()
                    # 1) DB 에러 로그 기록
                    if data_type:
                        try:
                            error_detail = traceback.format_exc()
                            insert_error_log("Naver News Backoff", data_type, error_msg, error_detail)
                        except Exception as log_error:
                            print(f"Error writing error_log: {log_error}")

                    # 마지막 시도 실패 → raise
                    if attempt == max_retries:
                        raise

                    # 백오프 (지수 증가)
                    time.sleep(delay)
                    delay *= 2

        return wrapper

    return decorator


@backoff_with_db_logging(
    max_retries=10,
    base_delay=3,
    data_type="NAVER_NEWS",
)
def safe_get(url, timeout=10):
    return requests.get(url, timeout=timeout)


# 조건을 걸어서 검색했을때 나오는 뉴스리스트 url
# base_url : root url, comp_name : 기업명, ceo_name : 대표자명, period : 검색기간(오늘부터 몇일 전까지의 뉴스를 검색할 것인지)
# 예시 return값 : https://search.naver.com/search.naver?where=news&query=벡스인텔리전스&pd=3&ds=2024.10.15&de=2025.10.15
def get_search_url(comp_name: str, ceo_name: str, period: int, start: int) -> str:
    BASE_URL = "https://search.naver.com/search.naver?"
    end_date = datetime.today().strftime("%Y.%m.%d")
    start_date = (datetime.today() - timedelta(days=period)).strftime("%Y.%m.%d")

    query = urllib.parse.quote(f"{comp_name} | {ceo_name}")

    # 조건을 걸어서 나오는 뉴스리스트들이 있는 url 생성
    search_url = (BASE_URL
                  + "where=news&"
                  + f"query={query}&"
                  + "sort=1&"
                  + "pd=3&"
                  + f"ds={start_date}&"
                  + f"de={end_date}&"
                  + "qdt=0&"
                  + f"start={start}"
                  )
    return search_url


# 검색결과로 나온 뉴스 리스트들 중 네이버 뉴스에 올라온 뉴스들만 url 수집
def get_news_url_list(search_url: str) -> list | None:
    """
    1. 웹 페이지에서 뉴스리스트들이 있는 group_news라는 클래스 명을 가지는 블록 선택
    2. group_news 블록에서 네이버 뉴스에 올라와 있는 뉴스들의 url 추출
    3. 추출한 url들 반환
    """
    try:
        response = safe_get(search_url, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")
        time.sleep(2)
        # 뉴스 리스트 선택
        newsList_tag = soup.find("div", class_="group_news")

        if not newsList_tag:
            return None

        # "네이버뉴스"를 텍스트로 가지는 a 태그들 선택
        a_tags = newsList_tag.find_all("a", string="네이버뉴스")

        # 선택한 a태그들에서 url 추출
        news_url_list = [a["href"] for a in a_tags if a.has_attr("href")]

        return news_url_list
    except requests.exceptions.RequestException as e:
        print(f"네트워크 오류 발생 : {e}")
        return []
    except Exception as e:
        print(f"에러 발생 : {e}")
        return []


"""
네이버에 올라온 뉴스 기사들에서 뉴스 타이틀, 뉴스 본문, 뉴스 작성일, 언론사, 뉴스 url을 추출하는 함수들
기본 뉴스 : get_naver_news
e 스포츠 뉴스 : get_e_sports_news
엔터, 스포츠 뉴스 : get_enter_sports_news

뉴스 통합 크롤링 함수 : get_news_article
"""


def get_news_article(news_url_list: list[str], comp_name: str) -> list[dict[str, str]]:
    news_attrs = []
    for url in news_url_list:
        try:
            if "n.news.naver.com" in url:
                news_attr = get_naver_news(url)
            elif "esports" in url:
                news_attr = get_e_sport_news(url)
            else:
                news_attr = get_enter_sports_news(url)

            if news_attr is None:
                continue

            # 이메일 본문 내용 -> 값이 None이면 메일을 보냄
            # body = ""
            # for key in news_attr:
            #     if news_attr[key] == None:
            #         body += (key + " ")

            # if body != "":
            #     body += "\n" + url
            #     try:
            #         # insert_error_log
            #         insert_error_log(f"Don't save {body}", "NAVER_NEWS", body + "가 저장되지 않음", "")
            #     except Exception as e:
            #         print(f"알림 전송 실패 : {e}")

            if comp_name in news_attr['NewsContent']:
                news_attrs.append(news_attr)

            time.sleep(2)
        except Exception as e:
            # insert_error_log
            error_log = f"뉴스 처리 중 에러 발생 {url} : {e}"
            insert_error_log("Handling New Error", "NAVER_NEWS", error_log, "")
            continue

    return news_attrs


# 기본 뉴스 크롤링
def get_naver_news(news_url: str) -> dict[str, str | list[str] | None]:
    try:
        response = safe_get(news_url)
        soup = BeautifulSoup(response.text, "html.parser")

        # 제목
        title_tag = soup.find("h2", id="title_area")
        title = title_tag.get_text().strip() if title_tag else None

        # 언론사명
        media_tag = soup.select_one("a.media_end_head_top_logo img[alt]")  # alt="전자신문"
        if media_tag:
            media = media_tag["alt"] if media_tag else None
        else:
            media_tag = soup.find("p", class_="c_text")
            if media_tag:
                text = media_tag.get_text().strip()
                m = re.search(r'ⓒ\s*([가-힣A-Za-z0-9·&()\s-]+?)\.', text)
                media = m.group(1).strip() if m else None
            else:
                media = None

        # 기자명
        reporter_tag = soup.find_all("em", class_="media_journalistcard_summary_name_text")
        if len(reporter_tag) > 1:
            reporter = [tag.get_text().replace("기자", "").strip() for tag in reporter_tag]
        elif len(reporter_tag) == 1:
            reporter = reporter_tag[0].get_text().replace("기자", "").strip()
        else:
            reporter_tag = soup.find("span", class_="byline_s")
            if reporter_tag:
                reporter = reporter_tag.get_text().strip()
                reporter = re.sub(r'[^가-힣\s]', '', reporter)
                reporter = reporter.replace("기자", "").strip()
            else:
                reporter = None

        # 작성일
        date_tag = soup.select_one("span._ARTICLE_DATE_TIME")
        date = date_tag.get_text().strip() if date_tag else None

        # elasticsearch format 타입에 맞게 변환
        if date:
            date = format_date(date)
        else:
            date = None

        # 본문
        article_tag = soup.find("article", id="dic_area")
        article = article_tag.get_text().strip() if article_tag else None

        # 기자명 보완 코드
        if reporter is None and article:
            pattern = re.compile(r'([가-힣·]{2,30})\s*기자\b')
            matches = pattern.findall(article)
            if matches:
                reporter = matches[-1].strip()
            else:
                reporter = None

        return {
            "NewsTitle": title,
            "PressName": media,
            "JstName": reporter,
            "NewsDate": date,
            "NewsContent": article,
            "UrlLink": news_url,
        }
    except Exception as e:
        error_detail = traceback.format_exc()
        insert_error_log("Failed Crawling Naver news", "NAVER_NEWS", f"네이버 뉴스 크롤링 실패 - {news_url}: {e}", error_detail)
        return None


# e 스포츠 뉴스 크롤링
def get_e_sport_news(news_url: str) -> dict[str, str]:
    try:
        BASE_URL = "https://m.sports.naver.com"
        extra_url = ""
        response = safe_get(news_url)
        soup = BeautifulSoup(response.text, "html.parser")

        meta_tag = soup.find("meta", id="__next-page-redirect")
        if meta_tag and 'content' in meta_tag.attrs:
            content_value = meta_tag['content']
            if "url" in content_value:
                extra_url = content_value.split("url=")[1]

        if extra_url:
            response = safe_get(BASE_URL + extra_url)
            soup = BeautifulSoup(response.text, "html.parser")
        else:
            response = safe_get(news_url + "?sid3=79e")
            soup = BeautifulSoup(response.text, "html.parser")

        title_tag = soup.find("h2", class_="ArticleHead_article_title__qh8GV")
        title = title_tag.get_text().strip() if title_tag else None

        media_tag = soup.find("em", class_="JournalistCard_press_name__s3Eup")
        media = media_tag.get_text().strip() if media_tag else None

        reporter_tag = soup.find_all("em", class_="JournalistCard_name__0ZSAO")
        if len(reporter_tag) > 1:
            reporter = [tag.get_text().replace("기자", "").strip() for tag in reporter_tag]
        elif len(reporter_tag) == 1:
            reporter = reporter_tag[0].get_text().replace("기자", "").strip()
        else:
            reporter = None

        date_tag = soup.find("em", class_="date")
        date = date_tag.get_text().strip() if date_tag else None

        if date:
            date = format_date(date)
        else:
            date = None

        article_tag = soup.find("div", class_="_article_content")
        article = article_tag.get_text().strip() if article_tag else None

        return {
            "NewsTitle": title,
            "PressName": media,
            "JstName": reporter,
            "NewsDate": date,
            "NewsContent": article,
            "UrlLink": news_url,
        }
    except Exception as e:
        error_detail = traceback.format_exc()
        insert_error_log("Failed Crawling Naver news", "NAVER_NEWS", f"네이버 E 스포츠 크롤링 실패 - {news_url}: {e}",
                         error_detail)
        return None


# 엔터, 스포츠 뉴스 크롤링
def get_enter_sports_news(news_url: str) -> dict[str, str | None]:
    try:
        response = safe_get(news_url)
        soup = BeautifulSoup(response.text, "html.parser")

        title_tag = soup.find("h2", class_="ArticleHead_article_title__qh8GV")
        title = title_tag.get_text().strip() if title_tag else None

        media_tag = soup.find("em", class_="JournalistCard_press_name__s3Eup")
        if media_tag:
            media = media_tag.get_text().strip()
        else:
            media_tag = soup.find("div", class_="Copyright_article_copyright__vN4Pg")
            if media_tag:
                text = media_tag.get_text().strip()
                m = re.search(r'ⓒ\s*([가-힣A-Za-z0-9·&()\s-]+?)\.', text)
                media = m.group(1).strip() if m else None
            else:
                media = None

        reporter_tag = soup.find_all("em", class_="JournalistCard_name__0ZSAO")
        if len(reporter_tag) > 1:
            reporter = [tag.get_text().replace("기자", "").strip() for tag in reporter_tag]
        elif len(reporter_tag) == 1:
            reporter = reporter_tag[0].get_text().replace("기자", "").strip()
        else:
            reporter = None

        date_tag = soup.find("em", class_="date")
        date = date_tag.get_text().strip() if date_tag else None

        if date:
            date = format_date(date)
        else:
            date = None

        article_tag = soup.find("div", class_="_article_content")
        article = article_tag.get_text().strip() if article_tag else None

        if reporter is None and article:
            pattern = re.compile(r'([가-힣·]{2,30})\s*기자\b')
            matches = pattern.findall(article)
            if matches:
                reporter = matches[-1].strip()
            else:
                reporter = None

        return {
            "NewsTitle": title,
            "PressName": media,
            "JstName": reporter,
            "NewsDate": date,
            "NewsContent": article,
            "UrlLink": news_url,
        }
    except Exception as e:
        error_detail = traceback.format_exc()
        insert_error_log("Failed Crawling Naver news", "NAVER_NEWS", f"네이버 스포츠/엔터 뉴스 크롤링 실패 - {news_url}: {e}",
                         error_detail)
        return None


# 크롤링 메인 로직 함수
def main():
    news_data = []
    es = None

    try:
        # 1. 필수 리소스 연결
        try:
            es = get_es_conn()
        except Exception as e:
            error_detail = traceback.format_exc()
            insert_error_log("Elasticsearch connection", "NAVER_NEWS", f"Elasticsearch 연결 실패 : {e}", error_detail)
            raise

        try:
            companies = get_cmp_list("NAVER_NEWS")
        except Exception as e:
            insert_error_log("Get Cmp List", "NAVER_NEWS", f"기업 목록 조회 실패: {e}", "")
            raise

        # 2. 기업별 처리
        for idx, company in enumerate(tqdm(companies, desc='기업 뉴스 수집', unit="개"), 1):
            biz_no = ""
            comp_name = ""
            now = datetime.now()

            try:
                # (주), (사) 같은 법인 표시를 제거한 기업명
                comp_name = company["CMP_NM"]
                clean_comp_name = re.sub(r'\(.*?\)', '', comp_name)

                # "외 1명"을 제거한 대표자명
                ceo_name = company["CEO_NM"]
                if ceo_name:
                    clean_ceo_name = re.sub(r'외\s*\d+명', '', ceo_name)
                else:
                    # error_log = f"{comp_name} 기업의 대표자 명 누락"
                    # insert_error_log("Does not exist Ceo_nm","NAVER_NEWS", error_log, "")
                    # time.sleep(1)
                    # continue
                    clean_ceo_name = ""

                # 시작번호
                start = 1
                news = []
                num = 0
                while True:  # 들여쓰기 수정
                    search_url = get_search_url(clean_comp_name, clean_ceo_name, PERIOD, start)
                    tqdm.write(f"\nsearch_url: {search_url}")
                    news_url_list = get_news_url_list(search_url)
                    if news_url_list is None or len(news_url_list) == 0:
                        break
                    else:
                        num += len(news_url_list)
                        news_attrs = get_news_article(news_url_list, clean_comp_name)
                        news_data.extend(news_attrs)
                        news.extend(news_attrs)
                        start += 10

                    if start > 1000:
                        break

                tqdm.write(f"\n뉴스 개수 : {num}")  # 들여쓰기 수정 (try 블록 안)

                # ES 적재 시 예외 처리 추가
                try:
                    insert_naver_news(es, news, company["BIZ_NO"])
                    insert_check_log(company["BIZ_NO"], "NAVER_NEWS", now)
                    insert_cmp_data_log(company["BIZ_NO"], "NAVER_NEWS", len(news), now)
                except Exception as e:
                    error_detail = traceback.format_exc()
                    insert_error_log("Insert data", "NAVER_NEWS", f"데이터 삽입 실패({company['BIZ_NO']}) : {e}", error_detail)

                time.sleep(1)

            except Exception as e:  # 들여쓰기 수정 (for 루프와 같은 레벨)
                error_log = f"{comp_name}({biz_no}) 기업 처리중 오류 발생 : {e}"
                print(error_log)
                insert_error_log("Process Company", 'NAVER_NEWS', error_log, "")

    finally:
        if es:
            es.close()

    return news_data


if __name__ == "__main__":
    email = os.getenv("EMAIL")
    password = os.getenv("PASSWORD")
    exit_message = "N/A"
    try:
        main()
        exit_message = "프로그램 정상 종료"
    except Exception as e:
        exit_message = f"프로그램 예외 종료 : {e}"
    finally:
        send_naver_alert(email, email, password, f"NAVER_NEWS 프로그램 종료됨: {exit_message}")