import time
import requests
import re
import traceback
from bs4 import BeautifulSoup
from typing import List, Dict
from tqdm import tqdm
import urllib.parse
from datetime import datetime, timedelta

from crawlers.naver.base import NaverCrawler
from services.retry import backoff_with_db_logging


class NewsCrawler(NaverCrawler):
    def __init__(self, period: int = 365):
        super().__init__("NAVER_NEWS", period)

    def _fetch_data(self, comp_name: str, ceo_name: str) -> List[Dict]:
        all_news = []
        start = 1
        max_pages = 100

        while start <= max_pages * 10:
            try:
                search_url = self._get_search_url(comp_name, ceo_name, start)
                tqdm.write(f" 페이지 {start//10 + 1} 수집 중")

                # URL 목록 수집
                news_urls = self._get_news_url_list(search_url)

                # 결과 없으면 종료
                if not news_urls:
                    break

                # 뉴스 본문 수집
                news_articles = self._get_news_articles(news_urls, comp_name)
                all_news.extend(news_articles)

                start += 10
                time.sleep(1)

            except Exception as e:
                tqdm.write(f" 페이지 {start//10 + 1} 수집 실패 : {e}")
                break

        return all_news

    def _get_search_url(self, comp_name: str, ceo_name: str, start: int) -> str:
        """검색 URL 생성"""
        BASE_URL = "https://search.naver.com/search.naver?"

        end_date = datetime.today().strftime("%Y.%m.%d")
        start_date = (datetime.today() - timedelta(days=self.period)).strftime("%Y.%m.%d")

        query = urllib.parse.quote(f"{comp_name} | {ceo_name}")

        search_url = (
            f"{BASE_URL}"
            f"where=news&"
            f"query={query}&"
            f"sort=1&"  # 최신순
            f"pd=3&"  # 기간 설정
            f"ds={start_date}&"
            f"de={end_date}&"
            f"qdt=0&"
            f"start={start}"
        )

        return search_url

    @backoff_with_db_logging(max_retries=5, base_delay=2, data_type="NAVER_NEWS")
    def _safe_get(self, url: str, timeout: int = 10) -> requests.Response:
        """재시도 포함 HTTP GET 요청"""
        return requests.get(url, timeout=timeout)

    def _get_news_url_list(self, search_url: str) -> List[str]:
        """검색 결과에서 네이버 뉴스 URL 목록 추출"""
        try:
            response = self._safe_get(search_url, timeout=10)
            soup = BeautifulSoup(response.text, "html.parser")
            time.sleep(2)

            news_list = soup.find("div", class_="group_news")

            if not news_list:
                return []

            a_tags = news_list.find_all("a", string="네이버뉴스")
            news_urls = [a["href"] for a in a_tags if a.has_attr("href")]

            return news_urls

        except Exception as e:
            tqdm.write(f" URL 목록 수집 실패 : {e}")
            return []

    def _get_news_articles(self, news_urls: List[str], comp_name: str) -> List[Dict]:
        """뉴스 URL 목록에서 본문 추출"""
        articles = []

        for url in news_urls:
            try:
                # URL 타입에 따라 다른 파서 사용
                if "n.news.naver.com" in url:
                    article = self._parse_general_news(url)
                elif "esports" in url:
                    article = self._parse_esports_news(url)
                else:
                    article = self._parse_sports_enter_news(url)

                if not article:
                    continue

                # 본문에 회사명 포함 여부 확인
                if comp_name in article.get('NewsContent', ''):
                    articles.append(article)

                time.sleep(1)  # Rate limiting

            except Exception as e:
                tqdm.write(f" 뉴스 파싱 실패 ({url}): {e}")
                continue

        return articles

    def _parse_general_news(self, url: str) -> Dict:
        """일반 뉴스 파싱"""
        try:
            response = self._safe_get(url)
            soup = BeautifulSoup(response.text, "html.parser")

            # 제목
            title_tag = soup.find("h2", id="title_area")
            title = title_tag.get_text().strip() if title_tag else None

            # 언론사
            media_tag = soup.select_one("a.media_end_head_top_logo img[alt]")
            media = media_tag["alt"] if media_tag else None

            # 기자명
            reporter_tags = soup.find_all("em", class_="media_journalistcard_summary_name_text")
            if reporter_tags:
                reporter = [tag.get_text().replace("기자", "").strip() for tag in reporter_tags]
                if len(reporter) == 1:
                    reporter = reporter[0]
            else:
                reporter = None

            # 작성일
            date_tag = soup.select_one("span._ARTICLE_DATE_TIME")
            date = date_tag.get_text().strip() if date_tag else None

            # 날짜 포맷 변환
            if date:
                date = self._format_date(date)

            # 본문
            article_tag = soup.find("article", id="dic_area")
            article = article_tag.get_text().strip() if article_tag else None

            # 기자명 보완 (본문에서 추출)
            if not reporter and article:
                reporter = self._extract_reporter_from_content(article)

            return {
                "NewsTitle": title,
                "PressName": media,
                "JstName": reporter,
                "NewsDate": date,
                "NewsContent": article,
                "UrlLink": url,
            }

        except Exception as e:
            self.repository.log_error(
                location="NewsCrawler._parse_general_news",
                data_type=self.data_type,
                message=f"뉴스 파싱 실패 ({url}): {e}",
                detail=traceback.format_exc()
            )
            return None

    def _parse_esports_news(self, url: str) -> Dict:
        """e스포츠 뉴스 파싱"""
        try:
            BASE_URL = "https://m.sports.naver.com"
            response = self._safe_get(url)
            soup = BeautifulSoup(response.text, "html.parser")

            # 리다이렉트 URL 확인
            meta_tag = soup.find("meta", id="__next-page-redirect")
            if meta_tag and 'content' in meta_tag.attrs:
                content_value = meta_tag['content']
                if "url=" in content_value:
                    extra_url = content_value.split("url=")[1]
                    response = self._safe_get(BASE_URL + extra_url)
                    soup = BeautifulSoup(response.text, "html.parser")

            # 제목
            title_tag = soup.find("h2", class_="ArticleHead_article_title__qh8GV")
            title = title_tag.get_text().strip() if title_tag else None

            # 언론사
            media_tag = soup.find("em", class_="JournalistCard_press_name__s3Eup")
            media = media_tag.get_text().strip() if media_tag else None

            # 기자명
            reporter_tags = soup.find_all("em", class_="JournalistCard_name__0ZSAO")
            if reporter_tags:
                reporter = [tag.get_text().replace("기자", "").strip() for tag in reporter_tags]
                if len(reporter) == 1:
                    reporter = reporter[0]
            else:
                reporter = None

            # 작성일
            date_tag = soup.find("em", class_="date")
            date = date_tag.get_text().strip() if date_tag else None
            if date:
                date = self._format_date(date)

            # 본문
            article_tag = soup.find("div", class_="_article_content")
            article = article_tag.get_text().strip() if article_tag else None

            return {
                "NewsTitle": title,
                "PressName": media,
                "JstName": reporter,
                "NewsDate": date,
                "NewsContent": article,
                "UrlLink": url,
            }

        except Exception as e:
            self.repository.log_error(
                location="NewsCrawler._parse_esports_news",
                data_type=self.data_type,
                message=f"e스포츠 뉴스 파싱 실패 ({url}): {e}",
                detail=traceback.format_exc()
            )
            return None

    def _parse_sports_enter_news(self, url: str) -> Dict:
        """스포츠/엔터 뉴스 파싱"""
        try:
            response = self._safe_get(url)
            soup = BeautifulSoup(response.text, "html.parser")

            # 제목
            title_tag = soup.find("h2", class_="ArticleHead_article_title__qh8GV")
            title = title_tag.get_text().strip() if title_tag else None

            # 언론사
            media_tag = soup.find("em", class_="JournalistCard_press_name__s3Eup")
            if media_tag:
                media = media_tag.get_text().strip()
            else:
                # Copyright에서 추출 시도
                media_tag = soup.find("div", class_="Copyright_article_copyright__vN4Pg")
                if media_tag:
                    text = media_tag.get_text().strip()
                    m = re.search(r'ⓒ\s*([가-힣A-Za-z0-9·&()\s-]+?)\.', text)
                    media = m.group(1).strip() if m else None
                else:
                    media = None

            # 기자명
            reporter_tags = soup.find_all("em", class_="JournalistCard_name__0ZSAO")
            if reporter_tags:
                reporter = [tag.get_text().replace("기자", "").strip() for tag in reporter_tags]
                if len(reporter) == 1:
                    reporter = reporter[0]
            else:
                reporter = None

            # 작성일
            date_tag = soup.find("em", class_="date")
            date = date_tag.get_text().strip() if date_tag else None
            if date:
                date = self._format_date(date)

            # 본문
            article_tag = soup.find("div", class_="_article_content")
            article = article_tag.get_text().strip() if article_tag else None

            # 기자명 보완
            if not reporter and article:
                reporter = self._extract_reporter_from_content(article)

            return {
                "NewsTitle": title,
                "PressName": media,
                "JstName": reporter,
                "NewsDate": date,
                "NewsContent": article,
                "UrlLink": url,
            }

        except Exception as e:
            self.repository.log_error(
                location="NewsCrawler._parse_sports_enter_news",
                data_type=self.data_type,
                message=f"스포츠/엔터 뉴스 파싱 실패 ({url}): {e}",
                detail=traceback.format_exc()
            )
            return None

    def _format_date(self, date_str: str) -> str:
        """날짜 형식 변환"""
        try:
            # '오전'/'오후' → 'AM'/'PM' 변환
            s = date_str.strip()
            s = s.replace("오전", "AM").replace("오후", "PM")

            # 파싱 시도
            formats = ["%Y.%m.%d. %p %I:%M", "%Y.%m.%d %p %I:%M"]
            for fmt in formats:
                try:
                    dt = datetime.strptime(s, fmt)
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    continue

            raise ValueError(f"지원하지 않는 날짜 형식: {date_str}")

        except Exception as e:
            tqdm.write(f" 날짜 변환 실패 ({date_str}): {e}")
            return None

    def _extract_reporter_from_content(self, content: str) -> str:
        """본문에서 기자명 추출"""
        try:
            # "XXX 기자" 패턴 추출
            pattern = re.compile(r'([가-힣·]{2,30})\s*기자\b')
            matches = pattern.findall(content)

            if matches:
                # 마지막 매치 반환 (보통 기사 끝에 위치)
                return matches[-1].strip()

            return None

        except Exception:
            return None


def main():
    """메인 실행 함수"""
    crawler = NewsCrawler(period=365)
    crawler.run()


if __name__ == "__main__":
    main()