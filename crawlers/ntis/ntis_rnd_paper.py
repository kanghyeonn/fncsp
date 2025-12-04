"""
NTIS 연구보고서 크롤러
R&D 연구보고서 정보 수집
"""
from typing import List, Dict, Optional
from datetime import datetime
from urllib.parse import urlencode
from tqdm import tqdm

from crawlers.ntis.base import NtisCrawler
from core.exceptions import DuplicateError


class RndPaperCrawler(NtisCrawler):
    """NTIS 연구보고서 크롤러"""

    def __init__(self):
        super().__init__("NTIS_RND_PAPER")

    def _fetch_api_data(self, comp_name: str, biz_no: str) -> Optional[Dict]:
        """
        연구보고서 정보 API 조회

        Args:
            comp_name: 회사명
            biz_no: 사업자번호

        Returns:
            API 응답 데이터
        """
        url = f"{self.BASE_URL}rresearchpdf"

        params = {
            "apprvKey": self.api_key,
            "collection": "researchpdf",
            "searchField": "PB",
            "addQuery": f"PB01={comp_name}",
            "sortdBy": "DATE/DESC",
            "startPosition": 1,
            "displayCnt": 1000,
            "returnType": "json"
        }

        query = urlencode(params, encoding="utf-8")
        full_url = f"{url}?{query}"

        try:
            response = self._api_request(full_url, {})

            if response.status_code != 200:
                return None

            # JSON 응답
            return response.json()

        except Exception as e:
            print(f"API 조회 실패: {e}")
            return None

    def _transform_data(self, raw_data: Dict, biz_no: str) -> Optional[List[Dict]]:
        """
        연구보고서 데이터 변환

        Args:
            raw_data: 원본 API 데이터
            biz_no: 사업자번호

        Returns:
            변환된 보고서 리스트
        """
        try:
            result_set = self._safe_get(raw_data, "RESULT", "RESULTSET")

            if not result_set:
                return None

            hits = result_set.get("HIT", [])
            papers = self._ensure_list(hits)

            if not papers:
                return None

            transformed = []

            for paper in papers:
                # 중복 체크
                research_public_no = paper.get("ResearchPublicNo")

                if research_public_no:
                    is_dup = self.repository.check_duplicate(
                        "ntis_rnd_paper",
                        biz_no,
                        research_public_no
                    )

                    if is_dup:
                        tqdm.write(f" ⚠️  중복 발견: {research_public_no}")
                        raise DuplicateError(f"Duplicate paper: {research_public_no}")

                # 데이터 변환
                item = self._transform_single_paper(paper)
                transformed.append(item)

            return transformed

        except DuplicateError:
            raise
        except Exception as e:
            print(f"데이터 변환 실패: {e}")
            return None

    def _transform_single_paper(self, paper: Dict) -> Dict:
        """단일 보고서 데이터 변환"""
        result = {}

        # 발행 연도
        pub_year = paper.get("PublicationYear")
        result["PublicationYear"] = self._format_date(
            str(pub_year),
            "%Y",
            "%Y"
        ) if pub_year else None

        # 보고서 번호
        result["ResearchPublicNo"] = paper.get("ResearchPublicNo")

        # 발행 기관
        result["PublicationAgency"] = paper.get("PublicationAgency")

        # 제목
        result_title = paper.get("ResultTitle") or {}
        result["ResultTitleKR"] = result_title.get("Korean")
        result["ResultTitleEN"] = result_title.get("English")

        # 요약
        abstract = paper.get("Abstract") or {}
        result["AbstractKR"] = abstract.get("Korean")
        result["AbstractEN"] = abstract.get("English")

        # 키워드
        keyword = paper.get("Keyword") or {}
        result["KeywordKR"] = keyword.get("Korean")
        result["KeywordEN"] = keyword.get("English")

        # 내용
        result["Contents"] = paper.get("Contents")

        # 발행 정보
        result["PublicationCountry"] = paper.get("PublicationCountry")
        result["PublicationLanguage"] = paper.get("PublicationLanguage")

        # 문서 URL
        result["DocUrl"] = paper.get("DocUrl")

        # 프로젝트 정보
        result["ProjectNumber"] = paper.get("ProjectNumber")
        result["ProjectTitle"] = paper.get("ProjectTitle")
        result["LeadAgency"] = paper.get("LeadAgency")
        result["ManagerName"] = paper.get("ManagerName")

        return result


# ==========================================
# 실행 진입점
# ==========================================

def main():
    """메인 실행 함수"""
    crawler = RndPaperCrawler()
    crawler.run()


if __name__ == "__main__":
    main()