"""
NTIS 수행기관 정보 크롤러
기관의 R&D 수행 현황 정보 수집
"""
from typing import Dict, Optional
from urllib.parse import urlencode

from crawlers.ntis.base import NtisCrawler


class OrgInfoCrawler(NtisCrawler):
    """NTIS 수행기관 정보 크롤러"""

    def __init__(self):
        super().__init__("NTIS_ORG_INFO")

    def _fetch_api_data(self, comp_name: str, biz_no: str) -> Optional[Dict]:
        """
        수행기관 정보 API 조회

        Args:
            comp_name: 회사명 (사용 안 함)
            biz_no: 사업자번호

        Returns:
            API 응답 데이터
        """
        url = f"{self.BASE_URL}orgRndInfo"

        params = {
            "apprvKey": self.api_key,
            "reqOrgBno": biz_no,
        }

        query = urlencode(params, encoding="utf-8")
        full_url = f"{url}?{query}"

        try:
            response = self._api_request(full_url, {})

            if response.status_code != 200:
                return None

            return self._parse_xml_response(response)

        except Exception as e:
            print(f"API 조회 실패: {e}")
            return None

    def _transform_data(self, raw_data: Dict, biz_no: str) -> Optional[Dict]:
        """
        수행기관 정보 변환 (단일 객체)

        Args:
            raw_data: 원본 API 데이터
            biz_no: 사업자번호

        Returns:
            변환된 기관 정보 (딕셔너리)
        """
        try:
            body = self._safe_get(raw_data, "response", "body")

            if not body:
                return None

            result = {}

            # 기본 정보
            result["orgName"] = body.get("orgName")
            result["orgPageInfo"] = body.get("orgPageInfo")

            # 키워드
            result["rndKorKeyword"] = body.get("rndKorKeword")  # API 오타 그대로
            result["rndEngKeyword"] = body.get("rndEngKeword")  # API 오타 그대로

            # 연구 분야
            result["rndCategory"] = body.get("rndCategory")

            # R&D 현황 리스트
            status_list = []
            rnd_status_raw = body.get("rndStatusList")

            if rnd_status_raw:
                rnd_status_list = self._ensure_list(rnd_status_raw)

                for status in rnd_status_list:
                    status_item = {
                        "year": self._format_date(
                            status.get("year"),
                            "%Y",
                            "%Y"
                        ),
                        "pjtCnt": status.get("pjtCnt"),
                        "rndBudget": status.get("rndBudget"),
                        "govBudget": status.get("govBudget"),
                        "paperCnt": status.get("paperCnt"),
                        "patentCnt": status.get("patentCnt"),
                        "reportCnt": status.get("reportCnt"),
                    }
                    status_list.append(status_item)

            result["rndStatusList"] = status_list if status_list else None

            return result

        except Exception as e:
            print(f"데이터 변환 실패: {e}")
            return None


# ==========================================
# 실행 진입점
# ==========================================

def main():
    """메인 실행 함수"""
    crawler = OrgInfoCrawler()
    crawler.run()


if __name__ == "__main__":
    main()