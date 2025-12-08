"""
NTIS 과제 정보 크롤러
국가 R&D 과제 정보 수집
"""
from typing import List, Dict, Optional
from datetime import datetime
from urllib.parse import urlencode
from tqdm import tqdm

from crawlers.ntis.base import NtisCrawler, split_semicolon_list
from core.exceptions import DuplicateError


class AssignCrawler(NtisCrawler):
    """NTIS 과제 정보 크롤러"""

    def __init__(self):
        super().__init__("NTIS_ASSIGN")

    def _fetch_api_data(self, comp_name: str, biz_no: str) -> Optional[Dict]:
        """
        과제 정보 API 조회

        Args:
            comp_name: 회사명
            biz_no: 사업자번호

        Returns:
            API 응답 데이터
        """
        url = f"{self.BASE_URL}public_project"

        params = {
            "apprvKey": self.api_key,
            "collection": "project",
            "addQuery": f"PB01={comp_name}",
            "searchRnkn": "DATE/DESC",
            "startPosition": 1,
            "displayCnt": 1000,
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

    def _transform_data(self, raw_data: Dict, biz_no: str) -> Optional[List[Dict]]:
        """
        과제 데이터 변환

        Args:
            raw_data: 원본 API 데이터
            biz_no: 사업자번호

        Returns:
            변환된 과제 리스트
        """
        try:
            result_set = self._safe_get(raw_data, "RESULT", "RESULTSET")

            if not result_set:
                return None

            hits = result_set.get("HIT", [])
            assigns = self._ensure_list(hits)

            if not assigns:
                return None

            transformed = []

            for assign in assigns:
                # 중복 체크
                project_no = assign.get("ProjectNumber")

                if project_no:
                    is_dup = self.repository.check_duplicate(
                        "ntis_assign",
                        biz_no,
                        project_no
                    )

                    if is_dup:
                        tqdm.write(f" ⚠️  중복 발견: {project_no}")
                        raise DuplicateError(f"Duplicate project: {project_no}")

                # 데이터 변환
                item = self._transform_single_assign(assign)
                transformed.append(item)

            return transformed

        except DuplicateError:
            raise
        except Exception as e:
            print(f"데이터 변환 실패: {e}")
            return None

    def _transform_single_assign(self, assign: Dict) -> Dict:
        """단일 과제 데이터 변환"""
        result = {}

        # 기본 정보
        result["ProjectNo"] = assign.get("ProjectNumber")
        result["ProjectNameKR"] = self._safe_get(assign, "ProjectTitle", "Korean")
        result["ProjectNameEN"] = self._safe_get(assign, "ProjectTitle", "English")

        # 관리자 정보
        managers = self._safe_get(assign, "Manager", "Name")
        result["ManagerName"] = split_semicolon_list(managers)

        # 연구자 정보
        researchers = self._safe_get(assign, "Researchers", "Name")
        result["ResearcherName"] = split_semicolon_list(researchers)
        result["ResManCount"] = self._safe_get(assign, "Researchers", "ManCount")
        result["ResWmanCount"] = self._safe_get(assign, "Researchers", "WomanCount")

        # 목표
        result["GoalFull"] = self._safe_get(assign, "Goal", "Full")
        result["GoalTeaser"] = self._safe_get(assign, "Goal", "Teaser")

        # 요약
        result["AbstractFull"] = self._safe_get(assign, "Abstract", "Full")
        result["AbstractTeaser"] = self._safe_get(assign, "Abstract", "Teaser")

        # 기대효과
        result["EffectFull"] = self._safe_get(assign, "Effect", "Full")
        result["EffectTeaser"] = self._safe_get(assign, "Effect", "Teaser")

        # 키워드
        result["KeywordKR"] = self._safe_get(assign, "Keyword", "Korean")
        result["KeywordEN"] = self._safe_get(assign, "Keyword", "English")

        # 기관 정보
        result["OrderAgencyName"] = self._safe_get(assign, "OrderAgency", "Name")
        result["ResearchAgencyName"] = self._safe_get(assign, "ResearchAgency", "Name")
        result["ManageAgencyName"] = self._safe_get(assign, "ManageAgency", "Name")
        result["MinistryName"] = self._safe_get(assign, "Ministry", "Name")

        # 사업 정보
        result["BudgetProjectName"] = self._safe_get(assign, "BudgetProject", "Name")
        result["BusinessName"] = assign.get("BusinessName")
        result["BigProjectTitle"] = assign.get("BigprojectTitle")

        # 날짜 정보
        project_year = assign.get("ProjectYear")
        result["ProjectYear"] = self._format_date(
            project_year, "%Y", "%Y"
        ) if project_year else None

        result["ProjectStart"] = self._format_date(
            self._safe_get(assign, "ProjectPeriod", "Start"),
            "%Y%m%d"
        )
        result["ProjectEnd"] = self._format_date(
            self._safe_get(assign, "ProjectPeriod", "End"),
            "%Y%m%d"
        )
        result["ProjectTotalStart"] = self._format_date(
            self._safe_get(assign, "ProjectPeriod", "TotalStart"),
            "%Y-%m-%d %H:%M:%S.%f"
        )
        result["ProjectTotalEnd"] = self._format_date(
            self._safe_get(assign, "ProjectPeriod", "TotalEnd"),
            "%Y-%m-%d %H:%M:%S.%f"
        )

        # 기타 정보
        result["OrganizationPNo"] = assign.get("OrganizationPNumber")

        # 과학 분류
        science_classes = self._ensure_list(assign.get("ScienceClass", []))
        seq1 = next((item for item in science_classes if item.get("@sequence") == "1"), {})

        result["ScienceClassNewLargeCode"] = self._safe_get(seq1, "Large", "@code")
        result["ScienceClassNewLarge"] = self._safe_get(seq1, "Large", "#text")
        result["ScienceClassMediumCode"] = self._safe_get(seq1, "Medium", "@code")
        result["ScienceClassMedium"] = self._safe_get(seq1, "Medium", "#text")
        result["ScienceClassSmallCode"] = self._safe_get(seq1, "Small", "@code")
        result["ScienceClassSmall"] = self._safe_get(seq1, "Small", "#text")

        # 부처 과학 분류
        result["MinistryScienceClassLarge"] = self._safe_get(assign, "MinistryScienceClass", "Large")
        result["MinistryScienceClassMedium"] = self._safe_get(assign, "MinistryScienceClass", "Medium")
        result["MinistryScienceClassSmall"] = self._safe_get(assign, "MinistryScienceClass", "Small")

        # 임시 과학 분류
        result["TempScienceClassLarge"] = self._safe_get(assign, "TempScienceClass", "Large")
        result["TempScienceClassMedium"] = self._safe_get(assign, "TempScienceClass", "Medium")
        result["TempScienceClassSmall"] = self._safe_get(assign, "TempScienceClass", "Small")

        # 수행 주체
        result["PerformAgentCode"] = self._safe_get(assign, "PerformAgent", "@code")
        result["PerformAgent"] = self._safe_get(assign, "PerformAgent", "#text")

        # 개발 단계
        result["DevelopmentPhaseCode"] = self._safe_get(assign, "DevelopmentPhase", "@code")
        result["DevelopmentPhase"] = self._safe_get(assign, "DevelopmentPhase", "#text")

        # 기술 수명주기
        result["TechLifecycleCode"] = self._safe_get(assign, "TechnologyLifecycle", "@code")
        result["TechLifecycle"] = self._safe_get(assign, "TechnologyLifecycle", "#text")

        # 지역
        result["RegionCode"] = self._safe_get(assign, "Region", "@code")
        result["Region"] = self._safe_get(assign, "Region", "#text")

        # 경제사회목표
        result["EconomicSocialGoal"] = assign.get("EconomicSocialGoal")

        # 6T 기술
        result["SixTechCode"] = self._safe_get(assign, "SixTechnology", "@code")
        result["SixTech"] = self._safe_get(assign, "SixTechnology", "#text")

        # 적용 분야
        apply_area = assign.get("ApplyArea", {})
        result["ApplyAreaFirstCode"] = self._safe_get(apply_area, "First", "@code")
        result["ApplyAreaFirst"] = self._safe_get(apply_area, "First", "#text")
        result["ApplyAreaSecondCode"] = self._safe_get(apply_area, "Second", "@code")
        result["ApplyAreaSecond"] = self._safe_get(apply_area, "Second", "#text")
        result["ApplyAreaThirdCode"] = self._safe_get(apply_area, "Third", "@code")
        result["ApplyAreaThird"] = self._safe_get(apply_area, "Third", "#text")

        # 플래그
        result["ContinuousFlag"] = assign.get("ContinuousFlag")
        result["PolicyProjectFlag"] = assign.get("PolicyProjectFlag")

        # 예산
        result["GovernmentFunds"] = assign.get("GovernmentFunds")
        result["SBusinessFunds"] = assign.get("SbusinessFunds")
        result["TotalFunds"] = assign.get("TotalFunds")

        # 기타
        result["CorporateRegistrationNo"] = assign.get("CorporateRegistrationNumber")
        result["SeriesProject"] = assign.get("SeriesProject")

        return result


# ==========================================
# 실행 진입점
# ==========================================

def main():
    """메인 실행 함수"""
    crawler = AssignCrawler()
    crawler.run()


if __name__ == "__main__":
    main()