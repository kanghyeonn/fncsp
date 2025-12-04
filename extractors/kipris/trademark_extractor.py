"""
KIPRIS 상표 데이터 추출기
"""
from typing import Dict, Optional
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.common.by import By

from extractors.kipris.base_extractor import BaseKiprisExtractor


class TrademarkExtractor(BaseKiprisExtractor):
    """상표 데이터 추출기"""

    def _extract_title(self, card: WebElement) -> str:
        """
        제목 추출 (상표는 한글만)

        Args:
            card: 카드 WebElement

        Returns:
            제목 문자열
        """
        try:
            title_div = card.find_element(
                By.XPATH,
                "//*[@id='mainResultDetail']/div[1]/div[2]"
            )

            title_kr = title_div.find_element(By.TAG_NAME, "h2").text.strip()
            return title_kr

        except Exception as e:
            print(f"제목 추출 오류: {e}")
            return ""

    def _process_section(self, section: WebElement, title: str) -> Optional[Dict]:
        """
        섹션별 처리

        Args:
            section: 섹션 WebElement
            title: 정규화된 제목

        Returns:
            추출된 데이터
        """
        # 서지정보
        if self._title_contains(title, "서지정보"):
            return self._extract_bibliography(section)

        # 인명정보 (출원인)
        elif self._title_contains(title, "인명정보"):
            return self._extract_people_info(section, "Applicant")

        # 대리인
        elif self._title_contains(title, "대리인"):
            return self._extract_people_info(section, "Agent")

        # 도형분류(비엔나)코드
        elif self._title_contains(title, "도형분류", "비엔나"):
            return self._extract_vienna_code(section)

        return None

    def _extract_bibliography(self, section: WebElement) -> Dict:
        """서지정보 추출"""
        field_mapping = {
            "법적상태": "RegisterStatus",
            "상품분류": "Classification",
            "출원번호(일자)": ["ApplicationNumber", "ApplicationDate"],
            "등록번호(일자)": ["RegisterNumber", "RegisterDate"],
            "출원공고번호(일자)": ["AppIPubINumber", "AppIPubIDate"]
        }

        result = {}

        try:
            rows = section.find_elements(By.CSS_SELECTOR, "table.table tbody tr")
        except Exception:
            return result

        for row in rows:
            try:
                th = row.find_element(By.TAG_NAME, "th").text.strip()
                td = row.find_element(By.TAG_NAME, "td")

                if th not in field_mapping:
                    continue

                field_name = field_mapping[th]
                td_text = td.get_attribute("innerText") or td.text

                # "번호(일자)" 형식 처리
                if "번호(일자)" in th and isinstance(field_name, list):
                    if td_text:
                        td_text = self._clean(td_text)
                        parts = td_text.split(" ")

                        if len(parts) >= 2:
                            number = parts[0]
                            date_str = parts[1].strip("()")
                            date = self._format_date(date_str, "%Y.%m.%d")

                            result[field_name[0]] = number
                            result[field_name[1]] = date
                        else:
                            result[field_name[0]] = td_text
                            result[field_name[1]] = None
                    else:
                        result[field_name[0]] = None
                        result[field_name[1]] = None

                # 일반 텍스트
                else:
                    result[field_name] = self._clean(td_text)

            except Exception as e:
                print(f"서지정보 행 처리 오류: {e}")
                continue

        return result

    def _extract_people_info(
            self,
            section: WebElement,
            field_name: str
    ) -> Dict:
        """
        인명정보 추출

        Args:
            section: 섹션 WebElement
            field_name: 필드명 (Applicant, Agent)

        Returns:
            인명 리스트
        """
        names = []

        # 하위 섹션 찾기
        subsections = section.find_elements(By.CSS_SELECTOR, "div.tab-section-02")

        for subsec in subsections:
            try:
                # 제목 확인
                title_elem = subsec.find_element(By.TAG_NAME, "h5")
                title = self._clean(title_elem.text)

                if not title:
                    continue

                # 출원인 또는 대리인인 경우
                target_title = "출원인" if field_name == "Applicant" else "대리인"

                if title != target_title:
                    continue

                # 이름 셀들 (2번째 컬럼)
                name_cells = subsec.find_elements(
                    By.CSS_SELECTOR,
                    "tbody tr td:nth-child(2)"
                )

                for cell in name_cells:
                    lines = cell.text.split("\n")
                    if lines:
                        name = self._clean(lines[0])
                        if name:
                            names.append(name)

            except Exception as e:
                print(f"인명정보 처리 오류: {e}")
                continue

        return {field_name: names if names else None}

    def _extract_vienna_code(self, section: WebElement) -> Dict:
        """
        비엔나 코드 추출

        Args:
            section: 섹션 WebElement

        Returns:
            비엔나 코드 리스트
        """
        vienna_codes = []

        try:
            # 코드 셀들 (2번째 컬럼)
            code_cells = section.find_elements(
                By.CSS_SELECTOR,
                "tbody tr td:nth-child(2)"
            )

            for cell in code_cells:
                code = self._clean(cell.text)
                if code:
                    vienna_codes.append(code)

        except Exception as e:
            print(f"비엔나 코드 추출 오류: {e}")

        return {"ViennaCode": vienna_codes if vienna_codes else None}


# ==========================================
# 편의 함수 (하위 호환성)
# ==========================================

def extract_from_trademark_details(card: WebElement) -> Dict:
    """
    상표 상세 정보 추출

    Args:
        card: 상세 카드 WebElement

    Returns:
        추출된 정보 딕셔너리
    """
    extractor = TrademarkExtractor()
    return extractor.extract(card)