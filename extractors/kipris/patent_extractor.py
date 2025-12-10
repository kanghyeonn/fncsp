"""
KIPRIS 특허 데이터 추출기
"""
from typing import Dict, Optional
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.common.by import By
import time

from extractors.kipris.base_extractor import BasePatentExtractor


class PatentExtractor(BasePatentExtractor):
    """특허 데이터 추출기"""

    def _process_section(self, section: WebElement, title: str) -> Optional[Dict]:
        """
        섹션별 처리

        Args:
            section: 섹션 WebElement
            title: 정규화된 제목

        Returns:
            추출된 데이터
        """
        start_time = time.time()

        # 서지정보
        if self._title_contains(title, "서지정보", "bibliography"):
            result = self._extract_bibliography(section)
            elapsed = time.time() - start_time
            return result

        # 인명정보
        elif self._title_contains(title, "인명정보", "people", "applicant", "inventor"):
            result = self._extract_patent_people_info(section)
            elapsed = time.time() - start_time
            return result

        # 인용/피인용
        elif self._title_contains(title, "인용", "피인용", "citation", "cited"):
            result = self._extract_citations(section)
            elapsed = time.time() - start_time
            return result

        # 패밀리정보
        elif self._title_contains(title, "패밀리정보", "family"):
            result = self._extract_family_info(section)
            elapsed = time.time() - start_time
            return result

        # 국가연구개발사업
        elif self._title_contains(title, "국가연구개발사업", "rnd", "research"):
            result = self._extract_national_rnd(section)
            elapsed = time.time() - start_time
            return result

        return None

    def _extract_bibliography(self, section: WebElement) -> Dict:
        """서지정보 추출 (디버깅)"""

        field_mapping = {
            "IPC": "IPCNumber",
            "CPC": "CPCNumber",
            "출원인": "ApplicantName",
            "법적상태": "RegisterStatus",
            "심사청구항수": "ExaminationCount",
            "출원번호(일자)": ["ApplicationNumber", "ApplicationDate"],
            "등록번호(일자)": ["RegisterNumber", "RegisterDate"],
            "공개번호(일자)": ["OpenNumber", "OpenDate"]
        }

        result = self._extract_bibliography_with_date(section, field_mapping)

        return result

    # 나머지 메서드들은 원본과 동일...
    def _extract_patent_people_info(self, section: WebElement) -> Dict:
        """인명정보 추출 (발명자 수 포함)"""
        result = {}

        # 하위 섹션들 찾기
        subsections = section.find_elements(By.CSS_SELECTOR, "div.tab-section-02")

        for subsec in subsections:
            try:
                title_elem = subsec.find_element(By.CSS_SELECTOR, "h5.title")
                title = self._clean(title_elem.text)

                if not title:
                    continue

                # 발명자인 경우 개수 카운트
                if title == "발명자":
                    rows = subsec.find_elements(By.CSS_SELECTOR, "table tbody tr")
                    result["InventorCount"] = len(rows)

            except Exception as e:
                print(f"인명정보 서브섹션 처리 오류: {e}")
                continue

        return result

    def _extract_citations(self, section: WebElement) -> Dict:
        """인용/피인용 정보 추출"""
        result = {"BackwardCitation": None, "ForwardCitation": None}

        # 하위 섹션들 (인용/피인용)
        subsections = section.find_elements(By.CSS_SELECTOR, "div.tab-section-02")

        for subsec in subsections:
            try:
                title_elem = subsec.find_element(By.CSS_SELECTOR, "h5.title")
                title = self._clean(title_elem.text)

                if not title:
                    continue

                # 테이블 찾기
                tables = subsec.find_elements(By.CSS_SELECTOR, "table.table.table-hrzn")

                if not tables:
                    continue

                table = tables[0]

                # 인용 (Forward Citation)
                if title == "인용":
                    field_mapping = {
                        "국가": "FCCountry",
                        "공보번호": "FCNumber",
                        "공보일자": "FCDate",
                        "발명의 명칭": "FCTitle",
                        "IPC": "FCIPC"
                    }
                    result["BackwardCitation"] = self._parse_citation_table(
                        table, field_mapping
                    )

                # 피인용 (Backward Citation)
                elif title == "피인용":
                    field_mapping = {
                        "출원번호(일자)": "BCNumber",
                        "출원 연월일": "BCDate",
                        "발명의 명칭": "BCTitle",
                        "IPC": "BCIPC"
                    }
                    result["ForwardCitation"] = self._parse_citation_table(
                        table, field_mapping
                    )

            except Exception as e:
                print(f"인용 정보 처리 오류: {e}")
                continue

        return result

    def _parse_citation_table(
            self,
            table: WebElement,
            field_mapping: Dict[str, str]
    ) -> Optional[list]:
        """인용 테이블 파싱"""
        # 빈 데이터 확인
        try:
            first_td = table.find_element(By.CSS_SELECTOR, "tbody tr td")
            if self._check_empty_data(first_td):
                return None
        except Exception:
            pass

        rows_out = []

        try:
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
        except Exception:
            return None

        for tr in rows:
            try:
                tds = tr.find_elements(By.TAG_NAME, "td")

                if not tds:
                    continue

                # 빈 데이터 행 스킵
                if len(tds) == 1 and self._check_empty_data(tds[0]):
                    continue

                row = {}
                headers = list(field_mapping.keys())

                for i, td in enumerate(tds):
                    if i >= len(headers):
                        break

                    header = headers[i]
                    field_name = field_mapping[header]
                    value = self._text_without_em(td)

                    # 날짜 변환
                    if "일자" in header or "연월일" in header:
                        value = self._format_date(value, "%Y.%m.%d")

                    # IPC 공백 제거
                    elif header == "IPC":
                        value = value.replace(" ", "") if value else None

                    row[field_name] = value

                rows_out.append(row)

            except Exception as e:
                print(f"인용 행 처리 오류: {e}")
                continue

        return rows_out if rows_out else None

    def _extract_family_info(self, section: WebElement) -> Dict:
        """패밀리 정보 추출"""
        result = {"Family": None, "DOCDBFamily": None}

        # 패밀리 정보
        try:
            op_table = section.find_element(
                By.CSS_SELECTOR,
                "table#opFamilyTable.table.table-hrzn"
            )

            field_mapping = {
                "패밀리번호": "FamilyNumber",
                "국가코드": "FamilyCountrycode",
                "국가명": "FamilyCountryname",
                "종류": "FamilyType"
            }

            result["Family"] = self._parse_family_table(op_table, field_mapping)

        except Exception as e:
            print(f"패밀리 정보 추출 오류: {e}")

        # DOCDB 패밀리 정보
        try:
            doc_table = section.find_element(
                By.CSS_SELECTOR,
                "table#docFamilyTable.table.table-hrzn"
            )

            field_mapping = {
                "패밀리번호": "DOCDBnumber",
                "국가코드": "DOCDBcountrycode",
                "국가명": "DOCDBcountryname",
                "종류": "DOCDBtype"
            }

            result["DOCDBFamily"] = self._parse_family_table(doc_table, field_mapping)

        except Exception as e:
            print(f"DOCDB 패밀리 정보 추출 오류: {e}")

        return result

    def _parse_family_table(
            self,
            table: WebElement,
            field_mapping: Dict[str, str]
    ) -> Optional[list]:
        """패밀리 테이블 파싱"""
        # 헤더 추출
        try:
            headers = [
                self._clean(th.get_attribute("innerText") or th.text)
                for th in table.find_elements(By.CSS_SELECTOR, "thead th")
            ]
        except Exception:
            return None

        # 빈 데이터 확인
        try:
            first_td = table.find_element(By.CSS_SELECTOR, "tbody tr td")
            if self._check_empty_data(first_td) and first_td.get_attribute("colspan"):
                return None
        except Exception:
            pass

        rows_out = []

        try:
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
        except Exception:
            return None

        for tr in rows:
            try:
                tds = tr.find_elements(By.TAG_NAME, "td")

                if not tds:
                    continue

                # 빈 데이터 행 스킵
                if len(tds) == 1 and self._check_empty_data(tds[0]):
                    continue

                row = {}

                for i, td in enumerate(tds):
                    if i >= len(headers):
                        break

                    key = headers[i]

                    if key not in field_mapping:
                        continue

                    field_name = field_mapping[key]
                    value = self._text_without_em(td)

                    # 패밀리번호는 공백 앞부분만
                    if field_name in ["FamilyNumber", "DOCDBnumber"]:
                        value = value.split()[0] if value else None

                    row[field_name] = value

                rows_out.append(row)

            except Exception as e:
                print(f"패밀리 행 처리 오류: {e}")
                continue

        return rows_out if rows_out else None

    def _extract_national_rnd(self, section: WebElement) -> Dict:
        """국가연구개발사업 정보 추출"""
        result = {"ResearchData": None}

        try:
            table = section.find_element(
                By.CSS_SELECTOR,
                "table.table.table-hrzn"
            )
        except Exception:
            return result

        # 빈 데이터 확인
        try:
            first_td = table.find_element(By.CSS_SELECTOR, "tbody tr td")
            if self._check_empty_data(first_td) and first_td.get_attribute("colspan"):
                return result
        except Exception:
            pass

        try:
            rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
        except Exception:
            return result

        for tr in rows:
            try:
                tds = tr.find_elements(By.TAG_NAME, "td")

                if not tds:
                    continue

                # 빈 데이터 행 스킵
                if len(tds) == 1 and self._check_empty_data(tds[0]):
                    continue

                # 필드 매핑 (순서 고정)
                field_mapping = {
                    1: "ResearchDepartment",  # 연구부처
                    2: "ResearchInstitution",  # 주관기관
                    3: "ResearchBusiness",  # 연구사업
                    4: "ResearchProject"  # 연구과제
                }

                row = {}

                for i, td in enumerate(tds):
                    if i == 0:  # 순번 스킵
                        continue

                    if i in field_mapping:
                        field_name = field_mapping[i]
                        row[field_name] = self._text_without_em(td)

                result["ResearchData"] = row
                break  # 첫 번째 행만 사용

            except Exception as e:
                print(f"국가연구개발사업 행 처리 오류: {e}")
                continue

        return result


# ==========================================
# 편의 함수 (하위 호환성)
# ==========================================

def extract_from_patent_details(card: WebElement) -> Dict:
    """
    특허 상세 정보 추출

    Args:
        card: 상세 카드 WebElement

    Returns:
        추출된 정보 딕셔너리
    """
    extractor = PatentExtractor()
    return extractor.extract(card)