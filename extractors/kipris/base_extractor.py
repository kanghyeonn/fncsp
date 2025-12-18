"""
KIPRIS Extractor 베이스 클래스
공통 추출 로직 제공
"""
import re
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple
from datetime import datetime

from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.remote.webdriver import WebDriver


class BaseKiprisExtractor(ABC):
    """KIPRIS 데이터 추출기 베이스 클래스"""

    def __init__(self):
        self.timeout = 3  # 백업용 타임아웃 (대부분 즉시 찾아짐)

    def extract(self, driver: WebDriver | WebElement) -> Dict:
        """
        상세 페이지에서 전체 정보 추출

        Args:
            driver: WebDriver 인스턴스

        Returns:
            추출된 정보 딕셔너리
        """
        info_dict = {}

        try:
            # 데이터 컨테이너 찾기 (일반적으로 이미 로드되어 있음)
            try:
                info_container = driver.find_element(
                    By.XPATH, '//*[@id="mainResultDetail"]/div[2]/div[1]/div[1]'
                )
            except Exception:
                # 혹시 아직 로드 중이면 짧게 대기
                print("⏳ 데이터 컨테이너 대기 중...")
                wait = WebDriverWait(driver, self.timeout)
                info_container = wait.until(
                    EC.presence_of_element_located(
                        (By.XPATH, '//*[@id="mainResultDetail"]/div[2]/div[1]/div[1]')
                    )
                )

            # 제목 추출
            info_dict['InventionTitle'] = self._extract_title(driver)

            # 섹션별 추출
            section_blocks = info_container.find_elements(By.CLASS_NAME, "tab-section-01")

            if not section_blocks:
                print("⚠ 섹션 블록을 찾을 수 없음")
                return info_dict

            for section_block in section_blocks:
                try:
                    title = self._get_section_title(section_block)

                    if not title:
                        continue

                    # 하위 클래스에서 구현한 섹션 처리
                    section_data = self._process_section(section_block, title)

                    if section_data:
                        info_dict.update(section_data)

                except Exception as e:
                    print(f"✗ 섹션 처리 오류 ({title}): {e}")
                    continue

        except Exception as e:
            print(f"✗ 데이터 추출 오류: {e}")
            import traceback
            traceback.print_exc()

        return info_dict

    @abstractmethod
    def _process_section(self, section, title: str) -> Optional[Dict]:
        """섹션별 데이터 처리 (하위 클래스에서 구현)"""
        pass

    def _extract_title(self, driver: WebDriver) -> str:
        """
        제목 추출

        Args:
            driver: WebDriver 인스턴스

        Returns:
            제목 문자열
        """
        try:
            title_div = driver.find_element(
                By.XPATH,
                "//*[@id='mainResultDetail']/div[1]/div[2]"
            )

            # 한글 제목 (필수)
            title_kr = title_div.find_element(By.TAG_NAME, "h2").text.strip()

            # 영문 제목 (선택)
            try:
                title_eng = title_div.find_element(By.TAG_NAME, "p").text.strip()
                return f"{title_kr} {title_eng}" if title_eng else title_kr
            except Exception:
                return title_kr

        except Exception as e:
            print(f"✗ 제목 추출 오류: {e}")
            return ""

    def _get_section_title(self, section) -> Optional[str]:
        """
        섹션 제목 추출 및 정규화

        Args:
            section: 섹션 WebElement

        Returns:
            정규화된 제목
        """
        try:
            title_elem = section.find_element(
                By.CSS_SELECTOR,
                ".title-box h4.title, .title-box h5.title"
            )
            title = (title_elem.text or "").strip()
            return self._normalize_title(title) if title else None
        except Exception:
            return None

    def _normalize_title(self, text: str) -> str:
        """제목 정규화 (비교용)"""
        import re
        if not text:
            return ""

        # 공백 제거
        normalized = re.sub(r"\s+", "", text)

        # 특수문자 제거 (한글/영문/숫자/슬래시만 유지)
        normalized = re.sub(r"[^0-9A-Za-z가-힣/]", "", normalized)

        return normalized.lower()

    def _title_contains(self, title: str, *keywords: str) -> bool:
        """제목에 키워드 포함 여부 확인"""
        normalized = self._normalize_title(title)
        return any(
            self._normalize_title(keyword) in normalized
            for keyword in keywords
        )

    def _clean(self, text: str) -> Optional[str]:
        """텍스트 정제"""
        if not text or not text.strip():
            return None

        cleaned = " ".join(text.split()).strip()
        return cleaned if cleaned else None

    def _text_without_em(self, element: WebElement) -> Optional[str]:
        """<em> 태그 제외하고 텍스트 추출"""
        text = self._clean(element.get_attribute("innerText") or element.text)

        try:
            em = element.find_element(By.CSS_SELECTOR, "em.th")
            em_text = self._clean(em.get_attribute("innerText") or em.text)

            if em_text and text and text.startswith(em_text):
                text = self._clean(text[len(em_text):])
        except Exception:
            pass

        return text

    def _parse_name_and_id(self, text: str) -> Tuple[Optional[str], Optional[str]]:
        """
        "이름(식별번호)" 형태에서 이름과 ID 분리

        Args:
            text: 원본 텍스트

        Returns:
            (이름, 식별번호) 튜플
        """
        text = self._clean(text)

        if not text:
            return None, None

        # 괄호 안의 ID 추출
        match = re.search(r"\(([^)]+)\)\s*$", text)

        if match:
            sid = self._clean(match.group(1))
            name = self._clean(text[:match.start()])
            return name, sid

        return text, None

    def _format_date(
        self,
        date_str: str,
        input_format: str = "%Y.%m.%d",
        output_format: str = "%Y-%m-%d"
    ) -> Optional[str]:
        """
        날짜 형식 변환

        Args:
            date_str: 날짜 문자열
            input_format: 입력 형식
            output_format: 출력 형식

        Returns:
            변환된 날짜 문자열
        """
        if not date_str:
            return None

        try:
            dt = datetime.strptime(date_str, input_format)
            return dt.strftime(output_format)
        except Exception as e:
            print(f"날짜 변환 실패 ({date_str}): {e}")
            return None

    def _extract_table_data(
        self,
        section: WebElement,
        field_mapping: Dict[str, str]
    ) -> Dict:
        """
        테이블에서 데이터 추출

        Args:
            section: 섹션 WebElement
            field_mapping: {원본필드명: 변환필드명} 매핑

        Returns:
            추출된 데이터 딕셔너리
        """
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

                # <a> 태그 텍스트 제거
                td_html = td.get_attribute("innerHTML") or ""
                if "<a" in td_html:
                    for a in td.find_elements(By.TAG_NAME, "a"):
                        a_text = a.text
                        if a_text:
                            td_text = td_text.replace(a_text, "")

                result[field_name] = self._clean(td_text)

            except Exception as e:
                print(f"행 처리 오류: {e}")
                continue

        return result

    def _extract_list_from_links(
        self,
        td: WebElement
    ) -> List[str]:
        """
        <td> 내의 <a> 링크들에서 텍스트 리스트 추출

        Args:
            td: td WebElement

        Returns:
            텍스트 리스트
        """
        links = td.find_elements(By.TAG_NAME, "a")
        values = [
            self._clean(a.text)
            for a in links
            if self._clean(a.text)
        ]
        return values

    def _check_empty_data(self, element: WebElement) -> bool:
        """
        "데이터가 존재하지 않습니다" 확인

        Args:
            element: 확인할 WebElement

        Returns:
            빈 데이터면 True
        """
        try:
            text = element.get_attribute("innerText") or element.text
            return "데이터가 존재하지 않습니다" in text
        except Exception:
            return False

    def _extract_people_info(
        self,
        section: WebElement,
        title: str
    ) -> Dict:
        """
        인명정보 추출 (공통)

        Args:
            section: 섹션 WebElement
            title: 섹션 제목

        Returns:
            인명 정보 딕셔너리
        """
        result = {}
        items = []

        try:
            rows = section.find_elements(By.CSS_SELECTOR, "table tbody tr")
        except Exception:
            return {title: []}

        # 빈 데이터 확인
        if rows and self._check_empty_data(rows[0]):
            return {title: []}

        for row in rows:
            try:
                tds = row.find_elements(By.TAG_NAME, "td")

                if not tds:
                    continue

                item = {}

                # 번호 (있는 경우)
                if len(tds) >= 3:
                    item["번호"] = self._text_without_em(tds[0])
                    name_td = tds[1]
                    addr_td = tds[2]
                else:
                    name_td = tds[0]
                    addr_td = tds[1] if len(tds) > 1 else None

                # 이름 및 식별번호
                name_text = self._clean(name_td.text)
                name, sid = self._parse_name_and_id(name_text)

                item["이름"] = name
                if sid:
                    item["식별번호"] = sid

                # 추가 정보 (드롭박스)
                extra = {}
                for box in name_td.find_elements(By.CSS_SELECTOR, ".dropbox-select"):
                    try:
                        key = self._clean(
                            box.find_element(By.CSS_SELECTOR, ".btn-dropbox").text
                        )
                        val = self._clean(
                            box.find_element(By.CSS_SELECTOR, ".dropbox-con .txt").text
                        )
                        if key and val:
                            extra[key] = val
                    except Exception:
                        continue

                if extra:
                    item["추가정보"] = extra

                # 주소
                if addr_td:
                    item["주소"] = self._clean(addr_td.get_attribute("innerText"))

                items.append(item)

            except Exception as e:
                print(f"인명 정보 행 처리 오류: {e}")
                continue

        result[title] = items
        return result


class BasePatentExtractor(BaseKiprisExtractor):
    """특허/실용신안 공통 Extractor"""

    def _extract_bibliography_with_date(
            self,
            section: WebElement,
            field_mapping: Dict[str, any]
    ) -> Dict:
        """
        날짜 포함 서지정보 추출

        Args:
            section: 섹션 WebElement
            field_mapping: 필드 매핑 (리스트면 [번호필드, 날짜필드])

        Returns:
            추출된 데이터
        """
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

                # IPC/CPC는 리스트로
                if th in ("IPC", "CPC"):
                    result[field_name] = self._extract_list_from_links(td)
                    continue

                td_text = td.get_attribute("innerText") or td.text

                # ✅ FIX: innerHTML을 사용하여 <a> 태그 존재 여부 확인
                td_html = td.get_attribute("innerHTML") or ""

                # <a> 태그가 실제로 있을 때만 제거 작업 수행
                if "<a" in td_html:
                    for a in td.find_elements(By.TAG_NAME, "a"):
                        a_text = a.text
                        if a_text:
                            td_text = td_text.replace(a_text, "")

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

                # 심사청구항수 (숫자)
                elif th == "심사청구항수":
                    try:
                        result[field_name] = int(self._clean(td_text) or 0)
                    except ValueError:
                        result[field_name] = 0

                # 출원인 (리스트)
                elif th == "출원인":
                    applicants = self._clean(td_text)
                    result[field_name] = applicants.split() if applicants else None

                # 일반 텍스트
                else:
                    result[field_name] = self._clean(td_text)

            except Exception as e:
                print(f"서지정보 행 처리 오류: {e}")
                continue

        # 요약 추출
        try:
            summary_tag = section.find_element(By.ID, "sum_all")
            summary_p = summary_tag.find_element(By.CSS_SELECTOR, "summary p")
            result["AstrtCont"] = self._clean(summary_p.text)
        except Exception:
            pass

        return result
# """
# KIPRIS Extractor 베이스 클래스 - 디버깅 버전
# 공통 추출 로직 제공
# """
# import re
# import time
# from abc import ABC, abstractmethod
# from typing import Dict, List, Optional, Tuple
# from datetime import datetime
#
# from selenium.webdriver.common.by import By
# from selenium.webdriver.remote.webelement import WebElement
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.remote.webdriver import WebDriver
#
#
# class BaseKiprisExtractor(ABC):
#     """KIPRIS 데이터 추출기 베이스 클래스"""
#
#     def __init__(self):
#         self.timeout = 3  # 백업용 타임아웃 (대부분 즉시 찾아짐)
#
#     def extract(self, driver: WebDriver) -> Dict:
#         """
#         상세 페이지에서 전체 정보 추출
#
#         Args:
#             driver: WebDriver 인스턴스
#
#         Returns:
#             추출된 정보 딕셔너리
#         """
#         info_dict = {}
#
#         try:
#             # 데이터 컨테이너 찾기 (일반적으로 이미 로드되어 있음)
#             try:
#                 info_container = driver.find_element(
#                     By.XPATH, '//*[@id="mainResultDetail"]/div[2]/div[1]/div[1]'
#                 )
#             except Exception:
#                 # 혹시 아직 로드 중이면 짧게 대기
#                 print("⏳ 데이터 컨테이너 대기 중...")
#                 wait = WebDriverWait(driver, self.timeout)
#                 info_container = wait.until(
#                     EC.presence_of_element_located(
#                         (By.XPATH, '//*[@id="mainResultDetail"]/div[2]/div[1]/div[1]')
#                     )
#                 )
#
#             # 제목 추출
#             info_dict['InventionTitle'] = self._extract_title(driver)
#
#             # 섹션별 추출
#             section_blocks = info_container.find_elements(By.CLASS_NAME, "tab-section-01")
#
#             if not section_blocks:
#                 print("⚠ 섹션 블록을 찾을 수 없음")
#                 return info_dict
#
#             for section_block in section_blocks:
#                 try:
#                     title = self._get_section_title(section_block)
#
#                     if not title:
#                         continue
#
#                     # 하위 클래스에서 구현한 섹션 처리
#                     section_data = self._process_section(section_block, title)
#
#                     if section_data:
#                         info_dict.update(section_data)
#
#                 except Exception as e:
#                     print(f"✗ 섹션 처리 오류 ({title}): {e}")
#                     continue
#
#         except Exception as e:
#             print(f"✗ 데이터 추출 오류: {e}")
#             import traceback
#             traceback.print_exc()
#
#         return info_dict
#
#     @abstractmethod
#     def _process_section(self, section, title: str) -> Optional[Dict]:
#         """섹션별 데이터 처리 (하위 클래스에서 구현)"""
#         pass
#
#     def _extract_title(self, driver: WebDriver) -> str:
#         """
#         제목 추출
#
#         Args:
#             driver: WebDriver 인스턴스
#
#         Returns:
#             제목 문자열
#         """
#         try:
#             title_div = driver.find_element(
#                 By.XPATH,
#                 "//*[@id='mainResultDetail']/div[1]/div[2]"
#             )
#
#             # 한글 제목 (필수)
#             title_kr = title_div.find_element(By.TAG_NAME, "h2").text.strip()
#
#             # 영문 제목 (선택)
#             try:
#                 title_eng = title_div.find_element(By.TAG_NAME, "p").text.strip()
#                 return f"{title_kr} {title_eng}" if title_eng else title_kr
#             except Exception:
#                 return title_kr
#
#         except Exception as e:
#             print(f"✗ 제목 추출 오류: {e}")
#             return ""
#
#     def _get_section_title(self, section) -> Optional[str]:
#         """
#         섹션 제목 추출 및 정규화
#
#         Args:
#             section: 섹션 WebElement
#
#         Returns:
#             정규화된 제목
#         """
#         try:
#             title_elem = section.find_element(
#                 By.CSS_SELECTOR,
#                 ".title-box h4.title, .title-box h5.title"
#             )
#             title = (title_elem.text or "").strip()
#             return self._normalize_title(title) if title else None
#         except Exception:
#             return None
#
#     # ==========================================
#     # 공통 유틸리티 메서드
#     # ==========================================
#
#     def _normalize_title(self, text: str) -> str:
#         """제목 정규화 (비교용)"""
#         if not text:
#             return ""
#
#         # 공백 제거
#         normalized = re.sub(r"\s+", "", text)
#
#         # 특수문자 제거 (한글/영문/숫자/슬래시만 유지)
#         normalized = re.sub(r"[^0-9A-Za-z가-힣/]", "", normalized)
#
#         return normalized.lower()
#
#     def _title_contains(self, title: str, *keywords: str) -> bool:
#         """제목에 키워드 포함 여부 확인"""
#         normalized = self._normalize_title(title)
#         return any(
#             self._normalize_title(keyword) in normalized
#             for keyword in keywords
#         )
#
#     def _clean(self, text: str) -> Optional[str]:
#         """텍스트 정제"""
#         if not text or not text.strip():
#             return None
#
#         cleaned = " ".join(text.split()).strip()
#         return cleaned if cleaned else None
#
#     def _text_without_em(self, element: WebElement) -> Optional[str]:
#         """<em> 태그 제외하고 텍스트 추출"""
#         text = self._clean(element.get_attribute("innerText") or element.text)
#
#         try:
#             em = element.find_element(By.CSS_SELECTOR, "em.th")
#             em_text = self._clean(em.get_attribute("innerText") or em.text)
#
#             if em_text and text and text.startswith(em_text):
#                 text = self._clean(text[len(em_text):])
#         except Exception:
#             pass
#
#         return text
#
#     def _parse_name_and_id(self, text: str) -> Tuple[Optional[str], Optional[str]]:
#         """
#         "이름(식별번호)" 형태에서 이름과 ID 분리
#
#         Args:
#             text: 원본 텍스트
#
#         Returns:
#             (이름, 식별번호) 튜플
#         """
#         text = self._clean(text)
#
#         if not text:
#             return None, None
#
#         # 괄호 안의 ID 추출
#         match = re.search(r"\(([^)]+)\)\s*$", text)
#
#         if match:
#             sid = self._clean(match.group(1))
#             name = self._clean(text[:match.start()])
#             return name, sid
#
#         return text, None
#
#     def _format_date(
#             self,
#             date_str: str,
#             input_format: str = "%Y.%m.%d",
#             output_format: str = "%Y-%m-%d"
#     ) -> Optional[str]:
#         """
#         날짜 형식 변환
#
#         Args:
#             date_str: 날짜 문자열
#             input_format: 입력 형식
#             output_format: 출력 형식
#
#         Returns:
#             변환된 날짜 문자열
#         """
#         if not date_str:
#             return None
#
#         try:
#             dt = datetime.strptime(date_str, input_format)
#             return dt.strftime(output_format)
#         except Exception as e:
#             print(f"날짜 변환 실패 ({date_str}): {e}")
#             return None
#
#     def _extract_table_data(
#             self,
#             section: WebElement,
#             field_mapping: Dict[str, str]
#     ) -> Dict:
#         """
#         테이블에서 데이터 추출
#
#         Args:
#             section: 섹션 WebElement
#             field_mapping: {원본필드명: 변환필드명} 매핑
#
#         Returns:
#             추출된 데이터 딕셔너리
#         """
#         result = {}
#
#         try:
#             rows = section.find_elements(By.CSS_SELECTOR, "table.table tbody tr")
#         except Exception:
#             return result
#
#         for row in rows:
#             try:
#                 th = row.find_element(By.TAG_NAME, "th").text.strip()
#                 td = row.find_element(By.TAG_NAME, "td")
#
#                 if th not in field_mapping:
#                     continue
#
#                 field_name = field_mapping[th]
#                 td_text = td.get_attribute("innerText") or td.text
#
#                 # <a> 태그 텍스트 제거
#                 for a in td.find_elements(By.TAG_NAME, "a"):
#                     a_text = a.text
#                     if a_text:
#                         td_text = td_text.replace(a_text, "")
#
#                 result[field_name] = self._clean(td_text)
#
#             except Exception as e:
#                 print(f"행 처리 오류: {e}")
#                 continue
#
#         return result
#
#     def _extract_list_from_links(
#             self,
#             td: WebElement
#     ) -> List[str]:
#         """
#         <td> 내의 <a> 링크들에서 텍스트 리스트 추출
#
#         Args:
#             td: td WebElement
#
#         Returns:
#             텍스트 리스트
#         """
#         links = td.find_elements(By.TAG_NAME, "a")
#         values = [
#             self._clean(a.text)
#             for a in links
#             if self._clean(a.text)
#         ]
#         return values
#
#     def _check_empty_data(self, element: WebElement) -> bool:
#         """
#         "데이터가 존재하지 않습니다" 확인
#
#         Args:
#             element: 확인할 WebElement
#
#         Returns:
#             빈 데이터면 True
#         """
#         try:
#             text = element.get_attribute("innerText") or element.text
#             return "데이터가 존재하지 않습니다" in text
#         except Exception:
#             return False
#
#     def _extract_people_info(
#             self,
#             section: WebElement,
#             title: str
#     ) -> Dict:
#         """
#         인명정보 추출 (공통)
#
#         Args:
#             section: 섹션 WebElement
#             title: 섹션 제목
#
#         Returns:
#             인명 정보 딕셔너리
#         """
#         result = {}
#         items = []
#
#         try:
#             rows = section.find_elements(By.CSS_SELECTOR, "table tbody tr")
#         except Exception:
#             return {title: []}
#
#         # 빈 데이터 확인
#         if rows and self._check_empty_data(rows[0]):
#             return {title: []}
#
#         for row in rows:
#             try:
#                 tds = row.find_elements(By.TAG_NAME, "td")
#
#                 if not tds:
#                     continue
#
#                 item = {}
#
#                 # 번호 (있는 경우)
#                 if len(tds) >= 3:
#                     item["번호"] = self._text_without_em(tds[0])
#                     name_td = tds[1]
#                     addr_td = tds[2]
#                 else:
#                     name_td = tds[0]
#                     addr_td = tds[1] if len(tds) > 1 else None
#
#                 # 이름 및 식별번호
#                 name_text = self._clean(name_td.text)
#                 name, sid = self._parse_name_and_id(name_text)
#
#                 item["이름"] = name
#                 if sid:
#                     item["식별번호"] = sid
#
#                 # 추가 정보 (드롭박스)
#                 extra = {}
#                 for box in name_td.find_elements(By.CSS_SELECTOR, ".dropbox-select"):
#                     try:
#                         key = self._clean(
#                             box.find_element(By.CSS_SELECTOR, ".btn-dropbox").text
#                         )
#                         val = self._clean(
#                             box.find_element(By.CSS_SELECTOR, ".dropbox-con .txt").text
#                         )
#                         if key and val:
#                             extra[key] = val
#                     except Exception:
#                         continue
#
#                 if extra:
#                     item["추가정보"] = extra
#
#                 # 주소
#                 if addr_td:
#                     item["주소"] = self._clean(addr_td.get_attribute("innerText"))
#
#                 items.append(item)
#
#             except Exception as e:
#                 print(f"인명 정보 행 처리 오류: {e}")
#                 continue
#
#         result[title] = items
#         return result
#
#
# class BasePatentExtractor(BaseKiprisExtractor):
#     """특허/실용신안 공통 Extractor - 디버깅 버전"""
#
#     def _extract_bibliography_with_date(
#             self,
#             section: WebElement,
#             field_mapping: Dict[str, any]
#     ) -> Dict:
#         """
#         날짜 포함 서지정보 추출 (디버깅)
#
#         Args:
#             section: 섹션 WebElement
#             field_mapping: 필드 매핑 (리스트면 [번호필드, 날짜필드])
#
#         Returns:
#             추출된 데이터
#         """
#         print(f"[DEBUG-BIBLIO] === 서지정보 추출 시작 ===")
#         biblio_start = time.time()
#
#         result = {}
#
#         # 테이블 찾기
#         print(f"[DEBUG-BIBLIO] 테이블 찾기 시작...")
#         table_start = time.time()
#
#         try:
#             rows = section.find_elements(By.CSS_SELECTOR, "table.table tbody tr")
#             table_end = time.time()
#             print(f"[DEBUG-BIBLIO] ✓ 테이블 찾기 완료: {len(rows)}행 ({table_end - table_start:.2f}초)")
#         except Exception as e:
#             print(f"[DEBUG-BIBLIO] ✗ 테이블 찾기 실패: {e}")
#             return result
#
#         # 각 행 처리
#         print(f"[DEBUG-BIBLIO] 행 처리 시작 (총 {len(rows)}행)")
#
#         for idx, row in enumerate(rows):
#             row_start = time.time()
#
#             try:
#                 # TH 찾기
#                 th_start = time.time()
#                 th = row.find_element(By.TAG_NAME, "th").text.strip()
#                 th_end = time.time()
#
#                 # TD 찾기
#                 td_start = time.time()
#                 td = row.find_element(By.TAG_NAME, "td")
#                 td_end = time.time()
#
#                 if th not in field_mapping:
#                     continue
#
#                 field_name = field_mapping[th]
#
#                 # IPC/CPC는 리스트로
#                 if th in ("IPC", "CPC"):
#                     ipc_start = time.time()
#                     result[field_name] = self._extract_list_from_links(td)
#                     ipc_end = time.time()
#
#                     row_elapsed = time.time() - row_start
#                     print(
#                         f"[DEBUG-BIBLIO]   행 {idx:2d} ({th:10s}): 전체 {row_elapsed:.2f}초 (링크추출: {ipc_end - ipc_start:.2f}초)")
#                     continue
#
#                 # innerText 가져오기
#                 text_start = time.time()
#                 td_text = td.get_attribute("innerText") or td.text
#                 text_end = time.time()
#
#                 # <a> 태그 제거
#                 a_start = time.time()
#                 a_tags = td.find_elements(By.TAG_NAME, "a")
#                 for a in a_tags:
#                     a_text = a.text
#                     if a_text:
#                         td_text = td_text.replace(a_text, "")
#                 a_end = time.time()
#
#                 # 나머지 처리 로직
#                 process_start = time.time()
#
#                 # "번호(일자)" 형식 처리
#                 if "번호(일자)" in th and isinstance(field_name, list):
#                     if td_text:
#                         td_text = self._clean(td_text)
#                         parts = td_text.split(" ")
#
#                         if len(parts) >= 2:
#                             number = parts[0]
#                             date_str = parts[1].strip("()")
#                             date = self._format_date(date_str, "%Y.%m.%d")
#
#                             result[field_name[0]] = number
#                             result[field_name[1]] = date
#                         else:
#                             result[field_name[0]] = td_text
#                             result[field_name[1]] = None
#                     else:
#                         result[field_name[0]] = None
#                         result[field_name[1]] = None
#
#                 # 심사청구항수 (숫자)
#                 elif th == "심사청구항수":
#                     try:
#                         result[field_name] = int(self._clean(td_text) or 0)
#                     except ValueError:
#                         result[field_name] = 0
#
#                 # 출원인 (리스트)
#                 elif th == "출원인":
#                     applicants = self._clean(td_text)
#                     result[field_name] = applicants.split() if applicants else None
#
#                 # 일반 텍스트
#                 else:
#                     result[field_name] = self._clean(td_text)
#
#                 process_end = time.time()
#
#                 # 행 처리 시간 출력 (1초 이상만)
#                 row_elapsed = time.time() - row_start
#                 if row_elapsed > 1.0:
#                     print(f"[DEBUG-BIBLIO] ⚠ 행 {idx:2d} ({th:10s}): 전체 {row_elapsed:.2f}초")
#                     print(f"               - TH찾기: {th_end - th_start:.3f}초, TD찾기: {td_end - td_start:.3f}초")
#                     print(
#                         f"               - getText: {text_end - text_start:.3f}초, <a>제거: {a_end - a_start:.3f}초 ({len(a_tags)}개)")
#                     print(f"               - 처리: {process_end - process_start:.3f}초")
#                 elif row_elapsed > 0.5:
#                     print(f"[DEBUG-BIBLIO]   행 {idx:2d} ({th:10s}): {row_elapsed:.2f}초")
#
#             except Exception as e:
#                 print(f"[DEBUG-BIBLIO] ✗ 행 {idx} 처리 오류: {e}")
#                 continue
#
#         # 요약 추출
#         print(f"[DEBUG-BIBLIO] 요약 추출 시작...")
#         summary_start = time.time()
#
#         try:
#             sum_find_start = time.time()
#             summary_tag = section.find_element(By.ID, "sum_all")
#             sum_find_end = time.time()
#             print(f"[DEBUG-BIBLIO]   sum_all 찾기: {sum_find_end - sum_find_start:.2f}초")
#
#             p_find_start = time.time()
#             summary_p = summary_tag.find_element(By.CSS_SELECTOR, "summary p")
#             p_find_end = time.time()
#             print(f"[DEBUG-BIBLIO]   summary p 찾기: {p_find_end - p_find_start:.2f}초")
#
#             clean_start = time.time()
#             result["AstrtCont"] = self._clean(summary_p.text)
#             clean_end = time.time()
#             print(f"[DEBUG-BIBLIO]   텍스트 정제: {clean_end - clean_start:.2f}초")
#
#             summary_elapsed = time.time() - summary_start
#             print(f"[DEBUG-BIBLIO] ✓ 요약 추출 완료: {summary_elapsed:.2f}초")
#         except Exception as e:
#             summary_elapsed = time.time() - summary_start
#             print(f"[DEBUG-BIBLIO] ✗ 요약 추출 실패 ({summary_elapsed:.2f}초): {type(e).__name__}")
#
#         biblio_elapsed = time.time() - biblio_start
#         print(f"[DEBUG-BIBLIO] === 서지정보 추출 완료: 총 {biblio_elapsed:.2f}초 ===\n")
#
#         return result