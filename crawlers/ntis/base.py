"""
NTIS 크롤러 베이스
NTIS API 공통 기능 제공
"""
import os
import re
import requests
import xmltodict
import traceback
from abc import abstractmethod
from typing import List, Dict, Optional
from tqdm import tqdm

from core.base_crawler import BaseCrawler
from core.exceptions import DuplicateError
from services.retry import backoff_with_db_logging


class NtisCrawler(BaseCrawler):
    """NTIS API 크롤러 베이스 클래스"""

    BASE_URL = "https://www.ntis.go.kr/rndopen/openApi/"

    def __init__(self, data_type: str):
        super().__init__(data_type)
        self.api_key = os.getenv("NTIS_API_KEY")

        if not self.api_key:
            raise ValueError("NTIS_API_KEY environment variable is not set")

    def _process_company(self, company: Dict) -> None:
        """회사별 데이터 수집"""
        biz_no = company['BIZ_NO']
        comp_name = company['CMP_NM']
        clean_comp_name = re.sub(r'\(.*?\)', '', comp_name).strip()

        try:
            # API 호출
            raw_data = self._fetch_api_data(clean_comp_name, biz_no)

            if not raw_data:
                tqdm.write(f" {clean_comp_name} - 데이터 없음")
                # 데이터 없어도 체크 로그는 기록
                self._save_data(biz_no, None)
                return

            # 데이터 변환
            transformed_data = self._transform_data(raw_data, biz_no)

            if not transformed_data:
                tqdm.write(f" {clean_comp_name} - 변환된 데이터 없음")
                self._save_data(biz_no, None)
                return

            # 저장
            self._save_data(biz_no, transformed_data)
            tqdm.write(
                f" ✓ {clean_comp_name} - {len(transformed_data) if isinstance(transformed_data, list) else 1}건 저장 완료")

        except DuplicateError:
            # 중복 발견 시 부분 저장
            if 'transformed_data' in locals() and transformed_data:
                self._save_data(biz_no, transformed_data)
                tqdm.write(f" ⚠️  {clean_comp_name} - 중복 발견 (부분 저장)")
            raise

        except Exception as e:
            error_msg = f"{clean_comp_name} 처리 중 오류: {e}"
            self.repository.log_error(
                location="NtisCrawler._process_company",
                data_type=self.data_type,
                message=error_msg,
                detail=traceback.format_exc()
            )
            raise

    @backoff_with_db_logging(
        max_retries=5,
        base_delay=2,
        allowed_statuses=(429, 500, 502, 503)
    )
    def _api_request(self, url: str, params: Dict) -> requests.Response:
        """
        API 요청 (재시도 포함)

        Args:
            url: API URL
            params: 쿼리 파라미터

        Returns:
            Response 객체
        """
        response = requests.get(url, params=params, timeout=30)
        return response

    @abstractmethod
    def _fetch_api_data(self, comp_name: str, biz_no: str) -> Optional[Dict]:
        """
        API 데이터 조회 (하위 클래스에서 구현)

        Args:
            comp_name: 회사명
            biz_no: 사업자번호

        Returns:
            원본 API 응답 데이터
        """
        pass

    @abstractmethod
    def _transform_data(self, raw_data: Dict, biz_no: str) -> Optional[List[Dict]]:
        """
        API 데이터 변환 (하위 클래스에서 구현)

        Args:
            raw_data: 원본 API 데이터
            biz_no: 사업자번호

        Returns:
            변환된 데이터 리스트
        """
        pass

    def _parse_xml_response(self, response: requests.Response) -> Dict:
        """
        XML 응답 파싱

        Args:
            response: Response 객체

        Returns:
            파싱된 딕셔너리
        """
        try:
            return xmltodict.parse(response.text)
        except Exception as e:
            print(f"XML 파싱 실패: {e}")
            raise

    def _ensure_list(self, data: any) -> List:
        """
        데이터를 리스트로 변환 (단일 항목인 경우 리스트로 감싸기)

        Args:
            data: 변환할 데이터

        Returns:
            리스트
        """
        if data is None:
            return []

        if isinstance(data, list):
            return data

        return [data]

    def _safe_get(self, data: Dict, *keys, default=None):
        """
        안전한 딕셔너리 값 추출 (중첩된 키 지원)

        Args:
            data: 딕셔너리
            *keys: 키 경로
            default: 기본값

        Returns:
            값 또는 기본값
        """
        result = data

        for key in keys:
            if isinstance(result, dict):
                result = result.get(key)
                if result is None:
                    return default
            else:
                return default

        return result if result is not None else default

    def _format_date(self, date_str: str, input_format: str, output_format: str = "%Y-%m-%d") -> Optional[str]:
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
            from datetime import datetime
            dt = datetime.strptime(date_str, input_format)
            return dt.strftime(output_format)
        except Exception as e:
            print(f"날짜 변환 실패 ({date_str}): {e}")
            return None

    def _clean_text(self, text: str) -> Optional[str]:
        """
        텍스트 정제

        Args:
            text: 원본 텍스트

        Returns:
            정제된 텍스트
        """
        if not text or not isinstance(text, str):
            return None

        cleaned = " ".join(text.split()).strip()
        return cleaned if cleaned else None


# ==========================================
# 공통 유틸리티 함수
# ==========================================

def split_semicolon_list(text: Optional[str]) -> Optional[List[str]]:
    """
    세미콜론으로 구분된 문자열을 리스트로 변환

    Args:
        text: "값1;값2;값3" 형태의 문자열

    Returns:
        ["값1", "값2", "값3"] 형태의 리스트
    """
    if not text:
        return None

    items = [item.strip() for item in text.split(";") if item.strip()]
    return items if items else None