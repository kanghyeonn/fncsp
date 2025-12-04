import re
import os
import requests
import traceback

from datetime import datetime, date
from typing import List, Dict, Optional
from tqdm import tqdm


from crawlers.naver.base import NaverCrawler
from services.retry import backoff_with_db_logging

class TrendCrawler(NaverCrawler):
    CLIENT_KEYS = [
        {"id": "5Z6LFF9LL821FNE9fUmE", "secret": "RCBO6gF8tE"},
        {"id": "4ZQu5OewzVjDSJ1cdSdA", "secret": "PrWJ55zRIn"},
        {"id": "FeCSP_QG9MBu_xfCirex", "secret": "RZBikPtMv9"},
        {"id": "5NgRqK_gWGT_5ZSPbLrg", "secret": "bM00_C5bUB"},
        {"id": "jE6R7hyS50Z4PklkKsnx", "secret": "e4Nm_y0c4N"},
        {"id": "ejfNJ43j1uYqVmzDebUL", "secret": "ErqevqGIq5"},
        {"id": "85RVPmnN1TjaYpYZLp2u", "secret": "PeZ99DlMTh"},
        {"id": "qX1a_btOZrCFqikMhCvq", "secret": "ZyQTpai51h"},
        {"id": "_TrXXSd3zQS8FdMW0sqX", "secret": "7H6_XK9iZl"},
        {"id": "vwN1Jpzb6tscNtr2dVEA", "secret": "O2n7exZQzk"},
        {"id": "aJ_17_xFhkOmyXWh3EbH", "secret": "ptPJNXECoN"},
        {"id": "hooF3Z_8ZBtZFLU624T8", "secret": "f1rY6nGbuT"},
        {"id": "mXRSqmSGfXiIK0uzFa2Q", "secret": "I8ydyFesmW"},
        {"id": "QvhnEXV0OeIvz3pHnXqM", "secret": "Z7sMI5JnZT"},
        {"id": "GZkay1kFpZjj8inSzroE", "secret": "zzu1hd5Bam"},
        {"id": "oZ4cFLBk_DdYpyLJf3OV", "secret": "6kF1E2mZz7"},
        {"id": "MLOH3AMDFqHIGd_tUdfV", "secret": "tCnDbDXp0P"},
        {"id": "VcSzKBRYkdgU7CkVbArq", "secret": "GT7dDQQq3H"},
        {"id": "HqUF2LkcAgOEyBqe_Nx6", "secret": "cHTN2ZtOiU"},
        {"id": "Bk3mikSGvjJwDTaGwm55", "secret": "cLYyMnyKtv"},
        {"id": "Sc5J0K5fTHNIOQrRJboR", "secret": "DolfNTfLks"},
        {"id": "ujkfY2gLms6WOh7xvQYL", "secret": "8NC0Pf8QCZ"},
        {"id": "UdFQ5GtC6y3Iq1vjIyTZ", "secret": "iziA9V1dna"},
        {"id": "YxWXwbCkmz8AoocOHuZO", "secret": "CJRbFE0Zn7"},
        {"id": "y55EPGcfrUPW5SdIvBpz", "secret": "kIiCCBv3zv"},
        {"id": "PTkVCkWPbNW7AvkZoAHK", "secret": "ECMYCJgYQO"},
        {"id": "SWUVoqXrGzRCaAmAivDD", "secret": "xAsMnkCyKo"},
        {"id": "M7nYHdcOIfm6r82az8CV", "secret": "1c5R_C3OxL"},
    ]

    def __init__(
            self,
            start_date: str = "2022-01-01",
            chunk_size: int = 5
    ):
        super().__init__("NAVER_TREND")
        self.start_date = start_date
        self.chunk_size = chunk_size
        self.end_date = date.today().strftime("%Y-%m-%d")
        self.current_key_index = 0

    def _process_company(self, company: Dict) -> None:
        pass

    def run(self) -> None:
        self.start_time = datetime.now()

        try:
            print(f"\n{'=' * 60}")
            print(f" {self.data_type} 크롤러 시작")
            print(f" 시작 시간 : {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f" 기간: {self.start_date} ~ {self.end_date}")
            print(f" 청크 크기: {self.chunk_size}개씩")
            print(f"{'=' * 60}\n")

            companies = self._load_companies()
            self.stats['total_companies'] = len(companies)

            if not companies:
                print("수집할 회사가 없습니다.")
                self.exit_message = "수집할 회사 없음"
                return

            print(f"총 {len(companies)}개 회사 수집 예정")
            print(f"총 {self._calculate_chunks(len(companies))}개 청크로 분할\n")

            # 청크 단위로 처리
            for i in tqdm(
                    range(0, len(companies), self.chunk_size),
                    desc=f'{self.data_type} 수집',
                    unit='청크'
            ):
                chunk = companies[i:i + self.chunk_size]

                try:
                    self._process_chunk(chunk)
                    self.stats['success_companies'] += len(chunk)

                except Exception as e:
                    self.stats['failed_companies'] += len(chunk)
                    self._handle_error(e, {
                        'chunk_index': i // self.chunk_size,
                        'chunk': chunk
                    })

            self.exit_message = "프로그램 정상 종료"

        except Exception as e:
            self.exit_message = f'프로그램 예외 종료: {e}'
            self._handle_error(e, {'stage': 'main'})

        finally:
            self._cleanup()
            self._send_notification()
            self._print_final_stats()


    def _process_chunk(self, chunk: List[Dict]) -> None:
        keyword_groups = []

        for company in chunk:
            comp_name = company['CMP_NM']
            clean_name = re.sub(r'\(.*?\)', '', comp_name).strip()

            keyword_groups.append({
                "groupName": clean_name,
                "keywords": [clean_name]
            })

        # API 호출
        try:
            result = self._call_api(keyword_groups)

            if not result or 'results' not in result:
                tqdm.write(f"API 응답 없음")
                return

            # 각 회사별로 데이터 저장
            for idx, company_result in enumerate(result['results']):
                if idx >= len(chunk):
                    break

                company = chunk[idx]
                biz_no = company['BIZ_NO']
                comp_name = company['CMP_NM']

                # 트렌드 데이터 변환
                trend_data = self._transform_trend_data(company_result)

                # 저장
                success = self._save_data(biz_no, trend_data)

                if success:
                    self.stats['total_data'] += len(trend_data)
                    tqdm.write(f" ✓ {comp_name} - {len(trend_data)}건 저장")
                else:
                    tqdm.write(f" ✗ {comp_name} - 저장 실패")

        except Exception as e:
            error_msg = f"청크 처리 중 오류: {e}"
            self.repository.log_error(
                location="TrendCrawler._process_chunk",
                data_type=self.data_type,
                message=error_msg,
                detail=traceback.format_exc()
            )
            raise

    @backoff_with_db_logging(
        max_retries=5,
        base_delay=2,
        data_type="NAVER_TREND",
        allowed_statuses=(429,)
    )
    def _call_api(self, keyword_groups: List[Dict]) -> Optional[Dict]:
        if self.current_key_index >= len(self.CLIENT_KEYS):
            raise Exception("모든 API 키의 한도 초과")

        key = self.CLIENT_KEYS[self.current_key_index]

        url = "https://openapi.naver.com/v1/datalab/search"
        headers = {
            "X-Naver-Client-Id": key["id"],
            "X-Naver-Client-Secret": key["secret"],
            "Content-Type": "application/json",
        }

        body = {
            "startDate": self.start_date,
            "endDate": self.end_date,
            "timeUnit": "month",
            "keywordGroups": keyword_groups,
        }

        response = requests.post(url, headers=headers, json=body)

        # 429 에러 시 다음 키로 전환
        if response.status_code == 429:
            tqdm.write(f"API 키 한도 도달, 다음 키로 전환 ({self.current_key_index + 1} → {self.current_key_index + 2})")
            self.current_key_index += 1
            raise Exception("Rate limit exceeded")

        # 성공
        if response.status_code == 200:
            return response.json()

        # 기타 에러
        response.raise_for_status()

    def _transform_trend_data(self, result: Dict) -> List[Dict]:
        trend_data = []

        for data_point in result.get('data', []):
            trend_data.append({
                "date": data_point.get('period'),
                "ratio": float(data_point.get('ratio', 0))
            })

        return trend_data

    def _calculate_chunks(self, total: int) -> int:

        return (total + self.chunk_size - 1) // self.chunk_size

    def _fetch_data(self, comp_name: str, ceo_name: str) -> List[Dict]:
        return []