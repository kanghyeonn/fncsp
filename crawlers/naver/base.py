from abc import abstractmethod
from typing import List, Dict, Optional
from tqdm import tqdm
import re
import traceback

from core.base_crawler import BaseCrawler
from services.retry import RetryService

class NaverCrawler(BaseCrawler):
    def __init__(self, data_type:str, period: int = 365):
        super().__init__(data_type)
        self.period = period
        self.retry_service = RetryService()

    def _process_company(self, company: Dict) -> None:
        biz_no = company["BIZ_NO"]
        comp_name = company["CMP_NM"]
        ceo_name = company["CEO_NM"]

        # (주), (사) 제거
        clean_comp_name = re.sub(r'\(.*?\)', '', comp_name).strip()

        # "외 N명" 제거
        clean_ceo_name = re.sub(r'외\s*\d+명', '', ceo_name).strip() if ceo_name else ''

        try:
            data = self._fetch_data(clean_comp_name, clean_ceo_name)

            self._save_data(biz_no, data)

            if data:
                tqdm.write(f" {clean_comp_name} - {len(data)}건 저장 완료")
            else:
                tqdm.write(f" {clean_comp_name} - 데이터 없음")

        except Exception as e:
            error_msg = f"{clean_comp_name} 처리 중 오류: {e}"
            self.repository.log_error(
                location="NaverCrawler._process_company",
                data_type=self.data_type,
                message=error_msg,
                detail=traceback.format_exc()
            )
            raise

    @abstractmethod
    def _fetch_data(self, comp_name: str, ceo_name:str) -> List[Dict]:
        pass

