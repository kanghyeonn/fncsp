import traceback
from abc import abstractmethod
from tqdm import tqdm
from typing import List, Dict
import time
import re

from core.base_crawler import BaseCrawler
from core.exceptions import DuplicateError
from extractors.kipris.common import KiprisExtractorFactory
from extractors.utils import (
    WebDriverManager,
    get_total_num,
    sort_by_applictaion_an,
    has_result,
    open_card,
    go_next_page
)

class KiprisCrawler(BaseCrawler):

    def __init__(self, category: str, data_type: str):
        super().__init__(data_type)
        self.category = category
        self.driver = None
        self.extractor = None
        self.es = None

    def _setup(self) -> None:
        try:
            print(f"브라우저 시작 중 ({self.category})")
            self.driver = webdriverManger.create_driver(self.category)
            self.resources.append(self.driver)

            self.extractor = KiprisExtractorFactory.create(self.category)
            print(f"Extractor 준비완료 ({self.category})")

            print(f"Elasticsearch 연결 중")
            self.es = self.repository.es.get_connection()
            self.resources.append(self.es)
            print("Elasticsearch 연결 완료")

        except Exception as e:
            error_msg = f"초기화 실패 : {e}"
            self.repository.log_error(
                "KiprisCrawler._setup",
                data_type=self.data_type,
                error_msg=error_msg

            )
            raise ConnectionError(error_msg)

    def _process_company(self, company: Dict) -> None:
        biz_no = company['BIZ_NO']
        comp_name = company['CMP_NM']
        clean_comp_name = re.sub(r'\(.*?\)', '', comp_name)
        data = []

        try:
            self._search(biz_no)
            time.sleep(1)

            total = self._get_total_count()

            if total == 0:
                tqdm.write(f" {clean_comp_name} 검색 결과 없음")
                return

            tqdm.write(f"{clean_comp_name} - {total}건 발견")

            data = self._crawl_pages(biz_no, comp_name)

            self._save_data(biz_no, data)
            tqdm.write(f" {clean_comp_name} - {len(data)}건 저장 완료")

        except DuplicateError:
            if data:
                self._save_data(biz_no, data)
                tqdm.write(f" {clean_comp_name} - 중복 발견 (부분 저장 : {len(data)}건)")
                raise
        except Exception as e:
            error_msg = f"{clean_comp_name} 처리 중 오류 : {e}"
            self.repository.log_error(
                location="KiprisCrawler._process_company",
                data_type=self.data_type,
                error_msg=error_msg,
                detail=traceback.format_exc()
            )
            raise

    @abstractmethod
    def _search(self, biz_no: str) -> None:
        pass

    @abstractmethod
    def _is_duplicate(self, card, biz_no:str) -> bool:
        pass

    def _get_total_count(self) -> int:
        return get_total_num(self.driver, self.category)

    def _calculate_pages(self, total: int, per_page: int = 30) -> int:
        return int((total / per_page) + 1)