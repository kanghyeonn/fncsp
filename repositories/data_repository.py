from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from datetime import datetime


class DataRepository:
    def __init__(self):
        self._es = None
        self._mysql = None

    def get_es_connection(self):
        if not self._es:
            from repositories.elasticsearch import get_es_conn
            self._es = get_es_conn()
        return self._es

    def get_mysql_connection(self):
        if not self._mysql:
            from repositories.mysql import get_mysql_conn
            self._mysql = get_mysql_conn()
        return self._mysql

    def get_company_list(self, data_type):
        pass

    def save(self, data_type: str, biz_no: str, data: List[Dict]):
        es = self.get_es_connection()
        savers = {
            "KIPRIS_PATENT" : self._save_patent,
            "KIPRIS_DESIGN" : self._save_design,
            "KIPRIS_TRADEMARK" : self._save_trademark,
            "KIPRIS_UTILITY" : self._save_utility,
            "NAVER_NEWS" : self._save_news,
            "NAVER_TREND" : self._save_trend,
            "NTIS_ASSIGN" : self._save_assign,
            "NTIS_ORG_INFO" : self._save_ntis_org,
            "NTIS_RND_PAPER" : self._save_ntis_paper
        }
        saver = savers.get(data_type)
        if saver:
            saver(es,data, biz_no)

    def log_check(self, biz_no, data_type, now):
        pass

    def log_data_count(self, biz_no, data_type, param, now):
        pass

    def log_error(self, param, data_type, param1, param2):
        pass