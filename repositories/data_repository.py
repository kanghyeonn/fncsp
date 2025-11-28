from typing import List, Dict, Optional
from datetime import datetime
from repositories.elasticsearch_repository import ElasticsearchRepository
from repositories.mysql_repository import MySQLRepository
from core.config import RepositoryConfig


class DataRepository:

    _instance = None

    def __new__(cls):
        """Singleton Pattern"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self.config = RepositoryConfig.get_instance()
            self._es_repo: Optional[ElasticsearchRepository] = None
            self._mysql_repo: Optional[MySQLRepository] = None
            self._initialized = True

    # ============================================================
    # Repository Access
    # ============================================================

    @property
    def es(self) -> ElasticsearchRepository:
        """ES Repository 반환 (Lazy Loading)"""
        if self._es_repo is None:
            self._es_repo = ElasticsearchRepository(self.config)
            self._es_repo.connect()
        return self._es_repo

    @property
    def mysql(self) -> MySQLRepository:
        """MySQL Repository 반환 (Lazy Loading)"""
        if self._mysql_repo is None:
            self._mysql_repo = MySQLRepository(self.config)
            self._mysql_repo.connect()
        return self._mysql_repo

    # ============================================================
    # High-Level Operations (크롤러가 사용하는 메인 API)
    # ============================================================

    def save_with_logging(
            self,
            data_type: str,
            biz_no: str,
            data: Optional[List[Dict]]
    ) -> bool:
        """
        데이터 저장 + 로깅 (원자적 트랜잭션)

        Args:
            data_type: 데이터 타입
            biz_no: 사업자번호
            data: 저장할 데이터

        Returns:
            성공 여부

        로직:
        1. ES에 데이터 삽입
        2. MySQL에 체크 로그 업데이트
        3. MySQL에 건수 로그 삽입
        4. 하나라도 실패하면 False 반환
        """
        now = datetime.now()

        try:
            # 1. ES 저장
            success_count, errors = self._save_to_es(data_type, biz_no, data)

            if errors:
                print(f"⚠️  일부 문서 저장 실패: {len(errors)}건")

            # 2. MySQL 체크 로그
            check_ok = self.mysql.update_check_log(biz_no, data_type, now)

            # 3. MySQL 건수 로그
            data_count = len(data) if data else 0
            count_ok = self.mysql.insert_count_log(
                biz_no, data_type, data_count, now
            )

            return check_ok and count_ok

        except Exception as e:
            self.mysql.insert_error_log(
                "save_with_logging",
                data_type,
                f"데이터 저장 실패 ({biz_no}): {e}",
                ""
            )
            return False

    def _save_to_es(
            self,
            data_type: str,
            biz_no: str,
            data: Optional[List[Dict]]
    ) -> tuple:
        # 데이터 타입별로 특별한 처리가 필요한 경우 여기서 분기
        type_handlers = {
            "KIPRIS_PATENT": self.es.insert_bulk,
            "KIPRIS_DESIGN": self.es.insert_bulk,
            "KIPRIS_TRADEMARK": self.es.insert_bulk,
            "KIPRIS_UTILITY": self.es.insert_bulk,
            "NAVER_NEWS": self.es.insert_bulk,
            "NAVER_TREND": self._save_naver_trend,  # 특별 처리
            "NTIS_ASSIGN": self.es.insert_bulk,
            "NTIS_ORG_INFO": self._save_ntis_org_info,  # 특별 처리
            "NTIS_RND_PAPER": self.es.insert_bulk,
        }


        handler = type_handlers.get(data_type, self.es.insert_bulk)
        return handler(data_type, biz_no, data)

    def _save_naver_trend(
            self,
            data_type: str,
            biz_no: str,
            data: Optional[List[Dict]]
    ) -> tuple:
        """Naver Trend는 리스트가 아닌 단일 문서로 저장"""
        if data:
            result = self.es.insert_single(data_type, biz_no, data)
            return (1, [])
        else:
            return self.es.insert_bulk(data_type, biz_no, None)

    def _save_ntis_org_info(
            self,
            data_type: str,
            biz_no: str,
            data: Optional[Dict]
    ) -> tuple:
        """NTIS Org Info는 딕셔너리 단일 문서로 저장"""
        if data:
            result = self.es.insert_single(data_type, biz_no, data)
            return (1, [])
        else:
            return self.es.insert_bulk(data_type, biz_no, None)

    def get_company_list(self, data_type: str) -> List[Dict]:
        """회사 목록 조회"""
        return self.mysql.get_company_list(data_type)

    # ============================================================
    # Duplicate Check Operations
    # ============================================================

    def check_duplicate(
            self,
            index: str,
            biz_no: str,
            key: str
    ) -> bool:
        """
        일반 중복 체크

        Args:
            index: 데이터 타입 (kipris_patent 등)
            biz_no: 사업자번호
            key: 체크할 값

        Returns:
            중복이면 True
        """
        # index에 따라 필드명 결정
        field_mapping = {
            "kipris_patent": "Data.ApplicationNumber",
            "kipris_design": "Data.ApplicationNumber",
            "kipris_trade": "Data.ApplicationNumber",
            "kipris_utility": "Data.ApplicationNumber",
            "ntis_assign": "Data.ProjectNo",
            "ntis_rnd_paper": "Data.ResearchPublicNo.keyword"
        }

        field = field_mapping.get(index, "Data.ApplicationNumber")
        return self.es.check_duplicate(index, biz_no, field, key)

    def log_error(
            self,
            location: str,
            data_type: str,
            message: str,
            detail: str = ""
    ):
        """에러 로그 기록"""
        self.mysql.insert_error_log(location, data_type, message, detail)

    def close_all(self):
        """모든 연결 종료"""
        if self._es_repo:
            self._es_repo.disconnect()
        if self._mysql_repo:
            self._mysql_repo.disconnect()

    def __enter__(self):
        """Context Manager 진입"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context Manager 종료"""
        self.close_all()
        return False