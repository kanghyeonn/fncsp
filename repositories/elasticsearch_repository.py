from elasticsearch import Elasticsearch, helpers
from typing import List, Dict, Optional
from datetime import datetime

from core.config import RepositoryConfig
from repositories.base_repository import BaseRepository

class ElasticsearchRepository(BaseRepository):
    def __init__(self, config: Optional[RepositoryConfig] = None):
        super().__init__()
        self.config = config or RepositoryConfig.get_instance()
        # connection
        self._es: Optional[Elasticsearch] = None
        # 인덱스 이름
        self._index_name = "source_data"

    def connect(self):
        if self._is_connected:
            return True

        try:
            self._es = Elasticsearch(
                    [self.config.es_host],
                    http_auth=(self.config.es_id, self.config.es_password),
                    verify_certs=False,
                )

            if self._es.ping():
                self._is_connected = True
                print("Connected to Elasticsearch")
                return True
            else:
                raise ConnectionError("ES ping failed")
        except Exception as e:
            print(f"Elasticsearch connection failed: {e}")
            self._is_connected = False
            raise

    def disconnect(self):
        if self._es:
            self._es.close()
            self._es = None
            self._is_connected = False
            print("Disconnected from Elasticsearch")

    def is_connected(self):
        return self._is_connected and self._es is not None

    def get_connection(self) -> Elasticsearch:
        if not self._is_connected:
            self.connect()

        return self._es

    def insert_bulk(
            self,
            data_type: str,
            biz_no: str,
            data: Optional[List[str]],
    ) -> tuple[int, List]:
        if not self._is_connected:
            raise ConnectionError("Not connected to Elasticsearch")

        if not data:
            actions = [{
                "_index": self._index_name,
                "_source": {
                    "BusinessNum": biz_no,
                    "DataType": data_type.lower(),
                    "SearchDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                    "SearchID": "autoSystem",
                    "Data": None
                }
            }]
        else:
            actions = [{
                "_index": self._index_name,
                "_source": {
                    "BusinessNum": biz_no,
                    "DataType": data_type.lower(),
                    "SearchDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                    "SearchID": "autoSystem",
                    "Data": doc
                }
            } for doc in data]

        try:
            success_count, errors = helpers.bulk(
                self._es,
                actions,
                raise_on_error=False,
                raise_on_exception=False
            )

            if errors:
                print(f"Bulk insert warnings: {len(errors)} errors")

            return success_count, errors

        except Exception as e:
            print(f"bulk_insert failed: {e}")
            raise

    def insert_single(
            self,
            data_type: str,
            biz_no: str,
            data: Optional[Dict]
    ) -> Dict:

        if not self._is_connected:
            raise ConnectionError("Not connected to Elasticsearch")

        document = {
            "BusinessNum": biz_no,
            "DataType": data_type.lower(),
            "SearchDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "SearchID": "autoSystem",
            "Data": data
        }

        try:
            result = self._es.index(
                index=self._index_name,
                body=document
            )
            return result

        except Exception as e:
            print(f"single_insert failed: {e}")

    def check_duplicate(
            self,
            data_type: str,
            biz_no: str,
            field: str,
            value: str
    ) -> bool:
        if not self._is_connected:
            raise ConnectionError("Not connected to Elasticsearch")

        query = {
            "query" : {
                "bool" : {
                    "must" : [
                        {"term": {"DataType": data_type}},
                        {"term": {"BusinessNum": biz_no}},
                        {"term": {field: value}}
                    ]
                }
            },
            "size": 0
        }

        try:
            response = self._es.search(
                index=self._index_name,
                body=query,
            )
            count = response["hits"]["total"]["value"]

            return count > 0

        except Exception as e:
            print(f"check_duplicate failed: {e}")
            raise

    def check_application_number_duplicate(
            self,
            data_type: str,
            biz_no: str,
            application_number: str,
    ) -> bool:
        return self.check_duplicate(
            data_type,
            biz_no,
            "Data.ApplicationNumber",
            application_number,
        )

    def check_project_number_duplicate(
            self,
            data_type: str,
            biz_no: str,
            project_number: str,
    ) -> bool:
        return self.check_duplicate(
            data_type,
            biz_no,
            "Data.ProjectNo",
            project_number,
        )

    def check_research_public_number_duplicate(
            self,
            data_type: str,
            biz_no: str,
            research_public_number: str,
    ) -> bool:
        return self.check_duplicate(
            data_type,
            biz_no,
            "Data.ResearchPublicNo.keyword",
            research_public_number,
        )

