import traceback
from abc import ABC, abstractmethod
from typing import List, Dict, Optional
from datetime import datetime
from repositories.base import DataRepository
from services.notification import NotificationService

class BaseCrawler(ABC):

    def __init__(self, data_type: str):
        self.data_type = data_type
        self.repository = DataRepository()
        self.notifier = NotificationService()
        self.resources = []

    def _load_companies(self) -> List[Dict]:
        return self.repository.get_company_list(self.data_type)

    def _save_data(self, biz_no: str, data: List[Dict]):
        now = datetime.now()
        self.repository.save(self.data_type, biz_no, data)
        self.repository.log_check(biz_no, self.data_type, now)
        self.repository.log_data_count(biz_no, self.data_type, len(data), now)

    def _cleanup(self):
        for resource in self.resources:
            if hasattr(resource, 'close'):
                resource.close()

    def _handle_error(self, error: Exception):
        self.repository.log_error(
            "Crawler Error",
            self.data_type,
            str(error),
            traceback.format_exc()
        )
