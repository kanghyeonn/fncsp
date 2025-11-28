import os
from dotenv import load_dotenv
from typing import Optional

load_dotenv()

class RepositoryConfig:
    _instance: Optional['RepositoryConfig'] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self.es_host = os.getenv('ELASTICSEARCH_HOST')
            self.es_id = os.getenv('ELASTICSEARCH_ID')
            self.es_password = os.getenv('ELASTICSEARCH_PASSWORD')

            self.mysql_host = os.getenv('LOCAL_MYSQL_HOST')
            self.mysql_user = os.getenv('LOCAL_MYSQL_USER')
            self.mysql_password = os.getenv('LOCAL_MYSQL_PASSWORD')
            self.mysql_database = os.getenv('LOCAL_MYSQL_DATABASE')

            self._initialized = True

    @classmethod
    def get_instance(cls) -> 'RepositoryConfig':
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def validate(self) -> bool:
        """설정 유효성 검증"""
        required_fields = [
            'es_host', 'es_id', 'es_password',
            'mysql_host', 'mysql_user', 'mysql_password', 'mysql_database'
        ]

        for field in required_fields:
            if not getattr(self, field):
                print(f"Missing config: {field}")
                return False

        print("Configuration validated")
        return True
