import pymysql
from typing import List, Dict, Optional
from datetime import datetime

from repositories.base_repository import BaseRepository
from core.config import RepositoryConfig
import traceback

class MySQLRepository(BaseRepository):

    def __init__(self, config: Optional[RepositoryConfig] = None):
        super().__init__()
        self.config = config or RepositoryConfig.get_instance()
        self._connection: Optional[pymysql.Connection] = None

    def connect(self):
        if self._is_connected:
            return True

        try:
            self._connection = pymysql.connect(
                host = self.config.mysql_host,
                user = self.config.mysql_user,
                password = self.config.mysql_password,
                database = self.config.mysql_database,
                cursorclass=pymysql.cursors.DictCursor
            )
            self._is_connected = True
            return True

        except Exception as e:
            print(f"MySQL connection error: {e}")
            self._is_connected = False
            raise

    def disconnect(self):
        if self._connection:
            self._connection.close()
            self._connection = None
            self._is_connected = False
            print("Disconnected from MySQL")

    def is_connected(self):
        if not self._connection:
            return False

        try:
            self._connection.ping(reconnect=True)
            return True
        except Exception as e:
            self._is_connected = False
            return False

    def get_connection(self) -> pymysql.Connection:
        if not self._is_connected:
            self.connect()
        return self._connection

    def get_company_list(self, data_type: str) -> List[Dict]:
        if not self._is_connected:
            self.connect()

        cursor = None

        try:
            cursor = self._connection.cursor()

            select_sql_null = f"""
            SELECT BIZ_NO, CMP_NM, CEO_NM
            FROM cmp_list
            WHERE {data_type} IS NULL
            ORDER BY BIZ_NO
            """

            cursor.execute(select_sql_null)
            result = cursor.fetchall()

            if result:
                return result

            select_sql_all = f"""
            SELECT BIZ_NO, CMP_NM, CEO_NM
            FROM cmp_list
            ORDER BY {data_type} ASC, BIZ_NO
            """

            cursor.execute(select_sql_all)
            result = cursor.fetchall()
            return result

        except Exception as e:
            error_msg = f"회사 목록 조회 실패 ({data_type}) : {e}"
            print(error_msg)
            self._log_error_internal("get_company_list", data_type, error_msg)
            return []

        finally:
            if cursor:
                cursor.close()

    def update_check_log(
            self,
            biz_no: str,
            data_type: str,
            timestamp: datetime
    ) -> bool:
        if not self._is_connected:
            self.connect()

        cursor = None

        try:
            cursor = self._connection.cursor()

            sql = f"""
            UPDATE cmp_list
            SET {data_type} = %s
            WHERE BIZ_NO = %s
            """
            cursor.execute(sql, (timestamp, biz_no))
            self._connection.commit()
            return True
        except Exception as e:
            error_msg = f"update_check_log failed ({biz_no}, {data_type}) : {e}"
            print(error_msg)
            self._log_error_internal("update_check_log", data_type, error_msg)

            if self._connection:
                self._connection.rollback()
            return False
        finally:
            if cursor:
                cursor.close()

    def insert_count_log(
            self,
            biz_no: str,
            data_type: str,
            count: int,
            timestamp: datetime
    ) -> bool:
        if not self._is_connected:
            self.connect()

        cursor = None

        try:
            cursor = self._connection.cursor()

            sql = f"""
            INSERT INTO cmp_data_log(biz_no, data_type, count, created_at)
            VALUES (%s, %s, %s, %s)
            """

            cursor.execute(sql, (biz_no, data_type, count, timestamp))
            self._connection.commit()
            return True

        except Exception as e:
            error_msg = f"insert_count_log failed ({biz_no}, {data_type}) : {e}"
            print(error_msg)
            self._log_error_internal("insert_count_log", data_type, error_msg)

            if self._connection:
                self._connection.rollback()
            return False

        finally:
            if cursor:
                cursor.close()

    def insert_error_log(
            self,
            location: str,
            data_type: str,
            error_msg: str,
            error_detail: str
    ) -> bool:
        return self._log_error_internal(location, data_type, error_msg, error_detail)

    def _log_error_internal(
            self,
            location: str,
            data_type: str,
            error_msg: str,
            error_detail: str = ""
    ) -> bool:
        conn = None
        cursor = None

        try:
            conn = pymysql.connect(
                host = self.config.mysql_host,
                user = self.config.mysql_user,
                password = self.config.mysql_password,
                database = self.config.mysql_database,
                cursorclass = pymysql.cursors.DictCursor
            )

            cursor = conn.cursor()

            sql = """
            INSERT INTO error_log (DATA_TYPE, ERROR_LOG, CREATED_AT)
            VALUES (%s, %s, %s)
            """

            full_msg = f"[{location} {error_msg}]"
            cursor.execute(sql, (data_type, full_msg, datetime.now()))
            conn.commit()

            # 콘솔에도 출력
            print(f"[ERROR] {location}({data_type}): {error_msg}")
            if error_detail:
                print(f"[DETAIL] {error_detail}")

            return True

        except Exception as e:
            print(f"insert_error_log failed: {e}")
            return False

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()




