import pymysql
from datetime import datetime
from dotenv import load_dotenv
import traceback
import os

load_dotenv()

HOST = os.getenv("LOCAL_MYSQL_HOST")
USER = os.getenv("LOCAL_MYSQL_USER")
PASSWORD = os.getenv("LOCAL_MYSQL_PASSWORD")
DATABASE = os.getenv("LOCAL_MYSQL_DATABASE")

# -----------------------------------------------------
# MySQL 연결 함수
# -----------------------------------------------------
def get_connection():
    return pymysql.connect(
        host=HOST,
        user=USER,
        password=PASSWORD,
        database=DATABASE,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor
    )


# -----------------------------------------------------
# 에러 로그 저장 함수
# -----------------------------------------------------
def insert_error_log(location, data_type, error_msg, error_detail):
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            sql = """
                  INSERT INTO error_log (DATA_TYPE, ERROR_LOG, CREATED_AT)
                  VALUES (%s, %s, %s) 
                  """
            cursor.execute(sql, (
                data_type, error_msg, datetime.now()
            ))
        print(f"[ERROR] : {location}({data_type}) : {error_msg}\n[DETAIL] : {error_detail}")
        conn.commit()
    except Exception as e:
        print("Error inserting error_log:", e)
    finally:
        if conn:
            conn.close()


# -----------------------------------------------------
# cmp_list 조회 함수
# -----------------------------------------------------
def get_cmp_list(data_type:str):
    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        sql = f"""
              SELECT BIZ_NO, CMP_NM, CEO_NM
              FROM cmp_list
              WHERE {data_type} IS NULL
              ORDER BY BIZ_NO 
              """
        cursor.execute(sql)
        result = cursor.fetchall()

        if not result:
            sql = f"""
                  SELECT BIZ_NO, CMP_NM, CEO_NM
                  FROM cmp_list
                  ORDER BY BIZ_NO, {data_type} ASC 
                  """
            cursor.execute(sql)
            result = cursor.fetchall()
            return result
        else:
            return result

    except Exception as e:
        error_log = f"{data_type} mysql select cmp_list : " + str(e)
        print(error_log)

        if conn:  # 연결이 되어 있을 때만 에러로그 적재
            insert_error_log("Select cmp list", data_type, error_log, "")

        return []  # 조회 실패 시 빈 리스트 반환

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# -----------------------------------------------------
#  적재 확인함수
# -----------------------------------------------------
def insert_check_log(biz_no: str, data_type:str, now:datetime):
    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = f"""
              UPDATE cmp_list
              SET {data_type} = %s
              WHERE BIZ_NO = %s 

              """
        cursor.execute(sql, (now, biz_no))
        conn.commit()

    except Exception as e:
        error_log = f"{data_type} mysql update check : " + str(e)
        print(error_log)
        if conn:
            try:
                insert_error_log("Insert check log", data_type, error_log, "")
            except Exception as e2:
                print(f"[ERROR] Error log 저장 실패: {e2}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def insert_cmp_data_log(biz_no: str, data_type:str, count:int, now:datetime):
    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()
        sql = f"""
                INSERT INTO cmp_data_log(biz_no, data_type, count, created_at)
                VALUES (%s, %s, %s, %s)
            """
        cursor.execute(sql, (
            biz_no, data_type, count, now
        ))
        conn.commit()

    except Exception as e:
        error_log = f"{data_type} mysql insert data log : " + str(e)
        insert_error_log("Insert cmp data log", data_type, error_log, "")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()