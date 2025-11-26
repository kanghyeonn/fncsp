import traceback

import pymysql
import requests
import functools
import time
import random
import re
from db.es import *
from db.mysql import *
from collector.alter import send_naver_alert
import datetime
from tqdm import tqdm

load_dotenv()

HOST = os.getenv("LOCAL_MYSQL_HOST")
USER = os.getenv("LOCAL_MYSQL_USER")
PASSWORD = os.getenv("LOCAL_MYSQL_PASSWORD")
DATABASE = os.getenv("LOCAL_MYSQL_DATABASE")

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

current_key_index = 0


# -----------------------------------------------------
# backoff 함수
# -----------------------------------------------------
def backoff_with_key_rotation(max_retries=5, base_delay=2, allowed_statuses=(429,)):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            global current_key_index

            for attempt in range(1, max_retries + 1):
                try:
                    response = func(*args, **kwargs)

                    # 정상 응답
                    if response.status_code == 200:
                        return response.json()

                    # 429 Too Many Requests → 다음 키로 교체
                    elif response.status_code in allowed_statuses:
                        print(f"[WARN] 429 Too Many Requests 발생. API 키 교체를 시도합니다.")
                        print(f"다음 key index: {current_key_index}")
                        current_key_index += 1

                        if current_key_index >= len(CLIENT_KEYS):
                            error_log = "모든 API 키가 한도에 도달했습니다. (429 연속 발생)"
                            insert_error_log("All key limits exceeded", "NAVER_TREND", error_log, "")
                            raise Exception(error_log)

                        delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                        print(f"[INFO] {delay:.1f}초 대기 후 새 키({current_key_index + 1}/{len(CLIENT_KEYS)})로 재시도")
                        time.sleep(delay)
                        continue

                    else:
                        response.raise_for_status()

                except Exception as e:
                    if "모든 API 키가 한도에 도달했습니다" not in str(e):
                        error_log = f"NAVER TREND API 요청 중 예외 발생 {attempt} : {e}"
                        insert_error_log("Failed API Requests", "NAVER_TREND", error_log, "")

                    delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                    time.sleep(delay)
                    continue

            error_log = "모든 재시도 실패 (backoff_with_key_rotation)"
            insert_error_log("Failed All Retry", "NAVER_TREND", error_log, "")
            raise Exception("모든 재시도 실패 (backoff_with_key_rotation)")

        return wrapper

    return decorator


# -----------------------------------------------------
# cmp_list 조회 함수
# -----------------------------------------------------
@backoff_with_key_rotation(max_retries=5, base_delay=2)
def call_naver_trend_api(start_date: str, end_date: str, keyword_groups: list):
    global current_key_index

    key = CLIENT_KEYS[current_key_index]
    client_id = key["id"]
    client_secret = key["secret"]

    url = "https://openapi.naver.com/v1/datalab/search"
    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret,
        "Content-Type": "application/json",
    }

    body = {
        "startDate": start_date,
        "endDate": end_date,
        "timeUnit": "month",
        "keywordGroups": keyword_groups,
    }

    return requests.post(url=url, headers=headers, json=body)


def main():
    try:
        company_list = get_cmp_list("NAVER_TREND")
        start_date = "2022-01-01"
        today = datetime.date.today().strftime("%Y-%m-%d")
        end_date = today

        chunk_size = 5

        es = None
    except Exception as e:
        print(e)
    try:
        try:
            es = get_es_conn()
        except Exception as e:
            error_detail = traceback.format_exc()
            insert_error_log("Elasticsearch connection", "NAVER_TREND", f"Elasticsearch 연결 실패 : {e}", error_detail)
            raise

        for i in tqdm(range(0, len(company_list), chunk_size), desc="기업 트랜드 수집", unit="개"):
            chunk = company_list[i:i + chunk_size]
            keyword_groups = []
            now = datetime.datetime.now()

            for c in chunk:
                clean_name = re.sub(r'\(.*?\)', '', c["CMP_NM"]).strip()
                keyword_groups.append({
                    "groupName": clean_name,
                    "keywords": [clean_name],
                })

            result = call_naver_trend_api(start_date, end_date, keyword_groups)

            if result:

                for idx, r in enumerate(result["results"]):
                    naver_trends = []
                    naver_trends.extend(
                        list(
                            map(
                                lambda o: {"date": o["period"], "ratio": float(o["ratio"])},
                                r["data"]
                            )
                        )
                    )

                    try:
                        insert_naver_trend(es, naver_trends, chunk[idx]["BIZ_NO"])
                        insert_check_log(chunk[idx]["BIZ_NO"], "NAVER_TREND", now)
                        insert_cmp_data_log(chunk[idx]["BIZ_NO"], "NAVER_TREND", len(naver_trends), now)
                        # print(f"{r['title']} - {len(naver_trends)} 저장 완료")
                    except Exception as e:
                        error_detail = traceback.format_exc()
                        insert_error_log("Insert data", "NAVER_TREND", f"데이터 삽입 실패({chunk[idx]['BIZ_NO']}) : {e}",
                                         error_detail)
    except Exception as e:
        print(e)
    finally:
        if es:
            es.close()


# ======================================================
# 실행부
# ======================================================
if __name__ == "__main__":
    email = os.getenv("EMAIL")
    password = os.getenv("APP_PW")
    exit_message = "N/A"
    try:
        main()
        exit_message = "프로그램 정상 종료"
    except Exception as e:
        exit_message = f"프로그램 예외 종료: {e}"
    finally:
        # 정상 종료든 예외 종료든 한번만 알림
        send_naver_alert(email, email, password, f"NAVER_TREND 프로그램 종료됨: {exit_message}")

