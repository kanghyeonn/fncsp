import os
import requests
import xmltodict
import functools
import time
import random
import json
import re
from tqdm import tqdm
from db.mysql import *
from datetime import datetime
from dateutil import parser
from db.es import *
from urllib.parse import urlencode

def backoff_retry(max_retries=5, base_delay=2, allowed_statuses=(429,)):
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    response = func(*args, **kwargs)

                    # 정상 응답이면 JSON 반환
                    if response.status_code == 200:
                        return response

                    # 허용된 상태코드(예: 429 Too Many Requests)
                    elif response.status_code in allowed_statuses:
                        print(f"[WARN] 상태코드 {response.status_code} 발생 → 재시도")

                    else:
                        # 나머지 상태코드는 즉시 예외 처리
                        response.raise_for_status()

                except Exception as e:
                    error_log = f"NTIS ORG INFO API 요청 중 예외 발생 {attempt}: {e}"
                    insert_error_log("Failed API Requests", "NTIS_ORG_INFO", error_log, "")

                # 백오프 대기 시간 계산
                delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                print(f"[INFO] {delay:.1f}초 대기 후 재시도 ({attempt}/{max_retries})")
                time.sleep(delay)

            error_log = "모든 재시도 실패(backoff_retry)"
            insert_error_log("Failed All Retry", "NTIS_ORG_INFO", error_log, "")
            raise Exception("모든 재시도 실패(backoff_retry)")
        return wrapper
    return decorator

@backoff_retry(max_retries=5, base_delay=2, allowed_statuses=(429,))
def get_ntis_org_info(biz_no: str) -> requests.Response:
    BASE_URL = "https://www.ntis.go.kr/rndopen/openApi/orgRndInfo?"
    API_KEY = os.getenv("NTIS_API_KEY")

    params = {
        "apprvKey": API_KEY,
        "reqOrgBno": biz_no,
    }

    query = urlencode(params, encoding="utf-8")
    return requests.get(BASE_URL + query)

def get_ntis_org_info_json(biz_no: str) -> dict:
    response = get_ntis_org_info(biz_no)
    return xmltodict.parse(response.text)

def main():
    es = None

    try:
        try:
            es = get_es_conn()
        except Exception as e:
            error_detail = traceback.format_exc()
            insert_error_log("Elasticsearch connection", "NTIS_ORG_INFO", f"Elasticsearch 연결 실패 : {e}", error_detail)
            raise

        try:
            companies = get_cmp_list("NTIS_ORG_INFO")
        except Exception as e:
            insert_error_log("Get Cmp List", "NTIS_ORG_INFO", f"기업 목록 조회 실패: {e}", "")
            raise

        # with open(r"/home/bax/fncsp/db/fianl_results.json", "r", encoding="utf-8") as f:
        #     companies = json.load(f)

        for idx, company in enumerate(tqdm(companies, desc="NTIS 수행 기관 정보 수집", unit="개"), 1):
            comp_name = ""
            biz_no = ""
            now = datetime.now()

            try:
                biz_no = company["BIZ_NO"]
                comp_name = company["CMP_NM"]
                clean_comp_name = re.sub(r'\(.*?\)', '', comp_name)

                ntis_org_info = get_ntis_org_info_json(biz_no)

                result = {}

                org_info = (ntis_org_info.get("response") or {}).get("body")

                if org_info:
                    result["orgName"] = org_info.get("orgName")
                    result["orgPageInfo"] = org_info.get("orgPageInfo")
                    result["rndKorKeyword"] = org_info.get("rndKorKeword")
                    result["rndEngKeyword"] = org_info.get("rndEngKeword")
                    result["rndCategory"] = org_info.get("rndCategory")

                    status_list = []
                    rnd_status = org_info.get("rndStatusList")
                    rnd_status_list = rnd_status if isinstance(rnd_status, list) else [rnd_status]

                    for rnd_status in rnd_status_list:
                        status = {}
                        status["year"] = datetime.strptime(rnd_status["year"], "%Y").strftime("%Y")
                        status["pjtCnt"] = rnd_status.get("pjtCnt")
                        status["rndBudget"] = rnd_status.get("rndBudget")
                        status["govBudget"] = rnd_status.get("govBudget")
                        status["paperCnt"] = rnd_status.get("paperCnt")
                        status["patentCnt"] = rnd_status.get("patentCnt")
                        status["reportCnt"] = rnd_status.get("reportCnt")

                        status_list.append(status)

                    result["rndStatusList"] = status_list
                else:
                    result = None
                    print(f"{comp_name} 검색결과 없음")

                try:
                    insert_ntis_org_info(es, result, biz_no)
                    insert_check_log(biz_no, "NTIS_ORG_INFO", now)
                    if result is None:
                        insert_cmp_data_log(biz_no, "NTIS_ORG_INFO", 0, now)
                    else:
                        insert_cmp_data_log(biz_no, "NTIS_ORG_INFO", len(result), now)
                    print(f"{comp_name} 저장 완료")
                    time.sleep(1)
                except Exception as e:
                    error_detail = traceback.format_exc()
                    insert_error_log("Insert data", "NTIS_ORG_INFO", f"데이터 삽입 실패({biz_no}) : {e}", error_detail)
            except Exception as e:
                error_detail = traceback.format_exc()
                insert_error_log("Process company", "NTIS_ORG_INFO", f"{comp_name}({biz_no}) 기업 처리중 오류 발생 : {e}", error_detail)
    finally:
        if es:
            es.close()

if __name__ == "__main__":
    email = os.getenv("EMAIL")
    password = os.getenv("PASSWORD")
    exit_message = "N/A"
    try:
        main()
        exit_message = "프로그램 정상 종료"
    except Exception as e:
        exit_message = f"프로그램 예외 종료 : {e}"
    finally:
        from alter import send_naver_alert

        send_naver_alert(email, email, password, f"NTIS_ORG_INFO 프로그램 종료 됨 : {exit_message}")

