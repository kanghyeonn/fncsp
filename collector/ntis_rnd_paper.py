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


class DuplicateError(Exception):
    pass


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
                    error_log = f"NTIS_RND_PAPER API 요청 중 예외 발생 {attempt}: {e}"
                    insert_error_log("Failed API Requests", "NTIS_RND_PAPER", error_log, "")

                # 백오프 대기 시간 계산
                delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                print(f"[INFO] {delay:.1f}초 대기 후 재시도 ({attempt}/{max_retries})")
                time.sleep(delay)

            error_log = "모든 재시도 실패(backoff_retry)"
            insert_error_log("Failed All Retry", "NTIS_RND_PAPER", error_log, "")
            raise Exception("모든 재시도 실패(backoff_retry)")

        return wrapper

    return decorator


@backoff_retry(max_retries=5, base_delay=2, allowed_statuses=(429,))
def get_ntis_rnd_paper(comp_name: str) -> requests.Response:
    BASE_URL = "https://www.ntis.go.kr/rndopen/openApi/rresearchpdf?"
    API_KEY = os.getenv("NTIS_API_KEY")

    params = {
        "apprvKey": API_KEY,
        "collection": "researchpdf",
        "searchField": "PB",
        "addQuery": f"PB01={comp_name}",
        "sortdBy": "DATE/DESC",
        "startPosition": 1,
        "displayCnt": 1000,
        "returnType": json
    }

    query = urlencode(params, encoding="utf-8")
    return requests.get(BASE_URL + query)


def get_ntis_rnd_paper_json(comp_name: str):
    return get_ntis_rnd_paper(comp_name).json()


def main():
    es = None

    try:
        try:
            es = get_es_conn()
        except Exception as e:
            error_detail = traceback.format_exc()
            insert_error_log("Elasticsearch connection", "NTIS_RND_PAPER", error_detail, error_detail)
            raise
        try:
            companies = get_cmp_list("NTIS_RND_PAPER")
        except Exception as e:
            insert_error_log("Get Cmp List", "NTIS_RND_PAPER", f"기업 목록 조회 실패: {e}", "")
            raise

        # with open(r"/home/bax/fncsp/db/fianl_results.json", "r", encoding="utf-8") as f:
        #     companies = json.load(f)

        for idx, company in enumerate(tqdm(companies, desc="NTIS 연구보고서 수집", unit="개"), 1):
            comp_name = ""
            biz_no = ""
            now = datetime.now()

            try:
                biz_no = company["BIZ_NO"]
                comp_name = company["CMP_NM"]
                clean_comp_name = re.sub(r'\(.*?\)', '', comp_name)

                ntis_rnd_paper = get_ntis_rnd_paper_json(clean_comp_name)

                results = []

                try:
                    ntis_rnd_paper_raw = ntis_rnd_paper["RESULT"]["RESULTSET"].get("HIT", [])
                    ntis_rnd_papers = ntis_rnd_paper_raw if isinstance(ntis_rnd_paper_raw, list) else [
                        ntis_rnd_paper_raw]

                except Exception as e:
                    print(f"{comp_name}({biz_no}  기업의 R&D 연구보고서 없음)")

                for rnd_paper in ntis_rnd_papers:
                    result = {}

                    result["PublicationYear"] = datetime.strptime(str(rnd_paper.get("PublicationYear")), "%Y").strftime(
                        "%Y")
                    result["ResearchPublicNo"] = rnd_paper.get("ResearchPublicNo")
                    dup = get_research_public_no(es, "ntis_rnd_paper", biz_no, result["ResearchPublicNo"])
                    if dup:
                        tqdm.write(f"{comp_name} : 중복")
                        raise DuplicateError

                    result["PublicationAgency"] = rnd_paper.get("PublicationAgency")
                    result["ResultTitleKR"] = (rnd_paper.get("ResultTitle") or None).get("Korean")
                    result["ResultTitleEN"] = (rnd_paper.get("ResultTitle") or None).get("English")
                    result["AbstractKR"] = (rnd_paper.get("Abstract") or None).get("Korean")
                    result["AbstractEN"] = (rnd_paper.get("Abstract") or None).get("English")
                    result["KeywordKR"] = (rnd_paper.get("Keyword") or None).get("Korean")
                    result["KeywordEN"] = (rnd_paper.get("Keyword") or None).get("English")
                    result["Contents"] = rnd_paper.get("Contents")
                    result["PublicationCountry"] = rnd_paper.get("PublicationCountry")
                    result["PublicationLanguage"] = rnd_paper.get("PublicationLanguage")
                    result["DocUrl"] = rnd_paper.get("DocUrl")
                    result["ProjectNumber"] = rnd_paper.get("ProjectNumber")
                    result["ProjectTitle"] = rnd_paper.get("ProjectTitle")
                    result["LeadAgency"] = rnd_paper.get("LeadAgency")
                    result["MangerName"] = rnd_paper.get("ManagerName")

                    results.append(result)

                try:
                    insert_ntis_rnd_paper(es, results, biz_no)
                    insert_check_log(biz_no, "NTIS_RND_PAPER", now)
                    insert_cmp_data_log(biz_no, "NTIS_RND_PAPER", len(results), now)
                    print(f"{comp_name} - {len(results)} 저장 완료")
                    time.sleep(1)
                except Exception as e:
                    error_detail = traceback.format_exc()
                    insert_error_log("Insert data", "NTIS_RND_PAPER", error_detail, error_detail)
            except DuplicateError as e:
                if results:
                    insert_ntis_rnd_paper(es, results, biz_no)
                    insert_check_log(biz_no, "NTIS_RND_PAPER", now)
                    print(f"{comp_name} - {len(results)}건 저장 완료")
                    time.sleep(1)
            except Exception as e:
                error_detail = traceback.format_exc()
                insert_error_log("Process company", "NTIS_RND_PAPER", f"{comp_name}({biz_no}) 기업 처리중 오류 발생 : {e}",
                                 error_detail)

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

        send_naver_alert(email, email, password, f"NTIS_RND_PAPER 프로그램 종료 됨 : {exit_message}")