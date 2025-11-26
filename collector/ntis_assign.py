import requests
import xmltodict
import functools
import time
import random
import json
import re
from tqdm import tqdm
from db.mysql import *
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
                    error_log = f"NTIS_ASSIGN API 요청 중 예외 발생 {attempt}: {e}"
                    insert_error_log("Failed API Requests", "NTIS_ASSIGN", error_log, "")

                # 백오프 대기 시간 계산
                delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                print(f"[INFO] {delay:.1f}초 대기 후 재시도 ({attempt}/{max_retries})")
                time.sleep(delay)

            error_log = "모든 재시도 실패(backoff_retry)"
            insert_error_log("Failed All Retry", "NTIS_ASSIGN", error_log, "")
            raise Exception("모든 재시도 실패(backoff_retry)")
        return wrapper
    return decorator

@backoff_retry(max_retries=5, base_delay=2, allowed_statuses=(429,))
def get_ntis_assign(comp_name: str) -> requests.Response:
    BASE_URL = "https://www.ntis.go.kr/rndopen/openApi/public_project?"
    API_KEY = os.getenv("NTIS_API_KEY")

    params = {
        "apprvKey": API_KEY,
        "collection": "project",
        "addQuery": f"PB01={comp_name}",
        "searchRnkn": "DATE/DESC",
        "startPosition": 1,
        "displayCnt": 1000,
    }

    query = urlencode(params, encoding="utf-8")
    # print(BASE_URL + query)
    return requests.get(BASE_URL + query)

def get_ntis_assign_json(comp_name: str) -> dict:
    response = get_ntis_assign(comp_name)
    return xmltodict.parse(response.text)

def main():
    es = None

    try:
        try:
            es = get_es_conn()
        except Exception as e:
            error_detail = traceback.format_exc()
            insert_error_log("Elasticsearch connection", "NTIS_ASSIGN", f"Elasticsearch 연결 실패 : {e}", error_detail)
            raise

        try:
            companies = get_cmp_list("NTIS_ASSIGN")
        except Exception as e:
            insert_error_log("Get Cmp List", "NTIS_ASSIGN", f"기업 목록 조회 실패: {e}", "")
            raise

        # with open(r"/home/bax/fncsp/db/fianl_results.json", "r", encoding="utf-8") as f:
        #     companies = json.load(f)

        for idx, company in enumerate(tqdm(companies, desc="기업 R&D 과제목록 수집", unit="개"), 1):
            comp_name = ""
            biz_no = ""
            now = datetime.now()

            try:
                biz_no = company["BIZ_NO"]
                comp_name = company["CMP_NM"]
                clean_comp_name = re.sub(r'\(.*?\)', '', comp_name)

                response = get_ntis_assign_json(comp_name)

                ntis_assigns = []

                try:
                    ntis_assigns_raw = response["RESULT"]["RESULTSET"].get("HIT", [])

                    # 리스트가 아니면 리스트로 변환
                    ntis_assigns = ntis_assigns_raw if isinstance(ntis_assigns_raw, list) else [ntis_assigns_raw]
                except Exception as e:
                    print(f"{comp_name}({biz_no}  기업의 R&D 과제목록이 없음)")

                results = []
                for assign in ntis_assigns:
                    result = {}

                    result["ProjectNo"] = assign.get("ProjectNumber")
                    dup = get_project_no(es, "ntis_assign", biz_no, result["ProjectNo"])
                    if dup:
                        tqdm.write(f"{comp_name} : 중복")
                        raise DuplicateError

                    result["ProjectNameKR"] = (assign.get("ProjectTitle") or {}).get("Korean")
                    result["ProjectNameEN"] = (assign.get("ProjectTitle") or {}).get("English")

                    managers = (assign.get("Manager") or {}).get("Name")
                    result["MangerName"] = managers.split(";") if managers else None

                    researchers = (assign.get("Researchers") or {}).get("Name")
                    result["ResearcherName"] = researchers.split(";") if researchers else None
                    result["ResManCount"] = (assign.get("Researchers") or {}).get("ManCount")
                    result["ResWmanCount"] = (assign.get("Researchers") or {}).get("WomanCount")

                    result["GoalFull"] = (assign.get("Goal") or {}).get("Full")
                    result["GaolTeaser"] = (assign.get("Goal") or {}).get("Teaser")

                    result["AbstractFull"] = (assign.get("Abstract") or {}).get("Full")
                    result["AbstractTeaser"] = (assign.get("Abstract") or {}).get("Teaser")

                    result["EffectFull"] = (assign.get("Effect") or {}).get("Full")
                    result["EffectTeaser"] = (assign.get("Effect") or {}).get("Teaser")

                    result["KeywordKR"] = (assign.get("Keyword") or {}).get("Korean")
                    result["KeywordEN"] = (assign.get("Keyword") or {}).get("English")

                    result["OrderAgencyName"] = (assign.get("OrderAgency") or {}).get("Name")
                    result["ResearchagencyName"] = (assign.get("ResearchAgency") or {}).get("Name")

                    result["BudgetProjectName"] = (assign.get("BudgetProject") or {}).get("Name")
                    result["BusinessName"] = assign.get("BusinessName")
                    result["BigprojectTitle"] = assign.get("BigprojectTitle")

                    result["ManageagencyName"] = (assign.get("ManageAgency") or {}).get("Name")
                    result["MinistryName"] = (assign.get("Ministry") or {}).get("Name")

                    project_year = assign.get("ProjectYear")
                    result["ProjectYear"] = datetime.strptime(project_year, "%Y").strftime(
                        "%Y") if project_year else None

                    project_start = (assign.get("ProjectPeriod") or {}).get("Start")
                    project_end = (assign.get("ProjectPeriod") or {}).get("End")
                    project_total_start = (assign.get("ProjectPeriod") or {}).get("TotalStart")
                    project_total_end = (assign.get("ProjectPeriod") or {}).get("TotalEnd")

                    result["ProjectStart"] = datetime.strptime(project_start, "%Y%m%d").strftime(
                        "%Y-%m-%d") if project_start else None
                    result["ProjectEnd"] = datetime.strptime(project_end, "%Y%m%d").strftime(
                        "%Y-%m-%d") if project_end else None
                    result["ProjectToStart"] = parser.parse(project_total_start).strftime(
                        "%Y-%m-%d") if project_total_start else None
                    result["ProjectToEnd"] = parser.parse(project_total_end).strftime(
                        "%Y-%m-%d") if project_total_end else None

                    result["OrganizationpNo"] = assign.get("OrganizationPNumber")

                    seq1 = next(
                        (item for item in assign.get("ScienceClass", []) if item.get("@sequence") == "1"),
                        {}
                    )
                    result["Scienceclass_New_1_Large_code"] = (seq1.get("Large") or {}).get("@code")
                    result["Scienceclass_New_1_Large"] = (seq1.get("Large") or {}).get("#text")
                    result["Scienceclass_Medium_1_Large_code"] = (seq1.get("Medium") or {}).get("@code")
                    result["Scienceclass_Medium_1_Large"] = (seq1.get("Medium") or {}).get("#text")
                    result["Scienceclass_Small_1_Large_code"] = (seq1.get("Small") or {}).get("@code")
                    result["Scienceclass_Small_1_Large"] = (seq1.get("Small") or {}).get("#text")

                    result["Ministryscience_Class_Large"] = (assign.get("MinistryScienceClass") or {}).get("Large")
                    result["Ministryscience_Class_Medium"] = (assign.get("MinistryScienceClass") or {}).get("Medium")
                    result["Ministryscience_Class_Small"] = (assign.get("MinistryScienceClass") or {}).get("Small")

                    result["Tempscience_Class_Large"] = (assign.get("TempScienceClass") or {}).get("Large")
                    result["Tempscience_Class_Medium"] = (assign.get("TempScienceClass") or {}).get("Medium")
                    result["Tempscience_Class_Small"] = (assign.get("TempScienceClass") or {}).get("Small")

                    result["PerformagentCode"] = (assign.get("PerformAgent") or {}).get("@code")
                    result["Performagent"] = (assign.get("PerformAgent") or {}).get("#text")

                    result["DevelopmentPhasesCode"] = (assign.get("DevelopmentPhase") or {}).get("@code")
                    result["DevelopmentPhase"] = (assign.get("DevelopmentPhase") or {}).get("#text")

                    result["TechLifecycleCode"] = (assign.get("TechnologyLifecycle") or {}).get("@code")
                    result["TechLifecycle"] = (assign.get("TechnologyLifecycle") or {}).get("#text")

                    result["RegionCode"] = (assign.get("Region") or {}).get("@code")
                    result["Region"] = (assign.get("Region") or {}).get("#text")

                    result["EconomicSocialGoal"] = assign.get("EconomicSocialGoal")

                    result["SixtechCode"] = (assign.get("SixTechnology") or {}).get("@code")
                    result["Sixtech"] = (assign.get("SixTechnology") or {}).get("#text")

                    apply_area = assign.get("ApplyArea") or {}
                    result["ApplyareaFirstCode"] = (apply_area.get("First") or {}).get("@code")
                    result["ApplyareaFirst"] = (apply_area.get("First") or {}).get("#text")
                    result["ApplyareaSecondCode"] = (apply_area.get("Second") or {}).get("@code")
                    result["ApplyareaSecond"] = (apply_area.get("Second") or {}).get("#text")
                    result["ApplyareaThirdCode"] = (apply_area.get("Third") or {}).get("@code")
                    result["ApplyareaThird"] = (apply_area.get("Third") or {}).get("#text")

                    result["ContinuousFlag"] = assign.get("ContinuousFlag")
                    result["PolicyProjectFlag"] = assign.get("PolicyProjectFlag")

                    result["GovernFunds"] = assign.get("GovernmentFunds")
                    result["SbusinessFunds"] = assign.get("SbusinessFunds")
                    result["TotFunds"] = assign.get("TotalFunds")

                    result["CorporateRegistrationNo"] = assign.get("CorporateRegistrationNumber")
                    result["SeriesProject"] = assign.get("SeriesProject")

                    results.append(result)

                try:
                    insert_ntis_assign(es, results, biz_no)
                    insert_check_log(biz_no, "NTIS_ASSIGN", now)
                    insert_cmp_data_log(biz_no, "NTIS_ASSIGN", len(results), now)
                    print(f"{comp_name} - {len(ntis_assigns)}건 저장 완료")
                    time.sleep(1)
                except Exception as e:
                    error_detail = traceback.format_exc()
                    insert_error_log("Insert data","NTIS_ASSIGN", f"데이터 삽입 실패({biz_no}) : {e}", error_detail)
            except DuplicateError as e:
                if results:
                    insert_ntis_rnd_paper(es, results, biz_no)
                    insert_check_log(biz_no, "NTIS_RND_PAPER", now)
                    print(f"{comp_name} - {len(results)}건 저장 완료")
                    time.sleep(1)
            except Exception as e:
                error_detail = traceback.format_exc()
                insert_error_log("Process company", "NTIS_ASSIGN", f"{comp_name}({biz_no}) 기업 처리중 오류 발생 : {e}", error_detail)
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

        send_naver_alert(email, email, password, f"NTIS_ASSIGN 프로그램 종료 됨 : {exit_message}")

