import os
import re
import math
import json
import pandas as pd
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers
from config import cd_nm_mapping, name_mapping

# ======================================================
# 1. 환경 변수
# ======================================================
load_dotenv()

ES_HOST = os.getenv("ELASTICSEARCH_HOST")
ES_ID = os.getenv("ELASTICSEARCH_ID")
ES_PASSWORD = os.getenv("ELASTICSEARCH_PASSWORD")

ES_INDEX = "source_data"
DATA_TYPE = "nicednb_fnl"
SEARCH_ID = "autoSystem"

BASE_DIR = Path("cmp_fnl_data")

# ======================================================
# 2. Elasticsearch Client
# ======================================================
es = Elasticsearch(
    hosts=ES_HOST,
    basic_auth=(ES_ID, ES_PASSWORD) if ES_ID else None,
    verify_certs=False,
    ssl_show_warn=False,
    request_timeout=60
)

# ======================================================
# 3. 유틸 함수
# ======================================================
def normalize_date(date_str):
    m = re.search(r"(\d{4})[.\-/](\d{2})[.\-/](\d{2})", str(date_str))
    return "".join(m.groups()) if m else None

def normalize_account_name(name):
    if name is None:
        return None
    return (
        str(name)
        .replace("\u00a0", " ")  # NBSP 제거
        .strip()                 # 앞뒤 공백 제거
    )

def to_null(v):
    if v is None:
        return None
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, str) and v.strip() == "":
        return None
    return v

def calc_growth(curr, prev):
    if curr is None or prev in (None, 0):
        return None
    return round((curr - prev) / prev * 100, 2)

# ======================================================
# 4. 지표 계산 함수
# ======================================================
# 유동비율
def calc_mc1100(curr):
    ca = curr.get("유동자산")
    cl = curr.get("유동부채")
    if ca is None or cl in (None, 0):
        return None
    return round(ca / cl * 100, 2)

# 총자산 증가율
def calc_ma1100(curr, prev):
    return calc_growth(curr.get("자산총계"), prev.get("자산총계"))

# 매출 증가율
def calc_ma1600(curr, prev):
    return calc_growth(curr.get("매출액"), prev.get("매출액"))

# 유형자산 증가율
def calc_ma1200(curr, prev):
    return calc_growth(curr.get("유형자산"), prev.get("유형자산"))

# ======================================================
# 5. 공통 ES 문서 생성 함수 (중복 제거 핵심)
# ======================================================
def build_metric_es_doc(
    biz_no, base_date, acct_cd, acct_nm,
    fs_rpt_gb, fs_rpt_nm, value
):
    if value is None:
        return None

    return {
        "_index": ES_INDEX,
        "_id": f"{biz_no}_{base_date}_{acct_cd}",
        "_source": {
            "BusinessNum": biz_no,
            "SearchDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
            "DataType": DATA_TYPE,
            "SearchID": SEARCH_ID,
            "Data": {
                "stYear": base_date[:4],
                "stGb": fs_rpt_gb,
                "stNm": fs_rpt_nm,
                "dtGb": None,
                "dtNm": None,
                "sttYyyyNm": base_date[:6],
                "acctCd": acct_cd,
                "acctNm": acct_nm,
                "acctAmt": None,
                "cmpsRate": value,
                "icdcRate": None
            }
        }
    }

# ======================================================
# 6. 메모리 캐시 & Bulk 버퍼
# ======================================================
financial_cache = defaultdict(lambda: defaultdict(dict))
bulk_actions = []

# ======================================================
# 7. 엑셀 → ES (재무 데이터)
# ======================================================
for file in BASE_DIR.rglob("*.xlsx"):
    parts = file.stem.split("_")
    if len(parts) < 2:
        continue

    biz_no = parts[1]
    print(f"[PROCESS] {file.name} / {biz_no}")

    df = pd.read_excel(file)
    df.columns = df.iloc[0]
    df = df.iloc[2:].reset_index(drop=True)

    date_cols = {c: normalize_date(c) for c in df.columns if normalize_date(c)}

    for col, base_date in date_cols.items():
        for _, row in df.iterrows():
            acct = normalize_account_name(row["계정명"])
            amt = to_null(row[col])

            if acct not in name_mapping:
                continue

            std_name = name_mapping[acct]
            if std_name not in cd_nm_mapping:
                continue

            meta = cd_nm_mapping[std_name]
            financial_cache[biz_no][base_date][std_name] = amt

            is_rate = std_name.endswith(("율", "률"))
            acct_amt = float(amt) if amt is not None else None

            bulk_actions.append({
                "_index": ES_INDEX,
                "_id": f"{biz_no}_{base_date}_{meta['cd']}",
                "_source": {
                    "BusinessNum": biz_no,
                    "SearchDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                    "DataType": DATA_TYPE,
                    "SearchID": SEARCH_ID,
                    "Data": {
                        "stYear": base_date[:4],
                        "stGb": meta["fs_rpt_gb"],
                        "stNm": meta["fs_rpt_nm"],
                        "dtGb": None,
                        "dtNm": None,
                        "sttYyyyNm": base_date[:6],
                        "acctCd": meta["cd"],
                        "acctNm": std_name,
                        "acctAmt": None if is_rate else acct_amt,
                        "cmpsRate": acct_amt if is_rate else None,
                        "icdcRate": None
                    }
                }
            })

# ======================================================
# 8. 지표 계산 → ES
# ======================================================
for biz_no, date_map in financial_cache.items():
    dates = sorted(date_map.keys())

    for i, curr_d in enumerate(dates):
        curr = date_map[curr_d]
        prev = date_map[dates[i - 1]] if i > 0 else None

        docs = [
            build_metric_es_doc(biz_no, curr_d, "MA1100", "총자산 증가율", "A", "성장성지표",
                                 calc_ma1100(curr, prev) if prev else None),
            build_metric_es_doc(biz_no, curr_d, "MA1600", "매출 증가율", "A", "성장성지표",
                                 calc_ma1600(curr, prev) if prev else None),
            build_metric_es_doc(biz_no, curr_d, "MA1200", "유형자산 증가율", "A", "성장성지표",
                                 calc_ma1200(curr, prev) if prev else None),
            build_metric_es_doc(biz_no, curr_d, "MC1100", "유동비율", "C", "안정성지표",
                                 calc_mc1100(curr)),
        ]

        for d in docs:
            if d:
                bulk_actions.append(d)

# ======================================================
# 9-0. 테스트용 JSON 파일로 저장  -> 테스트 안할 시에는 주석 처리
# ======================================================
# TEST_OUTPUT_DIR = Path("./test_output")
# TEST_OUTPUT_DIR.mkdir(exist_ok=True)
#
# timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
# json_path = TEST_OUTPUT_DIR / f"es_bulk_preview_{timestamp}.json"
#
# with open(json_path, "w", encoding="utf-8") as f:
#     json.dump(bulk_actions, f, ensure_ascii=False, indent=2)
#
# print(f"[TEST] ES bulk 데이터 JSON 생성 완료: {json_path}")

# ======================================================
# 9. Bulk Insert -> 테스트시에는 주석 처리
# ======================================================
if bulk_actions:
    helpers.bulk(es, bulk_actions)
    print(f"[DONE] ES 적재 완료: {len(bulk_actions)}건")
else:
    print("[DONE] 적재 데이터 없음")
