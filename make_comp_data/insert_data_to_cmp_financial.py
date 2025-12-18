import os
import pandas as pd
import re
import pymysql
import math
from pathlib import Path
from dotenv import load_dotenv
from collections import defaultdict
from datetime import datetime

load_dotenv()

# -------------------------------------------------------
# 계정명 매핑 (현재 → 기존)
# -------------------------------------------------------
NEW_TO_OLD_ACCOUNT_NAME_MAP = {
    "매출액(*)": "매출액",
    "매출원가(*)": "매출원가",
    "매출총이익(손실)": "매출총이익",
    "판매비와관리비(*)": "판매비와관리비",
    "영업이익(손실)": "영업이익",
    "이자비용": "이자비용",
    "법인세비용": "법인세비용",
    "당기순이익(순손실)": "당기순이익(손실)",
    "급여(*)": "인건비",
    "복리후생비": "복리후생비",
    "영업외비용(*)": "영업외비용",

    "유형자산(*)": "유형자산",
    "유동자산(*)": "유동자산",
    "당좌자산(*)": "당좌자산",
    "매출채권(*)": "매출채권",
    "재고자산(*)": "재고자산",
    "원재료(*)": "원재료",
    "투자자산(*)": "투자자산",
    "자산(*)": "자산총계",
    "유동부채(*)": "유동부채",
    "비유동부채(*)": "비유동부채",
    "부채(*)": "부채총계",
    "자본잉여금(*)": "자본잉여금",
    "이익잉여금(*)": "이익잉여금",
    "자본(*)": "자본총계",
}

# -------------------------------------------------------
# DB 연결
# -------------------------------------------------------
conn = pymysql.connect(
    host=os.getenv("DIC_MYSQL_HOST"),
    user=os.getenv("DIC_MYSQL_USER"),
    password=os.getenv("DIC_MYSQL_PASSWORD"),
    db=os.getenv("DIC_MYSQL_DATABASE"),
    cursorclass=pymysql.cursors.DictCursor
)
cursor = conn.cursor()

# -------------------------------------------------------
# 로그 수집용
# -------------------------------------------------------
fin_accounts_by_biz = defaultdict(set)
metric_accounts_by_biz = defaultdict(set)
printed_biz_set = set()
log_lines = []

# -------------------------------------------------------
# 공통 유틸
# -------------------------------------------------------
def normalize_date(date_str):
    match = re.search(r"(\d{4})[.\-/](\d{2})[.\-/](\d{2})", str(date_str))
    if match:
        y, m, d = match.groups()
        return f"{y}{m}{d}"
    return None

def to_null(value):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, str) and value.lower() in ["nan", "none", ""]:
        return None
    return value

def get_company_name(biz_no):
    cursor.execute(
        "SELECT CMP_NM FROM cmp_list WHERE BIZ_NO=%s LIMIT 1",
        (biz_no,)
    )
    row = cursor.fetchone()
    return row["CMP_NM"] if row else "회사명없음"

# -------------------------------------------------------
# tm_nicednb_cd 로드
# -------------------------------------------------------
cursor.execute("SELECT CD, CD_NM FROM tm_nicednb_cd")
ACCOUNT_CD_MAP = {row["CD_NM"]: row["CD"] for row in cursor.fetchall()}

# -------------------------------------------------------
# UPSERT SQL
# -------------------------------------------------------
UPSERT_SQL = """
INSERT INTO cmp_financial (
    BIZ_NO, BASE_DATE, ACCOUNT_CD,
    KSIC_CODE_LVL2, ACCOUNT_NAME, ACCOUNT_AMT,
    ES_YN, ES_DATE, REG_DATE, REG_USER, EDIT_DATE, EDIT_USER
)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s,NOW(),%s)
ON DUPLICATE KEY UPDATE
    ACCOUNT_AMT = VALUES(ACCOUNT_AMT),
    EDIT_DATE = NOW(),
    EDIT_USER = VALUES(EDIT_USER);
"""

# -------------------------------------------------------
# DB 조회
# -------------------------------------------------------
def fetch_amt(biz_no, base_date, acct_name):
    cursor.execute(
        """
        SELECT ACCOUNT_AMT
        FROM cmp_financial
        WHERE BIZ_NO=%s AND BASE_DATE=%s AND ACCOUNT_NAME=%s
        LIMIT 1
        """,
        (biz_no, base_date, acct_name)
    )
    row = cursor.fetchone()
    return row["ACCOUNT_AMT"] if row else None

def find_prev_base_date(biz_no, base_date):
    cursor.execute(
        """
        SELECT MAX(BASE_DATE) AS PREV
        FROM cmp_financial
        WHERE BIZ_NO=%s AND BASE_DATE < %s
        """,
        (biz_no, base_date)
    )
    row = cursor.fetchone()
    return row["PREV"] if row and row["PREV"] else None

# -------------------------------------------------------
# 지표 계산
# -------------------------------------------------------
def calc_growth(curr, prev):
    if curr is None or prev in (None, 0):
        return None
    return round((curr - prev) / prev * 100, 2)

def calc_ma1100(biz_no, base_date, prev_date):
    return calc_growth(
        fetch_amt(biz_no, base_date, "자산총계"),
        fetch_amt(biz_no, prev_date, "자산총계")
    )

def calc_ma1200(biz_no, base_date, prev_date):
    return calc_growth(
        fetch_amt(biz_no, base_date, "유형자산"),
        fetch_amt(biz_no, prev_date, "유형자산")
    )

def calc_ma1600(biz_no, base_date, prev_date):
    return calc_growth(
        fetch_amt(biz_no, base_date, "매출액"),
        fetch_amt(biz_no, prev_date, "매출액")
    )

def calc_ma5100(biz_no, base_date):
    return fetch_amt(biz_no, base_date, "자산총계")

def calc_ma5300(biz_no, base_date):
    return fetch_amt(biz_no, base_date, "자본총계")

def calc_mc1100(biz_no, base_date):
    ca = fetch_amt(biz_no, base_date, "유동자산")
    cl = fetch_amt(biz_no, base_date, "유동부채")
    if ca is None or cl in (None, 0):
        return None
    return round(ca / cl * 100, 2)

def upsert_metric(biz_no, base_date, cd, name, value):
    if value is None:
        return
    cursor.execute(
        UPSERT_SQL,
        (biz_no, base_date, cd, None, name, value, None, None, "etl_metric", "etl_metric")
    )
    metric_accounts_by_biz[biz_no].add(name)

def process_metrics(biz_no, base_date):
    prev = find_prev_base_date(biz_no, base_date)
    if prev:
        upsert_metric(biz_no, base_date, "MA1100", "총자산 증가율", calc_ma1100(biz_no, base_date, prev))
        upsert_metric(biz_no, base_date, "MA1600", "매출 증가율", calc_ma1600(biz_no, base_date, prev))
        upsert_metric(biz_no, base_date, "MA1200", "유형자산 증가율", calc_ma1200(biz_no, base_date, prev))
    upsert_metric(biz_no, base_date, "MA5100", "총자산", calc_ma5100(biz_no, base_date))
    upsert_metric(biz_no, base_date, "MA5300", "자기자본", calc_ma5300(biz_no, base_date))
    upsert_metric(biz_no, base_date, "MC1100", "유동비율", calc_mc1100(biz_no, base_date))

# -------------------------------------------------------
# 메인 ETL
# -------------------------------------------------------
BASE_DIR = Path("KODATA수집")

for file in BASE_DIR.rglob("*.xlsx"):
    parts = file.stem.split("_")
    if len(parts) < 2:
        continue

    biz_no = parts[1]

    df = pd.read_excel(file)
    df.columns = df.iloc[0]
    df = df.iloc[2:].reset_index(drop=True)

    date_cols = {c: normalize_date(c) for c in df.columns if normalize_date(c)}

    for col, base_date in date_cols.items():
        for _, row in df.iterrows():
            acct_name = to_null(row["계정명"]).strip()
            # print(acct_name)
            amt = to_null(row[col])

            if acct_name not in NEW_TO_OLD_ACCOUNT_NAME_MAP:
                continue

            std_name = NEW_TO_OLD_ACCOUNT_NAME_MAP[acct_name]

            if std_name not in ACCOUNT_CD_MAP:
                print(std_name)
                continue

            cursor.execute(
                UPSERT_SQL,
                (biz_no, base_date, ACCOUNT_CD_MAP[std_name], None,
                 std_name, amt, None, None, "etl", "etl")
            )
            fin_accounts_by_biz[biz_no].add(std_name)

        process_metrics(biz_no, base_date)

    # if biz_no not in printed_biz_set:
    #     printed_biz_set.add(biz_no)
    #
    #     cmp_nm = get_company_name(biz_no)
    #     fin_list = sorted(fin_accounts_by_biz[biz_no])
    #
    #     metric_list = sorted(metric_accounts_by_biz[biz_no])
    #
    #     block = [
    #         f"{cmp_nm}({biz_no})",
    #         f" ├─ FIN   : {', '.join(fin_list) if fin_list else '없음'}",
    #         f" └─ METRIC: {', '.join(metric_list) if metric_list else '없음'}",
    #         ""
    #     ]
    #
    #     for line in block:
    #         print(line)
    #         log_lines.append(line)

# 모든 ETL 끝난 후
for biz_no in fin_accounts_by_biz:
    cmp_nm = get_company_name(biz_no)

    fin_list = sorted(fin_accounts_by_biz[biz_no])
    metric_list = sorted(metric_accounts_by_biz[biz_no])

    block = [
        f"{cmp_nm}({biz_no})",
        f" ├─ FIN   : {', '.join(fin_list) if fin_list else '없음'}",
        f" └─ METRIC: {', '.join(metric_list) if metric_list else '없음'}",
        ""
    ]

    for line in block:
        print(line)
        log_lines.append(line)


# -------------------------------------------------------
# 로그 파일 저장
# -------------------------------------------------------
today = datetime.now().strftime("%Y%m%d")
log_file = f"삽입재무데이터/{today}_삽입재무데이터.log"

with open(log_file, "w", encoding="utf-8") as f:
    f.write("\n".join(log_lines))

conn.commit()
cursor.close()
conn.close()

print(f"\nETL 완료 / 로그 파일 생성: {log_file}")
