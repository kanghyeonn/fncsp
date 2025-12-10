# cmp_financial 테이블에 데이터를 삽입하는 코드

import os
import pandas as pd
import re
import pymysql
import math
from datetime import datetime
from pathlib import Path

"""
!! 파일 명 = "번호_사업자번호_회사명_재무제표명" 준수
!! ex) 1_8108602715_압테로코리아_손익계산서
"""

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
# 날짜 형식(Python datetime) 정규화
# -------------------------------------------------------
def normalize_date(date_str):
    # YYYY-MM-DD, YYYY.MM.DD, YYYY/MM/DD 모두 지원
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


# -------------------------------------------------------
# account code 매핑 테이블 로드
# -------------------------------------------------------
cursor.execute("SELECT CD, CD_NM FROM fs_account_code")
account_map = {row["CD_NM"]: row["CD"] for row in cursor.fetchall()}


# -------------------------------------------------------
# UPSERT SQL
# -------------------------------------------------------
upsert_sql = """
INSERT INTO cmp_financial (
    BIZ_NO, BASE_DATE, ACCOUNT_CD,
    KSIC_CODE_LVL2, ACCOUNT_NAME, ACCOUNT_AMT,
    ES_YN, ES_DATE, REG_DATE, REG_USER, EDIT_DATE, EDIT_USER
)
VALUES (
    %s, %s, %s,
    %s, %s, %s,
    %s, %s, NOW(), %s, NOW(), %s
)
ON DUPLICATE KEY UPDATE
    ACCOUNT_AMT = VALUES(ACCOUNT_AMT),
    EDIT_DATE = NOW(),
    EDIT_USER = VALUES(EDIT_USER);
"""


# -------------------------------------------------------
# 메인 ETL 로직
# -------------------------------------------------------
base = Path("KODATA수집")
insert_count = 0

for file in base.rglob("*.xlsx"):

    fname = file.name
    print(f"\n[파일 처리] {fname}")

    # 파일명에서 biz_no 추출
    parts = file.stem.split("_")
    if len(parts) < 2:
        print("[스킵] 파일명 구조 오류")
        continue

    biz_no = parts[1]

    # cmp_list에 존재하는 회사인지 확인
    cursor.execute("SELECT 1 FROM cmp_list WHERE BIZ_NO=%s", (biz_no,))
    if cursor.fetchone() is None:
        print(f"[스킵] cmp_list에 없는 BIZ_NO = {biz_no}")
        continue

    # 엑셀 로드
    try:
        df = pd.read_excel(file)

        df.columns = df.iloc[0]   # 날짜가 있는 행을 컬럼명으로 설정
        df = df.iloc[1:]          # 컬럼명으로 사용된 첫 행 제거

        df = df.reset_index(drop=True)
        df.columns.tolist()
    except Exception as e:
        print("[에러] 엑셀 읽기 실패:", e)
        continue

    # 계정명 및 모든 연도 컬럼 가져오기
    columns = df.columns.tolist()

    # 날짜 컬럼 자동 탐색 (YYYY-MM-DD 형태)
    date_cols = {}
    for col in columns:
        base_date = normalize_date(col)
        if base_date:  # 날짜 형태면 등록
            date_cols[col] = base_date

    if not date_cols:
        print("[스킵] 날짜 컬럼을 찾지 못함")
        continue

    print("발견된 연도:", date_cols)

    # 계정명 컬럼
    if "계정명" not in df.columns:
        print("[스킵] 계정명 컬럼 없음")
        continue

    df["ACCOUNT_NAME"] = df["계정명"].astype(str).str.strip()

    # 데이터 구간 (1~끝)
    df = df.iloc[1:]

    # 연도별 데이터 처리
    for col, base_date in date_cols.items():
        print(f"  ▶ {base_date} 데이터 처리 중...")

        for _, row in df.iterrows():
            acct_name = to_null(row["ACCOUNT_NAME"])
            acct_amt = to_null(row[col])

            if acct_name in ["", "nan", None]:
                continue

            # 계정코드 매핑
            if acct_name not in account_map:
                print(f"    [스킵] 계정명 매핑 실패 → {acct_name}")
                continue

            acct_cd = account_map[acct_name]

            try:
                cursor.execute(
                    upsert_sql,
                    (
                        biz_no,
                        base_date,
                        acct_cd,
                        None,
                        acct_name,
                        acct_amt,
                        None, None,
                        "etl", "etl"
                    )
                )
                insert_count += 1

            except Exception as e:
                print("[에러] upsert 실패:", e)

conn.commit()
cursor.close()
conn.close()

print("\n===============================================")
print("ETL 완료")
print(f"총 INSERT/UPDATE 처리 건수: {insert_count}")
