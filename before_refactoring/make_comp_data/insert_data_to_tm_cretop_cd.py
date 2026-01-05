from pathlib import Path
import pandas as pd
import pymysql
import os
import re
from dotenv import load_dotenv

"""
!! 파일 명 = "번호_사업자번호_회사명_재무제표명" 준수
!! ex) 1_8108602715_압테로코리아_손익계산서

재무재표명은 손익계산서, 재무상태표, 제조원가명세서, 이익잉여금처분계산서가 존재 
재무재표명을 추가할려면 code_base 매핑 테이블에 추가 필수 
"""

# 환경 변수 로드
load_dotenv()

conn = pymysql.connect(
    host=os.getenv("DIC_MYSQL_HOST"),
    user=os.getenv("DIC_MYSQL_USER"),
    password=os.getenv("DIC_MYSQL_PASSWORD"),
    db=os.getenv("DIC_MYSQL_DATABASE"),
    cursorclass=pymysql.cursors.DictCursor
)
cursor = conn.cursor()


# -------------------------------------------------------
# 1. 계정명 정규화 함수 (특수문자/괄호 제거 + 공백 정리)
# -------------------------------------------------------
def normalize_cd_nm(name: str) -> str:
    if not isinstance(name, str):
        return name

    # 괄호 및 괄호 안 문자 삭제 예: 매출액(*) → 매출액
    name = re.sub(r"\(\s*\*\s*\)", "", name)

    # 공백 여러개 ⇒ 하나로
    name = re.sub(r"\s+", " ", name)

    # 앞뒤 공백 제거
    return name.strip()


# -------------------------------------------------------
# 2. FS_RPT_NM 별 코드 생성 규칙
# -------------------------------------------------------
code_base = {
    "손익계산서": 10000,
    "재무상태표": 20000,
    "제조원가명세서": 30000,
    "이익잉여금처분계산서": 40000
}

# 각 재무제표 코드 증가 상태 저장용
code_counter = {
    "손익계산서": 1,
    "재무상태표": 1,
    "제조원가명세서": 1,
    "이익잉여금처분계산서": 1
}


# -------------------------------------------------------
# 3. 폴더 전체에서 계정명 + 재무제표명 추출
# -------------------------------------------------------
base = Path("../../insert_data_to_es/cmp_fnl_data")
rows = []

for path in base.rglob("*.xlsx"):
    file_name = path.name

    if "cmp_fnl_data" in file_name.lower():
        continue

    try:
        # 확장자 제거
        name_stem = path.stem
        rpt_candidate = name_stem.split("_")[-1]

        # 회사명-손익계산서 처리
        if "-" in rpt_candidate:
            rpt_name = rpt_candidate.split("-")[-1]
        else:
            rpt_name = rpt_candidate

        # 엑셀 로드
        df = pd.read_excel(path)

        # 계정명 추출
        df = df[['Unnamed: 1']].iloc[2:]
        df = df.rename(columns={'Unnamed: 1': 'CD_NM'})

        # 계정명 정규화 적용
        # df['CD_NM'] = df['CD_NM'].astype(str).apply(normalize_cd_nm)

        df['CD_NM'] = df['CD_NM'].astype(str).str.strip()

        df['FS_RPT_NM'] = rpt_name

        rows.append(df)

    except Exception as e:
        print("오류 발생:", path, e)

# -------------------------------------------------------
# 4. 모든 파일 합치기 + 중복 제거
# -------------------------------------------------------
result = pd.concat(rows, ignore_index=True)
result = result.drop_duplicates(subset=["CD_NM", "FS_RPT_NM"])


# -------------------------------------------------------
# 5. MySQL INSERT (자동 코드 생성 포함)
# -------------------------------------------------------
insert_sql = """
INSERT INTO fs_account_code (CD, CD_NM, FS_RPT_NM)
VALUES (%s, %s, %s)
"""

insert_count = 0

for _, row in result.iterrows():
    cd_nm = row["CD_NM"]
    rpt_nm = row["FS_RPT_NM"]

    if cd_nm in ["nan", "", None]:
        continue

    # 재무제표 코드 생성
    if rpt_nm not in code_base:
        print("알 수 없는 재무제표명 → 코드 생성 불가:", rpt_nm)
        continue

    next_cd = code_base[rpt_nm] + code_counter[rpt_nm]
    code_counter[rpt_nm] += 1

    try:
        cursor.execute(insert_sql, (next_cd, cd_nm, rpt_nm))
        insert_count += 1
    except pymysql.err.IntegrityError:
        pass
    except Exception as e:
        print("Insert Error:", e)

conn.commit()
cursor.close()
conn.close()

print(f"총 {insert_count}개의 코드가 INSERT 되었습니다.")
