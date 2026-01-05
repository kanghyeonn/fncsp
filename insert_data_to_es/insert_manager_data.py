import pandas as pd
from elasticsearch import Elasticsearch, helpers
from datetime import datetime
from dotenv import load_dotenv
import os
import re

# ----------------------------------
# 환경 변수 로드
# ----------------------------------
load_dotenv()

HOST = os.getenv("ELASTICSEARCH_HOST")
ID = os.getenv("ELASTICSEARCH_ID")
PASSWORD = os.getenv("ELASTICSEARCH_PASSWORD")

INDEX_NAME = "source_data"

# ----------------------------------
# Elasticsearch 연결
# ----------------------------------
es = Elasticsearch(
    hosts=[HOST],
    basic_auth=(ID, PASSWORD),
    verify_certs=False,
    ssl_show_warn=False,
)

# ----------------------------------
# 엑셀 로드 및 컬럼 정리
# ----------------------------------
file = "./cmp_info_data/기업정보수집현황.xlsx"

cmp_info = pd.read_excel(file, sheet_name="Sheet2").iloc[1:]

cmp_info = cmp_info.rename(columns={
    '기업프로필': '사업자번호',
    'Unnamed: 2': '기업명',
    'Unnamed: 3': '대표자명',
    'Unnamed: 4': '기업유형',
    'Unnamed: 5': '기업규모',
    'Unnamed: 6': '전화번호',
    'Unnamed: 7': '주소',
    'Unnamed: 8': '설립일자',
    'Unnamed: 9': '산업코드',
    'Unnamed: 10': '종업원수',
    '경영진': '성명',
    'Unnamed: 24': '직위',
    'Unnamed: 25': '담당업무명',
    'Unnamed: 26': '학력',
    'Unnamed: 27': '최근경력'
})

cmp_info = cmp_info.fillna("")

# ----------------------------------
# 공통 유틸
# ----------------------------------
def split_lines(value: str):
    if not value or not isinstance(value, str):
        return []
    return [v.strip() for v in value.splitlines() if v.strip()]

def clean_edu(value: str):
    if not value or not isinstance(value, str):
        return None

    # [] 안, () 안 내용 제거
    cleaned = re.sub(r"\[[^\]]*\]|\([^)]*\)", "", value)

    # 공백 정리
    return cleaned.strip()

# ----------------------------------
# 경영진 파싱 (확정 로직)
# ----------------------------------
def parse_managers_from_row(row):
    biz_no = str(row['사업자번호']).replace("-", "")
    names = split_lines(row.get("성명", ""))
    positions = split_lines(row.get("직위", ""))
    tasks = split_lines(row.get("담당업무명", ""))
    edus = split_lines(row.get("학력", ""))
    careers = split_lines(row.get("최근경력", ""))

    manager_count = len(names)
    if manager_count == 0:
        return []

    managers = []
    for i in range(manager_count):
        managers.append({
            "bizNo": biz_no,
            "mgrNm": names[i].replace(";", ""),
            "pstnCdNm": positions[i].replace(";", "") if i < len(positions) else None,
            "chrgTaskNm": tasks[i].replace(";", "") if i < len(tasks) else None,
            "eduCont": clean_edu(edus[i].replace(";", "")) if i < len(edus) else None,
            # ✅ 사람당 한 줄 → 그대로 문자열 저장
            "crrCont": careers[i].replace(";", "") if i < len(careers) else None
        })

    return managers

# ----------------------------------
# Elasticsearch 문서 생성
# ----------------------------------
now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

def generate_manager_docs(df):
    for _, row in df.iterrows():
        managers = parse_managers_from_row(row)

        if not managers:
            continue

        doc = {
            "_index": INDEX_NAME,
            "_id": f"{row['사업자번호']}_nicednb_manager",
            "_source": {
                "BusinessNum": str(row["사업자번호"]).replace("-", ""),
                "DataType": "nicednb_manager",
                "SearchDate": now_str,
                "SearchID": "autoSystem",
                "Data": managers
            }
        }
        yield doc

# ----------------------------------
# 🔍 적재 전 확인 (1건)
# ----------------------------------
if __name__ == "__main__":
    from pprint import pprint

    # test_doc = next(generate_manager_docs(cmp_info))
    # pprint(test_doc)

    # ----------------------------------
    # ✅ 단건 테스트 인서트 (선택)
    # ----------------------------------
    # es.index(
    #     index=INDEX_NAME,
    #     id=test_doc["_id"],
    #     document=test_doc["_source"]
    # )

    # ----------------------------------
    # 🚀 Bulk Insert (확인 후 실행)
    # ----------------------------------
    from elasticsearch.helpers import BulkIndexError

    try:
        helpers.bulk(es, generate_manager_docs(cmp_info))
    except BulkIndexError as e:
        print(f"실패 문서 수: {len(e.errors)}")
        for error in e.errors[:5]:  # 앞 5개만 확인
            print(error)
