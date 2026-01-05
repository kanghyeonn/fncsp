import pandas as pd
from elasticsearch import Elasticsearch, helpers
from datetime import datetime
from pprint import pprint
from dotenv import load_dotenv
import re
import os

load_dotenv()

HOST = os.getenv("ELASTICSEARCH_HOST")
ID = os.getenv("ELASTICSEARCH_ID")
PASSWORD = os.getenv("ELASTICSEARCH_PASSWORD")

def parse_industry(value: str):
    """
    J62010(컴퓨터 프로그래밍 서비스업)
    → indCd1 = 62010
    → indNm  = 컴퓨터 프로그래밍 서비스업
    """
    if not value or not isinstance(value, str):
        return None, None

    pattern = r"[A-Z]?(\d+)\((.+)\)"
    match = re.search(pattern, value)

    if match:
        indCd1 = match.group(1)
        indNm = match.group(2)
        return indCd1, indNm

    return None, None

# ----------------------------------
# Elasticsearch 연결
# ----------------------------------
es = Elasticsearch(
    hosts=[HOST],
    basic_auth=(ID, PASSWORD),  # 필요 시
    verify_certs=False,
    ssl_show_warn=False
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
# Bulk 적재용 문서 생성
# ----------------------------------
now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

def generate_docs_cmp_info(df):
    for _, row in df.iterrows():
        indCd1, indNm = parse_industry(row['산업코드'])
        biz_no = str(row['사업자번호']).replace("-", "")
        doc = {
            "_id": f"NICEDNB_INFO_{biz_no}",
            "_index": "source_data",
            "_source": {
                "BusinessNum": str(biz_no),
                "DataType": "nicednb_info",
                "SearchDate": now_str,
                "SearchID": "autoSystem",
                "Data": {
                    "bizNo": str(biz_no),
                    "cmpNm": row["기업명"] if row["기업명"] else None,
                    "ceoNm": row["대표자명"] if row["대표자명"] else None,
                    "telNo": row['전화번호'] if row['전화번호'] else None,
                    "adr": row["주소"] if row['주소'] else None,
                    "cmpTypNm": row["기업유형"] if row['기업유형'] else None,
                    "cmpSclNm": row["기업규모"] if row['기업규모'] else None,
                    'indCd1': indCd1,
                    'indNm': indNm,
                    'estbDate': row['설립일자'] if row['설립일자'] else None,
                    'empCnt': row['종업원수'] if row['종업원수'] else None
                }
            }
        }

        yield doc

if __name__ == "__main__":
    # doc = next(generate_docs_cmp_info(cmp_info))
    # pprint(doc)

    # ----------------------------------
    # 단건 적재
    # ----------------------------------
    # doc = next(generate_docs_cmp_info(cmp_info))
    # es.index(index="source_data", id=doc["_id"], document=doc["_source"])

    # ----------------------------------
    # Bulk Insert
    # ----------------------------------
    from elasticsearch.helpers import BulkIndexError

    try:
        helpers.bulk(es, generate_docs_cmp_info(cmp_info))
    except BulkIndexError as e:
        print(f"실패 문서 수: {len(e.errors)}")
        for error in e.errors[:5]:  # 앞 5개만 확인
            print(error)

