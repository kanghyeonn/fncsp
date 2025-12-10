from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
import os
import urllib3

load_dotenv()

# SSL 인증서 검증 경고 숨기기
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_es_conn():
    host = os.getenv("ELASTICSEARCH_HOST")
    id = os.getenv("ELASTICSEARCH_ID")
    password = os.getenv("ELASTICSEARCH_PASSWORD")

    try:
        es = Elasticsearch(
            [host],
            http_auth=(id, password),
            verify_certs=False,
        )
        if es.ping():
            print("Connected to Elasticsearch")
        return es
    except Exception as e:
        print(e)
        return "Elasticsearch Connection Error"

# 출원번호로 중복인지 확인하는 함수
def get_application_an(es: Elasticsearch, data_type:str, biz_no:str, an:str) -> bool:
    index_name = "source_data_test"

    query_body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "DataType": {
                                "value": data_type
                            }
                        }
                    },
                    {
                        "term": {
                            "BusinessNum": {
                                "value": biz_no
                            }
                        }
                    },
                    {
                        "term": {
                            "Data.ApplicationNumber": {
                                "value": an
                            }
                        }
                    }
                ]
            }
        }
    }

    response = es.search(index=index_name, body=query_body)

    return response["hits"]["total"]["value"] > 0

# 프로젝트 번호로 중복인지 확인하는 함수
def get_project_no(es: Elasticsearch, data_type:str, biz_no:str, no:str) -> bool:
    index_name = "source_data_test"

    query_body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "DataType": {
                                "value": data_type
                            }
                        }
                    },
                    {
                        "term": {
                            "BusinessNum": {
                                "value": biz_no
                            }
                        }
                    },
                    {
                        "term": {
                            "Data.ProjectNo": {
                                "value": no
                            }
                        }
                    }
                ]
            }
        }
    }

    response = es.search(index=index_name, body=query_body)

    return response["hits"]["total"]["value"] > 0

# 보고서 등록번호로 중복인지 확인하는 함수
def get_research_public_no(es: Elasticsearch, data_type:str, biz_no:str, no:str) -> bool:
    index_name = "source_data_test"

    query_body = {
        "query": {
            "bool": {
                "must": [
                    {
                        "term": {
                            "DataType": {
                                "value": data_type
                            }
                        }
                    },
                    {
                        "term": {
                            "BusinessNum": {
                                "value": biz_no
                            }
                        }
                    },
                    {
                        "term": {
                            "Data.ResearchPublicNo.keyword": {
                                "value": no
                            }
                        }
                    }
                ]
            }
        }
    }

    response = es.search(index=index_name, body=query_body)

    return response["hits"]["total"]["value"] > 0

# elasticsearch에 네이버 뉴스 적재 함수
def insert_naver_news(es:Elasticsearch, news_attrs:list | None, business_num:str | None):
    if not news_attrs:
        actions = [{
            "_index": "source_data_test",
            "_source": {
                "BusinessNum": business_num,
                "DataType": "naver_news",
                "SearchDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": None
            }
        }]
    else:
        actions = [{
            "_index": "source_data_test",
            "_source": {
                "BusinessNum": business_num,
                "DataType": "naver_news",
                "SearchDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": doc
            }
        } for doc in news_attrs]

    success_count, errors = helpers.bulk(es, actions)

# elasticsearch에 kipris patent(특허) 적재 함수
def insert_kipris_patent(es:Elasticsearch, kipris_patent_attrs:list | None, business_num:str | None):
    if not kipris_patent_attrs:
        actions = [{
            "_index": "source_data_test",
            "_source": {
                "BusinessNum": business_num,
                "DataType": "kipris_patent",
                "SearchDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": None
            }
        }]
    else:
        actions = [{
            "_index": "source_data_test",
            "_source": {
                "BusinessNum": business_num,
                "DataType": "kipris_patent",
                "SearchDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": doc
            }
        } for doc in kipris_patent_attrs]

    success_count, errors = helpers.bulk(es, actions)

# elasticsearch에 kipris patent(특허) 적재 함수
def insert_kipris_utility(es:Elasticsearch, kipris_patent_attrs:list | None, business_num:str | None):
    if not kipris_patent_attrs:
        actions = [{
            "_index": "source_data_test",
            "_source": {
                "BusinessNum": business_num,
                "DataType": "kipris_utility",
                "SearchDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": None
            }
        }]
    else:
        actions = [{
            "_index": "source_data_test",
            "_source": {
                "BusinessNum": business_num,
                "DataType": "kipris_utility",
                "SearchDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": doc
            }
        } for doc in kipris_patent_attrs]

    success_count, errors = helpers.bulk(es, actions)

def insert_kipris_design(es:Elasticsearch, kipris_design_attrs:list | None, business_num:str | None):
    if not kipris_design_attrs:
        actions = [{
            "_index": "source_data_test",
            "_source": {
                "BusinessNum": business_num,
                "DataType": "kipris_design",
                "SearchDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": None
            }
        }]
    else:
        actions = [{
            "_index": "source_data_test",
            "_source": {
                "BusinessNum": business_num,
                "DataType": "kipris_design",
                "SearchDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": doc
            }
        } for doc in kipris_design_attrs]

    success_count, errors = helpers.bulk(es, actions)

def insert_kipris_trade(es:Elasticsearch, kipris_trademark_attrs:list | None, business_num:str | None):
    if not kipris_trademark_attrs:
        actions = [{
            "_index": "source_data_test",
            "_source": {
                "BusinessNum": business_num,
                "DataType": "kipris_trade",
                "SearchDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": None
            }
        }]
    else:
        actions = [{
            "_index": "source_data_test",
            "_source": {
                "BusinessNum": business_num,
                "DataType": "kipris_trade",
                "SearchDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": doc
            }
        } for doc in kipris_trademark_attrs]

    success_count, errors = helpers.bulk(es, actions)

def insert_naver_trend(es:Elasticsearch, naver_trend: list | None, business_num:str | None):
    if not naver_trend:
        actions = [{
            "_index": "source_data_test",
            "_source": {
                "BusinessNum": business_num,
                "DataType": "naver_trend",
                "SearchDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": None
            }
        }]
    else:
        actions = [{
            "_index": "source_data_test",
            "_source": {
                "BusinessNum": business_num,
                "DataType": "naver_trend",
                "SearchDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": naver_trend
            }
        }]

    success_count, errors = helpers.bulk(es, actions)

def insert_ntis_assign(es:Elasticsearch, ntis_assign: list | None, business_num:str | None):
    if not ntis_assign:
        actions = [{
            "_index": "source_data_test",
            "_source": {
                "BusinessNum": business_num,
                "DataType": "ntis_assign",
                "SearchDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": None
            }
        }]
    else:
        actions = [{
            "_index": "source_data_test",
            "_source": {
                "BusinessNum": business_num,
                "DataType": "ntis_assign",
                "SearchDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": doc
            }
        }for doc in ntis_assign]

    success_count, errors = helpers.bulk(es, actions)

def insert_ntis_org_info(es:Elasticsearch, ntis_org_info: dict | None, business_num:str | None):
    if not ntis_org_info:
        actions = [{
            "_index": "source_data_test",
            "_source": {
                "BusinessNum": business_num,
                "DataType": "ntis_org_info",
                "SearchDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": None
            }
        }]
    else:
        actions = [{
            "_index": "source_data_test",
            "_source": {
                "BusinessNum": business_num,
                "DataType": "ntis_org_info",
                "SearchDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": ntis_org_info
            }
        }]

    success_count, errors = helpers.bulk(es, actions)

def insert_ntis_rnd_paper(es:Elasticsearch, ntis_rnd_paper: list | None, business_num:str | None):
    if not ntis_rnd_paper:
        actions = [{
            "_index": "source_data_test",
            "_source": {
                "BusinessNum": business_num,
                "DataType": "ntis_rnd_paper",
                "SearchDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": None
            }
        }]
    else:
        actions = [{
            "_index": "source_data_test",
            "_source": {
                "BusinessNum": business_num,
                "DataType": "ntis_rnd_paper",
                "SearchDate": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f"),
                "SearchID": "autoSystem",
                "Data": doc
            }
        }for doc in ntis_rnd_paper]

    success_count, errors = helpers.bulk(es, actions)

