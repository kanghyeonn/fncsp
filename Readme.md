# Data Crawler System

한국의 다양한 데이터 소스(KIPRIS, NAVER, NTIS)로부터 기업 정보를 자동으로 수집하는 크롤링 시스템입니다.

## 📋 목차

- [시스템 개요](#시스템-개요)
- [프로젝트 구조](#프로젝트-구조)
- [설치 방법](#설치-방법)
- [환경 설정](#환경-설정)
- [실행 방법](#실행-방법)

## 🎯 시스템 개요

이 시스템은 기업의 사업자번호를 기반으로 다음 데이터를 자동 수집합니다:

- **KIPRIS**: 특허, 실용신안, 디자인, 상표 정보
- **NAVER**: 뉴스 기사, 검색 트렌드 데이터
- **NTIS**: 국가 R&D 과제, 연구보고서, 수행기관 정보

## 📁 프로젝트 구조

```
crawler-system/
│
├── core/                          # 핵심 모듈
│   ├── base_crawler.py           # 크롤러 베이스 클래스
│   ├── config.py                 # 설정 관리
│   └── exceptions.py             # 커스텀 예외
│
├── crawlers/                      # 크롤러 구현
│   ├── kipris/                   # KIPRIS 크롤러
│   │   ├── base.py              # KIPRIS 베이스
│   │   ├── patent.py            # 특허
│   │   ├── utility.py           # 실용신안
│   │   ├── design.py            # 디자인
│   │   └── trademark.py         # 상표
│   │
│   ├── naver/                    # NAVER 크롤러
│   │   ├── base.py              # NAVER 베이스
│   │   ├── news.py              # 뉴스
│   │   └── trend.py             # 트렌드
│   │
│   └── ntis/                     # NTIS 크롤러
│       ├── base.py              # NTIS 베이스
│       ├── assign.py            # 과제 정보
│       ├── rnd_paper.py         # 연구보고서
│       └── org_info.py          # 수행기관 정보
│
│
├── make_comp_data              # mysql 데이터를 삽입
│   ├── insert_data_to_cmp_financial.py # 재무데이터 테이블 
│   ├── insert_data_to_tm_cretop_cd.py # 재무데이터 코드명 매핑 테이블
│   └── insert_data_to_es.py      # 재무데이터를 Elasticsearch에 적재
│
├── repositories/                  # 데이터 저장소
│   ├── base_repository.py        # 저장소 베이스
│   ├── data_repository.py        # 데이터 저장소 (Facade)
│   ├── elasticsearch_repository.py  # Elasticsearch
│   └── mysql_repository.py       # MySQL
│
├── services/                      # 공통 서비스
│   ├── notification.py           # 이메일 알림
│   └── retry.py                  # 재시도 로직
│
├── extractors/                    # 데이터 추출 (크롤러별 파서)
│   └── kipris/
│       └── common.py             # KIPRIS 공통 추출기
│
├── .env                          # 환경 변수 (생성 필요)
├── requirements.txt              # 의존성
└── README.md                     # 문서
```

## 🚀 설치 방법

### 1. 시스템 요구사항

```
Python: 3.8+
Elasticsearch: 7.x+
MySQL: 5.7+
Chrome/ChromeDriver (KIPRIS 크롤링용)
```

### 2. 저장소 클론

```bash
git clone <repository-url>
cd crawler-system
```

### 3. 가상환경 생성 (권장)

```bash
# 가상환경 생성
python -m venv venv

# 가상환경 활성화
# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate
```

### 4. 의존성 설치

```bash
pip install -r requirements.txt
```

**requirements.txt:**
```txt
selenium>=4.0.0
beautifulsoup4>=4.11.0
requests>=2.28.0
elasticsearch>=7.0.0
pymysql>=1.0.0
python-dotenv>=0.20.0
tqdm>=4.64.0
xmltodict>=0.13.0
lxml>=4.9.0
```

### 5. ChromeDriver 설치 (KIPRIS 크롤링용)

```bash
# macOS (Homebrew)
brew install chromedriver

# Linux
apt-get install chromium-chromedriver

# Windows
# https://chromedriver.chromium.org/ 에서 다운로드 후 PATH에 추가
```

## ⚙️ 환경 설정

### 1. `.env` 파일 생성

프로젝트 루트에 `.env` 파일을 생성:

```bash
# Elasticsearch 설정
ELASTICSEARCH_HOST=http://localhost:9200
ELASTICSEARCH_ID=elastic
ELASTICSEARCH_PASSWORD=your_password

# MySQL 설정
LOCAL_MYSQL_HOST=localhost
LOCAL_MYSQL_USER=root
LOCAL_MYSQL_PASSWORD=your_password
LOCAL_MYSQL_DATABASE=crawler_db

# NTIS API 키
NTIS_API_KEY=your_ntis_api_key

# 이메일 알림 설정 (네이버 메일)
EMAIL=your_email@naver.com
PASSWORD=your_password
SMTP_SERVER=smtp.naver.com
SMTP_PORT=465

# Gmail 사용 시
# EMAIL=your_email@gmail.com
# PASSWORD=your_app_password
# SMTP_SERVER=smtp.gmail.com
# SMTP_PORT=465
```

### 2. MySQL 데이터베이스 설정

```sql
-- 데이터베이스 생성
CREATE DATABASE crawler_db DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE crawler_db;

-- 기업 목록 테이블
CREATE TABLE cmp_list (
    BIZ_NO VARCHAR(20) PRIMARY KEY COMMENT '사업자번호',
    CMP_NM VARCHAR(200) NOT NULL COMMENT '회사명',
    CEO_NM VARCHAR(100) COMMENT '대표자명',
    KIPRIS_PATENT DATETIME COMMENT '특허 수집일시',
    KIPRIS_UTILITY DATETIME COMMENT '실용신안 수집일시',
    KIPRIS_DESIGN DATETIME COMMENT '디자인 수집일시',
    KIPRIS_TRADEMARK DATETIME COMMENT '상표 수집일시',
    NAVER_NEWS DATETIME COMMENT '뉴스 수집일시',
    NAVER_TREND DATETIME COMMENT '트렌드 수집일시',
    NTIS_ASSIGN DATETIME COMMENT '과제 수집일시',
    NTIS_RND_PAPER DATETIME COMMENT '연구보고서 수집일시',
    NTIS_ORG_INFO DATETIME COMMENT '기관정보 수집일시'
) COMMENT '기업 목록';

-- 수집 로그 테이블
CREATE TABLE cmp_data_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    biz_no VARCHAR(20) NOT NULL COMMENT '사업자번호',
    data_type VARCHAR(50) NOT NULL COMMENT '데이터 타입',
    count INT DEFAULT 0 COMMENT '수집 건수',
    created_at DATETIME NOT NULL COMMENT '수집일시'
) COMMENT '데이터 수집 로그';

-- 에러 로그 테이블
CREATE TABLE error_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    DATA_TYPE VARCHAR(50) COMMENT '데이터 타입',
    ERROR_LOG TEXT COMMENT '에러 메시지',
    CREATED_AT DATETIME NOT NULL COMMENT '발생일시'
) COMMENT '에러 로그';

-- 샘플 데이터 삽입
INSERT INTO cmp_list (BIZ_NO, CMP_NM, CEO_NM) VALUES
('1234567890', '삼성전자', '김홍길'),
('0987654321', 'LG전자', '이순신');
```

### 3. Elasticsearch 인덱스 생성

```bash
# Elasticsearch가 실행 중인지 확인
curl -X GET "localhost:9200"

# 인덱스 생성
curl -X PUT "localhost:9200/source_data" -H 'Content-Type: application/json' -d'
{
  "settings": {
    "number_of_shards": 3,
    "number_of_replicas": 1
  },
  "mappings": {
    "properties": {
      "BusinessNum": { "type": "keyword" },
      "DataType": { "type": "keyword" },
      "SearchDate": { 
        "type": "date", 
        "format": "yyyy-MM-dd HH:mm:ss.SSS" 
      },
      "SearchID": { "type": "keyword" },
      "Data": { "type": "object", "enabled": true }
    }
  }
}'
```

## 🎮 실행 방법

### KIPRIS 크롤러

```cmd (이 방법으로 실행!!!)
# 특허 정보 수집
python main.py kipris-patent

# 실용신안 수집
python main.py kipris-utility

# 디자인 수집
python main.py kipris-design

# 상표 수집
python main.py kipris-trademark
```

**예상 출력:**
```
============================================================
 KIPRIS_PATENT 크롤러 시작
 시작 시간 : 2024-01-15 09:00:00
============================================================

이 100개 회사 수집 예정

KIPRIS_PATENT 수집: 100%|██████████| 100/100 [01:30:45<00:00, 0.91초/회사]
 ✓ 삼성전자 - 234건 저장 완료
 ✓ LG전자 - 156건 저장 완료

============================================================
  KIPRIS_PATENT 크롤링 완료
============================================================
  종료 상태  : 프로그램 정상 종료
  소요 시간  : 01:30:45
  전체 회사  : 100
  성공       : 98
  실패       : 2
  수집 데이터: 3,456건
============================================================
```

### NAVER 크롤러

```cmd (이 방법으로 실행!!!)
# 뉴스 수집 (기본: 최근 365일)
python main.py naver-news

# 트렌드 수집 (기본: 2022-01-01부터 현재까지)
python main.py naver-trend
```

**프로그래밍 방식:**
```python
from crawlers.naver.news import NewsCrawler
from crawlers.naver.trend import TrendCrawler

# 뉴스 크롤러 (최근 180일)
news_crawler = NewsCrawler(period=180)
news_crawler.run()

# 트렌드 크롤러 (2023년부터, 3개씩 청크)
trend_crawler = TrendCrawler(
    start_date="2023-01-01",
    chunk_size=3
)
trend_crawler.run()
```

### NTIS 크롤러

```cmd (이 방법으로 실행!!!)
# 과제 정보 수집
python main.py ntis-assign

# 연구보고서 수집
python main.py ntis-rnd-paper

# 수행기관 정보 수집
python main.py ntis-org-info
```

### 배치 실행 예시

모든 크롤러를 순차적으로 실행하는 셸 스크립트:

```bash
#!/bin/bash
# run_all_crawlers.sh

echo "========== 크롤링 시작 =========="
date

# KIPRIS
echo ">>> KIPRIS 특허 수집 시작"
python -m crawlers.kipris.patent

echo ">>> KIPRIS 디자인 수집 시작"
python -m crawlers.kipris.design

echo ">>> KIPRIS 상표 수집 시작"
python -m crawlers.kipris.trademark

# NAVER
echo ">>> NAVER 뉴스 수집 시작"
python -m crawlers.naver.news

echo ">>> NAVER 트렌드 수집 시작"
python -m crawlers.naver.trend

# NTIS
echo ">>> NTIS 과제 수집 시작"
python -m crawlers.ntis.assign

echo ">>> NTIS 연구보고서 수집 시작"
python -m crawlers.ntis.rnd_paper

echo ">>> NTIS 기관정보 수집 시작"
python -m crawlers.ntis.org_info

echo "========== 크롤링 완료 =========="
date
```

실행:
```bash
chmod +x run_all_crawlers.sh
./run_all_crawlers.sh
```

### Python 스크립트로 실행

```python
# run_crawlers.py
from crawlers.kipris.patent import PatentCrawler
from crawlers.kipris.design import DesignCrawler
from crawlers.naver.news import NewsCrawler
from crawlers.ntis.assign import AssignCrawler

def main():
    crawlers = [
        ("KIPRIS 특허", PatentCrawler()),
        ("KIPRIS 디자인", DesignCrawler()),
        ("NAVER 뉴스", NewsCrawler(period=365)),
        ("NTIS 과제", AssignCrawler()),
    ]
    
    for name, crawler in crawlers:
        print(f"\n{'='*60}")
        print(f" {name} 수집 시작")
        print(f"{'='*60}\n")
        
        try:
            crawler.run()
            print(f"✓ {name} 수집 완료")
        except Exception as e:
            print(f"✗ {name} 수집 실패: {e}")
        
        print()

if __name__ == "__main__":
    main()
```

실행:
```bash
python run_crawlers.py
```

## 📝 주요 명령어 요약

```bash
# 개별 크롤러 실행
python -m crawlers.kipris.patent           # 특허
python -m crawlers.kipris.design           # 디자인
python -m crawlers.kipris.trademark        # 상표
python -m crawlers.naver.news             # 뉴스
python -m crawlers.naver.trend            # 트렌드
python -m crawlers.ntis.assign            # 과제
python -m crawlers.ntis.rnd_paper         # 연구보고서
python -m crawlers.ntis.org_info          # 기관정보

# 데이터 확인
# MySQL
mysql -u root -p crawler_db
SELECT * FROM cmp_list LIMIT 10;

# Elasticsearch
curl -X GET "localhost:9200/source_data/_search?pretty"
```

## ⚠️ 주의사항

1. **환경 변수**: `.env` 파일이 올바르게 설정되어 있는지 확인
2. **DB 연결**: MySQL과 Elasticsearch가 실행 중인지 확인
3. **API 키**: NTIS API 키가 유효한지 확인
4. **ChromeDriver**: KIPRIS 크롤러 실행 시 ChromeDriver 및 ChromeDriver 버전 확인 필요 
5. **Rate Limit**: NAVER API는 일일 호출 제한이 있을 수 있음


