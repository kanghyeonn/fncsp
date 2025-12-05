"""
개별 크롤러 실행 스크립트
"""
import sys
import os
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


# ==========================================
# KIPRIS 크롤러 실행
# ==========================================

def run_kipris_patent():
    """KIPRIS 특허 크롤러 실행"""
    from crawlers.kipris.patent import PatentCrawler

    print("=" * 60)
    print("KIPRIS 특허 크롤러 시작")
    print("=" * 60)

    crawler = PatentCrawler()
    crawler.run()


def run_kipris_utility():
    """KIPRIS 실용신안 크롤러 실행"""
    from crawlers.kipris.utility import UtilityCrawler

    print("=" * 60)
    print("KIPRIS 실용신안 크롤러 시작")
    print("=" * 60)

    crawler = UtilityCrawler()
    crawler.run()


def run_kipris_design():
    """KIPRIS 디자인 크롤러 실행"""
    from crawlers.kipris.design import DesignCrawler

    print("=" * 60)
    print("KIPRIS 디자인 크롤러 시작")
    print("=" * 60)

    crawler = DesignCrawler()
    crawler.run()


def run_kipris_trademark():
    """KIPRIS 상표 크롤러 실행"""
    from crawlers.kipris.trademark import TrademarkCrawler

    print("=" * 60)
    print("KIPRIS 상표 크롤러 시작")
    print("=" * 60)

    crawler = TrademarkCrawler()
    crawler.run()


# ==========================================
# Naver 크롤러 실행
# ==========================================

def run_naver_news(period: int = 365):
    """Naver 뉴스 크롤러 실행"""
    from crawlers.naver.news import NewsCrawler

    print("=" * 60)
    print(f"Naver 뉴스 크롤러 시작 (기간: {period}일)")
    print("=" * 60)

    crawler = NewsCrawler(period=period)
    crawler.run()


def run_naver_trend(start_date: str = "2022-01-01", chunk_size: int = 5):
    """Naver 트렌드 크롤러 실행"""
    from crawlers.naver.trend import TrendCrawler

    print("=" * 60)
    print(f"Naver 트렌드 크롤러 시작 (시작일: {start_date})")
    print("=" * 60)

    crawler = TrendCrawler(start_date=start_date, chunk_size=chunk_size)
    crawler.run()


# ==========================================
# NTIS 크롤러 실행
# ==========================================

def run_ntis_assign():
    """NTIS 과제 정보 크롤러 실행"""
    from crawlers.ntis.assign import AssignCrawler

    print("=" * 60)
    print("NTIS 과제 정보 크롤러 시작")
    print("=" * 60)

    crawler = AssignCrawler()
    crawler.run()


def run_ntis_org_info():
    """NTIS 수행기관 정보 크롤러 실행"""
    from crawlers.ntis.org_info import OrgInfoCrawler

    print("=" * 60)
    print("NTIS 수행기관 정보 크롤러 시작")
    print("=" * 60)

    crawler = OrgInfoCrawler()
    crawler.run()


def run_ntis_rnd_paper():
    """NTIS 연구보고서 크롤러 실행"""
    from crawlers.ntis.rnd_paper import RndPaperCrawler

    print("=" * 60)
    print("NTIS 연구보고서 크롤러 시작")
    print("=" * 60)

    crawler = RndPaperCrawler()
    crawler.run()


# ==========================================
# 메인 실행
# ==========================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='데이터 수집 크롤러 실행')
    parser.add_argument(
        'crawler',
        choices=[
            'kipris-patent',
            'kipris-utility',
            'kipris-design',
            'kipris-trademark',
            'naver-news',
            'naver-trend',
            'ntis-assign',
            'ntis-org-info',
            'ntis-rnd-paper'
        ],
        help='실행할 크롤러 선택'
    )
    parser.add_argument(
        '--period',
        type=int,
        default=365,
        help='뉴스 수집 기간 (일) - naver-news용'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        default='2022-01-01',
        help='트렌드 시작 날짜 - naver-trend용'
    )
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=5,
        help='트렌드 청크 크기 - naver-trend용'
    )

    args = parser.parse_args()

    # 크롤러 실행
    crawler_map = {
        'kipris-patent': run_kipris_patent,
        'kipris-utility': run_kipris_utility,
        'kipris-design': run_kipris_design,
        'kipris-trademark': run_kipris_trademark,
        'naver-news': lambda: run_naver_news(args.period),
        'naver-trend': lambda: run_naver_trend(args.start_date, args.chunk_size),
        'ntis-assign': run_ntis_assign,
        'ntis-org-info': run_ntis_org_info,
        'ntis-rnd-paper': run_ntis_rnd_paper,
    }

    try:
        crawler_map[args.crawler]()
    except KeyboardInterrupt:
        print("\n\n크롤러가 사용자에 의해 중단되었습니다.")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n크롤러 실행 중 오류 발생: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)