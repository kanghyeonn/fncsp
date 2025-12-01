import traceback
from tqdm import tqdm
from abc import ABC, abstractmethod
from typing import List, Dict, Optional, Any
from datetime import datetime

from core.exceptions import CrawlerException, DuplicateError
from repositories.data_repository import DataRepository
from services.notification import NotificationService

class BaseCrawler(ABC):

    def __init__(self, data_type: str):
        self.start_time: Optional[datetime] = None
        self.exit_message = "N/A"
        self.data_type = data_type
        self.repository = DataRepository()
        self.notifier = NotificationService()
        self.resources: List[Any] = []

        self.stats = {
            'total_companies': 0,
            'success_companies': 0,
            'failed_companies': 0,
            'total_data' : 0,
            'errors' : []
        }

    @abstractmethod
    def _process_company(self, company: Dict) -> None:
        pass

    def run(self) -> None:
        self.start_time = datetime.now()

        try:
            print(f"\n{'='*60}")
            print(f" {self.data_type} 크롤러 시작")
            print(f" 시작 시간 : {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"\n{'='*60}")

            self._setup()

            companies = self._load_companies()
            self.stats['total_companies'] = len(companies)

            if not companies:
                print("수집할 회사가 없습니다.")
                self.exit_message = "수집할 회사 없음"
                return

            print(f"총 {len(companies)}개 회사 수집 예정\n")

            for idx, company in enumerate(tqdm(
                companies,
                desc=f'{self.data_type} 수집',
                unit='회사'
            ), 1):
                try:
                    self._process_company(company)
                    self.stats['success_companies'] += 1

                except DuplicateError:
                    self.stats['success_companies'] += 1

                except Exception as e:
                    self.stats['failed_companies'] += 1
                    self._handle_error(e, {
                        'company': company,
                        'index': idx,
                        'total': len(companies)
                    })
            self.exit_message = "프로그램 정상 종료"

        except Exception as e:
            self.exit_message = f'프로그램 예외 종료: {e}'
            self._handle_error(e, {'stage': 'main'})

        finally:
            self._cleanup()
            self._send_notification()
            self._print_final_stats()


    def _setup(self) -> None:
        pass

    def _load_companies(self) -> List[Dict]:
        return self.repository.get_company_list(self.data_type)

    def _save_data(self, biz_no: str, data: List[Dict]):
        success = self.repository.save_with_logging(
            self.data_type,
            biz_no,
            data
        )

        if success and data:
            self.stats['total_data'] += len(data)

        return success

    def _cleanup(self):
        for resource in self.resources:
            try:
                if hasattr(resource, 'close'):
                    resource.close()
                elif hasattr(resource, 'quit'):
                    resource.quit()
                elif hasattr(resource, 'disconnect'):
                    resource.disconnect()
            except Exception as e:
                print(f"resource clean up failed: {e}")

    def _handle_error(self, error: Exception, context: Dict):
        self.repository.log_error(
            location= f"{self.__class__.__name__}._process_company",
            data_type=self.data_type,
            message=str(error),
            detail=traceback.format_exc()
        )

        comp_name = context.get('company', {}).get('CMP_NM', 'Unknown')
        print(f"에러발생 : {comp_name} - {str(error)}")

    def _send_notification(self) -> None:
        subject = f"{self.data_type} 크롤링 완료"
        body = f"""
        {self.data_type} 프로그램 종료됨
        
        종료상태 : {self.exit_message}
        시작시간 : {self.start_time.strftime('%Y-%m-%d %H:%M:%S') if self.start_time else 'N/A'}
        종료시간 : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        소요시간 : {self._get_elapsed_time()}
        
        ===== 수집 통계 =====
        전체회사 : {self.stats['total_companies']}
        성공 : {self.stats['success_companies']}
        실패 : {self.stats['failed_companies']}
        수집 데이터 : {self.stats['total_data']}건
        """

        try:
            self.notifier.send_alert(subject, body)
        except Exception as e:
            print(f'send notification failed: {e}')

    def _get_elapsed_time(self) -> str:
        if not self.start_time:
            return 'N/A'

        elapsed = datetime.now() - self.start_time
        hours, remainder = divmod(int(elapsed.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)

        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def _update_stats(self, key:str, value:Any) -> None:
        self.stats[key] = value

    def _print_final_stats(self) -> None:
        """최종 통계 출력"""
        print(f"\n{'=' * 60}")
        print(f"  {self.data_type} 크롤링 완료")
        print(f"{'=' * 60}")
        print(f"  종료 상태  : {self.exit_message}")
        print(f"  소요 시간  : {self._get_elapsed_time()}")
        print(f"  전체 회사  : {self.stats['total_companies']}")
        print(f"  성공       : {self.stats['success_companies']}")
        print(f"  실패       : {self.stats['failed_companies']}")
        print(f"  수집 데이터: {self.stats['total_data']}건")

        if self.stats['errors']:
            print(f"  에러 발생  : {len(self.stats['errors'])}건")

        print(f"{'=' * 60}\n")