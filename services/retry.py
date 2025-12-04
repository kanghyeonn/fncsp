import functools
import time
import random
from typing import Callable, Any, Optional, Tuple, Type
from datetime import datetime


class RetryService:
    def __init__(
            self,
            max_retries: int = 5,
            base_delay: float = 2.0,
            max_delay: float = 60.0,
            exponential_base: float = 2.0
    ):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base

    def calculate_delay(
            self,
            attempt: int,
            add_jitter: bool = True
    ) -> float:
        delay = self.base_delay * (self.exponential_base ** (attempt - 1))
        delay = min(delay, self.max_delay)

        if add_jitter:
            jitter = random.uniform(0, delay * 0.1)
            delay = delay + jitter

        return delay

    def execute_with_retry(
            self,
            func: Callable,
            *args,
            allowed_exceptions: Tuple[Type[Exception], ...] = (Exception,),
            on_retry: Optional[Callable] = None,
            **kwargs
    ) -> Any:
        last_exception = None

        for attempt in range(1, self.max_retries + 1):
            try:
                return func(*args, **kwargs)

            except allowed_exceptions as e:
                last_exception = e

                if attempt == self.max_retries:
                    raise

                # 재시도 콜백 실행
                if on_retry:
                    on_retry(attempt, e)

                # 대기
                delay = self.calculate_delay(attempt)
                print(f"재시도 {attempt}/{self.max_retries} - {delay:.1f}초 대기")
                time.sleep(delay)

        # 모든 재시도 실패
        raise last_exception


def backoff_with_db_logging(
        max_retries: int = 5,
        base_delay: float = 2.0,
        data_type: Optional[str] = None,
        allowed_statuses: Tuple[int, ...] = (429, 500, 502, 503, 504)
):
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(1, max_retries + 1):
                try:
                    result = func(*args, **kwargs)

                    # HTTP Response 객체인 경우 상태 코드 확인
                    if hasattr(result, 'status_code'):
                        if result.status_code == 200:
                            return result

                        # 재시도 가능한 상태 코드
                        elif result.status_code in allowed_statuses:
                            print(f"[WARN] HTTP {result.status_code} 발생 → 재시도")

                            if attempt == max_retries:
                                return result

                        # 재시도 불가능한 에러
                        else:
                            return result
                    else:
                        # HTTP Response가 아닌 경우 그대로 반환
                        return result

                except Exception as e:
                    last_exception = e

                    # 에러 로깅
                    error_msg = f"[{func.__name__}] 시도 {attempt}/{max_retries} 실패: {e}"
                    print(error_msg)

                    if data_type:
                        try:
                            from repositories.mysql_repository import MySQLRepository
                            from core.config import RepositoryConfig

                            mysql = MySQLRepository(RepositoryConfig.get_instance())
                            mysql.insert_error_log(
                                location=func.__name__,
                                data_type=data_type,
                                error_msg=error_msg,
                                error_detail=str(e)
                            )
                        except Exception as log_error:
                            print(f"로그 기록 실패: {log_error}")

                    if attempt == max_retries:
                        raise

                # 백오프 대기
                delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, 1)
                delay = min(delay, 60.0)  # 최대 60초
                print(f"[INFO] {delay:.1f}초 대기 후 재시도 ({attempt}/{max_retries})")
                time.sleep(delay)

            # 모든 재시도 실패
            if last_exception:
                raise last_exception

            raise Exception(f"모든 재시도 실패: {func.__name__}")

        return wrapper

    return decorator


def simple_retry(
        max_retries: int = 3,
        delay: float = 1.0,
        exceptions: Tuple[Type[Exception], ...] = (Exception,)
):

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries:
                        raise
                    print(f"재시도 {attempt}/{max_retries}: {e}")
                    time.sleep(delay)

            raise Exception(f"{func.__name__} 모든 재시도 실패")

        return wrapper

    return decorator

class RateLimitRetry:
    """Rate Limit 처리 특화 재시도"""

    def __init__(self, max_retries: int = 10):
        self.max_retries = max_retries
        self.retry_count = 0

    def __call__(self, func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            for attempt in range(1, self.max_retries + 1):
                try:
                    result = func(*args, **kwargs)

                    # 429 Too Many Requests 처리
                    if hasattr(result, 'status_code') and result.status_code == 429:
                        # Retry-After 헤더 확인
                        retry_after = result.headers.get('Retry-After')

                        if retry_after:
                            wait_time = int(retry_after)
                        else:
                            # 기본 대기 시간 (점진적 증가)
                            wait_time = min(2 ** attempt, 300)  # 최대 5분

                        print(f"Rate limit 도달. {wait_time}초 대기 중...")
                        time.sleep(wait_time)
                        continue

                    # 성공
                    self.retry_count = 0
                    return result

                except Exception as e:
                    if attempt == self.max_retries:
                        raise

                    wait_time = 2 ** attempt
                    print(f"에러 발생. {wait_time}초 대기 중... ({attempt}/{self.max_retries})")
                    time.sleep(wait_time)

            raise Exception("Rate limit retry 실패")

        return wrapper
