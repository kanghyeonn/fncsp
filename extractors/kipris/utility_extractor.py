"""
KIPRIS 실용신안 데이터 추출기
(특허와 거의 동일한 구조)
"""
from typing import Dict
from selenium.webdriver.remote.webelement import WebElement

from extractors.kipris.patent_extractor import PatentExtractor


class UtilityExtractor(PatentExtractor):
    """
    실용신안 데이터 추출기

    특허와 동일한 구조이므로 PatentExtractor를 상속
    """
    pass


# ==========================================
# 편의 함수 (하위 호환성)
# ==========================================

def extract_from_utility_details(card: WebElement) -> Dict:
    """
    실용신안 상세 정보 추출

    Args:
        card: 상세 카드 WebElement

    Returns:
        추출된 정보 딕셔너리
    """
    extractor = UtilityExtractor()
    return extractor.extract(card)