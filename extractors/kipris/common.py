"""
KIPRIS Extractor Factory
카테고리별 Extractor 인스턴스를 생성하는 팩토리 클래스
"""
from typing import Protocol
from selenium.webdriver.remote.webelement import WebElement


class KiprisExtractor(Protocol):
    """KIPRIS Extractor 인터페이스"""

    def extract(self, card: WebElement) -> dict:
        """상세 정보 추출"""
        ...


class PatentExtractor:
    """특허 데이터 추출기"""

    def extract(self, card: WebElement) -> dict:
        from extractors.kipris.patent_extractor import extract_from_patent_details
        return extract_from_patent_details(card)


class UtilityExtractor:
    """실용신안 데이터 추출기"""

    def extract(self, card: WebElement) -> dict:
        from extractors.kipris.utility_extractor import extract_from_utility_details
        return extract_from_utility_details(card)


class DesignExtractor:
    """디자인 데이터 추출기"""

    def extract(self, card: WebElement) -> dict:
        from extractors.kipris.design_extractor import extract_from_design_details
        return extract_from_design_details(card)


class TrademarkExtractor:
    """상표 데이터 추출기"""

    def extract(self, card: WebElement) -> dict:
        from extractors.kipris.trademark_extractor import extract_from_trademark_details
        return extract_from_trademark_details(card)


class KiprisExtractorFactory:
    """KIPRIS Extractor 팩토리"""

    _extractors = {
        'patent': PatentExtractor,
        'utility': UtilityExtractor,
        'design': DesignExtractor,
        'trademark': TrademarkExtractor,
    }

    @classmethod
    def create(cls, category: str) -> KiprisExtractor:
        """
        카테고리에 맞는 Extractor 생성

        Args:
            category: 'patent', 'utility', 'design', 'trademark'

        Returns:
            해당 카테고리의 Extractor 인스턴스

        Raises:
            ValueError: 지원하지 않는 카테고리인 경우
        """
        extractor_class = cls._extractors.get(category)

        if not extractor_class:
            raise ValueError(
                f"Unknown category: {category}. "
                f"Available: {list(cls._extractors.keys())}"
            )

        return extractor_class()

    @classmethod
    def is_supported(cls, category: str) -> bool:
        """카테고리 지원 여부 확인"""
        return category in cls._extractors

    @classmethod
    def get_supported_categories(cls) -> list:
        """지원 가능한 카테고리 목록"""
        return list(cls._extractors.keys())

# Extractor 임포트는 lazy loading으로 처리
# (순환 참조 방지)