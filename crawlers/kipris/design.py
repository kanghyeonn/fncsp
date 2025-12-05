import re
from selenium.webdriver.common.by import By

from crawlers.kipris.base import KiprisCrawler
from extractors.utils import search_by_ap

class DesignCrawler(KiprisCrawler):
    def __init__(self):
        super().__init__(
            category="design",
            data_type="KIPRIS_DESIGN"
        )

    def _search(self, biz_no: str) -> None:
        search_by_ap(self.driver, "sd010201_g07_text_01",  biz_no)

    def _is_duplicate(self, card, biz_no: str) -> bool:
        an_element = card.find_element(By.CLASS_NAME, "txt")
        an_text = an_element.text.strip()

        an = re.sub(r'\((.*?)\)', "", an_text).strip()

        return self.repository.check_duplicate(
            data_type="kipris_design",
            biz_no=biz_no,
            key=an
        )
