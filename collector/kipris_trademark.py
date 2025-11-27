from collector.kipris_extractor.kipris_trademark_extractor import *
from collector.alter import send_naver_alert
from db.es import *
from db.mysql import *
from tqdm import tqdm
import time
import json


def search_by_ap(driver: WebDriver, comp_name: str):
    try:
        modal_search_detail = driver.find_element(By.ID, "modalSearchDetail")

        search_box = modal_search_detail.find_element(By.ID, "sd010301_g07_text_01")
        driver.execute_script("arguments[0].value = arguments[1];", search_box, comp_name)

        button = driver.find_element(By.CSS_SELECTOR, "button.btn-search[data-lang-id='adsr.search']")
        button.click()
    except Exception as e:
        print("search_by_ap : ", e)


def extract_from_trademark_details(card: WebDriver):
    info_dict = {}
    wait = WebDriverWait(card, 10)
    info_container = wait.until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="mainResultDetail"]/div[2]/div[1]/div[1]'))
    )

    invention_title = ""

    try:
        title_div = card.find_element(By.XPATH, "//*[@id='mainResultDetail']/div[1]/div[2]")
        title_kr = title_div.find_element(By.TAG_NAME, "h2").text.strip()
        invention_title = title_kr
    except Exception as e:
        print("extract_from_patent_details title : ", e)

    info_dict['InventionTitle'] = invention_title

    section_blocks = info_container.find_elements(By.CLASS_NAME, "tab-section-01")

    for section_block in section_blocks:
        title = ""
        try:
            title = get_section_title(section_block)
            if not title:
                continue

            if title == "서지정보":
                info_dict.update(extract_trademark_bibliography(section_block))
            elif title == "인명정보":
                info_dict.update(extract_trademark_people_info(section_block))
            elif title == "도형분류비엔나코드":
                info_dict.update(extract_trademark_vienna(section_block))
        except Exception as e:
            error_detail = traceback.format_exc()
            insert_error_log("Extract from trademark details", "KIPRIS_TRADEMARK", f"{title} 처리중 에러 발생 : {e}",
                             error_detail)

    return info_dict


def main():
    es = None

    try:
        driver = open_browser("trademark")
        try:
            # elasticsearch 연결
            es = get_es_conn()
        except Exception as e:
            error_detail = traceback.format_exc()
            insert_error_log("Elasticsearch connection", "KIPRIS_TRADEMARK", f"Elasticsearch 연결 실패 : {e}", error_detail)
            raise

        # try:
        #     companies = get_cmp_list("KIPRIS_PATENT")
        # except Exception as e:
        #     conn = get_connection()
        #     insert_error_log(conn, "KIPRIS_PATENT", f"기업 목록 조회 실패")
        #     conn.close()
        #     raise
        with open(r"/home/bax/fncsp/db/final_results.json", "r", encoding="utf-8") as f:
            companies = json.load(f)

        for idx, company in enumerate(tqdm(companies, desc='kipris_trademark 수집', unit="회사"), 1):
            biz_no = ""
            comp_name = ""
            trademarks = []
            now = datetime.now()

            try:
                biz_no = company['BIZ_NO']
                comp_name = company['CMP_NM']
                clean_comp_name = re.sub(r'\(.*?\)', '', comp_name)
                driver.get("https://www.kipris.or.kr/khome/search/searchResult.do?tab=trademark")
                time.sleep(1)
                search_by_ap(driver, biz_no)
                total = get_total_num(driver, "trademark")

                if total == 0:
                    print(f"{clean_comp_name} - 상표 : 검색 결과 없음")
                    # continue
                else:
                    sort_by_application_an(driver)
                    time.sleep(1)

                    current_page = 1
                    total_pages = int((total / 30) + 1)

                    while current_page <= total_pages:
                        has_result_flag, result_cards = has_result(driver)

                        for card in result_cards:
                            # 중복 확인
                            recent_trademark_an = card.find_element(By.CSS_SELECTOR, "button.tit.under").text.strip()
                            print(recent_trademark_an)
                            an = re.sub(r'\((.*?)\)', "", recent_trademark_an)
                            dup = get_application_an(es, "kipris_trade", biz_no, an)

                            if dup:
                                tqdm.write(f"{comp_name} : 중복")
                                raise DuplicateError

                            open_card(driver, card)
                            trademarks.append(extract_from_trademark_details(card))
                        if current_page < total_pages:
                            go_next_page(driver)

                        current_page += 1
                # with open("trademarks_test.json", "w", encoding="utf-8") as f:
                #     json.dump(trademarks, f, ensure_ascii=False, indent=2)

                try:
                    insert_kipris_trade(es, trademarks, biz_no)
                    insert_check_log(biz_no, "KIPRIS_TRADEMARK", now)
                    insert_cmp_data_log(biz_no, "KIPRIS_TRADEMARK", len(trademarks), now)
                    print(f"{comp_name} - {len(trademarks)}건 저장 완료")
                except Exception as e:
                    error_detail = traceback.format_exc()
                    insert_error_log("Insert data", "KIPRIS_TRADEMARK", f"데이터 삽입 실패({biz_no}) : {e}", error_detail)
            except DuplicateError as e:
                if trademarks:
                    insert_kipris_trade(es, trademarks, biz_no)
                    insert_check_log(biz_no, "KIPRIS_TRADEMARK", now)
                    insert_cmp_data_log(biz_no, "KIPRIS_TRADEMARK", len(trademarks), now)
                    print(f"{comp_name} - {len(trademarks)}건 저장 완료")
                else:
                    continue
            except DataInsertError as e:
                raise
            except Exception as e:
                error_detail = traceback.format_exc()
                insert_error_log("Process company", "KIPRIS_TRADEMARK", f"{comp_name}({biz_no}) 기업 처리중 오류 발생 : {e}",
                                 error_detail)
    except Exception as e:
        insert_error_log("Open Browser", "KIPRIS_TRADEMARK", "Cannot Open Browser", traceback.format_exc())
    finally:
        if es:
            es.close()


if __name__ == "__main__":
    email = os.getenv("EMAIL")
    password = os.getenv("PASSWORD")
    exit_message = "N/A"
    try:
        main()
        exit_message = "프로그램 정상 종료"
    except Exception as e:
        exit_message = f"프로그램 예외 종료 : {e}"
    finally:
        send_naver_alert(email, email, password, f"KIPRIS_TRADEMARK 프로그램 종료 됨 : {exit_message}")