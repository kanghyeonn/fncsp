from collector.kipris_extractor.kipris_utility_extractor import *
from db.es import *
from db.mysql import *
from tqdm import tqdm
from datetime import datetime
from collector.alter import send_naver_alert
import os
import json
import time


# kipris에서 특허 데이터를 추출하는 함수
def extract_from_utility_details(card: WebElement) -> dict:
    info_dict = {}
    wait = WebDriverWait(card, 10)
    info_container = wait.until(
        EC.presence_of_element_located((By.XPATH, '//*[@id="mainResultDetail"]/div[2]/div[1]/div[1]'))
    )
    invention_title = ""
    section_blocks = info_container.find_elements(By.CLASS_NAME, "tab-section-01")
    # 특허 명칭 추출
    try:
        title_div = card.find_element(By.XPATH, "//*[@id='mainResultDetail']/div[1]/div[2]")
        title_kr = title_div.find_element(By.TAG_NAME, "h2").text.strip()
        title_eng = title_div.find_element(By.TAG_NAME, "p").text.strip()
        invention_title = title_kr + " " + title_eng
    except Exception as e:
        print("extract_from_utility_details title : ", e)

    info_dict['InventionTitle'] = invention_title

    for section_block in section_blocks:
        title = ""
        try:
            title = get_section_title(section_block)
            if title_contains(title, "서지정보", "bibliography"):
                info_dict.update(extract_patent_bibliography(section_block))
            elif title_contains(title, "인명정보", "people", "applicant", "inventor"):
                info_dict.update(extract_patent_people_info(section_block))
            elif title_contains(title, "인용/피인용", "인용", "피인용", "citation", "cited"):
                info_dict.update(extract_citations(section_block))
            elif title_contains(title, "패밀리정보", "family"):
                info_dict.update(extract_family_info(section_block))
            elif title_contains(title, "국가연구개발사업", "rnd", "research"):
                info_dict.update(extract_national_rnd(section_block))
        except Exception as e:
            error_detail = traceback.format_exc()
            insert_error_log("Extract from utility details", "KIPRIS_UTILITY", f"{title} 처리중 에러 발생 : {e}", error_detail)
    return info_dict


def save_results_to_json(filename: str, comp_name: str, result: list):
    # 기존 파일이 있으면 불러오기
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = []

    # 현재 회사 결과 추가
    data.append({
        "company": comp_name,
        "results": result
    })

    # 다시 파일로 저장
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main():
    es = None
    try:
        driver = open_browser("patent", "utility")
        # elasticsearch 연결
        try:
            es = get_es_conn()
        except Exception as e:
            error_detail = traceback.format_exc()
            insert_error_log("Elasticsearch connection", "KIPRIS_UTILITY", f"Elasticsearch 연결 실패 : {e}", error_detail)
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

        for idx, company in enumerate(tqdm(companies, desc='kipris_utility 수집', unit="회사"), 1):
            biz_no = ""
            comp_name = ""
            utilities = []
            now = datetime.now()

            try:
                biz_no = company["BIZ_NO"]
                comp_name = company['CMP_NM']
                clean_comp_name = re.sub(r'\(.*?\)', '', comp_name)
                driver.get("https://www.kipris.or.kr/khome/search/searchResult.do?tab=patent")
                # driver = open_browser("patent", "utility")
                time.sleep(2)
                search_by_ap(driver, "sd01_ck0202", biz_no)
                total = get_total_num(driver, "patent")

                if total == 0:
                    print(f"{clean_comp_name} - 실용신안 : 검색 결과 없음")
                    continue

                sort_by_application_an(driver)
                time.sleep(1)

                current_page = 1
                total_pages = int((total / 30) + 1)

                while current_page <= total_pages:
                    has_result_flag, result_cards = has_result(driver)

                    for card in result_cards:
                        # 중복 확인
                        recent_utility_an = card.find_element(By.CLASS_NAME, "txt").text.strip()
                        print(recent_utility_an)
                        an = re.sub(r'\((.*?)\)', "", recent_utility_an)
                        dup = get_application_an(es, "kipris_utility", biz_no, an)

                        if dup:
                            tqdm.write(f"{comp_name} : 중복")
                            raise DuplicateError

                        open_card(driver, card)
                        utilities.append(extract_from_utility_details(card))
                    if current_page < total_pages:
                        go_next_page(driver)

                    current_page += 1

                # with open("patent_test.json", "w", encoding="utf-8") as f:
                #     json.dump(result, f, ensure_ascii=False, indent=2)

                try:
                    insert_kipris_utility(es, utilities, biz_no)
                    insert_check_log(biz_no, 'KIPRIS_UTILITY', now)
                    insert_cmp_data_log(biz_no, "KIPRIS_UTILITY", len(utilities), now)
                    print(f"{comp_name} - {len(utilities)}건 저장 완료")
                except Exception as e:
                    error_detail = traceback.format_exc()
                    insert_error_log("Insert data", "KIPRIS_UTILITY", f"데이터 삽입 실패({biz_no}) : {e}", error_detail)
            except DuplicateError as e:
                if utilities:
                    insert_kipris_utility(es, utilities, biz_no)
                    insert_check_log(biz_no, "KIPRIS_UTILITY", now)
                    insert_cmp_data_log(biz_no, "KIPRIS_UTILITY", len(utilities), now)
                    print(f"{comp_name} - {len(utilities)}건 저장 완료")
                else:
                    continue
            except DataInsertError as e:
                raise
            except Exception as e:
                error_detail = traceback.format_exc()
                insert_error_log("Process company", "KIPRIS_UTILITY", f"{comp_name}({biz_no}) 기업 처리중 오류 발생 : {e}",
                                 error_detail)
    except Exception as e:
        insert_error_log("Open Browser", "KIPRIS_UTILITY", "Cannot Open Browser", traceback.format_exc())
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
        send_naver_alert(email, email, password, f"KIPRIS_UTILITY 프로그램 종료 됨 : {exit_message}")
