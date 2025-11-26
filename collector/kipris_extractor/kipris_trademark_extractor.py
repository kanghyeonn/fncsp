from collector.kipris_extractor.kipris_utils import *

"""
registerStatus(법적상태) : text
classification(상품분류) : text
applicationNumber(출원번호) : keyword
applicationDate(출원일자) : date -> yyyy-MM-dd
registerNumber(등록번호) : keyword
registerDate(등록일자) : date
appIPubINumber(출원공고번호) : keyword
appIPubINumber(출원공고일자) : date
"""
def extract_trademark_bibliography(info_div):
    field_name_mapping_table = {
        "법적상태" : "RegisterStatus",
        "상품분류" : "Classification",
        "출원번호(일자)" : ["ApplicationNumber", "ApplicationDate"],
        "등록번호(일자)" : ["RegisterNumber", "RegisterDate"],
        "출원공고번호(일자)" : ["AppIPubINumber", "AppIPubIDate"]
    }

    bib = {}

    try:
        rows = info_div.find_elements(By.CSS_SELECTOR, "table.table tbody tr")
    except Exception as e:
        print("extract_trademark_bibliography select table : ", e)
        rows = []

    for row in rows:
        try:
            th = row.find_element(By.TAG_NAME, "th").text.strip()
            td = row.find_element(By.TAG_NAME, "td")
        except Exception as e:
            print("extract_trademark_bibliography select td,tr : ", e)
            continue

        if th not in field_name_mapping_table:
            continue

        field_name = field_name_mapping_table[th]
        td_text = td.get_attribute("innerText") or td.text

        if "번호(일자)" in th:
            if td_text:
                td_text = clean(td_text)
                td_list = td_text.split(" ")
                date_str = td_list[1]
                cleand = date_str.strip("()")
                date = datetime.strptime(cleand, "%Y.%m.%d").strftime("%Y-%m-%d")
                bib[field_name[0]] = td_list[0]
                bib[field_name[1]] = date
            else:
                bib[field_name[0]] = None
                bib[field_name[1]] = None
        else:
            bib[field_name] = clean(td_text)

    return bib

"""
applicant(출원인) : list > text
agent(대리인) : list > text
"""
def extract_trademark_people_info(info_div):
    people_info = {}

    sections = info_div.find_elements(By.CSS_SELECTOR, "div.tab-section-02")
    for sec in sections:
        names = []
        try:
            title = sec.find_element(By.TAG_NAME, "h5").text.strip()
        except Exception as e:
            print("extract_trademark_people_info : ", e)
            continue
        if title and title in ["출원인", "대리인"]:
            name_cells = sec.find_elements(By.CSS_SELECTOR, "tbody tr td:nth-child(2)")
            for cell in name_cells:
                lines = cell.text.split("\n")
                names.append(lines[0])
                #print("names : ", names)
            if len(names) == 0:
                names = None
            if title == "출원인":
                people_info["Applicant"] = names
            elif title == "대리인":
                people_info["Agent"] = names

    return people_info

"""
viennaCode(도형코드) : list > text
"""
def extract_trademark_vienna(info_div):
    vienna_info = {}
    vienna_codes = []
    try:
        name_cells = info_div.find_elements(By.CSS_SELECTOR, "tbody tr td:nth-child(2)")
        if name_cells:
            for cell in name_cells:
                vienna_code = cell.text.strip()
                vienna_codes.append(vienna_code)
            vienna_info["ViennaCode"] = vienna_codes
        else:
            vienna_info["ViennaCode"] = None
    except Exception as e:
        print("extract_trademark_vienna : ", e)

    return vienna_info