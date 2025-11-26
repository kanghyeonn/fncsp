from collector.kipris_extractor.kipris_utils import *

"""
registerStatus(법적상태) : text
designClass(한국분류) : text
locarnoClass(국제분류) : text
applicationNumber(출원번호) : keyword
applicationDate(출원일자) : date -> yyyy-MM-dd
applicantName(출원인) : list > text
registerNumber(등록번호) : keyword
registerDate(등록일자) : date
openNumber(공개번호) : keyword
openDate(공개일자) : date
"""
def extract_design_bibliography(info_div):
    field_name_mapping_table = {
        "법적상태" : "RegisterStatus",
        "한국분류" : "DesignClass",
        "국제분류" : "LocarnoClass",
        "출원번호(일자)" : ["ApplicationNumber", "ApplicationDate"],
        "등록번호(일자)" : ["RegisterNumber", "RegisterDate"],
        "공개번호(일자)" : ["OpenNumber", "OpenDate"]
    }

    bib = {}

    try:
        rows = info_div.find_elements(By.CSS_SELECTOR, "table.table tbody tr")
    except Exception as e:
        print("extract_design_bibliography select table : ", e)
        rows = []

    for row in rows:
        try:
            th = row.find_element(By.TAG_NAME, "th").text.strip()
            td = row.find_element(By.TAG_NAME, "td")
        except Exception as e:
            print("extract_design_bibliography select td,tr : ", e)
            continue

        if th not in field_name_mapping_table:
            continue

        field_name = field_name_mapping_table[th]
        td_text = td.get_attribute("innerText") or td.text

        if "번호(일자)" in th:
            if td_text:
                td_text = clean(td_text)
                td_list = td_text.split("(")
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
inventor(창작자) : list > text
agent(대리인) : list > text
"""
def extract_design_people_info(info_div, title):
    people_info = {}
    names = []

    name_cells = info_div.find_elements(By.CSS_SELECTOR, "tbody tr td:nth-child(2)")
    for cell in name_cells:
        lines = cell.text.split("\n")
        names.append(lines[0])
    if len(names) == 0:
        names = None
    if title == "인명정보":
        people_info["Applicant"] = names
    elif title == "창작자":
        people_info["Inventor"] = names
    elif title == "대리인":
        people_info["Agent"] = names

    return people_info