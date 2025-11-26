from collector.kipris_extractor.kipris_utils import *

"""""
ipc 번호 : list
cpc 번호 : list
applicationNumber(출원번호) : keyword
applicationDate(출원일자) : date -> yyyy-MM-dd
applicantName(출원인) : list > text
registerNumber(등록번호) : keyword
registerDate(등록일자) : date
openNumber(공개번호) : keyword
openDate(공개일자) : date
registerStatus(법적상태) : keyword
examinationCount(심사청구항수) : integer
astrtCont(요약) : text
"""
def extract_patent_bibliography(info_div):
    field_name_mapping_table = {
        "IPC" : "IPCNumber",
        "CPC" : "CPCNumber",
        "출원인" : "ApplicantName",
        "법적상태" : "RegisterStatus",
        "심사청구항수" : "ExaminationCount",
        "요약" : "AstrtCont",
        "출원번호(일자)" : ["ApplicationNumber", "ApplicationDate"],
        "등록번호(일자)" : ["RegisterNumber", "RegisterDate"],
        "공개번호(일자)" : ["OpenNumber", "OpenDate"]
    }

    bib = {}

    try:
        rows = info_div.find_elements(By.CSS_SELECTOR, "table.table tbody tr")
    except Exception:
        rows = []

    for row in rows:
        try:
            th = row.find_element(By.TAG_NAME, "th").text.strip()
            td = row.find_element(By.TAG_NAME, "td")
        except Exception:
            continue

        if th not in field_name_mapping_table:
            continue

        field_name = field_name_mapping_table[th]

        # IPC/CPC는 리스트로 수집
        if th in ("IPC", "CPC"):
            links = td.find_elements(By.TAG_NAME, "a")
            values = [
                re.sub(r'\(.*?\)', '', clean(a.text)).replace(' ', '')
                for a in links
                if clean(a.text)
            ]
            bib[field_name] = values
        else:
            # 그 외 항목은 <a> 텍스트(전문다운 등) 제거 후 td 텍스트만
            td_text = td.get_attribute("innerText") or td.text
            # a 텍스트 제거
            for a in td.find_elements(By.TAG_NAME, "a"):
                a_txt = a.text
                if a_txt:
                    td_text = td_text.replace(a_txt, "")
            if th == "심사청구항수":
                bib[field_name] = int(td_text)
            elif "번호(일자)" in th:
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
            elif th == "출원인":
                if td_text:
                    td_text = td_text.split(" ")
                    bib[field_name] = td_text
            else:
                bib[field_name] = clean(td_text)

    # 요약
    summary_text = ""
    try:
        summary_tag = info_div.find_element(By.ID, "sum_all")
        summary_p = summary_tag.find_element(By.CSS_SELECTOR, "summary p")
        summary_text = clean(summary_p.text)
    except Exception as e:
        print("extract_patent_bibliography summary : ", e)

    # 요약도 매핑된 필드명으로 저장
    mapped_summary_key = field_name_mapping_table.get("요약", "AstrtCont")
    bib[mapped_summary_key] = summary_text

    return bib

"""
inventorCounter(발명자수) : integer
"""
def extract_patent_people_info(info_div):
    data = {}

    # 인명정보 블록 내부의 각 하위 섹션(.tab-section-02) 순회
    sections = info_div.find_elements(By.CSS_SELECTOR, "div.tab-section-02")
    for sec in sections:
        # 섹션 제목(h5)
        try:
            title = sec.find_element(By.CSS_SELECTOR, "h5.title").text.strip()
        except Exception:
            # 일부 섹션은 h5 없이 table만 있을 수 있으므로 스킵/방어
            continue
        if title and title == "발명자":
            # 표의 본문 행
            try:
                rows = sec.find_elements(By.CSS_SELECTOR, "table tbody tr")
            except Exception:
                rows = []
        else:
            continue
        data["InventorCount"] = len(rows)
    return data

"""
backwordCitation(인용) : list
backwordCitation.FCCountry(국가) : keyword
backwordCitation.FCNumber(공고번호) : keyword
backwordCitation.FCDate(공보일자) : date
backwordCitation.FCTitle(발명의명칭) : text
backwordCitation.FCIPC(IPC) : keyword
forwardCitation(피인용) : list
forwardCitation.BCNumber(출원번호) : keyword
forwardCitation.BCDate(출원일자) : date
forwardCitation.BCTitle(발명의명칭) : text
forwardCitation.BCIPC(IPC) : keyword
"""
def extract_citations(info_div):
    def _parse_table_rows(table_el, expected_headers, mapping_table):
        rows_out = []
        try:
            first_td = table_el.find_element(By.CSS_SELECTOR, "tbody tr td")
            only_text = clean(first_td.get_attribute("innerText") or first_td.text)
            if "데이터가 존재하지 않습니다." in only_text:
                return None
        except Exception:
            pass

        for tr in table_el.find_elements(By.CSS_SELECTOR, "tbody tr"):
            tds = tr.find_elements(By.TAG_NAME, "td")
            if not tds:
                continue
            if len(tds) == 1 and "데이터가 존재하지 않습니다." in clean(tds[0].get_attribute("innerText") or tds[0].text):
                continue

            row = {}
            for i, td in enumerate(tds):
                header = expected_headers[i] if i < len(expected_headers) else f"col_{i+1}"
                field_name = mapping_table[header]
                if header == "공보일자" or header == "출원 연월일":
                    date = text_without_em(td)
                    date = datetime.strptime(date, "%Y.%m.%d").strftime("%Y-%m-%d")
                    row[field_name] = date
                elif header == "IPC":
                    ipc = text_without_em(td).replace(" ", "")
                    row[field_name] = ipc
                else:
                    row[field_name] = text_without_em(td)
            rows_out.append(row)
        return rows_out

    result = {}

    for sec in info_div.find_elements(By.CSS_SELECTOR, "div.tab-section-02"):
        try:
            title_el = sec.find_element(By.CSS_SELECTOR, "h5.title")
            title = clean(title_el.get_attribute("innerText") or title_el.text)
        except Exception:
            continue

        tables = sec.find_elements(By.CSS_SELECTOR, "table.table.table-hrzn")
        if not tables:
            continue
        table = tables[0]

        if title == "인용":
            headers = ["국가", "공보번호", "공보일자", "발명의 명칭", "IPC"]
            field_name_mapping_table = {
                "국가" : "FCCountry",
                "공보번호" : "FCNumber",
                "공보일자" : "FCDate",
                "발명의 명칭" : "FCTitle",
                "IPC" : "FCIPC"
            }
            result["BackwardCitation"] = _parse_table_rows(table, headers,field_name_mapping_table)
        elif title == "피인용":
            headers = ["출원번호(일자)", "출원 연월일", "발명의 명칭", "IPC"]
            field_name_mapping_table = {
                "출원번호(일자)" : "BCNumber",
                "출원 연월일" : "BCDate",
                "발명의 명칭" : "BCTitle",
                "IPC" : "BCIPC"
            }
            result["ForwardCitation"] = _parse_table_rows(table, headers,field_name_mapping_table)

    return result

"""
family.FamilyNumber(패밀리번호) : keyword
family.FamilyCountrycode(국가코드) : keyword
family.FamilyCountryname(국가명) : keyword
family.FamilyType(종류) : keyword
dOCDBFamily(DOCDB패밀리정보) : list
dOCDBFamily.DOCDBnumber(패밀리번호) : keyword
dOCDBFamily.DOCDBcountrycode(국가코드) : keyword
dOCDBFamily.DOCDBcountryname(국가명) : keyword
dOCDBFamily.DOCDBtype(종류) : keyword
"""
def extract_family_info(info_div):
    def _parse_table(table_el, mapping_table):
        # 헤더 동적 수집
        headers = [
            clean(th.get_attribute("innerText") or th.text)
            for th in table_el.find_elements(By.CSS_SELECTOR, "thead th")
        ]

        # 빈 데이터 처리
        try:
            first_td = table_el.find_element(By.CSS_SELECTOR, "tbody tr td")
            only_text = clean(first_td.get_attribute("innerText") or first_td.text)
            # colspan으로 "데이터가 존재하지 않습니다." 한 줄만 있는 경우
            if "데이터가 존재하지 않습니다." in only_text and first_td.get_attribute("colspan"):
                return None
        except Exception as e:
            print("extract_family_info _parse_table : ", e)
            pass

        rows_out = []
        for tr in table_el.find_elements(By.CSS_SELECTOR, "tbody tr"):
            tds = tr.find_elements(By.TAG_NAME, "td")
            if not tds:
                continue
            if len(tds) == 1 and "데이터가 존재하지 않습니다." in clean(tds[0].get_attribute("innerText") or tds[0].text):
                continue

            row = {}
            for i, td in enumerate(tds):
                key = headers[i] if i < len(headers) else f"col_{i+1}"
                if key not in mapping_table:
                    continue
                field_name = mapping_table[key]
                if field_name == "FamilyNumber":
                    row[field_name] = text_without_em(td).split(" ")[0]
                else :
                    row[field_name] = text_without_em(td)
            rows_out.append(row)
        return rows_out

    result = {}

    # 첫 번째 섹션 테이블(opFamilyTable)
    try:
        op_table = info_div.find_element(By.CSS_SELECTOR, "table#opFamilyTable.table.table-hrzn")
        field_name_mapping_table = {
            "패밀리번호" : "FamilyNumber",
            "국가코드" : "FamilyCountrycode",
            "국가명" : "FamilyCountryname",
            "종류" : "FamilyType"
        }
        result["Family"] = _parse_table(op_table, field_name_mapping_table)
    except Exception as e:
        print("extract_family_info family : ", e)
        pass

    # 두 번째 섹션 테이블(docFamilyTable)
    try:
        doc_table = info_div.find_element(By.CSS_SELECTOR, "table#docFamilyTable.table.table-hrzn")
        field_name_mapping_table = {
            "패밀리번호" : "DOCDBnumber",
            "국가코드" : "DOCDBcountrycode",
            "국가명" : "DOCDBcountryname",
            "종류" : "DOCDBtype"
        }
        result["DOCDBFamily"] = _parse_table(doc_table, field_name_mapping_table)
    except Exception as e:
        print("extract_family_info DOCDBFamily : ", e)
        pass

    return result

"""
researchData(국가개발연구사업) : list
researchData.ResearchDepartment(연구부처) : textㄴ
researchData.ResearchInstitution(주관기관) : text
researchData.ResearchBusiness(연구사업) : text
researchData.ResearchProject(연구과제) : text

"""
def extract_national_rnd(info_div):
    result = {}
    result["ResearchData"] = None

    # 섹션 내 단일 테이블
    table = info_div.find_element(By.CSS_SELECTOR, "table.table.table-hrzn")

    # 빈 데이터 한 줄(colspan) 처리
    try:
        first_td = table.find_element(By.CSS_SELECTOR, "tbody tr td")
        only_text = clean(first_td.get_attribute("innerText") or first_td.text)
        if "데이터가 존재하지 않습니다." in only_text and first_td.get_attribute("colspan"):
            return result
    except Exception as e:
        print("extract_national_rnd : ", e)
        return result

    for tr in table.find_elements(By.CSS_SELECTOR, "tbody tr"):
        tds = tr.find_elements(By.TAG_NAME, "td")
        if not tds:
            return result
        if len(tds) == 1 and "데이터가 존재하지 않습니다." in clean(tds[0].get_attribute("innerText") or tds[0].text):
            return result

        # 컬럼: 순번, 연구부처, 주관기관, 연구사업, 연구과제 (순서 고정)
        row = {}
        labels = ["순번", "연구부처", "주관기관", "연구사업", "연구과제"]
        field_name_mapping_table = {
            "연구부처" : "ResearchDepartment",
            "주관기관" : "ResearchInstitution",
            "연구사업" : "ResearchBusiness",
            "연구과제" : "ResearchProject"
        }
        for i, key in enumerate(labels):
            if key in field_name_mapping_table:
                field_name = field_name_mapping_table[key]
                if i < len(tds):
                    row[field_name] = text_without_em(tds[i])
                else:
                    row[field_name] = None

        result["ResearchData"] = row

    return result

    return result