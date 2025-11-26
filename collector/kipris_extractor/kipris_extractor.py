from collector.kipris_extractor.kipris_utils import *


"""
특허 실용신안
서지정보 -> extract_patent_bibliography
인명정보 -> extract_people_info
지정국 -> extract_designated_countries
인용/피인용 -> extract_citations
청구항 -> extract_claims
패밀리정보 -> extract_family_info
국가연구개발사업 -> extract_national_rnd
"""

# 특허/실용신안 서지정보 추출 함수
def extract_patent_bibliography(info_div):
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

        # IPC/CPC는 리스트로 수집
        if th in ("IPC", "CPC"):
            links = td.find_elements(By.TAG_NAME, "a")
            values = [clean(a.text) for a in links if clean(a.text)]
            bib[th] = values
        else:
            # 그 외 항목은 <a> 텍스트(전문다운 등) 제거 후 td 텍스트만
            td_text = td.get_attribute("innerText") or td.text
            # a 텍스트 제거
            for a in td.find_elements(By.TAG_NAME, "a"):
                a_txt = a.text
                if a_txt:
                    td_text = td_text.replace(a_txt, "")
            bib[th] = clean(td_text)

    # 요약
    summary_text = ""
    try:
        p = info_div.find_element(By.CSS_SELECTOR, ".tit-summary + p")
        summary_text = clean(p.text)
        if not summary_text:
            summary_el = info_div.find_element(By.CSS_SELECTOR, "summary")
            summary_text = clean(summary_el.text)
    except Exception:
        try:
            summary_el = info_div.find_element(By.CSS_SELECTOR, "summary")
            summary_text = clean(summary_el.text)
        except Exception:
            pass

    bib["요약"] = summary_text
    return bib

# 인명정보 추출 함수
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

        # 표의 본문 행
        try:
            rows = sec.find_elements(By.CSS_SELECTOR, "table tbody tr")
        except Exception:
            rows = []

        items = []
        empty_section = False
        for tr in rows:
            tds = tr.find_elements(By.TAG_NAME, "td")
            if not tds:
                continue

            # "데이터가 존재하지 않습니다." 처리
            if any("데이터가 존재하지 않습니다." in clean(td.text) for td in tds):
                empty_section = True
                break

            # 1열: 번호
            no_text = clean(tds[0].get_attribute("innerText"))
            # "번호" 라벨 제거
            no_text = clean(no_text.replace("번호", ""))

            # 2열: 이름(번호) + 드롭박스 추가정보
            name_td = tds[1]
            name_inner = name_td.get_attribute("innerText") or name_td.text
            # "이름(번호)" 라벨 제거
            name_inner = clean(name_inner.replace("이름(번호)", ""))
            name, sid = parse_name_and_id(name_inner)

            # 드롭박스(법인번호/사업자번호 등) 추가정보
            extra = {}
            for box in name_td.find_elements(By.CSS_SELECTOR, ".dropbox-select"):
                try:
                    key = clean(box.find_element(By.CSS_SELECTOR, ".btn-dropbox").text)
                    val = clean(box.find_element(By.CSS_SELECTOR, ".dropbox-con .txt").text)
                    if key and val:
                        extra[key] = val
                except Exception:
                    continue

            # 3열: 주소
            addr = clean(tds[2].get_attribute("innerText") if len(tds) > 2 else "")

            item = {
                "번호": no_text,
                "이름": name,
                "주소": addr
            }
            if sid:
                item["식별번호"] = sid
            if extra:
                item["추가정보"] = extra

            items.append(item)

        data[title] = [] if empty_section else items

    return data

# 지정국 추출 함수
def extract_designated_countries(info_div):
    result_rows = []

    # 섹션 안의 지정국 테이블 찾기 (class=table-hrzn)
    table = info_div.find_element(By.CSS_SELECTOR, "table.table.table-hrzn")

    # 헤더 추출
    headers = [
        clean(th.get_attribute("innerText") or th.text)
        for th in table.find_elements(By.CSS_SELECTOR, "thead th")
    ]

    # 본문 행
    rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
    for tr in rows:
        tds = tr.find_elements(By.TAG_NAME, "td")
        if not tds:
            continue

        # '데이터가 존재하지 않습니다.' 처리
        only_text = clean(tds[0].get_attribute("innerText") or tds[0].text)
        if "데이터가 존재하지 않습니다." in only_text:
            return {"지정국": []}

        row_data = {}
        for i, td in enumerate(tds):
            text = clean(td.get_attribute("innerText") or td.text)
            if i < len(headers):
                row_data[headers[i]] = text
            else:
                # 혹시 헤더보다 컬럼이 많다면 예비 키로 보관
                row_data[f"col_{i+1}"] = text

        result_rows.append(row_data)

    return {"지정국": result_rows}

# 인용/피인용 추출 함수
def extract_citations(info_div):
    def _parse_table_rows(table_el, expected_headers):
        rows_out = []
        try:
            first_td = table_el.find_element(By.CSS_SELECTOR, "tbody tr td")
            only_text = clean(first_td.get_attribute("innerText") or first_td.text)
            if "데이터가 존재하지 않습니다." in only_text:
                return []
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
                row[header] = text_without_em(td)
            rows_out.append(row)
        return rows_out

    result = {"인용": [], "피인용": []}

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
            result["인용"] = _parse_table_rows(table, headers)
        elif title == "피인용":
            headers = ["출원번호(일자)", "출원 연월일", "발명의 명칭", "IPC"]
            result["피인용"] = _parse_table_rows(table, headers)

    return result

# 청구항 추출 함수
def extract_claims(info_div):
    def _claim_text(td_el) -> str:
        # 우선 <claim-text> 우선 추출
        try:
            ct = td_el.find_element(By.CSS_SELECTOR, "claim-text")
            html = ct.get_attribute("innerHTML") or ""
        except Exception:
            html = td_el.get_attribute("innerHTML") or ""

        # <br> -> 개행
        html = re.sub(r"(?i)<br\s*/?>", "\n", html)
        # 태그 제거
        txt = re.sub(r"<[^>]+>", "", html)
        # 공백 정리
        lines = [line.strip() for line in txt.split("\n")]
        txt = "\n".join([ln for ln in lines if ln != ""])
        return clean(txt)

    result = {"청구항": []}

    # 섹션 내 테이블
    table = info_div.find_element(By.CSS_SELECTOR, "table.table.table-hrzn")

    # 빈 데이터 검사
    try:
        first_td = table.find_element(By.CSS_SELECTOR, "tbody tr td")
        only_text = clean(first_td.get_attribute("innerText") or first_td.text)
        if "데이터가 존재하지 않습니다." in only_text:
            return result
    except Exception:
        pass

    for tr in table.find_elements(By.CSS_SELECTOR, "tbody tr"):
        tds = tr.find_elements(By.TAG_NAME, "td")
        if not tds:
            continue
        if len(tds) == 1 and "데이터가 존재하지 않습니다." in clean(tds[0].get_attribute("innerText") or tds[0].text):
            continue

        번호 = text_without_em(tds[0])
        청구항 = _claim_text(tds[1]) if len(tds) > 1 else ""

        result["청구항"].append({"번호": 번호, "청구항": 청구항})

    return result

# 패밀리정보 추출 함수
def extract_family_info(info_div):
    def _parse_table(table_el):
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
                return []
        except Exception:
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
                row[key] = text_without_em(td)
            rows_out.append(row)
        return rows_out

    result = {"패밀리정보": [], "DOCDB 패밀리정보": []}

    # 첫 번째 섹션 테이블(opFamilyTable)
    try:
        op_table = info_div.find_element(By.CSS_SELECTOR, "table#opFamilyTable.table.table-hrzn")
        result["패밀리정보"] = _parse_table(op_table)
    except Exception:
        pass

    # 두 번째 섹션 테이블(docFamilyTable)
    try:
        doc_table = info_div.find_element(By.CSS_SELECTOR, "table#docFamilyTable.table.table-hrzn")
        result["DOCDB 패밀리정보"] = _parse_table(doc_table)
    except Exception:
        pass

    return result

# 국가연구개발사업 추출 함수
def extract_national_rnd(info_div):
    result = {"국가연구개발사업": []}

    # 섹션 내 단일 테이블
    table = info_div.find_element(By.CSS_SELECTOR, "table.table.table-hrzn")

    # 빈 데이터 한 줄(colspan) 처리
    try:
        first_td = table.find_element(By.CSS_SELECTOR, "tbody tr td")
        only_text = clean(first_td.get_attribute("innerText") or first_td.text)
        if "데이터가 존재하지 않습니다." in only_text and first_td.get_attribute("colspan"):
            return result
    except Exception:
        pass

    for tr in table.find_elements(By.CSS_SELECTOR, "tbody tr"):
        tds = tr.find_elements(By.TAG_NAME, "td")
        if not tds:
            continue
        if len(tds) == 1 and "데이터가 존재하지 않습니다." in clean(tds[0].get_attribute("innerText") or tds[0].text):
            continue

        # 컬럼: 순번, 연구부처, 주관기관, 연구사업, 연구과제 (순서 고정)
        row = {}
        labels = ["순번", "연구부처", "주관기관", "연구사업", "연구과제"]
        for i, key in enumerate(labels):
            if i < len(tds):
                row[key] = text_without_em(tds[i])
            else:
                row[key] = ""

        result["국가연구개발사업"].append(row)

    return result

"""
상표
서지정보 -> extract_trademark_bibliography
인명정보 -> extract_person_info
도형분류(비엔나)코드 -> extract_vienna_codes
상표설명/지정상품 -> extract_trademark_data

마드리드출원정보와 공존동의상표는 데이터가 거의 없으므로 추출 x
"""

# 상표 서지정보 추출함수
def extract_trademark_bibliography(info_div):
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

        # td 내부 텍스트를 가져오되, <a> 텍스트는 제외
        td_html = td.get_attribute("innerHTML")

        # <br> 기준으로 분리 (여러 값 존재 가능)
        parts = td_html.split("<br")
        values = []
        for part in parts:
            # <a> 태그 제거 및 텍스트 추출
            part_text = td.text if "<a" not in part else td.get_attribute("innerText")
            part_text = clean(part_text)
            if part_text:
                values.append(part_text)

        # <a> 요소들 중 실제로 텍스트가 있는 것도 추가
        a_tags = td.find_elements(By.TAG_NAME, "a")
        for a in a_tags:
            a_text = clean(a.text)
            if a_text and a_text not in values:
                values.append(a_text)

        # 값이 여러 개면 리스트로 저장, 하나면 문자열로 저장
        # 예: 상품분류 → ["12판 41", "12판 42"]
        td_text = td.get_attribute("innerText") or td.text
        td_text = clean(td_text)
        td_values = [v.strip() for v in td_text.split("\n") if v.strip()]

        bib[th] = td_values if len(td_values) > 1 else (td_values[0] if td_values else "")

    # 요약이 있는 경우 추가
    summary_text = ""
    try:
        p = info_div.find_element(By.CSS_SELECTOR, ".tit-summary + p")
        summary_text = clean(p.text)
        if not summary_text:
            summary_el = info_div.find_element(By.CSS_SELECTOR, "summary")
            summary_text = clean(summary_el.text)
    except Exception:
        try:
            summary_el = info_div.find_element(By.CSS_SELECTOR, "summary")
            summary_text = clean(summary_el.text)
        except Exception:
            pass

    if summary_text:
        bib["요약"] = summary_text

    return bib

# 상표 인명정보
def extract_trademark_person_info(info_div):
    """
    인명정보(div.tab-section-01) 블록에서
    출원인, 대리인, 최종권리자, 등록 이후 대리인 정보를 추출
    """
    data = {}

    # 각 소제목(h5.title) 기준으로 테이블을 탐색
    subsections = info_div.find_elements(By.CSS_SELECTOR, ".tab-section-02")

    for sub in subsections:
        try:
            title = sub.find_element(By.CSS_SELECTOR, "h5.title").text.strip()
        except Exception:
            continue

        table_data = []
        try:
            rows = sub.find_elements(By.CSS_SELECTOR, "table tbody tr")
        except Exception:
            rows = []

        for row in rows:
            entry = {}
            cols = row.find_elements(By.TAG_NAME, "td")
            if not cols:
                continue

            # 테이블 구조에 따라 컬럼 개수 2~3개로 구분
            if len(cols) == 3:
                no = clean(cols[0].text)
                name_field = cols[1]
                addr = clean(cols[2].text)
                entry["번호"] = no
            elif len(cols) == 2:
                name_field = cols[0]
                addr = clean(cols[1].text)
            else:
                continue

            # 이름, 번호, 법인번호 등 정제
            name_text = clean(name_field.text)

            name_list = name_text.split(" ")
            cop_name = ""
            for name in name_list:
                if name != "법인번호" and name != "사업자번호":
                    cop_name += (name + " ")
                elif name == "법인번호":
                    cno_tag = sub.find_element(By.CSS_SELECTOR, "[data-lang-id='dtvw.trademark.cno']")
                    if cno_tag:
                        value = cno_tag.get_attribute('data-dropbox-btn')
                        cno = re.sub(r"[A-Za-z]", "", value)
                        entry['법인번호'] = cno
                elif name == "사업자번호":
                    brn_tag = sub.find_element(By.CSS_SELECTOR, "[data-lang-id='dtvw.trademark.brn']")
                    if brn_tag:
                        value = brn_tag.get_attribute('data-dropbox-btn')
                        brn = re.sub(r"[A-Za-z]", "", value)
                        entry["사업자번호"] = brn

            entry["이름"] = cop_name.strip()
            entry["주소"] = addr

            table_data.append(entry)

        data[title] = table_data

    return data

# 도형분류(비엔나)코드
def extract_vienna_codes(info_div):
    result_rows = []

    rows = info_div.find_elements(By.CSS_SELECTOR, "table.table-hrzn tbody tr")

    for row in rows:
        cols = row.find_elements(By.TAG_NAME, "td")
        entry = {}

        for col in cols:
            try:
                # <em> 태그 안의 텍스트를 키로 사용
                # <em>을 제외한 나머지 텍스트를 값으로 추출
                key = col.find_element(By.TAG_NAME, "em").get_attribute("innerText")
                value = col.text
                entry[key] = value
            except Exception:
                # <em>이 없는 경우: 그냥 텍스트 전체를 값으로 저장
                entry["값"] = col.text.strip()

        result_rows.append(entry)

    return result_rows

# 상표설명/지정상품
def extract_trademark_data(info_div):
    result_data = {}
    try:
        desc_td = info_div.find_elements(By.CSS_SELECTOR, "caption[data-lang-id='dtvw.trademark.tdinfo'] ~ tbody td")
        description = desc_td.get_attribute("innerText").strip()
        result_data["상표설명"] = description if description else "데이터가 없습니다"
    except:
        result_data["상표설명"] = "데이터가 없습니다"

    items_list = []
    try:
        rows = info_div.find_elements(By.CSS_SELECTOR, "#goodsList tbody tr")
        if not rows:
            result_data["지정상품"] = "데이터가 없습니다"
        else:
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 4:
                    items = {
                        "번호" : cols[0].text.strip(),
                        "상품분류" : cols[1].text.strip(),
                        "유사군코드" : cols[2].text.strip(),
                        "지정상품(영문)" : cols[3].text.strip()
                    }
                    items_list.append(items)
            result_data["지정상품"] = items_list if items_list else "데이터가 없습니다"
    except:
        result_data["지정상품"] = "데이터가 없습니다"

    return result_data

"""
심판
상세정보 -> extract_judgement_details
심판(번원사건) 이력사항 -> extract_judgement_history
"""

# 심판 상세정보
def extract_judgement_details(info_div):

    result_rows = {}

    try:
        sub_section = info_div.find_element(By.CSS_SELECTOR, ".tab-section-02")
        table = sub_section.find_element(By.CSS_SELECTOR, "caption[data-lang-id='dtvw.judgment.dtls'] ~ tbody")

        rows = table.find_elements(By.TAG_NAME, "tr")

        for row in rows:
           th = row.find_element(By.TAG_NAME, "th").text.strip()
           td = row.find_element(By.TAG_NAME, "td")

           links = td.find_elements(By.TAG_NAME, "a")
           lis =  td.find_elements(By.TAG_NAME, "li")

           if links:
               values = [a.text.strip() for a in links if a.text.strip()]
               result_rows[th] = values if values else "데이터가 없습니다"
           elif lis:
               values = [li.text.strip() for li in lis if li.text.strip()]
               result_rows[th] = values if values else "데이터가 없습니다"
           else:
               text = td.text.strip()
               result_rows[th] = text if text else "데이터가 없습니다"
    except Exception as e:
        print(e)
        result_rows['상세정보'] = "데이터가 없습니다"

    return result_rows

# 심판(법원사건)이력사항
def extract_judgement_history(info_div):
    result_rows= []

    try:
        table = info_div.find_element(By.CSS_SELECTOR, "table.table-hrzn")

        rows = table.find_elements(By.CSS_SELECTOR, "tbody tr")
        for row in rows:
            cols = row.find_elements(By.TAG_NAME, "td")
            if len(cols) >= 5:
                data = {
                    "번호" : cols[0].text.strip(),
                    "서류명" : cols[1].text.strip(),
                    "접수/발송일자": cols[2].text.strip(),
                    "처리상태" : cols[3].text.strip(),
                    "접수/발송번호" : cols[4].text.strip(),
                }
                result_rows.append(data)

    except Exception as e:
        print(e)
        result_rows.append("데이터가 없습니다")

    return result_rows


