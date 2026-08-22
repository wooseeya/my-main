"""
등기부등본(등기사항증명서) 파싱 + 권리분석 룰엔진
============================================================

main.py가 기대하는 공개 인터페이스:
  - class RegistryError(Exception)
  - async def parse_uploaded_registry_pdf(data: bytes, filename: str) -> dict
  - async def parse_uploaded_registry_files(file_payloads: list[tuple[str, bytes]]) -> dict

두 함수 모두 동일한 결과 스키마를 돌려준다 (main.py의 run_verification()과
index.html의 renderRegStandaloneResult()가 그대로 소비하는 형태):

{
  "ok": True,
  "warnings": [str, ...],                 # OCR 대체 인식 등 경고 메시지
  "표제부_원문": str,                        # 첫 문서의 표제부 원문(지번 대조용)
  "표제부_목록": [{"문서명":..., "원문":...}],
  "갑구": [row, ...],
  "을구": [row, ...],
  "분석": {                                 # 룰엔진 결과
    "분석결과": "계약가능" | "주의" | "위험" | "확인필요",
    "종합_요약": str,
    "기준권리": {...} | None,
    "기준권리_판단사유": [str, ...],
    "소멸되는_권리": [row, ...],
    "인수되는_권리": [row, ...],
    "확인필요_권리": [row, ...],
    "위험도": "낮음" | "중간" | "높음",
    "체크리스트": [str, ...],
    "법률상_유의사항": [str, ...],
  },
  "공동담보목록_원문": [{"번호":..., "유형":..., "표시":...}, ...],
  "공동담보_병합": [{"권리종류":..., "권리자":..., "금액":..., "접수일":..., "문서목록":[...], "건수":...}],
  "문서수": int,
}

이 로직은 autorun4.py(감정예상가 통합 서버)의 "등기권리분석" 탭 백엔드를
그대로 옮긴 것이다 - 텍스트 정규화 → 표제부/갑구/을구 구간분리 → 행파싱 →
말소반영 → 권리종류 분류 → 공동담보 병합 → 말소기준권리 판단 → 인수/소멸/
확인필요 분류 → 위험도·체크리스트 산출 순서의 파이프라인은 완전히 동일하다.
"""

from __future__ import annotations

import io
import re
import asyncio
from datetime import datetime

# ---------------------------------------------------------------------------
# PyMuPDF(fitz)/Pillow/pytesseract 지연 임포트
# ---------------------------------------------------------------------------
# 이 라이브러리들은 등기부등본 파일이 실제로 업로드됐을 때만 필요하다(그것도
# 스캔본이라 텍스트 레이어가 없을 때만 pytesseract까지 쓴다). 예전엔 모듈
# 최상단에서 무조건 임포트했는데, main.py가 `from integrations import ... registry`로
# 이 파일을 물고 있어서 서버를 켤 때마다(그리고 launcher.py의 사전 점검에서도 한 번
# 더) 매번 이 무거운 라이브러리들을 콜드 임포트하는 비용을 치르고 있었다. 실제로
# 파일을 처리하는 시점에 딱 한 번만 임포트하고 결과를 캐시해서, 서버 기동 자체는
# 가볍게 유지한다.
_fitz = None
_PILImage = None
_pytesseract = None
_deps_loaded = False


def _load_optional_deps():
    """PyMuPDF(fitz)/Pillow/pytesseract를 최초 1회만 임포트해서 모듈 전역에 캐시한다.
    설치 안 된 라이브러리는 None으로 남고, 호출부가 None 체크로 안내 메시지를 낸다."""
    global _fitz, _PILImage, _pytesseract, _deps_loaded
    if _deps_loaded:
        return
    try:
        # PyMuPDF는 예전엔 'import fitz'만 지원했지만, 최신 버전부터는 'import pymupdf'가
        # 권장 이름이고 'fitz'는 하위호환용이라 deprecation 경고가 뜬다. 새 이름을 우선
        # 시도하고, 혹시 구버전 PyMuPDF만 깔려 있으면(pymupdf 모듈명 자체가 없음) fitz로
        # 폴백한다 - 아래 코드의 fitz.open()/fitz.Matrix() 등 호출부는 그대로 둬도 된다.
        import pymupdf as _fitz_mod  # PyMuPDF (신규 이름)
    except Exception:
        try:
            import fitz as _fitz_mod  # PyMuPDF (구버전 전용 이름)
        except Exception:
            _fitz_mod = None
    _fitz = _fitz_mod

    try:
        from PIL import Image as _PILImage_mod
        import pytesseract as _pytesseract_mod  # pyright: ignore[reportMissingImports]
    except Exception:
        _PILImage_mod = None
        _pytesseract_mod = None
    _PILImage = _PILImage_mod
    _pytesseract = _pytesseract_mod
    _deps_loaded = True


class RegistryError(Exception):
    """등기사항증명서 파싱/분석에 실패했을 때 발생한다 (main.py가 422로 변환)."""


async def _run_blocking(func, *args, **kwargs):
    """블로킹(동기) 함수를 별도 스레드에서 실행한다 (OCR 등 CPU/IO 바운드 작업용)."""
    return await asyncio.to_thread(func, *args, **kwargs)


# ---------------------------------------------------------------------------
# ① 권리종류 분류 키워드
# ---------------------------------------------------------------------------

_REGISTRY_RIGHT_KEYWORDS = [
    ("소유권", ["소유권보존", "소유권이전", "소유권경정"]),
    ("근저당권", ["근저당권"]),
    ("저당권", ["저당권"]),
    ("가압류", ["가압류"]),
    ("압류", ["압류"]),
    ("가처분", ["가처분"]),
    ("전세권", ["전세권"]),
    ("질권", ["질권"]),
    ("임차권", ["임차권"]),
    ("신탁", ["신탁"]),
    ("경매개시결정", ["경매개시결정"]),
    ("예고등기", ["예고등기"]),
    ("가등기", ["가등기"]),
    ("지상권", ["지상권"]),
    ("지역권", ["지역권"]),
    ("환매권", ["환매특약", "환매권"]),
    ("매매예약", ["매매예약"]),
    ("파산선고", ["파산선고"]),
    ("회생절차개시결정", ["회생절차개시결정", "회생절차"]),
    ("임대주택등록", ["임대주택", "임대사업자등록", "민간임대주택"]),
]


def _registry_classify_purpose(purpose: str) -> str:
    p = (purpose or "").replace(" ", "")
    for cat, kws in _REGISTRY_RIGHT_KEYWORDS:
        for kw in kws:
            if kw in p:
                return cat
    return "기타"


_REGISTRY_DATE_RE = re.compile(r"(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일")
_REGISTRY_RCPT_NO_RE = re.compile(r"제\s*([0-9]+)\s*호")
_REGISTRY_AMOUNT_RE = re.compile(r"(?:채권최고액|청구금액|전세금|보증금)\s*금?\s*([0-9,]+)\s*원")
_REGISTRY_ROW_START_RE = re.compile(r"(?m)^\s*([0-9]+(?:-[0-9]+)?)\s+(?=\S)")

_REGISTRY_HANGUL_SPACE_RE = re.compile(
    r"(?<![\uAC00-\uD7A3])((?:[\uAC00-\uD7A3][ \t]+)+[\uAC00-\uD7A3])(?![\uAC00-\uD7A3])"
)
_REGISTRY_NUMERIC_SPACE_RE = re.compile(
    r"(?<![0-9,\-])((?:[0-9,\-][ \t]+)+[0-9,\-])(?![0-9,\-])"
)


def _registry_normalize_text(text: str) -> str:
    """PDF 표 조판 때문에 생기는 '글자 사이 자간 공백'만 골라서 제거한다.
    - 한글 낱글자 2개 이상이 공백 하나씩만 사이에 두고 연속될 때만 그 공백을 지운다
      (예: '표 제 부' -> '표제부'). 여러 글자짜리 단어 사이의 정상적인 공백은 그대로 둔다.
    - 숫자(콤마·하이픈 포함)도 마찬가지로 한 자리씩 연속될 때만 붙인다."""
    if not text:
        return text
    text = _REGISTRY_HANGUL_SPACE_RE.sub(lambda m: re.sub(r"[ \t]+", "", m.group(1)), text)
    text = _REGISTRY_NUMERIC_SPACE_RE.sub(lambda m: re.sub(r"[ \t]+", "", m.group(1)), text)
    return text


def _registry_parse_date(text: str):
    m = _REGISTRY_DATE_RE.search(text or "")
    if not m:
        return None, None
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        datetime(y, mo, d)
    except Exception:
        return None, None
    return f"{y:04d}-{mo:02d}-{d:02d}", (y, mo, d)


_REGISTRY_SECTION_BRACKET_RE = re.compile(r"[【\[〔《]\s*(표제부|갑구|을구)\s*[】\]〕》]")
_REGISTRY_SECTION_LINE_RE = re.compile(r"(?m)^[ \t]*(표제부|갑구|을구)[ \t]*(?:[\(（]|$)")
_REGISTRY_JOINT_LIST_HEADING_RE = re.compile(r"공동담보목록")
_REGISTRY_SALE_LIST_HEADING_RE = re.compile(r"매매목록")


def _registry_split_sections(full_text: str) -> dict:
    """전체 텍스트를 【표제부】/【갑구】/【을구】 구간으로 분리한다.
    을구는 보통 문서 맨 마지막 핵심 구간이라, 그 뒤에 【매매목록】·【공동담보목록】 같은
    부록 표가 더 있어도 끝을 표시하는 다음 표제부/갑구/을구 대괄호가 없으면 문서
    끝까지를 전부 을구로 잘못 잡아 부록 표의 번호 있는 행들까지 을구 행으로 잘못
    파싱될 수 있다. 매매목록/공동담보목록 헤딩도 구간 종료 지점 후보에 넣는다."""
    sections = {"표제부": "", "갑구": "", "을구": ""}
    markers = list(_REGISTRY_SECTION_BRACKET_RE.finditer(full_text))
    if len(markers) < 2:
        line_markers = list(_REGISTRY_SECTION_LINE_RE.finditer(full_text))
        if len(line_markers) > len(markers):
            markers = line_markers
    appendix_starts = [x.start() for x in _REGISTRY_JOINT_LIST_HEADING_RE.finditer(full_text)]
    appendix_starts += [x.start() for x in _REGISTRY_SALE_LIST_HEADING_RE.finditer(full_text)]
    for i, m in enumerate(markers):
        name = m.group(1)
        start = m.end()
        end = markers[i + 1].start() if i + 1 < len(markers) else len(full_text)
        appendix_end = min((p for p in appendix_starts if start <= p < end), default=None)
        if appendix_end is not None:
            end = appendix_end
        sections[name] += full_text[start:end] + "\n"
    return sections


def _registry_parse_rows(section_text: str) -> list:
    """갑구/을구 텍스트를 순위번호 기준 행으로 분리하고 각 필드를 추출한다.
    권리종류 분류는 여기서 하지 않는다 - Rule Engine(_registry_analyze)의
    '④ 권리종류 분류' 단계에서 말소사항 제거·정렬 이후에 수행한다."""
    starts = list(_REGISTRY_ROW_START_RE.finditer(section_text))
    rows = []
    for i, m in enumerate(starts):
        rank = m.group(1)
        body_start = m.start()
        body_end = starts[i + 1].start() if i + 1 < len(starts) else len(section_text)
        body = section_text[body_start:body_end].strip()
        rest = body[len(rank):].strip()
        date_iso, date_tuple = _registry_parse_date(rest)
        rcpt_m = _REGISTRY_RCPT_NO_RE.search(rest)
        rcpt_no = rcpt_m.group(1) if rcpt_m else None
        dm = _REGISTRY_DATE_RE.search(rest)
        purpose = rest[:dm.start()].strip() if dm else rest.split("\n")[0].strip()
        purpose = re.sub(r"\s+", "", purpose)
        amount_m = _REGISTRY_AMOUNT_RE.search(rest)
        amount = amount_m.group(1).replace(",", "") if amount_m else None
        holder = None
        holder_m = re.search(
            r"(소유자|근저당권자|저당권자|가압류권자|압류권자|가처분권자|전세권자|임차권자|채권자|수탁자)\s*[:\s]*([^\n,(]{1,40})",
            rest)
        if holder_m:
            holder = holder_m.group(2).strip()
            holder = re.sub(r"\s*[0-9]{6}-[0-9*]{6,7}\s*$", "", holder).strip()
        is_cancel_entry = "말소" in purpose
        cancels = re.findall(r"([0-9]+(?:-[0-9]+)?)\s*번", purpose) if is_cancel_entry else []
        rows.append({
            "순위번호": rank, "등기목적": purpose, "접수일": date_iso,
            "접수번호": rcpt_no, "권리자": holder, "금액": amount,
            "권리종류": None,
            "말소등기여부": is_cancel_entry, "말소대상순위": cancels,
            "_date_tuple": date_tuple, "raw": rest[:300],
        })
    return rows


def _registry_apply_cancellations(rows: list) -> list:
    """말소 등기 행이 참조하는 순위번호를 실제 '말소됨' 상태로 반영한다."""
    cancelled_ranks = set()
    for r in rows:
        if r["말소등기여부"]:
            cancelled_ranks.update(r["말소대상순위"])
    for r in rows:
        r["말소됨"] = (r["순위번호"] in cancelled_ranks) or r["말소등기여부"]
    return rows


_REGISTRY_BASIS_CATS = {"근저당권", "저당권", "가압류", "압류", "경매개시결정"}
_REGISTRY_ALWAYS_EXTINGUISH_CATS = {"근저당권", "저당권", "가압류", "압류"}
_REGISTRY_TAKEOVER_CANDIDATE_CATS = {"전세권", "지상권", "지역권", "임차권", "가등기", "가처분", "환매권"}


def _pick_display_fields(r: dict) -> dict:
    return {k: r.get(k) for k in (
        "순위번호", "등기목적", "접수일", "권리종류", "권리자", "금액",
        "_section", "_사유", "문서명", "공동담보_문서목록", "공동담보_건수",
    )}


# ---------------------------------------------------------------------------
# ② 공동담보목록 파싱 / 병합
# ---------------------------------------------------------------------------

def _registry_extract_joint_mortgage_blocks(text: str, filename: str) -> list:
    """텍스트에서 '공동담보목록' 표를 번호(일련번호)/유형(토지·건물)/부동산 표시로 파싱한다.
    PDF 표 셀이 "1" / "토지" / "경기도 ..." 처럼 각각 별도 줄로 쪼개져 나오는 경우가
    많아, 번호로 시작하는 줄을 "항목 경계"로 먼저 찾고 그 경계 사이(여러 줄에 걸쳐도
    됨)를 하나의 항목 본문으로 보고 유형·주소를 추출한다."""
    row_start_re = re.compile(r"(?m)^\s*([0-9]+)\s*[.\)]?\s+(?=\S)")
    kind_re = re.compile(r"\[?\s*(토지|건물|집합건물)\s*\]?")
    addr_cutoff_re = re.compile(r"\S{0,6}(?:지방법원|등기소)")
    addr_year_cutoff_re = re.compile(r"[0-9]{4}년")

    _ADDR_NOISE_RES = [
        re.compile(r"열람일시\s*[:：]?\s*[0-9년월일시분초\s]+[0-9]+\s*/\s*[0-9]+"),
        re.compile(r"열\s*람\s*용"),
        re.compile(r"고유번호\s*[0-9\-]+"),
        re.compile(r"등기사항전부증명서\S*"),
        re.compile(r"-\s*(?:토지|건물|집합건물)\s*-"),
        re.compile(r"일련번호"),
        re.compile(r"부동산에\s*관한\s*권리의\s*표시"),
        re.compile(r"부동산의\s*표시"),
        re.compile(r"관할등기소명"),
        re.compile(r"순위번호"),
        re.compile(r"생성원인"),
        re.compile(r"변경\s*/?\s*소멸"),
        re.compile(r"기\s*타\s*사\s*항"),
        re.compile(r"예\s*비\s*란"),
        re.compile(r"등기원인"),
        re.compile(r"경정원인"),
        re.compile(r"목록번호\s*[0-9\-]+"),
        re.compile(r"거래가액\s*금?[0-9,]+\s*원?"),
        re.compile(r"\S{0,10}(?:지방법원|등기소)\s*제\s*[0-9]+\s*호"),
        re.compile(r"[0-9]{4}년\s*[0-9]{1,2}월\s*[0-9]{1,2}일"),
        re.compile(r"\S{0,10}(?:지방법원|등기소)"),
        re.compile(r"설정계약으로"),
        re.compile(r"인\s*하\s*여"),
        re.compile(r"매매"),
        re.compile(r"-{1,2}\s*이\s*하\s*여\s*백\s*-{1,2}"),
        re.compile(r"공동담보목록"),
        re.compile(r"[\*＊][^\n]*"),
    ]
    _ADDR_HINT_RE = re.compile(r"(?:특별시|광역시|특별자치시|특별자치도|[가-힣]도|[가-힣]시|[가-힣]군|[가-힣]구|[가-힣]읍|[가-힣]면|[가-힣]동|[가-힣]리)")
    _ADDR_NOISE_LEFTOVER_RE = re.compile(r"지방법원|등기소|열람|고유번호|일련번호")
    _ADDR_LINE_NOISE_RE = re.compile(
        r"열람일시.*|열\s*람\s*용|고유번호\s*[0-9\-]*|등기사항전부증명서\S*|일련번호|"
        r"부동산에\s*관한\s*권리의\s*표시|부동산의\s*표시|관할등기소명|순위번호|생성원인|"
        r"변경\s*/?\s*소멸|기\s*타\s*사\s*항|예\s*비\s*란|등기원인|경정원인|목록번호\s*[0-9\-]*|"
        r"거래가액\s*금?[0-9,]*\s*원?|[0-9]+\s*/\s*[0-9]+|공동담보목록|매매목록|"
        r"-{1,2}\s*이\s*하\s*여\s*백\s*-{1,2}"
    )
    _SHARE_NAME_RE = re.compile(r"[가-힣]{2,10}\s*지분")

    def _addr_subtractive(body_text: str) -> str:
        flat = re.sub(r"\s+", " ", body_text).strip()
        for pat in _ADDR_NOISE_RES:
            flat = pat.sub(" ", flat)
        flat = re.sub(r"\s+", " ", flat).strip()
        return flat

    blocks = []
    for m in _REGISTRY_JOINT_LIST_HEADING_RE.finditer(text):
        start = m.end()
        lookahead = text[start:start + 20]
        if "목록번호" not in lookahead:
            continue
        next_heading = _REGISTRY_JOINT_LIST_HEADING_RE.search(text, start)
        next_sale_list = _REGISTRY_SALE_LIST_HEADING_RE.search(text, start)
        next_bracket = _REGISTRY_SECTION_BRACKET_RE.search(text, start)
        candidates = [x.start() for x in (next_heading, next_sale_list, next_bracket) if x]
        end = min(candidates) if candidates else min(len(text), start + 4000)
        chunk = text[start:end]

        _rank_no_counts: dict = {}
        for _ln in chunk.split("\n"):
            _lns = _ln.strip()
            if re.fullmatch(r"[0-9]{1,3}", _lns):
                _rank_no_counts[_lns] = _rank_no_counts.get(_lns, 0) + 1
        common_rank_no = None
        if _rank_no_counts:
            _best_rank, _best_count = max(_rank_no_counts.items(), key=lambda kv: kv[1])
            if _best_count >= 2:
                common_rank_no = _best_rank

        raw_starts = list(row_start_re.finditer(chunk))
        real_starts = []
        for cand in raw_starts:
            tail = chunk[cand.end():cand.end() + 30].lstrip()
            if kind_re.match(tail):
                real_starts.append(cand)
        if not real_starts:
            real_starts = raw_starts

        boundaries = [{"no": rm.group(1), "marker_start": rm.start(),
                       "kind": None, "content_start": rm.end()} for rm in real_starts]

        items = []
        for i, b in enumerate(boundaries):
            no = b["no"]
            body_start = b["content_start"]
            body_end = boundaries[i + 1]["marker_start"] if i + 1 < len(boundaries) else len(chunk)
            body = chunk[body_start:body_end]
            kind_m = kind_re.search(body)
            kind = kind_m.group(1) if kind_m else ""
            addr_src = body[kind_m.end():] if kind_m else body
            body_lines = [ln.strip() for ln in addr_src.split("\n") if ln.strip()]

            first_line = body_lines[0] if body_lines else ""
            cutoff_m = addr_cutoff_re.search(first_line) or addr_year_cutoff_re.search(first_line)
            addr1 = (first_line[:cutoff_m.start()] if cutoff_m else first_line).strip()
            _share_hit1 = _SHARE_NAME_RE.search(addr1)
            if _share_hit1:
                addr1 = (addr1[:_share_hit1.start()] + addr1[_share_hit1.end():]).strip()
            addr_parts = []
            prev_was_registry_office = False
            rank_no_seen = None
            for ln in body_lines[1:]:
                if ln.lstrip().startswith(("*", "＊")):
                    prev_was_registry_office = False
                    continue
                _share_hit = _SHARE_NAME_RE.search(ln)
                if _share_hit:
                    ln = (ln[:_share_hit.start()] + ln[_share_hit.end():]).strip()
                    if not ln:
                        prev_was_registry_office = False
                        continue
                if kind_re.match(ln) or _ADDR_LINE_NOISE_RE.fullmatch(ln):
                    prev_was_registry_office = False
                    continue
                if re.fullmatch(r"[0-9]{4}년\s*[0-9]{1,2}월\s*[0-9]{1,2}일", ln):
                    prev_was_registry_office = False
                    continue
                if re.fullmatch(r"(설정계약으로|인\s*하\s*여|매매)", ln):
                    prev_was_registry_office = False
                    continue
                if re.fullmatch(r"제\s*[0-9]+\s*호", ln):
                    was_after_office = prev_was_registry_office
                    prev_was_registry_office = False
                    if was_after_office:
                        continue
                    addr_parts.append(ln)
                    continue
                cand_cut = addr_cutoff_re.search(ln)
                if cand_cut and not ln[cand_cut.end():].strip():
                    prev_was_registry_office = True
                    continue
                prev_was_registry_office = False
                cand = (ln[:cand_cut.start()] if cand_cut else ln).strip()
                if not cand or re.fullmatch(r"[0-9]+", cand):
                    if cand and re.fullmatch(r"[0-9]+", cand):
                        rank_no_seen = cand
                    continue
                addr_parts.append(cand)
            addr2 = " ".join(addr_parts)
            for _rank_no in (rank_no_seen, common_rank_no):
                if not _rank_no:
                    continue
                _rank_tail_re = re.compile(r"(?:^|\s)" + re.escape(_rank_no) + r"$")
                if _rank_tail_re.search(addr1):
                    stripped = _rank_tail_re.sub("", addr1).strip()
                    if stripped:
                        addr1 = stripped
                    break
            addr_line = re.sub(r"\s+", " ", f"{addr1} {addr2}").strip()
            if not addr1:
                addr_line = " ".join(body_lines)
                addr_line = re.sub(r"\s+", " ", addr_line).strip()
                cutoff_m2 = addr_cutoff_re.search(addr_line) or addr_year_cutoff_re.search(addr_line)
                if cutoff_m2:
                    addr_line = addr_line[:cutoff_m2.start()].strip()

            addr_sub = _addr_subtractive(addr_src)
            line_dirty = bool(_ADDR_NOISE_LEFTOVER_RE.search(addr_line))
            sub_dirty = bool(_ADDR_NOISE_LEFTOVER_RE.search(addr_sub))
            if line_dirty and not sub_dirty and _ADDR_HINT_RE.search(addr_sub):
                addr = addr_sub
            elif not addr_line and addr_sub:
                addr = addr_sub
            else:
                addr = addr_line
            share_m = _SHARE_NAME_RE.search(body)
            share = re.sub(r"\s+", "", share_m.group(0)) if share_m else ""
            if not addr or len(addr) > 200:
                continue
            item = {"번호": no, "유형": kind, "표시": addr}
            if share:
                item["지분"] = share
            items.append(item)
        if items:
            blocks.append({"문서명": filename, "항목": items[:30]})
    return blocks


def _registry_consolidate_joint_mortgage_items(blocks: list) -> list:
    """여러 문서(토지·건물 등)에 중복 등재된 동일한 "공동담보목록" 표를 하나로 합친다."""
    seen = set()
    items = []
    for b in blocks:
        for it in (b.get("항목") or []):
            addr = it.get("표시") or ""
            key = re.sub(r"\s+", "", addr)
            if not key or key in seen:
                continue
            seen.add(key)
            items.append(dict(it))

    def _no_key(it):
        try:
            return int(it.get("번호") or 0)
        except Exception:
            return 0
    items.sort(key=_no_key)
    for i, it in enumerate(items, start=1):
        it["번호"] = str(i)
    return items


def _registry_merge_joint_mortgages(rows: list) -> tuple:
    """서로 다른 등기부 문서(문서명이 다름)에서 유래했지만, 권리종류(근저당권/저당권)·
    권리자·채권최고액(금액)·접수일이 전부 동일한 행을 "공동담보로 같은 근저당권이 여러
    등기부에 중복 등재된 것"으로 판단해 1건으로 합친다. 반드시 "서로 다른 문서 2곳
    이상"에서 나온 경우만 병합 대상으로 삼는다."""
    mortgage_cats = ("근저당권", "저당권")
    groups: dict = {}
    others = []
    for r in rows:
        if r.get("권리종류") not in mortgage_cats:
            others.append(r)
            continue
        key = (r.get("권리종류"), re.sub(r"\s+", "", r.get("권리자") or ""), r.get("금액"), r.get("접수일"))
        groups.setdefault(key, []).append(r)

    merged_rows = []
    merge_logs = []
    for key, group in groups.items():
        doc_names = sorted({g.get("문서명") for g in group if g.get("문서명")})
        if len(group) > 1 and len(doc_names) > 1:
            rep = dict(group[0])
            rep["공동담보_문서목록"] = doc_names
            rep["공동담보_건수"] = len(group)
            merged_rows.append(rep)
            merge_logs.append({
                "권리종류": rep.get("권리종류"), "권리자": rep.get("권리자"),
                "금액": rep.get("금액"), "접수일": rep.get("접수일"),
                "문서목록": doc_names, "건수": len(group),
            })
        else:
            merged_rows.extend(group)
    return others + merged_rows, merge_logs


# ---------------------------------------------------------------------------
# ③ 권리분석 룰엔진 (말소기준권리 판단 -> 소멸/인수/확인필요 분류 -> 위험도)
# ---------------------------------------------------------------------------

def _registry_analyze(gapgu_rows: list, eulgu_rows: list) -> dict:
    all_rows = [dict(r, _section="갑구") for r in gapgu_rows] + [dict(r, _section="을구") for r in eulgu_rows]

    active = [r for r in all_rows if not r.get("말소됨") and r.get("_date_tuple")]
    active.sort(key=lambda r: (r["_date_tuple"], int(r["접수번호"] or 0)))

    for r in active:
        r["권리종류"] = _registry_classify_purpose(r["등기목적"])

    basis_candidates = [r for r in active if r["권리종류"] in _REGISTRY_BASIS_CATS]
    basis = basis_candidates[0] if basis_candidates else None

    _eulgu_basis_types = {"근저당권", "저당권"}
    _gapgu_basis_types = {"가압류", "압류", "경매개시결정"}
    if basis:
        basis_reasons = [
            f"{basis.get('_section','-')}에 접수된 {basis['권리종류']}(순위번호 {basis.get('순위번호','-')}, 접수일 {basis.get('접수일','-')})이 확인됩니다.",
            "갑구·을구를 접수일자 순으로 다시 정렬한 결과, 말소기준권리 후보(근저당권·저당권·가압류·압류·경매개시결정) 중 이 권리의 접수일이 가장 빠릅니다.",
        ]
    else:
        _has_eulgu_mortgage = any(r["권리종류"] in _eulgu_basis_types for r in active)
        _has_gapgu_seizure = any(r["권리종류"] in _gapgu_basis_types for r in active)
        basis_reasons = [
            f"을구에 근저당권·저당권이 존재{'합니다' if _has_eulgu_mortgage else '하지 않습니다'}.",
            f"갑구에 압류·가압류·경매개시결정이 존재{'합니다' if _has_gapgu_seizure else '하지 않습니다'}.",
            "말소기준권리가 될 권리를 확인하지 못했습니다.",
        ]

    senior_candidates, junior_candidates, undetermined = [], [], []
    for r in active:
        if r["권리종류"] == "소유권":
            continue
        if not basis:
            r["_사유"] = "말소기준권리를 찾지 못해 이 권리의 선순위·후순위 여부(인수·소멸 여부)를 판단할 수 없습니다."
            undetermined.append(r)
            continue
        is_senior = r["_date_tuple"] < basis["_date_tuple"]
        (senior_candidates if is_senior else junior_candidates).append(r)

    extinguished, taken_over, review_needed = [], [], list(undetermined)
    _basis_desc = f"말소기준권리({basis['권리종류']}, 접수일 {basis.get('접수일','-')})" if basis else "말소기준권리"
    for r in senior_candidates:
        if r["권리종류"] in _REGISTRY_ALWAYS_EXTINGUISH_CATS:
            r["_사유"] = f"{_basis_desc}보다 선순위이지만, 근저당권·저당권·가압류·압류 같은 담보물권·보전처분은 경매로 소멸되는 것이 원칙이라 소멸로 분류됩니다."
            extinguished.append(r)
        elif r["권리종류"] in _REGISTRY_TAKEOVER_CANDIDATE_CATS:
            r["_사유"] = f"{_basis_desc}보다 선순위인 {r['권리종류']}로, 원칙적으로 낙찰자가 인수해야 하는 권리 후보로 분류됩니다."
            taken_over.append(r)
        else:
            r["_사유"] = f"{_basis_desc}보다 선순위이나 {r['권리종류']}는 정형 분류 규칙에 해당하지 않아 소멸·인수 여부를 자동 판정할 수 없어 확인이 필요합니다."
            review_needed.append(r)
    for r in junior_candidates:
        if basis is r:
            r["_사유"] = "이 권리 자체가 말소기준권리이며, 경매개시(매각)로 인해 함께 소멸됩니다."
        else:
            r["_사유"] = f"{_basis_desc}보다 후순위 권리로, 유형과 무관하게 원칙적으로 소멸로 분류됩니다."
        extinguished.append(r)

    for r in extinguished + taken_over + review_needed:
        if r.get("공동담보_건수", 0) > 1:
            docs = "·".join(r.get("공동담보_문서목록") or [])
            r["_사유"] = (r.get("_사유") or "") + \
                f" (공동담보 확인: 첨부하신 등기부 {r['공동담보_건수']}건({docs})에 권리자·채권최고액·접수일이 모두 동일한 근저당권이 반복 등재되어 있어, 같은 대출을 중복 집계하지 않도록 1건으로 합쳐 표시했습니다.)"

    checklist = []
    risk = "낮음"
    if any(r["권리종류"] == "가등기" for r in taken_over):
        checklist.append("선순위 가등기가 있습니다 → 본등기가 이루어지면 소유권을 상실할 위험이 있어 반드시 전문가 확인이 필요합니다.")
        risk = "높음"
    if any(r["권리종류"] == "가처분" for r in taken_over):
        checklist.append("선순위 가처분이 있습니다 → 관련 소송 결과에 따라 소유권·권리관계가 달라질 수 있습니다.")
        risk = "높음"
    if any(r["권리종류"] == "전세권" for r in taken_over):
        checklist.append("선순위 전세권이 있습니다 → 배당요구 여부와 보증금 인수 여부를 확인해야 합니다.")
        risk = "높음"
    if any(r["권리종류"] == "임차권" for r in taken_over):
        checklist.append("선순위 임차권이 있습니다 → 전입일·확정일자 기준 대항력 여부는 등기부만으로 판단할 수 없어 별도 확인이 필요합니다.")
        if risk == "낮음":
            risk = "중간"
    if any(r["권리종류"] == "신탁" for r in active):
        checklist.append("신탁등기가 있습니다 → 등기부상 소유자가 아닌 수탁자 명의이므로 신탁원부·수탁자 동의 여부 확인이 필요합니다.")
        risk = "높음"
    if any(r["권리종류"] == "예고등기" for r in active):
        checklist.append("예고등기가 있습니다 → 등기원인의 무효·취소를 다투는 소송이 계속 중일 수 있어 확인이 필요합니다.")
        if risk == "낮음":
            risk = "중간"
    if basis is None and active:
        checklist.append("말소기준권리(저당권·가압류·압류·경매개시결정)를 찾지 못했습니다 — 경매 목적 문서가 아니거나 첨부 내용이 일부 누락되었을 수 있습니다.")
    if not checklist:
        checklist.append("등기부상 특이 위험요소는 확인되지 않았습니다. 다만 배당요구·대항력 등 등기부에 나타나지 않는 사항은 별도 확인이 필요합니다.")

    _overall_label_map = {"낮음": "계약가능", "중간": "주의", "높음": "위험"}
    overall_result = _overall_label_map.get(risk, "확인필요")
    if risk == "낮음":
        overall_summary = "등기부에 기재된 권리관계를 기준으로 중대한 권리상 위험은 확인되지 않았습니다."
    elif risk == "중간":
        overall_summary = "일부 권리관계에서 등기부만으로는 확정할 수 없는, 추가 확인이 필요한 사항이 발견되었습니다."
    else:
        overall_summary = "낙찰 후 인수될 수 있는 선순위 권리 등 중대한 위험 요소가 발견되었습니다."

    return {
        "분석결과": overall_result,
        "종합_요약": overall_summary,
        "기준권리": ({k: basis[k] for k in ("순위번호", "등기목적", "접수일", "권리종류", "권리자")} if basis else None),
        "기준권리_판단사유": basis_reasons,
        "소멸되는_권리": [_pick_display_fields(r) for r in extinguished],
        "인수되는_권리": [_pick_display_fields(r) for r in taken_over],
        "확인필요_권리": [_pick_display_fields(r) for r in review_needed],
        "위험도": risk,
        "체크리스트": checklist,
        "법률상_유의사항": [
            "본 분석은 등기사항증명서 텍스트만으로 산출한 참고용 자동 판정이며 법률 자문이 아닙니다.",
            "실제 인수·소멸 여부는 배당요구 여부, 전입일·확정일자(대항력), 매각물건명세서 등 등기부 외 정보에 따라 달라질 수 있습니다.",
            "중요한 의사결정 전에는 반드시 법무사·변호사 또는 경매 전문가의 확인을 받으시기 바랍니다.",
        ],
    }


async def _registry_ai_review(full_text: str, rule_result: dict):
    """LLM 보완 분석 확장 포인트(현재 미사용). {"설명": str, "특약해석": [str], "추가_예외소견": [str]} | None"""
    return None


def _registry_build_final_result(rule_result: dict, ai_result: dict | None) -> dict:
    if not ai_result:
        return rule_result
    merged = dict(rule_result)
    merged["ai_설명"] = ai_result.get("설명")
    merged["ai_특약해석"] = ai_result.get("특약해석") or []
    merged["ai_추가_예외소견"] = ai_result.get("추가_예외소견") or []
    return merged


# ---------------------------------------------------------------------------
# ④ 파일(PDF/이미지) -> 텍스트 추출 (텍스트 레이어 우선, 없으면 OCR)
# ---------------------------------------------------------------------------

def _registry_ocr_image(img) -> str:
    """PIL 이미지 -> OCR 텍스트 (동기/블로킹 — 반드시 _run_blocking으로 호출)."""
    try:
        return _pytesseract.image_to_string(img, lang="kor+eng")
    except Exception:
        return _pytesseract.image_to_string(img)


def _registry_ocr_pdf_bytes(data: bytes) -> str:
    """텍스트 레이어 없는 스캔 PDF -> 페이지별 고해상도 렌더링 후 OCR (동기/블로킹)."""
    doc = _fitz.open(stream=data, filetype="pdf")
    try:
        texts = []
        for page in doc:
            pix = page.get_pixmap(matrix=_fitz.Matrix(3, 3))
            img = _PILImage.open(io.BytesIO(pix.tobytes("png")))
            texts.append(_registry_ocr_image(img))
        return "\n".join(texts)
    finally:
        doc.close()


def _registry_ocr_image_bytes(data: bytes) -> str:
    """사진/스캔 이미지 파일(jpg/png) 바이트 -> OCR 텍스트 (동기/블로킹)."""
    img = _PILImage.open(io.BytesIO(data))
    return _registry_ocr_image(img)


async def _registry_extract_text_from_upload(filename: str, data: bytes):
    """(텍스트, 경고메시지) 튜플 반환. 텍스트 레이어가 있는 PDF는 fitz로 바로 뽑고
    (가장 정확·빠름), 텍스트 레이어가 없는 스캔 PDF와 jpg/png 이미지는 OCR(pytesseract)로
    대체 인식한다. OCR은 화질에 따라 오탈자가 섞일 수 있어 성공해도 경고를 함께 준다."""
    _load_optional_deps()  # fitz/Pillow/pytesseract는 여기, 실제로 파일을 받은 시점에만 임포트한다
    ext = filename.lower().rsplit(".", 1)[-1] if filename and "." in filename else ""
    if ext == "pdf":
        if _fitz is None:
            return "", f"{filename}: PyMuPDF(fitz)가 설치되어 있지 않습니다. 서버에서 `pip install pymupdf` 실행 후 다시 시도해주세요."
        try:
            doc = _fitz.open(stream=data, filetype="pdf")
            text = "\n".join(page.get_text() for page in doc)
            doc.close()
        except Exception as e:
            return "", f"{filename}: PDF 처리 중 오류 — {e}"
        if text.strip():
            return _registry_normalize_text(text), None
        if _pytesseract is None or _PILImage is None:
            return "", f"{filename}: 텍스트 레이어가 없는 스캔 PDF입니다. OCR 처리를 위한 pytesseract/Pillow가 서버에 설치되어 있지 않습니다(`pip install pytesseract pillow` 및 시스템에 tesseract-ocr, tesseract-ocr-kor 설치 필요)."
        try:
            ocr_text = await _run_blocking(_registry_ocr_pdf_bytes, data)
        except Exception as e:
            return "", f"{filename}: 스캔 PDF OCR 처리 중 오류 — {e}"
        if not ocr_text.strip():
            return "", f"{filename}: 스캔 PDF에서 OCR로도 텍스트를 인식하지 못했습니다(화질이 너무 낮거나 손상된 파일일 수 있습니다)."
        return (
            _registry_normalize_text(ocr_text),
            f"{filename}: 텍스트 레이어가 없는 스캔 PDF라 OCR로 인식했습니다 — 원본 화질에 따라 오탈자가 있을 수 있으니 결과를 원문과 대조해주세요.",
        )
    elif ext in ("jpg", "jpeg", "png"):
        if _pytesseract is None or _PILImage is None:
            return "", f"{filename}: OCR 처리를 위한 pytesseract/Pillow가 서버에 설치되어 있지 않습니다(`pip install pytesseract pillow` 및 시스템에 tesseract-ocr, tesseract-ocr-kor 설치 필요)."
        try:
            ocr_text = await _run_blocking(_registry_ocr_image_bytes, data)
        except Exception as e:
            return "", f"{filename}: 이미지 OCR 처리 중 오류 — {e}"
        if not ocr_text.strip():
            return "", f"{filename}: 이미지에서 텍스트를 인식하지 못했습니다(화질이 너무 낮거나 등기사항증명서 사진이 아닐 수 있습니다)."
        return (
            _registry_normalize_text(ocr_text),
            f"{filename}: 이미지 파일이라 OCR로 인식했습니다 — 원본 화질에 따라 오탈자가 있을 수 있으니 결과를 원문과 대조해주세요.",
        )
    else:
        return "", f"{filename}: 지원하지 않는 파일 형식입니다(PDF, JPG, PNG 지원)."


# ---------------------------------------------------------------------------
# ⑤ 공개 인터페이스 - main.py가 호출하는 함수들
# ---------------------------------------------------------------------------

def _strip_internal(rows: list) -> list:
    return [{k: v for k, v in r.items() if not k.startswith("_") and k != "raw"} for r in rows]


async def _analyze_files(file_payloads: list) -> dict:
    """file_payloads: [(filename, bytes), ...] -> 결과 dict.
    ⚠ 파일마다 따로 구간분리 -> 행파싱 -> 말소반영을 하고(순위번호는 각 등기부 안에서만
    유효하므로 합쳐서 처리하면 안 됨), 각 행에 출처 문서명을 남긴 뒤, 을구에서만
    공동담보 병합을 적용한다(토지·건물 등에 같은 근저당권이 중복 등재되는 경우 대응)."""
    warnings: list = []
    full_text_parts: list = []
    gapgu_rows: list = []
    eulgu_rows: list = []
    title_blocks: list = []
    joint_mortgage_blocks: list = []

    for filename, data in file_payloads:
        text, warn = await _registry_extract_text_from_upload(filename, data)
        if warn:
            warnings.append(warn)
        if not text:
            continue
        full_text_parts.append(text)
        doc_sections = _registry_split_sections(text)
        doc_gapgu = _registry_apply_cancellations(_registry_parse_rows(doc_sections.get("갑구", "")))
        doc_eulgu = _registry_apply_cancellations(_registry_parse_rows(doc_sections.get("을구", "")))
        for r in doc_gapgu + doc_eulgu:
            r["문서명"] = filename
        gapgu_rows.extend(doc_gapgu)
        eulgu_rows.extend(doc_eulgu)
        if doc_sections.get("표제부", "").strip():
            title_blocks.append({"문서명": filename, "원문": doc_sections["표제부"].strip()[:2000]})
        joint_mortgage_blocks.extend(_registry_extract_joint_mortgage_blocks(text, filename))

    full_text = "\n".join(full_text_parts)
    if not full_text.strip():
        raise RegistryError("텍스트를 추출할 수 있는 파일이 없습니다." + (" " + " / ".join(warnings) if warnings else ""))

    for _r in gapgu_rows + eulgu_rows:
        _r["권리종류"] = _registry_classify_purpose(f"{_r.get('등기목적','')} {_r.get('raw','')}")

    eulgu_rows, joint_mortgage_merges = _registry_merge_joint_mortgages(eulgu_rows)

    rule_result = _registry_analyze(gapgu_rows, eulgu_rows)
    ai_result = await _registry_ai_review(full_text, rule_result)
    analysis = _registry_build_final_result(rule_result, ai_result)

    joint_mortgage_items = _registry_consolidate_joint_mortgage_items(joint_mortgage_blocks)

    return {
        "ok": True,
        "warnings": warnings,
        "표제부_원문": (title_blocks[0]["원문"] if title_blocks else ""),
        "표제부_목록": title_blocks,
        "갑구": _strip_internal(gapgu_rows),
        "을구": _strip_internal(eulgu_rows),
        "분석": analysis,
        "공동담보목록_원문": joint_mortgage_items,
        "공동담보_병합": joint_mortgage_merges,
        "문서수": len(title_blocks) or len({r.get("문서명") for r in gapgu_rows + eulgu_rows if r.get("문서명")}),
    }


async def parse_uploaded_registry_pdf(data: bytes, filename: str = "등기사항증명서.pdf") -> dict:
    """단일 파일(PDF/JPG/PNG) 업로드 -> 구조화 + 권리분석 결과."""
    return await _analyze_files([(filename, data)])


async def parse_uploaded_registry_files(file_payloads: list) -> dict:
    """여러 파일(PDF/JPG/PNG) 업로드 -> 구조화 + 권리분석 결과.
    file_payloads: [(filename, bytes), ...]"""
    if not file_payloads:
        raise RegistryError("업로드된 파일이 없습니다.")
    return await _analyze_files(list(file_payloads))
