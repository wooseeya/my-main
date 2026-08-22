"""건축HUB 건축물대장정보 서비스 (국토교통부, 공공데이터포털, 무료).

신청: https://www.data.go.kr -> "건축HUB_건축물대장정보 서비스" 검색 -> 활용신청
표제부(getBrTitleInfo) 기준으로 구현. 층별개요/전유공용면적 등 다른 상세정보가
필요하면 같은 방식으로 엔드포인트만 바꿔서 함수를 추가하면 된다.

엔드포인트/파라미터는 실제 운영 중인 코드 기준으로 검증된 값을 사용한다
(platGbCd 파라미터 누락 시 조회가 비정상 동작하는 경우가 있어 반드시 포함).
"""

import asyncio
import json
import xml.etree.ElementTree as ET

import httpx
import config

BASE_URL = "https://apis.data.go.kr/1613000/BldRgstHubService/getBrTitleInfo"
_OK_CODES = ("00", "0", "000", "0000")
_TRANSIENT_ERRORS = (
    httpx.RemoteProtocolError, httpx.ConnectError, httpx.ReadError,
    httpx.ReadTimeout, httpx.ConnectTimeout, httpx.PoolTimeout,
)


class BuildingLedgerError(Exception):
    pass


async def _resilient_get(client: httpx.AsyncClient, url: str, params: dict,
                          retries: int = 2, backoff: float = 0.4):
    for attempt in range(retries + 1):
        try:
            return await client.get(url, params=params)
        except _TRANSIENT_ERRORS:
            if attempt >= retries:
                raise
            await asyncio.sleep(backoff * (attempt + 1))


def _parse_items(raw_text: str) -> list[dict]:
    """건축HUB 응답을 파싱한다. 문서/신청 가이드에는 XML로 나와있지만,
    실제 운영 중 API가 JSON으로 응답하는 경우가 확인되어(2026-08, 실제 오류
    사례: resultCode="00" 정상인데 XML 파서가 그대로 실패) 두 형식을 모두
    처리하도록 방어적으로 분기한다. 응답 텍스트가 "{"로 시작하면 JSON,
    아니면 XML로 간주한다."""
    text = (raw_text or "").strip()
    if text.startswith("{"):
        return _parse_items_json(text)
    return _parse_items_xml(text)


def _parse_items_xml(xml_text: str) -> list[dict]:
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        raise BuildingLedgerError(f"건축HUB 응답 파싱 실패(XML): {xml_text[:200]}")

    result_code = root.findtext(".//resultCode") or ""
    if result_code and result_code not in _OK_CODES:
        msg = root.findtext(".//resultMsg") or ""
        raise BuildingLedgerError(f"건축HUB API 오류(resultCode={result_code}): {msg}")

    items = []
    for el in root.iter("item"):
        row = {c.tag: c.text.strip() for c in el if c.text and c.text.strip()}
        if row:
            items.append(row)
    return items


def _parse_items_json(json_text: str) -> list[dict]:
    try:
        body = json.loads(json_text)
    except json.JSONDecodeError:
        raise BuildingLedgerError(f"건축HUB 응답 파싱 실패(JSON): {json_text[:200]}")

    header = (body.get("header")
              or (body.get("response") or {}).get("header")
              or {})
    result_code = str(header.get("resultCode") or "")
    if result_code and result_code not in _OK_CODES:
        msg = header.get("resultMsg") or ""
        raise BuildingLedgerError(f"건축HUB API 오류(resultCode={result_code}): {msg}")

    body_data = (body.get("body")
                 or (body.get("response") or {}).get("body")
                 or {})
    items_wrap = body_data.get("items") or {}
    raw_items = items_wrap.get("item") if isinstance(items_wrap, dict) else items_wrap
    if raw_items is None:
        return []
    if isinstance(raw_items, dict):
        # 건물이 1건뿐이면 배열이 아니라 객체 하나로 오는 경우가 있어 방어.
        raw_items = [raw_items]

    items = []
    for it in raw_items:
        if not isinstance(it, dict):
            continue
        row = {k: str(v).strip() for k, v in it.items() if v not in (None, "")}
        if row:
            items.append(row)
    return items


async def get_title_info(sigungu_cd: str, bdong_cd: str, bun: str, ji: str = "0000",
                          plat_gb_cd: str = "0") -> dict | None:
    """표제부 조회. 불법건축물 여부(violationStatus), 주용도(mainPurpsCdNm),
    사용승인일(useAprDay), 연면적(totArea) 등을 담아 반환한다.
    plat_gb_cd: "0"=대지, "1"=산(mountain_yn=='Y'인 경우) - geocode.address_to_codes()가 계산해 준다."""
    if not config.BUILDING_HUB_SERVICE_KEY:
        raise BuildingLedgerError("BUILDING_HUB_SERVICE_KEY(또는 DATA_SERVICE_KEY)가 설정되어 있지 않습니다.")

    params = {
        "serviceKey": config.BUILDING_HUB_SERVICE_KEY,
        "sigunguCd": sigungu_cd,
        "bjdongCd": bdong_cd,
        "platGbCd": plat_gb_cd,
        "bun": bun,
        "ji": ji,
        "numOfRows": "10",
        "pageNo": "1",
    }

    async with httpx.AsyncClient(timeout=config.HTTP_TIMEOUT_SECONDS) as http:
        resp = await _resilient_get(http, BASE_URL, params)
        resp.raise_for_status()

    items = _parse_items(resp.text)
    if not items:
        return None

    it = items[0]
    return {
        "building_name": it.get("bldNm", ""),
        "main_purpose": it.get("mainPurpsCdNm", ""),
        "violation_status": it.get("violationStatus", "정상") or "정상",
        "use_approval_date": it.get("useAprDay", ""),
        "total_floor_area_m2": it.get("totArea", ""),
        "structure": it.get("strctCdNm", ""),
        "ground_floors": it.get("grndFlrCnt", ""),
        "basement_floors": it.get("ugrndFlrCnt", ""),
    }


RECAP_BASE_URL = "https://apis.data.go.kr/1613000/BldRgstHubService/getBrRecapTitleInfo"


async def get_recap_title_info(sigungu_cd: str, bdong_cd: str, bun: str, ji: str = "0000",
                                plat_gb_cd: str = "0") -> dict | None:
    """총괄표제부 조회. 한 대지 위에 건물이 여러 동 있을 때 표제부(getBrTitleInfo)는
    동 하나하나의 정보만 주지만, 총괄표제부는 그 대지 전체를 합산한 값(연면적
    totArea, 건축면적 archArea)과 대지 위 주된 건축물 동 수(mainBldCnt)를 준다.
    건물이 한 동뿐인 대지라도 총괄표제부 자체는 존재하는 게 보통이라(단, 일부
    소규모/구옥은 총괄표제부가 아예 없을 수 있음 - 그럴 땐 None을 반환하고,
    호출부가 get_title_info의 값으로 대체해야 한다), 이 함수를 표제부 조회와
    함께 항상 호출해 연면적/건축면적은 이쪽 값을 우선 쓰는 걸 권장한다."""
    if not config.BUILDING_HUB_SERVICE_KEY:
        raise BuildingLedgerError("BUILDING_HUB_SERVICE_KEY(또는 DATA_SERVICE_KEY)가 설정되어 있지 않습니다.")

    params = {
        "serviceKey": config.BUILDING_HUB_SERVICE_KEY,
        "sigunguCd": sigungu_cd,
        "bjdongCd": bdong_cd,
        "platGbCd": plat_gb_cd,
        "bun": bun,
        "ji": ji,
        "numOfRows": "10",
        "pageNo": "1",
    }

    async with httpx.AsyncClient(timeout=config.HTTP_TIMEOUT_SECONDS) as http:
        resp = await _resilient_get(http, RECAP_BASE_URL, params)
        resp.raise_for_status()

    items = _parse_items(resp.text)
    if not items:
        return None

    it = items[0]
    return {
        "total_floor_area_m2": it.get("totArea", ""),   # 연면적(총괄표제부 기준 - 대지 전체 합산)
        "building_coverage_area_m2": it.get("archArea", ""),  # 건축면적
        "main_building_count": it.get("mainBldCnt", ""),      # 주건축물수
    }
