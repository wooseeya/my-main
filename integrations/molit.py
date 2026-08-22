"""국토교통부 부동산 매매 실거래가 API - 전체 유형 지원 (공공데이터포털, 무료).

v0.4: 이전 버전(v0.3)에서 "공장창고등" 서비스ID로 썼던 RTMSDataSvcIndTrade는
추정치였고 실제로는 틀린 값이었다. 참고용으로 받은 유사 프로젝트(autorun4.py)의
코드에 "서비스ID를 Ind로 잘못 적어 400 에러가 계속 났다 -> Indu가 맞다"는 실제
운영 중 발견된 버그 수정 이력이 남아있어, 그 값(RTMSDataSvcInduTrade)으로
교체했다. 나머지 6개 유형 엔드포인트도 같은 소스 기준으로 재확인했다.

신청: https://www.data.go.kr 에서 아래 자료명으로 각각 활용신청
  - 국토교통부_아파트 매매 실거래 상세 자료         (RTMSDataSvcAptTradeDev)
  - 국토교통부_연립다세대 매매 실거래자료           (RTMSDataSvcRHTrade)
  - 국토교통부_단독/다가구 매매 실거래자료          (RTMSDataSvcSHTrade)
  - 국토교통부_오피스텔 매매 신고 자료              (RTMSDataSvcOffiTrade)
  - 국토교통부_상업업무용 부동산 매매 신고 자료      (RTMSDataSvcNrgTrade)
  - 국토교통부_공장 및 창고 등 부동산 매매 신고 자료 (RTMSDataSvcInduTrade)
  - 국토교통부_토지 매매 신고 자료                  (RTMSDataSvcLandTrade)
승인은 보통 즉시~1~2시간. 일일 호출 한도는 기본 1,000건(신청 시 트래픽 증설 가능).

get_trades()는 여러 달치를 한 번에 모아준다 (months=조회할 개월 수,
end_months_ago=조회 종료 시점을 현재로부터 몇 개월 전으로 미룰지 - 기본 0=현재까지).
지번이 개인정보 보호를 위해 끝자리가 "*"로 마스킹된 거래는 "지번마스킹됨": True로
표시만 하고 제외하지는 않는다 (제외하면 표본이 너무 줄어드는 지역이 많음 - 대신
main.py가 이 플래그를 보고 사용자에게 "시군구 평균이며 원문 대조가 필요하다"고
안내한다).
"""

import asyncio
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional

import httpx
import config

PROPERTY_TYPES: tuple = (
    "아파트", "연립다세대", "단독다가구", "오피스텔",
    "상업업무용", "공장창고등", "토지",
)

_BASE_URL = "https://apis.data.go.kr/1613000"

# 유형 -> 서비스ID / 이름 필드 후보 / 면적 필드 후보
# (실제 국토부 응답 필드 태그명 기준 - 유사 프로젝트에서 실제 호출 검증된 값)
_REGISTRY: dict = {
    "아파트": {
        "service": "RTMSDataSvcAptTradeDev",
        "name_tags": ("aptNm",),
        "area_tags": ("excluUseAr",),
    },
    "연립다세대": {
        "service": "RTMSDataSvcRHTrade",
        "name_tags": ("mhouseNm",),
        "area_tags": ("excluUseAr", "landAr"),
    },
    "단독다가구": {
        "service": "RTMSDataSvcSHTrade",
        # 단독/다가구는 필지 전체 거래라 "단지명" 개념이 없고 주택유형만 있다.
        "name_tags": ("houseType",),
        "area_tags": ("totalFloorAr", "plottageAr"),
    },
    "오피스텔": {
        "service": "RTMSDataSvcOffiTrade",
        "name_tags": ("offiNm",),
        "area_tags": ("excluUseAr",),
    },
    "상업업무용": {
        "service": "RTMSDataSvcNrgTrade",
        "name_tags": ("buildingUse",),
        "area_tags": ("buildingAr", "plottageAr"),
    },
    "공장창고등": {
        "service": "RTMSDataSvcInduTrade",  # ✔ 검증된 값 (아래 docstring 참고)
        "name_tags": ("buildingUse",),
        "area_tags": ("buildingAr", "plottageAr"),
    },
    "토지": {
        "service": "RTMSDataSvcLandTrade",
        "name_tags": ("jimok",),
        "area_tags": ("dealArea",),
    },
}

_TRANSIENT_ERRORS = (
    httpx.RemoteProtocolError, httpx.ConnectError, httpx.ReadError,
    httpx.ReadTimeout, httpx.ConnectTimeout, httpx.PoolTimeout,
)


class MolitApiError(Exception):
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


def _first(it: dict, tags: tuple) -> Optional[str]:
    for tag in tags:
        v = it.get(tag)
        if v:
            return v
    return None


def _first_float(it: dict, tags: tuple) -> Optional[float]:
    raw = _first(it, tags)
    if raw is None:
        return None
    try:
        return float(str(raw).replace(",", "").strip())
    except (ValueError, TypeError):
        return None


def _yyyymm_range(months: int, end_months_ago: int = 0) -> list:
    """오늘 기준 최근 `months`개월치 "YYYYMM" 목록을 오래된 순으로 만든다.
    end_months_ago만큼 조회 종료 시점을 과거로 민다 (기본 0 = 이번 달까지)."""
    end = datetime.now()
    ey, em = end.year, end.month - end_months_ago
    while em <= 0:
        em += 12
        ey -= 1
    y, m = ey, em - (months - 1)
    while m <= 0:
        m += 12
        y -= 1
    out = []
    while (y, m) <= (ey, em):
        out.append(f"{y:04d}{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return out


async def _fetch_one_month(client: httpx.AsyncClient, url: str, lawd_cd: str, deal_ymd: str) -> list:
    """한 달치를 페이지네이션(numOfRows/pageNo/totalCount) 처리하며 전부 모은다."""
    rows = []
    page = 1
    num_of_rows = 1000
    while True:
        params = {
            "serviceKey": config.MOLIT_SERVICE_KEY,
            "LAWD_CD": lawd_cd,
            "DEAL_YMD": deal_ymd,
            "numOfRows": str(num_of_rows),
            "pageNo": str(page),
        }
        resp = await _resilient_get(client, url, params)
        resp.raise_for_status()

        try:
            root = ET.fromstring(resp.text)
        except ET.ParseError:
            raise MolitApiError(f"실거래가 응답 파싱 실패(url={url}): {resp.text[:200]}")

        result_code = (root.findtext(".//resultCode") or "").strip()
        if result_code not in ("00", "000"):
            result_msg = (root.findtext(".//resultMsg") or "").strip()
            raise MolitApiError(f"실거래가 API 오류(resultCode={result_code}): {result_msg}")

        items = root.findall(".//item")
        for it in items:
            rows.append({child.tag: (child.text or "").strip() for child in it})

        total_count = int(root.findtext(".//totalCount") or "0")
        if page * num_of_rows >= total_count or not items:
            break
        page += 1
    return rows


async def get_trades(property_type: str, lawd_cd: str, months: int = 6, end_months_ago: int = 0) -> list:
    """유형에 맞는 국토교통부 매매 실거래가 API를 최근 `months`개월치 호출해
    공통 포맷으로 정규화한다.

    lawd_cd: 시군구 5자리 (예: 서울 마포구 11440)

    반환: [{"name":..., "deal_amount_manwon":..., "area_m2":..., "floor":...,
            "build_year":..., "deal_date":..., "jibun":..., "지번마스킹됨": bool}, ...]
    name/area_m2/floor/build_year/jibun은 유형에 따라 빈 값일 수 있다.
    """
    if property_type not in _REGISTRY:
        raise MolitApiError(
            f"지원하지 않는 매물 유형입니다: {property_type} "
            f"(지원: {', '.join(PROPERTY_TYPES)})"
        )
    if not config.MOLIT_SERVICE_KEY:
        raise MolitApiError("MOLIT_SERVICE_KEY(또는 DATA_SERVICE_KEY)가 설정되어 있지 않습니다.")

    entry = _REGISTRY[property_type]
    service = entry["service"]
    url = f"{_BASE_URL}/{service}/get{service}"

    yms = _yyyymm_range(months, end_months_ago)
    rows = []
    async with httpx.AsyncClient(timeout=config.HTTP_TIMEOUT_SECONDS) as http:
        for ym in yms:
            rows.extend(await _fetch_one_month(http, url, lawd_cd, ym))

    out = []
    for it in rows:
        amount_raw = it.get("dealAmount")
        if amount_raw is None:
            continue
        try:
            amount = int(str(amount_raw).replace(",", "").strip())
        except (ValueError, TypeError):
            continue

        deal_year = it.get("dealYear")
        deal_month = it.get("dealMonth")
        deal_day = it.get("dealDay")
        deal_date = (
            f"{deal_year}-{str(deal_month).zfill(2)}-{str(deal_day).zfill(2)}"
            if deal_year and deal_month and deal_day else None
        )
        jibun = it.get("jibun") or ""

        out.append({
            "name": _first(it, entry["name_tags"]) or "",
            "deal_amount_manwon": amount,
            "area_m2": _first_float(it, entry["area_tags"]),
            "floor": it.get("floor"),
            "build_year": it.get("buildYear"),
            "deal_date": deal_date,
            "jibun": jibun,
            "지번마스킹됨": "*" in jibun,
        })
    return out


# 하위호환용 얇은 래퍼
async def get_apt_trades(lawd_cd: str, deal_ymd: str) -> list:
    """구버전 호출부 호환용. deal_ymd(예: '202601') 한 달만 조회한다."""
    entry = _REGISTRY["아파트"]
    url = f"{_BASE_URL}/{entry['service']}/get{entry['service']}"
    if not config.MOLIT_SERVICE_KEY:
        raise MolitApiError("MOLIT_SERVICE_KEY(또는 DATA_SERVICE_KEY)가 설정되어 있지 않습니다.")
    async with httpx.AsyncClient(timeout=config.HTTP_TIMEOUT_SECONDS) as http:
        rows = await _fetch_one_month(http, url, lawd_cd, deal_ymd)
    out = []
    for it in rows:
        try:
            out.append({
                "apt_name": it.get("aptNm", "").strip(),
                "deal_amount_manwon": int(str(it.get("dealAmount", "0")).replace(",", "").strip()),
                "area_m2": float(it.get("excluUseAr", 0)),
                "floor": it.get("floor"),
                "build_year": it.get("buildYear"),
                "deal_date": f"{it.get('dealYear')}-{str(it.get('dealMonth')).zfill(2)}-{str(it.get('dealDay')).zfill(2)}",
                "jibun": it.get("jibun"),
            })
        except (ValueError, TypeError):
            continue
    return out
