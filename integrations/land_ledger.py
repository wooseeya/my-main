"""VWorld 토지(임야)대장 API (국토교통부 국가공간정보포털, 무료).

신청: https://www.vworld.kr -> 오픈API -> 인증키 발급 (도메인 등록 필요,
      로컬 테스트는 domain=localhost로 등록)

토지 매물은 건축물대장이 없으므로(건물이 아니라 땅 자체이므로), 이 연동이
지목·면적·소유구분 등을 자동으로 확인하는 유일한 수단이다. main.py는 건축물대장이
없을 때(토지 매물, 또는 건축물대장 조회 결과가 없을 때) 이 모듈로 대신 확인한다.

PNU(필지고유번호, 19자리) 조립 방식:
  법정동코드(10자리) + 산여부(1자리: 1=대지,2=산) + 본번(4자리) + 부번(4자리)
geocode.address_to_codes()가 반환하는 plat_gb_cd는 건축HUB 컨벤션("0"=대지,
"1"=산)이라 PNU 컨벤션과 다르다 - build_pnu()에서 변환한다.

VWorld 토지(임야)대장 API(ladfrlList)는 지목/면적 필드명을 명확히 문서화하지
않는 경우가 많아, 아래 필드 후보(candidate) 방식으로 여러 이름을 시도한다.
실제 서비스키로 첫 호출 후 응답 원문을 한 번 찍어보고 후보에 없는 필드명이면
_CANDIDATES에 추가하면 된다.

용도지역(get_land_use_zones)은 별도의 VWorld 토지특성정보 API(getLandCharacteristics)를
쓴다. 같은 도메인·같은 키를 재사용하지만, 이 API는 기준연도(stdrYear)별로 데이터가
발행되는 구조라 연도를 지정해야 한다 - 올해부터 과거로 6개년을 역순으로 시도해서
첫 성공 응답을 쓴다. 응답의 prposArea1Nm/prposArea2Nm 필드가 각각 용도지역1/2다.
용도지역2는 실제로 안 걸쳐있는 필지에서도 "지정되지않음" 같은 플레이스홀더 값으로
오는 경우가 많아 그런 무효 값은 걸러낸다.
"""

import os
from datetime import datetime
from typing import Optional
import xml.etree.ElementTree as ET

import httpx

_LADFRL_URL = "https://api.vworld.kr/ned/data/ladfrlList"
_LANDCHAR_URL = "https://api.vworld.kr/ned/data/getLandCharacteristics"
_TIMEOUT = 10.0
_INVALID_ZONE_VALUES = {"", "지정되지않음", "지정되지 않음", "해당없음", "해당 없음", "미지정", "없음"}


class LandLedgerError(Exception):
    pass


def _vworld_key() -> str:
    return os.environ.get("VWORLD_KEY", "")


def _vworld_domain() -> str:
    return os.environ.get("VWORLD_DOMAIN", "localhost")


def is_configured() -> bool:
    return bool(_vworld_key())


def build_pnu(codes: dict) -> Optional[str]:
    """geocode.address_to_codes()가 반환한 codes -> PNU(19자리).
    구성 요소가 부족하면 None."""
    sigungu = (codes.get("sigungu_cd") or "").strip()
    bdong = (codes.get("bdong_cd") or "").strip()
    bun = str(codes.get("bun") or "0").zfill(4)
    ji = str(codes.get("ji") or "0").zfill(4)
    if len(sigungu) != 5 or len(bdong) != 5:
        return None
    # 건축HUB 컨벤션("0"=대지,"1"=산) -> PNU 컨벤션("1"=대지,"2"=산)
    plat = "2" if codes.get("plat_gb_cd") == "1" else "1"
    pnu = f"{sigungu}{bdong}{plat}{bun}{ji}"
    return pnu if len(pnu) == 19 else None


def _parse_ladfrl_records(body: dict) -> list:
    """ladfrlList JSON 응답 - ladfrlVO(구)/ladfrlVOList(신) 두 스키마 모두 지원."""
    vo_list = body.get("ladfrlVOList", {})
    if not isinstance(vo_list, dict):
        return []
    records = vo_list.get("ladfrlVO") or vo_list.get("ladfrlVOList") or []
    if isinstance(records, dict):
        return [records]
    return records if isinstance(records, list) else []


def _first(record: dict, keys: tuple) -> Optional[str]:
    for k in keys:
        v = record.get(k)
        if v not in (None, ""):
            return v
    return None


async def get_land_title_info(pnu: str) -> Optional[dict]:
    """토지(임야)대장 조회. 반환: {"지목","면적","소유구분","공유인수"} | None(결과 없음).
    실패(키 미설정/네트워크/파싱 오류)는 LandLedgerError로 raise."""
    if not is_configured():
        raise LandLedgerError("VWORLD_KEY가 설정되어 있지 않습니다.")

    params = {
        "key": _vworld_key(),
        "domain": _vworld_domain(),
        "format": "json",
        "numOfRows": "10",
        "pageNo": "1",
        "pnu": pnu,
    }
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.get(_LADFRL_URL, params=params)
        resp.raise_for_status()
        body = resp.json()
    except httpx.HTTPError as e:
        raise LandLedgerError(f"VWorld 토지대장 API 호출 실패: {e}")
    except ValueError:
        raise LandLedgerError("VWorld 토지대장 응답을 JSON으로 파싱하지 못했습니다.")

    if "error" in body:
        err = body["error"]
        raise LandLedgerError(f"VWorld API 오류(code={err.get('code', '?')}): {err.get('text', '')}")

    records = _parse_ladfrl_records(body)
    if not records:
        return None

    r = records[0]
    return {
        "지목": _first(r, ("lndcgrCodeNm", "jimok")),
        "면적": _first(r, ("lndpclAr", "ar", "area")),
        "소유구분": _first(r, ("posesnSeCodeNm", "ownshipSeCodeNm")),
        "공유인수": _first(r, ("cnrsPsnCo", "coOwnerCnt")),
        "raw": r,  # 후보 필드명이 안 맞을 때 디버깅용 원본 보존
    }


def _vw_xml_to_list(xml_text: str, item_tag: str = "field") -> list:
    """VWorld XML 목록 응답 파싱 (getLandCharacteristics는 JSON이 아니라 XML로 받는다)."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return []
    err_cd = root.findtext(".//errCode") or ""
    if err_cd and err_cd != "0":
        return []
    result = []
    for el in root.iter(item_tag):
        row = {c.tag: c.text.strip() for c in el if c.text and c.text.strip()}
        if row:
            result.append(row)
    return result


async def get_land_use_zones(pnu: str) -> Optional[dict]:
    """용도지역1/용도지역2 조회 (VWorld 토지특성정보, getLandCharacteristics).
    반환: {"용도지역": [str, ...], "기준연도": str, "raw": list} | None(6개년 모두 결과 없음).
    실패(키 미설정/네트워크 오류)는 LandLedgerError로 raise."""
    if not is_configured():
        raise LandLedgerError("VWORLD_KEY가 설정되어 있지 않습니다.")

    this_year = datetime.now().year
    items = None
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            for offset in range(6):  # 발행 연도가 지역마다 달라 최근 6개년을 역순으로 시도
                year = str(this_year - offset)
                params = {
                    "key": _vworld_key(), "domain": _vworld_domain(), "format": "xml",
                    "numOfRows": "10", "pageNo": "1", "pnu": pnu, "stdrYear": year,
                }
                resp = await client.get(_LANDCHAR_URL, params=params)
                resp.raise_for_status()
                found = _vw_xml_to_list(resp.text, "field")
                if found:
                    items = found
                    break
    except httpx.HTTPError as e:
        raise LandLedgerError(f"VWorld 토지특성 API 호출 실패: {e}")

    if not items:
        return None

    # 필지가 용도지역 경계에 걸쳐 있으면 VWorld가 그 해의 결과를 레코드
    # 하나가 아니라 여러 개(field)로 나눠 줄 수 있다 - 예전엔 그중 가장 최신
    # 레코드 하나만 골라 썼는데, 그러면 다른 레코드에만 있는 용도지역이 누락될
    # 수 있었다. 그래서 그 해에 반환된 모든 레코드를 훑어 prposArea1Nm/
    # prposArea2Nm을 전부 모으고 중복만 제거한다.
    names = []
    latest_year = ""
    for record in items:
        stdr_year = record.get("stdrYear", "")
        if stdr_year > latest_year:
            latest_year = stdr_year
        for k in ("prposArea1Nm", "prposArea2Nm"):
            v = (record.get(k) or "").strip()
            if v and v not in _INVALID_ZONE_VALUES and v not in names:
                names.append(v)

    if not names:
        for record in items:
            fallback = _first(record, ("prposAreaDstrcNm", "ladUseSittnNm", "ladUseSittn"))
            if fallback and fallback not in names:
                names.append(fallback)

    return {"용도지역": names, "기준연도": latest_year, "raw": items}


_PARCEL_DATA_URL = "https://api.vworld.kr/req/data"
_PARCEL_LAYER = "LP_PA_CBND_BUBUN"  # 연속지적도(임야도 포함) - VWorld 데이터API 레이어명


async def get_parcel_geometry(pnu: str) -> Optional[list]:
    """PNU(필지고유번호, 19자리) 하나의 필지 경계 폴리곤 좌표를 조회한다.

    카카오맵 JS SDK의 "지적편집도"(MapTypeId.USE_DISTRICT)는 지적 경계가 그려진
    통짜 타일 이미지일 뿐이라, 특정 필지 하나만 뽑아 하이라이트로 그릴 수 있는
    벡터 데이터가 아니다. 실제 필지 하나의 경계선을 지도 위에 그리려면(카카오맵
    웹사이트에서 지번 검색 시 나오는 빨간 테두리처럼) 좌표 목록이 있는 폴리곤
    데이터가 필요한데, 이건 VWorld의 연속지적도 데이터API(레이어 LP_PA_CBND_BUBUN)로
    받아온다 - 같은 VWorld 계정/키를 토지대장 조회(get_land_title_info)와
    공유한다.

    반환: [[lat, lng], [lat, lng], ...] (필지 외곽선 1개, 시계/반시계 방향은
    신경쓰지 않음) | None(결과 없음/좌표 못 읽음). 실패(키 미설정/네트워크/파싱
    오류)는 LandLedgerError.

    ⚠️ 이 프로젝트의 다른 VWorld 연동(get_land_title_info 등)은 실제 서비스키로
    호출 이력이 있는 값들이지만, 이 함수는 VWorld 데이터API 문서 스펙대로
    작성만 해둔 상태다 - 레이어명(LP_PA_CBND_BUBUN)이나 응답 필드가 실제와 다를
    수 있다. 실 서비스 전에 반드시 실제 키로 PNU 하나를 조회해 아래 파싱 로직이
    실제 응답 구조와 맞는지 확인해보길 권장한다(안 맞으면 응답 원문을 한 번
    찍어서 features 경로를 재조정하면 된다)."""
    if not is_configured():
        raise LandLedgerError("VWORLD_KEY가 설정되어 있지 않습니다.")

    params = {
        "service": "data",
        "request": "GetFeature",
        "data": _PARCEL_LAYER,
        "key": _vworld_key(),
        "domain": _vworld_domain(),
        "format": "json",
        "crs": "EPSG:4326",  # 위경도(lat/lng) 그대로 받기 위해 명시 - 없으면 VWorld 기본 좌표계(EPSG:5179 등)로 와서 별도 좌표변환이 필요해진다
        "attrFilter": f"pnu:=:{pnu}",
        "size": "1",
    }
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.get(_PARCEL_DATA_URL, params=params)
        resp.raise_for_status()
        body = resp.json()
    except httpx.HTTPError as e:
        raise LandLedgerError(f"VWorld 지적도 API 호출 실패: {e}")
    except ValueError:
        raise LandLedgerError("VWorld 지적도 응답을 JSON으로 파싱하지 못했습니다.")

    result = (body.get("response") or {}).get("result") or {}
    features = ((result.get("featureCollection") or {}).get("features")) or []
    if not features:
        return None

    geom = features[0].get("geometry") or {}
    coords = geom.get("coordinates")
    if not coords:
        return None

    # GeoJSON 좌표는 [lng, lat] 순서 - Polygon은 [[[lng,lat], ...]], MultiPolygon은
    # [[[[lng,lat], ...]]] 구조다. 구멍(내부 링)이나 부속 폴리곤은 무시하고 가장
    # 바깥쪽 외곽선 하나만 쓴다(경계선 표시용이라 이걸로 충분하다).
    ring = None
    if geom.get("type") == "Polygon" and coords:
        ring = coords[0]
    elif geom.get("type") == "MultiPolygon" and coords and coords[0]:
        ring = coords[0][0]
    if not ring:
        return None

    try:
        return [[float(lat), float(lng)] for lng, lat in ring]
    except (TypeError, ValueError):
        return None

