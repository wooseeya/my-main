"""주소 -> 법정동코드/지번 변환 (카카오 로컬 API, 무료).

신청: https://developers.kakao.com -> 애플리케이션 추가 -> "REST API 키" 발급
      (카카오 로컬 API는 별도 활용신청 없이 앱 생성만으로 바로 사용 가능)

카카오 주소검색 API(v2/local/search/address.json) 응답의 address.b_code는
10자리 법정동코드다: 앞 5자리가 시군구코드, 뒤 5자리가 읍면동코드.
  - sigungu_cd (5자리)  : 건축HUB(sigunguCd)·국토부 실거래가(LAWD_CD) 공용
  - bdong_cd   (5자리)  : 건축HUB(bjdongCd)
  - bun/ji     (각 4자리, zero-padded) : 건축HUB(bun/ji), PNU 조립(land_ledger)
  - plat_gb_cd ("0"=대지,"1"=산) : 건축HUB 컨벤션. mountain_yn 필드로 판정.
  - lawd_cd (5자리) : sigungu_cd와 동일 (국토부 실거래가 LAWD_CD 파라미터용 별칭)

이 파일은 (참고용으로 검토한) 유사 프로젝트의 카카오 주소검색 파싱 로직을
그대로 가져와 이 프로젝트가 기대하는 반환 스키마(sigungu_cd/bdong_cd/bun/ji/
plat_gb_cd/lawd_cd)에 맞춰 정리한 것이다.
"""

import os

import httpx

_ADDRESS_URL = "https://dapi.kakao.com/v2/local/search/address.json"
_TIMEOUT = 8.0


class GeocodeError(Exception):
    pass


def _kakao_key() -> str:
    # config.py를 거치지 않고 직접 읽는다 (key.env: KAKAO_KEY=...)
    return os.environ.get("KAKAO_KEY", "")


async def address_to_codes(address: str) -> dict:
    """주소 문자열 -> {"sigungu_cd","bdong_cd","bun","ji","plat_gb_cd","lawd_cd",
    "road_address","jibun_address","lat","lng"} 딕셔너리. 실패 시 GeocodeError.

    lat/lng: 카카오 주소검색 API가 문서마다 함께 내려주는 좌표(x=경도,y=위도)를
    그대로 float로 파싱한 값이다 - 별도 API 호출 없이 지도에 마커를 찍을 수
    있다. 좌표를 못 읽으면(형식 이상 등) None으로 남긴다(값을 지어내지 않음)."""
    key = _kakao_key()
    if not key:
        raise GeocodeError("KAKAO_KEY가 설정되어 있지 않습니다.")
    if not address or not address.strip():
        raise GeocodeError("주소가 비어 있습니다.")

    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.get(
                _ADDRESS_URL,
                headers={"Authorization": f"KakaoAK {key}"},
                params={"query": address, "analyze_type": "similar", "page": 1, "size": 5},
            )
        resp.raise_for_status()
        docs = resp.json().get("documents", [])
    except httpx.HTTPError as e:
        raise GeocodeError(f"카카오 주소검색 API 호출 실패: {e}")
    except ValueError:
        raise GeocodeError("카카오 주소검색 응답을 JSON으로 파싱하지 못했습니다.")

    if not docs:
        raise GeocodeError(f"'{address}'에 대한 주소 검색 결과가 없습니다.")

    for doc in docs:
        addr = doc.get("address") or {}
        b_code = addr.get("b_code", "")
        main_no = addr.get("main_address_no", "")
        sub_no = addr.get("sub_address_no", "") or "0"
        mountain_yn = addr.get("mountain_yn", "N")

        if not (b_code and len(b_code) == 10 and main_no):
            continue

        road = doc.get("road_address") or {}

        # 카카오 주소검색 응답은 문서 최상위에 x(경도)/y(위도)를 문자열로 함께 준다.
        # 못 읽어도(형식 이상 등) 나머지 코드 정보는 그대로 반환한다 - 좌표만 못 쓰는
        # 상태로 두고 지도 기능만 비활성화되게(GeocodeError로 전체를 실패시키지 않음).
        lat = lng = None
        try:
            lng = float(doc.get("x"))
            lat = float(doc.get("y"))
        except (TypeError, ValueError):
            lat = lng = None

        return {
            "sigungu_cd": b_code[:5],
            "bdong_cd": b_code[5:10],
            "bun": str(main_no).zfill(4),
            "ji": str(sub_no).zfill(4),
            "plat_gb_cd": "1" if mountain_yn == "Y" else "0",
            "lawd_cd": b_code[:5],
            "road_address": road.get("address_name", ""),
            "jibun_address": addr.get("address_name") or doc.get("address_name", ""),
            "lat": lat,
            "lng": lng,
        }

    raise GeocodeError(
        f"'{address}' 검색 결과에서 지번 기반 법정동코드(b_code)를 확인하지 못했습니다. "
        "도로명 주소만 있는 신축 건물이거나 주소 형식이 인식되지 않았을 수 있습니다."
    )


_KEYWORD_URL = "https://dapi.kakao.com/v2/local/search/keyword.json"


async def search_addresses(query: str, size: int = 15) -> list:
    """지번/도로명 주소 문자열로 카카오 **주소검색** API(v2/local/search/address.json)를
    호출해 여러 후보를 좌표와 함께 반환한다 - 지도검색 화면에서 "역삼동 736-1"처럼
    지번 주소를 입력했을 때 쓰기 위한 함수다.

    address_to_codes()와 차이점: address_to_codes()는 "법정동코드 변환"이 목적이라
    b_code가 있는 첫 매칭 1건만 쓰고 나머지는 버리며, 매칭 실패 시 GeocodeError를
    던져 호출부(매물 접수)가 확실히 실패를 인지하게 한다. 반면 이 함수는 "지도에
    후보를 여러 개 보여주는" 지도검색 UI가 목적이라 매칭되는 문서를 전부(최대 size개)
    반환하고, 결과가 0건이어도 예외를 던지지 않고 빈 리스트를 반환한다(지도검색에서
    "결과 없음"은 흔한 정상 상황이지, 화면 전체를 막을 오류가 아니다)."""
    key = _kakao_key()
    if not key:
        raise GeocodeError("KAKAO_KEY가 설정되어 있지 않습니다.")
    if not query or not query.strip():
        return []

    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.get(
                _ADDRESS_URL,
                headers={"Authorization": f"KakaoAK {key}"},
                params={"query": query, "analyze_type": "similar", "page": 1, "size": max(1, min(size, 30))},
            )
        resp.raise_for_status()
        docs = resp.json().get("documents", [])
    except httpx.HTTPError as e:
        raise GeocodeError(f"카카오 주소검색 API 호출 실패: {e}")
    except ValueError:
        raise GeocodeError("카카오 주소검색 응답을 JSON으로 파싱하지 못했습니다.")

    results = []
    for doc in docs:
        try:
            lat = float(doc.get("y"))
            lng = float(doc.get("x"))
        except (TypeError, ValueError):
            continue  # 좌표를 못 읽는 항목은 지도에 찍을 수 없으므로 건너뛴다
        addr = doc.get("address") or {}
        road = doc.get("road_address") or {}
        # 주소검색 결과엔 상호명 개념이 없으니 지번주소 자체를 카드 제목으로 쓴다.
        display_name = addr.get("address_name") or doc.get("address_name", "")
        results.append({
            "name": display_name,
            "address": display_name,
            "road_address": road.get("address_name", ""),
            "category": "지번주소",
            "phone": "",
            "lat": lat,
            "lng": lng,
        })
    return results


async def search_places(query: str, size: int = 15) -> list:
    """키워드(장소명/주소 등)로 카카오 로컬 키워드검색을 호출한다 - 지도검색 화면의
    좌측 검색결과 목록용. address_to_codes()는 "정확한 지번 주소 1건"을 법정동코드로
    바꾸는 데 특화된 반면, 이 함수는 "강남역", "역삼동 스타벅스"처럼 느슨한 키워드로
    여러 후보 장소를 좌표와 함께 받아온다.

    반환: [{"name","address","road_address","category","phone","lat","lng"}, ...].
    검색 자체가 실패하면(키 없음, API 오류 등) GeocodeError. 결과가 0건이면(정상
    응답이지만 매칭 없음) 빈 리스트를 반환한다 - 이건 오류가 아니다."""
    key = _kakao_key()
    if not key:
        raise GeocodeError("KAKAO_KEY가 설정되어 있지 않습니다.")
    if not query or not query.strip():
        raise GeocodeError("검색어가 비어 있습니다.")

    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.get(
                _KEYWORD_URL,
                headers={"Authorization": f"KakaoAK {key}"},
                params={"query": query, "size": max(1, min(size, 15))},
            )
        resp.raise_for_status()
        docs = resp.json().get("documents", [])
    except httpx.HTTPError as e:
        raise GeocodeError(f"카카오 키워드검색 API 호출 실패: {e}")
    except ValueError:
        raise GeocodeError("카카오 키워드검색 응답을 JSON으로 파싱하지 못했습니다.")

    results = []
    for doc in docs:
        try:
            lat = float(doc.get("y"))
            lng = float(doc.get("x"))
        except (TypeError, ValueError):
            continue  # 좌표를 못 읽는 항목은 지도에 찍을 수 없으므로 건너뛴다
        results.append({
            "name": doc.get("place_name", ""),
            "address": doc.get("address_name", ""),
            "road_address": doc.get("road_address_name", ""),
            "category": doc.get("category_group_name") or doc.get("category_name", ""),
            "phone": doc.get("phone", ""),
            "lat": lat,
            "lng": lng,
        })
    return results
