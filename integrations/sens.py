"""NCP(네이버클라우드플랫폼) SENS - SMS/카카오 알림톡 발송 연동.

신청 순서:
  1. https://www.ncloud.com 가입 -> 마이페이지 -> 계정관리 -> 인증키 관리에서
     API 인증키(Access Key ID / Secret Key) 발급
  2. Console -> Simple & Easy Notification Service(SENS) -> "SMS" 서비스 생성
     -> 발신번호 등록(본인/사업자 명의 인증 필요, 통신사 심사 보통 1~2일 소요)
     -> 서비스 ID 확인
  3. (알림톡을 쓸 경우) SENS -> "Kakaotalk Bizmessage" 서비스를 별도로 생성하고,
     카카오 비즈니스 채널(플러스친구)을 연동한 뒤, 실제 보낼 문구를 "템플릿"으로
     만들어 카카오 쪽에 사전 심사를 요청한다(승인까지 보통 1~3일). 발송할 내용은
     승인된 템플릿과 형식이 같아야(변수만 채워넣은 형태) 실제로 전달된다 - 임의로
     문구를 바꾸면 반려되거나 실패할 수 있다.

main.py는 이 두 방식(SMS/알림톡) 중 문의 화면에서 중개사가 고른 쪽으로 발송을
요청한다 - 초안 검토(문의 응답 흐름의 기존 원칙과 동일)는 여전히 사람이 하고,
"발송" 버튼을 누르는 순간부터만 이 모듈이 대신 API를 호출한다.

⚠️ 주의: 이 파일은 NCP 공식 API 문서 스펙을 따라 작성됐지만, 아직 실제 키로 발송
테스트를 해보지 않은 상태다(2026-08 기준). 지난 점검에서 두 가지가 문서 스펙과
어긋나 있어 고쳤다 - ① SMS의 type 필드는 'SMS'|'LMS'|'MMS'만 허용하는데
'AUTO'라는 값을 보내고 있었음(→ 바이트 길이 기준 자동 판정으로 수정),
② 알림톡 대체발송 필드가 평평한 'fallbackContent'였는데 실제로는
'failoverConfig'(type/from/content) 객체여야 함(→ 수정). 실제 키가 준비되면
SMS/알림톡 각 한 통씩 시험발송해서 서명 인증(401/403 여부)과 실제 도착 여부를
NCP 콘솔 발송이력에서 확인해보길 권장한다.
"""

import base64
import hashlib
import hmac
import os
import time
from typing import Optional

import httpx

_BASE_URL = "https://sens.apigw.ntruss.com"
_TIMEOUT = 10.0


class SensError(Exception):
    pass


# config.py를 거치지 않고 직접 읽는다 (geocode.py/land_ledger.py와 같은 패턴 -
# key.env: NCP_ACCESS_KEY=... 등). SMS와 알림톡은 서로 다른 SENS "서비스 인스턴스"라
# 서비스ID가 따로지만, 인증키(Access/Secret Key)와 서명 방식은 계정 공용이라 같다.
def _access_key() -> str:
    return os.environ.get("NCP_ACCESS_KEY", "")


def _secret_key() -> str:
    return os.environ.get("NCP_SECRET_KEY", "")


def _sms_service_id() -> str:
    return os.environ.get("SENS_SMS_SERVICE_ID", "")


def _sms_sender() -> str:
    # 사전등록된 발신번호. 하이픈 유무 상관없이 받아서 호출 시 숫자만 남긴다.
    return os.environ.get("SENS_SMS_SENDER", "")


def _alimtalk_service_id() -> str:
    return os.environ.get("SENS_ALIMTALK_SERVICE_ID", "")


def _alimtalk_plus_friend_id() -> str:
    # "@플러스친구ID" 형식 (카카오 비즈니스 채널 관리자센터에서 확인)
    return os.environ.get("SENS_ALIMTALK_PLUS_FRIEND_ID", "")


def _alimtalk_template_code() -> str:
    return os.environ.get("SENS_ALIMTALK_TEMPLATE_CODE", "")


def is_sms_configured() -> bool:
    return bool(_access_key() and _secret_key() and _sms_service_id() and _sms_sender())


def is_alimtalk_configured() -> bool:
    return bool(
        _access_key() and _secret_key() and _alimtalk_service_id()
        and _alimtalk_plus_friend_id() and _alimtalk_template_code()
    )


def _clean_phone(phone: str) -> str:
    """하이픈/공백 제거 - SENS API는 숫자만 받는다."""
    return "".join(ch for ch in (phone or "") if ch.isdigit())


def _signature(method: str, uri: str, timestamp: str) -> str:
    """NCP API Gateway 공용 서명 방식(HMAC-SHA256, base64) - SENS 하위의 모든
    API(SMS/알림톡)가 이 서명 방식을 그대로 공유한다.
    message = "{method} {uri}\\n{timestamp}\\n{access_key}" 형태를 시크릿키로 HMAC-SHA256
    한 뒤 base64 인코딩한다. uri는 쿼리스트링을 제외한 경로만 포함한다."""
    message = f"{method} {uri}\n{timestamp}\n{_access_key()}"
    digest = hmac.new(_secret_key().encode("utf-8"), message.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def _headers(method: str, uri: str) -> dict:
    timestamp = str(int(time.time() * 1000))
    return {
        "Content-Type": "application/json; charset=utf-8",
        "x-ncp-apigw-timestamp": timestamp,
        "x-ncp-iam-access-key": _access_key(),
        "x-ncp-apigw-signature-v2": _signature(method, uri, timestamp),
    }


async def _handle_response(resp: httpx.Response, label: str) -> dict:
    try:
        body = resp.json()
    except ValueError:
        raise SensError(f"{label} 발송 응답을 파싱하지 못했습니다(status={resp.status_code}): {resp.text[:200]}")
    if resp.status_code not in (200, 202):
        err_msg = body.get("errorMessage") or body.get("message") or str(body)
        raise SensError(f"{label} 발송 실패(status={resp.status_code}): {err_msg}")
    return body


def _sms_byte_len(text: str) -> int:
    """SMS/LMS 자동 판정을 위한 바이트 길이 계산 - 한글은 EUC-KR 기준 2byte로
    어림잡는다(NCP SENS도 EUC-KR 인코딩으로 발송하므로 동일 기준)."""
    total = 0
    for ch in text:
        try:
            total += len(ch.encode("euc-kr"))
        except UnicodeEncodeError:
            total += 2
    return total


async def send_sms(to: str, content: str, msg_type: Optional[str] = None) -> dict:
    """SMS/LMS 발송. NCP SENS API의 type 필드는 'SMS' | 'LMS' | 'MMS' 세 값만
    허용하고 'AUTO' 같은 값은 지원하지 않는다(공식 문서 기준) - msg_type을 직접
    지정하지 않으면 content의 바이트 길이를 기준으로 90byte 이하면 'SMS', 넘으면
    'LMS'로 자동 판정해서 채운다. 반환값은 NCP 응답 바디(requestId, statusCode 등)
    그대로다 - 실제 통신사 전달 성공 여부는 비동기라 이 응답만으로는 알 수 없고
    (202 Accepted는 "접수됨"이지 "도착함"이 아님), 확인하려면 NCP 콘솔의 발송
    이력을 봐야 한다."""
    if not is_sms_configured():
        raise SensError(
            "SENS SMS 설정(NCP_ACCESS_KEY/NCP_SECRET_KEY/SENS_SMS_SERVICE_ID/SENS_SMS_SENDER)이 "
            "되어 있지 않습니다."
        )
    to_clean = _clean_phone(to)
    if not to_clean:
        raise SensError("수신번호가 올바르지 않습니다.")
    if not content or not content.strip():
        raise SensError("보낼 내용이 비어 있습니다.")

    resolved_type = msg_type or ("SMS" if _sms_byte_len(content) <= 90 else "LMS")
    uri = f"/sms/v2/services/{_sms_service_id()}/messages"
    body = {
        "type": resolved_type,
        "contentType": "COMM",
        "countryCode": "82",
        "from": _clean_phone(_sms_sender()),
        "content": content,
        "messages": [{"to": to_clean}],
    }
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.post(_BASE_URL + uri, headers=_headers("POST", uri), json=body)
    return await _handle_response(resp, "SMS")


async def send_alimtalk(to: str, content: str, sms_fallback_content: Optional[str] = None) -> dict:
    """카카오 알림톡 발송. `content`는 승인받은 템플릿(SENS_ALIMTALK_TEMPLATE_CODE)의
    변수만 채운 형태여야 한다 - 문구 자체를 임의로 바꾸면 카카오 쪽에서 반려/실패
    처리될 수 있다.

    SMS 발신번호(SENS_SMS_SENDER)까지 설정되어 있으면 useSmsFailover를 켠다 -
    수신자가 카카오톡을 안 쓰거나 채널을 차단한 경우 등 알림톡이 실패하면 SENS가
    자동으로 문자로 대체발송해준다. sms_fallback_content를 안 주면 content를
    대체발송 문구로 그대로 재사용한다(알림톡 템플릿 문구가 문자로 보내기에 크게
    부자연스럽지 않은 경우가 많아 기본값을 이렇게 뒀다 - 90자를 넘으면 SENS가
    자동으로 LMS 처리한다)."""
    if not is_alimtalk_configured():
        raise SensError(
            "SENS 알림톡 설정(NCP_ACCESS_KEY/NCP_SECRET_KEY/SENS_ALIMTALK_SERVICE_ID/"
            "SENS_ALIMTALK_PLUS_FRIEND_ID/SENS_ALIMTALK_TEMPLATE_CODE)이 되어 있지 않습니다."
        )
    to_clean = _clean_phone(to)
    if not to_clean:
        raise SensError("수신번호가 올바르지 않습니다.")
    if not content or not content.strip():
        raise SensError("보낼 내용이 비어 있습니다.")

    use_failover = is_sms_configured()
    message = {"to": to_clean, "content": content, "useSmsFailover": use_failover}
    if use_failover:
        # 대체발송 필드명은 평평한 'fallbackContent'가 아니라 NCP 공식 스펙대로
        # failoverConfig 객체(type/from/content)로 감싸야 한다. from은 실제
        # 발신번호(SMS 발신 사전등록 번호)여야 하고, type은 SMS/LMS 중 하나다.
        fallback_text = sms_fallback_content or content
        message["failoverConfig"] = {
            "type": "SMS" if _sms_byte_len(fallback_text) <= 90 else "LMS",
            "from": _clean_phone(_sms_sender()),
            "content": fallback_text,
        }

    uri = f"/alimtalk/v2/services/{_alimtalk_service_id()}/messages"
    body = {
        "plusFriendId": _alimtalk_plus_friend_id(),
        "templateCode": _alimtalk_template_code(),
        "messages": [message],
    }
    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        resp = await client.post(_BASE_URL + uri, headers=_headers("POST", uri), json=body)
    return await _handle_response(resp, "알림톡")
