"""매물 검증 데스크용 원격 키 서버 (Render 배포용).

이 파일은 launcher.py/main.py가 있는 프로젝트와는 완전히 별개의, 아주 작은
FastAPI 앱이다. 하는 일은 딱 하나 - "올바른 토큰으로 요청하면 API 키 JSON을
돌려준다"뿐이다.

★ 중요: 이 파일 자체에는 실제 키 값을 절대 적지 않는다. 실제 키는 Render
대시보드의 Environment 탭에 등록해두고, 이 코드는 os.environ에서 읽기만
한다 - 그래야 이 코드를 깃허브에 공개 저장소로 올려도 키가 새어나가지 않는다.

배포 방법 (README.md 참고):
  1. 이 폴더(key-server/)를 새 GitHub 저장소로 만들어 올린다.
  2. render.com → New + → Web Service → 그 저장소 연결.
  3. Build Command: pip install -r requirements.txt
     Start Command: uvicorn key_server:app --host 0.0.0.0 --port $PORT
  4. Render의 Environment 탭에서 아래 환경변수를 전부 등록한다:
       ANTHROPIC_API_KEY, KAKAO_KEY, DATA_SERVICE_KEY, VWORLD_KEY,
       VWORLD_DOMAIN, ACCESS_TOKEN(임의의 긴 랜덤 문자열 - 인증용)
  5. 배포되면 생기는 https://<서비스이름>.onrender.com/keys 를
     launcher.py 고급 설정 탭의 "원격 키 서버 URL"에, ACCESS_TOKEN을
     "액세스 토큰"에 넣으면 된다.

로컬에서 미리 테스트해보려면:
  ACCESS_TOKEN=test123 ANTHROPIC_API_KEY=sk-xxx uvicorn key_server:app --reload
  curl -H "Authorization: Bearer test123" http://127.0.0.1:8000/keys
"""

import os

from fastapi import FastAPI, Header, HTTPException

app = FastAPI(title="매물 검증 데스크 - 원격 키 서버")

# launcher.py의 SETTING_KEYS와 정확히 같은 이름을 써야 한다 - 이름이 다르면
# launcher.py가 응답을 받고도 값을 못 채워넣는다.
KEY_NAMES = [
    "ANTHROPIC_API_KEY",
    "KAKAO_KEY",
    "DATA_SERVICE_KEY",
    "VWORLD_KEY",
    "VWORLD_DOMAIN",
]


@app.get("/")
async def health():
    """Render 헬스체크 및 수동 확인용. 키는 절대 반환하지 않는다."""
    return {"ok": True, "service": "매물 검증 데스크 키 서버"}


@app.get("/keys")
async def get_keys(authorization: str = Header(default="")):
    """Authorization: Bearer <ACCESS_TOKEN> 헤더가 정확히 일치할 때만 키를 반환한다.

    ACCESS_TOKEN 자체가 Render 환경변수로 설정돼 있지 않으면(설정을 깜빡한
    경우) 안전한 쪽으로 기본값을 막아버린다 - 즉 토큰 없이는 절대 통과 못 하게
    한다(빈 문자열끼리 비교돼 우회되는 걸 방지)."""
    expected = os.environ.get("ACCESS_TOKEN", "")
    provided = authorization[7:] if authorization.startswith("Bearer ") else ""
    if not expected or provided != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")

    return {name: os.environ.get(name, "") for name in KEY_NAMES}
