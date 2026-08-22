"""프로젝트 공용 설정.

integrations/molit.py, integrations/building_ledger.py가 여기서 서비스키와
타임아웃을 가져간다. main.py가 `import config`보다 먼저 key.env를 로드하고
DATA_SERVICE_KEY를 BUILDING_HUB_SERVICE_KEY/MOLIT_SERVICE_KEY로 매핑해두므로,
여기서는 os.environ에서 읽기만 하면 된다.

(geocode.py, land_ledger.py는 이 파일을 거치지 않고 KAKAO_KEY/VWORLD_KEY를
os.environ에서 직접 읽는다 - main.py 상단 docstring 참고.)
"""

import os

BUILDING_HUB_SERVICE_KEY = os.environ.get("BUILDING_HUB_SERVICE_KEY", "")
MOLIT_SERVICE_KEY = os.environ.get("MOLIT_SERVICE_KEY", "")
HTTP_TIMEOUT_SECONDS = float(os.environ.get("HTTP_TIMEOUT_SECONDS", "15"))
