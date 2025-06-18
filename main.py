import sys
import os
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

import uvicorn
from api.schedule_api import app

if __name__ == "__main__":
    # FastAPI 서버 실행
    uvicorn.run(
        "api.schedule_api:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )