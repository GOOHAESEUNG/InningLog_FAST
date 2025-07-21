# config.py
import os
from dotenv import load_dotenv
from pathlib import Path

# .env 파일 불러오기

env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")

class config:
    SPRING_API_BASE_URL = os.getenv("SPRING_API_BASE_URL")
    DB_HOST = os.getenv("DB_HOST")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_NAME = os.getenv("DB_NAME")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
    # 필요한 값 추가
    
    # ChromeDriver 설정
    CHROME_DRIVER_PATH = os.getenv('CHROME_DRIVER_PATH', None)  # None이면 자동 탐지
    
    # 크롤링 설정
    CRAWL_DELAY = int(os.getenv('CRAWL_DELAY', '2'))  # 페이지 간 대기 시간(초)
    TIMEOUT = int(os.getenv('TIMEOUT', '15'))         # 요소 대기 시간(초)
    
    # 로깅 설정
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# 사용 예시를 위한 인스턴스
config = config()