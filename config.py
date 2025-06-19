import os
from dotenv import load_dotenv

# .env 파일 로드 (있다면)
load_dotenv()

class Config:
    """설정 클래스"""
    
    # Spring Boot API 설정
    SPRING_API_BASE_URL = os.getenv('SPRING_API_BASE_URL', 'http://localhost:8080')
    
    # MySQL 데이터베이스 설정
    DB_CONFIG = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'user': os.getenv('DB_USER', 'root'),
        'password': os.getenv('DB_PASSWORD', 'pw930516'),
        'database': os.getenv('DB_NAME', 'inningLog'),
        'charset': 'utf8mb4'
    }
    
    # ChromeDriver 설정
    CHROME_DRIVER_PATH = os.getenv('CHROME_DRIVER_PATH', None)  # None이면 자동 탐지
    
    # 크롤링 설정
    CRAWL_DELAY = int(os.getenv('CRAWL_DELAY', '2'))  # 페이지 간 대기 시간(초)
    TIMEOUT = int(os.getenv('TIMEOUT', '15'))         # 요소 대기 시간(초)
    
    # 로깅 설정
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')

# 사용 예시를 위한 인스턴스
config = Config()