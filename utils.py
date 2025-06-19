import re
import time
import logging
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse, parse_qs
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

def setup_logging(level: str = "INFO"):
    """로깅 설정"""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def create_chrome_driver(headless: bool = True, driver_path: Optional[str] = None):
    """Chrome WebDriver 생성"""
    chrome_options = Options()
    
    if headless:
        chrome_options.add_argument("--headless=new")
    
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    chrome_options.add_argument("--window-size=1920,1080")
    
    try:
        if driver_path:
            service = Service(driver_path)
            driver = webdriver.Chrome(service=service, options=chrome_options)
        else:
            # ChromeDriverManager로 자동 설치/관리
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # WebDriver 탐지 방지
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        logging.info("Chrome WebDriver 생성 완료")
        return driver
        
    except Exception as e:
        logging.error(f"WebDriver 생성 실패: {e}")
        raise

def extract_game_id_from_url(url: str) -> Optional[str]:
    """
    URL에서 gameId를 추출합니다.
    
    Args:
        url: 박스스코어 URL
        
    Returns:
        str: 추출된 gameId 또는 None
    """
    try:
        if not url:
            return None
        
        logging.debug(f"gameId 추출 시도: {url}")
        
        # URL 파싱
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        
        # gameId 파라미터 직접 추출
        if 'gameId' in query_params:
            game_id = query_params['gameId'][0]
            logging.debug(f"URL에서 gameId 추출 성공: {game_id}")
            return game_id
        
        # 정규식으로 gameId 패턴 찾기
        game_id_match = re.search(r'gameId=([^&]+)', url)
        if game_id_match:
            game_id = game_id_match.group(1)
            logging.debug(f"정규식으로 gameId 추출 성공: {game_id}")
            return game_id
        
        logging.warning(f"gameId를 찾을 수 없음: {url}")
        return None
        
    except Exception as e:
        logging.error(f"gameId 추출 중 오류: {e}")
        return None

def generate_game_id_from_teams_date(date_str: str, away_team: str, home_team: str) -> str:
    """
    날짜와 팀명으로 gameId를 생성합니다.
    
    Args:
        date_str: 날짜 (MM.DD 형식)
        away_team: 원정팀
        home_team: 홈팀
        
    Returns:
        str: 생성된 gameId
    """
    try:
        # MM.DD -> YYYYMMDD 변환
        current_year = datetime.now().year
        month, day = date_str.split('.')
        date_formatted = f"{current_year}{month.zfill(2)}{day.zfill(2)}"
        
        # 팀명을 약어로 변환
        team_mapping = {
            '두산': 'OB', 'LG': 'LG', '키움': 'WO', 'KT': 'KT',
            'SSG': 'SK', '롯데': 'LT', '삼성': 'SS', '한화': 'HH',
            'KIA': 'HT', 'NC': 'NC'
        }
        
        away_code = team_mapping.get(away_team, away_team[:2].upper())
        home_code = team_mapping.get(home_team, home_team[:2].upper())
        
        # gameId 생성: YYYYMMDDHHA0 형식
        game_id = f"{date_formatted}{home_code}{away_code}0"
        
        logging.debug(f"gameId 생성: {date_str} {away_team}vs{home_team} -> {game_id}")
        return game_id
        
    except Exception as e:
        logging.error(f"gameId 생성 실패: {e}")
        # 실패 시 타임스탬프 기반 ID 생성
        timestamp = int(time.time()) % 10000
        return f"TEMP{timestamp}"

def safe_sleep(seconds: int):
    """안전한 대기"""
    if seconds > 0:
        time.sleep(seconds)

def to_kbo_date_format(input_date: str) -> str:
    """날짜 형식 변환: YYYY-MM-DD -> MM.DD"""
    try:
        date_obj = datetime.strptime(input_date, "%Y-%m-%d")
        return date_obj.strftime("%m.%d")
    except ValueError as e:
        logging.error(f"날짜 형식 변환 실패: {input_date} -> {e}")
        raise