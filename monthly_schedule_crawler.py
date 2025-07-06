import re
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from utils import create_chrome_driver, generate_game_id_from_teams_date, to_kbo_date_format, safe_sleep
from config import config

logger = logging.getLogger(__name__)

class KboMonthlyScheduleCrawler:
    """월별 경기 일정 전용 크롤러 (점수, 박스스코어 URL 제외)"""
    
    def __init__(self):
        """크롤러 초기화"""
        self.driver = create_chrome_driver(headless=True, driver_path=config.CHROME_DRIVER_PATH)
        self.wait = WebDriverWait(self.driver, config.TIMEOUT)
        
    def __del__(self):
        """소멸자 - WebDriver 종료"""
        self.close()
    
    def close(self):
        """WebDriver 종료"""
        if hasattr(self, 'driver') and self.driver:
            self.driver.quit()
            logger.info("Monthly Schedule WebDriver 종료 완료")
    
    def get_monthly_schedule(self, year: int, month: int) -> List[Dict]:
        """
        특정 월의 전체 경기 일정 수집 (일정만, 결과 제외)
        
        Args:
            year: 연도 (2025)
            month: 월 (6)
            
        Returns:
            List[Dict]: 경기 일정 목록 (점수=0, boxscoreUrl=null)
        """
        all_games = []
        
        try:
            # 해당 월의 모든 날짜에 대해 크롤링
            start_date = datetime(year, month, 1)
            
            # 다음 달 1일에서 1일 빼서 해당 월의 마지막 날 구하기
            if month == 12:
                end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
            else:
                end_date = datetime(year, month + 1, 1) - timedelta(days=1)
            
            current_date = start_date
            
            logger.info(f"{year}년 {month}월 전체 일정 크롤링 시작 ({start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')})")
            
            while current_date <= end_date:
                date_str = current_date.strftime("%Y-%m-%d")
                daily_games = self._get_daily_schedule(date_str)
                all_games.extend(daily_games)
                
                # 너무 빠른 요청 방지
                safe_sleep(1)
                current_date += timedelta(days=1)
            
            logger.info(f"{year}년 {month}월 일정 크롤링 완료: 총 {len(all_games)}경기")
            
        except Exception as e:
            logger.error(f"월별 일정 크롤링 실패: {e}")
        
        return all_games
    
    def _get_daily_schedule(self, date_string: str) -> List[Dict]:
        """특정 날짜의 경기 일정만 수집 (점수 무시)"""
        games = []
        
        try:
            target_kbo_date = to_kbo_date_format(date_string)
            url = f"https://www.koreabaseball.com/Schedule/Schedule.aspx?date={date_string}"
            
            logger.debug(f"일정 크롤링: {date_string}")
            
            self.driver.get(url)
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.tbl")))
            safe_sleep(1)
            
            rows = self.driver.find_elements(By.CSS_SELECTOR, "table.tbl tbody tr")
            current_date = ""
            is_target_date = False
            
            for row_index, row in enumerate(rows):
                try:
                    tds = row.find_elements(By.TAG_NAME, "td")
                    if len(tds) < 6:
                        continue
                    
                    first_td = tds[0].text.strip()
                    
                    # 날짜가 포함된 행 (첫 번째 경기)
                    if re.match(r"\d{2}\.\d{2}.*", first_td):
                        current_date = first_td[:5]  # MM.DD 추출
                        is_target_date = current_date == target_kbo_date
                        
                        if not is_target_date:
                            continue
                        
                        # 첫 번째 경기 일정 파싱
                        game = self._parse_first_schedule_row(tds, current_date)
                        if game:
                            games.append(game)
                    
                    # 시간만 있는 행 (두 번째 이후 경기)
                    elif re.match(r"\d{2}:\d{2}", first_td) and is_target_date:
                        game = self._parse_subsequent_schedule_row(tds, current_date)
                        if game:
                            games.append(game)
                
                except Exception as e:
                    logger.debug(f"Row {row_index} 파싱 실패: {e}")
                    continue
        
        except Exception as e:
            logger.debug(f"날짜 {date_string} 크롤링 실패: {e}")
        
        return games
    
    def _parse_first_schedule_row(self, tds: List, current_date: str) -> Optional[Dict]:
        """첫 번째 경기 행에서 일정만 파싱 (점수 무시)"""
        try:
            if len(tds) < 8:
                return None
            
            time = tds[1].text.strip()
            match_info = tds[2].text.strip()
            stadium = tds[7].text.strip()
            
            return self._parse_schedule_info(current_date, time, match_info, stadium)
        
        except Exception as e:
            logger.debug(f"첫 번째 일정 행 파싱 실패: {e}")
            return None
    
    def _parse_subsequent_schedule_row(self, tds: List, current_date: str) -> Optional[Dict]:
        """두 번째 이후 경기 행에서 일정만 파싱 (점수 무시)"""
        try:
            if len(tds) < 7:
                return None
            
            time = tds[0].text.strip()
            match_info = tds[1].text.strip()
            stadium = tds[6].text.strip()
            
            return self._parse_schedule_info(current_date, time, match_info, stadium)
        
        except Exception as e:
            logger.debug(f"후속 일정 행 파싱 실패: {e}")
            return None
    
    def _parse_schedule_info(self, date: str, time: str, match_info: str, stadium: str) -> Optional[Dict]:
        """경기 일정 정보 파싱 (점수 제외, 팀명만)"""
        try:
            if "vs" not in match_info:
                return None
            
            # "팀A vs 팀B" 또는 "팀A점수 vs 점수팀B" 형태에서 팀명만 추출
            parts = match_info.split("vs")
            if len(parts) != 2:
                return None
            
            left_part = parts[0].strip()
            right_part = parts[1].strip()
            
            # 왼쪽에서 팀명만 추출 (숫자 제거)
            away_team = re.sub(r'\d+', '', left_part).strip()
            
            # 오른쪽에서 팀명만 추출 (숫자 제거)
            home_team = re.sub(r'\d+', '', right_part).strip()
            
            # 빈 팀명 체크
            if not away_team or not home_team:
                return None
            
            # gameId 생성 (일정용)
            game_id = generate_game_id_from_teams_date(date, away_team, home_team)
            
            result = {
                "awayTeam": away_team,
                "homeTeam": home_team,
                "awayScore": 0,           # 일정 단계에서는 0
                "homeScore": 0,           # 일정 단계에서는 0
                "stadium": stadium,
                "gameDateTime": time,
                "boxscoreUrl": None,      # 일정 단계에서는 null
                "gameId": game_id,
                "status": "SCHEDULED"     # 상태: 예정
            }
            
            logger.debug(f"일정 파싱 완료: {away_team} vs {home_team}, gameId: {game_id}")
            return result
        
        except Exception as e:
            logger.debug(f"일정 정보 파싱 실패: {match_info} -> {e}")
            return None