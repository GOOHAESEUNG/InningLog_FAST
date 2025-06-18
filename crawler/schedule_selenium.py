# crawler/schedule_selenium.py
import re
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import logging

logger = logging.getLogger(__name__)

class KboScheduleCrawler:
    def __init__(self):
        """크롤러 초기화"""
        self.driver = None
        self._init_driver()
    
    def _init_driver(self):
        """Chrome WebDriver 초기화"""
        try:
            chrome_options = Options()
            chrome_options.add_argument("--headless=new")  # 헤드리스 모드
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            
            # ChromeDriver 경로 설정 (필요시 수정)
            # service = Service('/usr/local/bin/chromedriver')  # Mac/Linux
            # self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.info("Chrome WebDriver 초기화 완료")
            
        except Exception as e:
            logger.error(f"WebDriver 초기화 실패: {e}")
            raise
    
    def close(self):
        """WebDriver 종료"""
        if self.driver:
            self.driver.quit()
            logger.info("WebDriver 종료 완료")
    
    def to_kbo_date_format(self, input_date: str) -> str:
        """날짜 형식 변환: YYYY-MM-DD -> MM.DD"""
        try:
            date_obj = datetime.strptime(input_date, "%Y-%m-%d")
            return date_obj.strftime("%m.%d")
        except ValueError as e:
            logger.error(f"날짜 형식 변환 실패: {input_date} -> {e}")
            raise
    
    def get_games_by_date(self, date_string: Optional[str] = None) -> List[Dict]:
        """특정 날짜의 KBO 경기 일정 조회"""
        games = []
        
        try:
            # 날짜 파라미터 처리
            if not date_string or date_string.strip() == "":
                date_string = datetime.now().strftime("%Y-%m-%d")
            
            target_kbo_date = self.to_kbo_date_format(date_string)
            url = f"https://www.koreabaseball.com/Schedule/Schedule.aspx?date={date_string}"
            
            logger.info(f"크롤링 시작: {url}")
            
            self.driver.get(url)
            
            # 페이지 로딩 대기
            wait = WebDriverWait(self.driver, 15)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.tbl")))
            
            # 테이블 행 조회
            rows = self.driver.find_elements(By.CSS_SELECTOR, "table.tbl tbody tr")
            current_date = ""
            is_target_date = False
            
            logger.info(f"총 {len(rows)}개 행 발견")
            
            for row_index, row in enumerate(rows):
                try:
                    tds = row.find_elements(By.TAG_NAME, "td")
                    if len(tds) < 6:
                        continue
                    
                    first_td = tds[0].text.strip()
                    logger.debug(f"Row {row_index}: First TD = '{first_td}', Total TDs = {len(tds)}")
                    
                    # 날짜가 포함된 행 (첫 번째 경기)
                    if re.match(r"\d{2}\.\d{2}.*", first_td):
                        current_date = first_td[:5]  # MM.DD 추출
                        is_target_date = current_date == target_kbo_date
                        
                        if not is_target_date:
                            continue
                        
                        # 첫 번째 경기 정보 파싱
                        game = self._parse_first_game_row(tds, current_date)
                        if game:
                            games.append(game)
                            logger.info(f"첫 번째 경기 파싱 성공: {game}")
                    
                    # 시간만 있는 행 (두 번째 이후 경기)
                    elif re.match(r"\d{2}:\d{2}", first_td) and is_target_date:
                        game = self._parse_subsequent_game_row(tds, current_date)
                        if game:
                            games.append(game)
                            logger.info(f"후속 경기 파싱 성공: {game}")
                
                except Exception as e:
                    logger.error(f"Row {row_index} 파싱 실패: {e}")
                    self._debug_row_contents(tds, row_index)
        
        except Exception as e:
            logger.error(f"전체 크롤링 실패: {e}")
        
        logger.info(f"총 {len(games)}개 경기 파싱 완료")
        return games
    
    def _parse_first_game_row(self, tds: List, current_date: str) -> Optional[Dict]:
        """첫 번째 경기 행 파싱 (날짜 포함)"""
        try:
            if len(tds) < 8:
                logger.warning(f"첫 번째 경기 행의 TD 개수 부족: {len(tds)}")
                return None
            
            time = tds[1].text.strip()
            match_info = tds[2].text.strip()
            stadium = tds[7].text.strip()
            
            # 리뷰 URL 찾기
            review_url = self._find_review_url_in_row(tds)
            
            return self._parse_game_info(current_date, time, match_info, stadium, review_url)
        
        except Exception as e:
            logger.error(f"첫 번째 경기 행 파싱 실패: {e}")
            return None
    
    def _parse_subsequent_game_row(self, tds: List, current_date: str) -> Optional[Dict]:
        """두 번째 이후 경기 행 파싱 (날짜 없음)"""
        try:
            if len(tds) < 7:
                logger.warning(f"후속 경기 행의 TD 개수 부족: {len(tds)}")
                return None
            
            time = tds[0].text.strip()
            match_info = tds[1].text.strip()
            stadium = tds[6].text.strip()
            
            # 리뷰 URL 찾기
            review_url = self._find_review_url_in_row(tds)
            
            return self._parse_game_info(current_date, time, match_info, stadium, review_url)
        
        except Exception as e:
            logger.error(f"후속 경기 행 파싱 실패: {e}")
            return None
    
    def _find_review_url_in_row(self, tds: List) -> Optional[str]:
        """행 전체에서 리뷰 URL 찾기"""
        for i, td in enumerate(tds):
            try:
                links = td.find_elements(By.TAG_NAME, "a")
                
                for link in links:
                    href = link.get_attribute("href")
                    link_text = link.text.strip()
                    
                    # 게임센터, 리뷰, 하이라이트 링크 찾기
                    if href and (
                        "gameId=" in href or
                        "section=HIGHLIGHT" in href or
                        "section=REVIEW" in href or
                        "게임센터" in link_text or
                        "리뷰" in link_text
                    ):
                        # HIGHLIGHT → REVIEW 로 강제 치환
                        if "section=HIGHLIGHT" in href:
                            href = href.replace("section=HIGHLIGHT", "section=REVIEW")
                        
                        final_url = href if href.startswith("http") else f"https://www.koreabaseball.com{href}"
                        logger.debug(f"리뷰 URL 발견 (TD {i}): {final_url}")
                        return final_url
            
            except Exception:
                continue
        
        logger.warning("리뷰 URL을 찾을 수 없음")
        return None
    
    def _debug_row_contents(self, tds: List, row_index: int):
        """디버깅용: 행의 모든 TD 내용 출력"""
        logger.debug(f"=== Row {row_index} Debug Info ===")
        for i, td in enumerate(tds):
            try:
                text = td.text.strip()
                links = td.find_elements(By.TAG_NAME, "a")
                link_info = "No links" if not links else f"{len(links)} links"
                logger.debug(f"  TD[{i}]: '{text}' ({link_info})")
            except Exception:
                logger.debug(f"  TD[{i}]: Error reading content")
        logger.debug("========================")
    
    def _parse_game_info(self, date: str, time: str, match_info: str, stadium: str, review_url: Optional[str]) -> Optional[Dict]:
        """경기 정보 파싱"""
        try:
            if "vs" not in match_info:
                return None
            
            parts = match_info.split("vs")
            if len(parts) != 2:
                return None
            
            left_part = parts[0].strip()
            right_part = parts[1].strip()
            
            # 원정팀과 점수 분리
            away_team = ""
            away_score = ""
            for i in range(len(left_part) - 1, -1, -1):
                if left_part[i].isdigit():
                    away_score = left_part[i] + away_score
                else:
                    away_team = left_part[:i + 1]
                    away_score = left_part[i + 1:]
                    break
            
            # 홈팀과 점수 분리
            home_score = ""
            home_team = ""
            for i, char in enumerate(right_part):
                if char.isdigit():
                    home_score += char
                else:
                    home_team = right_part[i:]
                    break
            
            away_score_int = int(away_score)
            home_score_int = int(home_score)
            
            return {
                "awayTeam": away_team.strip(),
                "homeTeam": home_team.strip(),
                "awayScore": away_score_int,
                "homeScore": home_score_int,
                "stadium": stadium,
                "gameDateTime": time,
                "boxscore_url": review_url
            }
        
        except Exception as e:
            logger.error(f"경기 정보 파싱 실패: {match_info} -> {e}")
            return None
    
    def get_games_by_date_range(self, start_date: str, end_date: str) -> List[Dict]:
        """날짜 범위의 KBO 경기 일정 조회"""
        all_games = []
        
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
            
            current = start
            while current <= end:
                logger.info(f"크롤링 중: {current.strftime('%Y-%m-%d')}")
                daily_games = self.get_games_by_date(current.strftime("%Y-%m-%d"))
                all_games.extend(daily_games)
                current += timedelta(days=1)
                
                # 서버 부하 방지를 위한 대기
                time.sleep(1)
        
        except Exception as e:
            logger.error(f"날짜 범위 조회 중 오류 발생: {start_date} ~ {end_date}, {e}")
        
        logger.info(f"전체 크롤링 완료: {len(all_games)}개 경기")
        return all_games

