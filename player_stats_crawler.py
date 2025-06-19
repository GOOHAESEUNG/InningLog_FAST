import time
import logging
from typing import List, Dict
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from utils import create_chrome_driver, safe_sleep
from config import config

logger = logging.getLogger(__name__)

class KboPlayerStatsCrawler:
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
            logger.info("Player Stats WebDriver 종료 완료")
    
    def get_review_stats(self, review_url: str) -> Dict:
        """
        리뷰 페이지에서 선수 기록을 파싱합니다.
        
        Args:
            review_url: KBO 리뷰 페이지 URL
            
        Returns:
            dict: {"pitchers": [...], "hitters": [...]}
        """
        logger.info(f"선수 기록 크롤링 시작: {review_url}")
        
        try:
            self.driver.get(review_url)
            
            # 페이지 로딩 대기
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.tbl")))
            safe_sleep(3)  # 동적 콘텐츠 로딩 대기
            
            # 모든 .tbl 테이블 찾기
            tables = self.driver.find_elements(By.CSS_SELECTOR, "table.tbl")
            logger.info(f"발견된 테이블 수: {len(tables)}")
            
            pitchers = []
            hitters = []
            
            for i, table in enumerate(tables):
                logger.debug(f"테이블 {i+1} 처리 중...")
                
                try:
                    # 헤더 추출
                    header_elements = table.find_elements(By.CSS_SELECTOR, "thead th")
                    headers = [elem.text.strip() for elem in header_elements]
                    logger.debug(f"테이블 {i+1} 헤더: {headers}")
                    
                    # 투수/타자 테이블 구분
                    is_pitcher_table = "이닝" in headers and "자책" in headers
                    is_hitter_table = "타수" in headers and "안타" in headers
                    
                    if not (is_pitcher_table or is_hitter_table):
                        logger.debug(f"테이블 {i+1}: 투타자 기록 테이블 아님, 스킵")
                        continue
                        
                    table_type = "투수" if is_pitcher_table else "타자"
                    logger.info(f"테이블 {i+1}: {table_type} 기록 테이블")
                    
                    # tbody의 모든 행 처리
                    tbody = table.find_element(By.TAG_NAME, "tbody")
                    rows = tbody.find_elements(By.TAG_NAME, "tr")
                    
                    current_team = ""
                    
                    for row_idx, row in enumerate(rows):
                        try:
                            # 팀명 헤더 행인지 확인 (th 태그가 있는 행)
                            team_headers = row.find_elements(By.TAG_NAME, "th")
                            if team_headers:
                                current_team = team_headers[0].text.strip()
                                logger.info(f"팀 변경: '{current_team}'")  # 더 명확한 로그
                                continue
                            
                            # 데이터 행 처리 (td 태그들)
                            cells = row.find_elements(By.TAG_NAME, "td")
                            if len(cells) < len(headers):
                                logger.warning(f"행 {row_idx}: 셀 수 부족 ({len(cells)} < {len(headers)})")
                                continue
                            
                            # 선수명 (첫 번째 열)
                            player_name = cells[0].text.strip()
                            if not player_name:
                                continue
                            
                            # 팀명 유효성 체크
                            if not current_team:
                                logger.warning(f"팀명이 설정되지 않음. 선수: {player_name}")
                                continue
                                
                            logger.debug(f"선수 처리: 팀='{current_team}', 선수='{player_name}'")
                            
                            if is_pitcher_table:
                                # 투수 기록 파싱
                                pitcher_data = self._parse_pitcher_row(cells, headers, current_team, player_name)
                                if pitcher_data:
                                    pitchers.append(pitcher_data)
                                    logger.debug(f"투수 기록 추가: {pitcher_data}")
                                
                            elif is_hitter_table:
                                # 타자 기록 파싱
                                hitter_data = self._parse_hitter_row(cells, headers, current_team, player_name)
                                if hitter_data:
                                    hitters.append(hitter_data)
                                    logger.debug(f"타자 기록 추가: {hitter_data}")
                        
                        except Exception as row_error:
                            logger.error(f"행 {row_idx} 처리 중 오류: {row_error}")
                            continue
                    
                except Exception as table_error:
                    logger.error(f"테이블 {i+1} 처리 중 오류: {table_error}")
                    continue
            
            result = {
                "pitchers": pitchers,
                "hitters": hitters
            }
            
            logger.info(f"크롤링 완료: 투수 {len(pitchers)}명, 타자 {len(hitters)}명")
            return result
            
        except Exception as e:
            logger.error(f"크롤링 중 오류 발생: {e}")
            return {"pitchers": [], "hitters": []}
    
    def _parse_pitcher_row(self, cells: List, headers: List[str], team: str, player_name: str) -> Dict:
        """투수 기록 행 파싱"""
        try:
            innings_idx = headers.index("이닝")
            earned_idx = headers.index("자책")
            
            innings_text = cells[innings_idx].text.strip()
            earned_text = cells[earned_idx].text.strip()
            
            # 숫자 변환
            try:
                earned_runs = int(earned_text) if earned_text.isdigit() else 0
            except ValueError:
                earned_runs = 0
            
            return {
                "team": team,
                "playerName": player_name,
                "innings": innings_text,
                "earnedRuns": earned_runs
            }
            
        except Exception as e:
            logger.error(f"투수 기록 파싱 실패: {player_name} - {e}")
            return None
    
    def _parse_hitter_row(self, cells: List, headers: List[str], team: str, player_name: str) -> Dict:
        """타자 기록 행 파싱"""
        try:
            at_bats_idx = headers.index("타수")
            hits_idx = headers.index("안타")
            
            at_bats_text = cells[at_bats_idx].text.strip()
            hits_text = cells[hits_idx].text.strip()
            
            # 숫자 변환
            try:
                at_bats = int(at_bats_text) if at_bats_text.isdigit() else 0
                hits = int(hits_text) if hits_text.isdigit() else 0
            except ValueError:
                at_bats = hits = 0
            
            return {
                "team": team,
                "playerName": player_name,
                "atBats": at_bats,
                "hits": hits
            }
            
        except Exception as e:
            logger.error(f"타자 기록 파싱 실패: {player_name} - {e}")
            return None