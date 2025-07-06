import re
import logging
from datetime import datetime
from typing import List, Dict, Optional
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from utils import create_chrome_driver, extract_game_id_from_url, generate_game_id_from_teams_date, to_kbo_date_format, safe_sleep
from config import config

logger = logging.getLogger(__name__)

class KboGameResultCrawler:
    """경기 결과 업데이트 전용 크롤러 (점수 + 박스스코어 URL)"""
    
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
            logger.info("Game Result WebDriver 종료 완료")
    
    def update_game_results(self, date_string: str) -> List[Dict]:
        """
        특정 날짜의 경기 결과 업데이트 (점수 + 박스스코어 URL)
        
        Args:
            date_string: 경기 날짜 (YYYY-MM-DD)
            
        Returns:
            List[Dict]: 경기 결과 목록 (점수, boxscoreUrl 포함)
        """
        games = []
        
        try:
            target_kbo_date = to_kbo_date_format(date_string)
            url = f"https://www.koreabaseball.com/Schedule/Schedule.aspx?date={date_string}"
            
            logger.info(f"경기 결과 업데이트 크롤링: {date_string}")
            
            self.driver.get(url)
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.tbl")))
            safe_sleep(2)
            
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
                    
                    # 날짜가 포함된 행 (첫 번째 경기)
                    if re.match(r"\d{2}\.\d{2}.*", first_td):
                        current_date = first_td[:5]  # MM.DD 추출
                        is_target_date = current_date == target_kbo_date
                        
                        if not is_target_date:
                            continue
                        
                        # 첫 번째 경기 결과 파싱
                        game = self._parse_first_result_row(tds, current_date)
                        if game:
                            games.append(game)
                            logger.info(f"첫 번째 경기 결과 파싱: {game['awayTeam']} {game['awayScore']}-{game['homeScore']} {game['homeTeam']}")
                    
                    # 시간만 있는 행 (두 번째 이후 경기)
                    elif re.match(r"\d{2}:\d{2}", first_td) and is_target_date:
                        game = self._parse_subsequent_result_row(tds, current_date)
                        if game:
                            games.append(game)
                            logger.info(f"후속 경기 결과 파싱: {game['awayTeam']} {game['awayScore']}-{game['homeScore']} {game['homeTeam']}")
                
                except Exception as e:
                    logger.error(f"Row {row_index} 결과 파싱 실패: {e}")
                    continue
        
        except Exception as e:
            logger.error(f"경기 결과 크롤링 실패: {e}")
        
        logger.info(f"경기 결과 업데이트 완료: 총 {len(games)}경기")
        return games
    
    def _parse_first_result_row(self, tds: List, current_date: str) -> Optional[Dict]:
        """첫 번째 경기 행에서 결과 파싱 (점수 + URL 포함)"""
        try:
            if len(tds) < 8:
                logger.warning(f"첫 번째 경기 행의 TD 개수 부족: {len(tds)}")
                return None
            
            time = tds[1].text.strip()
            match_info = tds[2].text.strip()
            stadium = tds[7].text.strip()
            
            # 리뷰 URL 찾기 (중요!)
            review_url = self._find_review_url_in_row(tds)
            
            return self._parse_result_info(current_date, time, match_info, stadium, review_url)
        
        except Exception as e:
            logger.error(f"첫 번째 경기 결과 파싱 실패: {e}")
            return None
    
    def _parse_subsequent_result_row(self, tds: List, current_date: str) -> Optional[Dict]:
        """두 번째 이후 경기 행에서 결과 파싱 (점수 + URL 포함)"""
        try:
            if len(tds) < 7:
                logger.warning(f"후속 경기 행의 TD 개수 부족: {len(tds)}")
                return None
            
            time = tds[0].text.strip()
            match_info = tds[1].text.strip()
            stadium = tds[6].text.strip()
            
            # 리뷰 URL 찾기 (중요!)
            review_url = self._find_review_url_in_row(tds)
            
            return self._parse_result_info(current_date, time, match_info, stadium, review_url)
        
        except Exception as e:
            logger.error(f"후속 경기 결과 파싱 실패: {e}")
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
        
        logger.warning("리뷰 URL을 찾을 수 없음 - 경기가 아직 진행되지 않았을 수 있음")
        return None
    
    def _parse_result_info(self, date: str, time: str, match_info: str, stadium: str, review_url: Optional[str]) -> Optional[Dict]:
        """경기 결과 정보 파싱 (점수 포함)"""
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
            
            # 점수가 없으면 아직 경기가 시작되지 않은 것
            if not away_score or not home_score:
                logger.debug(f"점수 없음 - 아직 진행되지 않은 경기: {match_info}")
                return None
            
            away_score_int = int(away_score) if away_score else 0
            home_score_int = int(home_score) if home_score else 0
            
            # gameId 추출 또는 생성
            game_id = None
            if review_url:
                game_id = extract_game_id_from_url(review_url)
            
            if not game_id:
                game_id = generate_game_id_from_teams_date(date, away_team.strip(), home_team.strip())
            
            result = {
                "awayTeam": away_team.strip(),
                "homeTeam": home_team.strip(), 
                "awayScore": away_score_int,
                "homeScore": home_score_int,
                "stadium": stadium,
                "gameDateTime": time,
                "boxscoreUrl": review_url,
                "gameId": game_id,
                "status": "COMPLETED"  # 상태: 완료
            }
            
            logger.debug(f"경기 결과 파싱 완료: {away_team} {away_score_int}-{home_score_int} {home_team}, gameId: {game_id}")
            return result
        
        except Exception as e:
            logger.error(f"경기 결과 파싱 실패: {match_info} -> {e}")
            return None