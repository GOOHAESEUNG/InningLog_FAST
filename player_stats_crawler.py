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
    
    def _parse_innings(self, text: str) -> float:
        """이닝 텍스트를 float로 변환"""
        t = text.strip()
        if "/" in t:
            n, d = t.split("/", 1)
            try:
                return float(n) / float(d)
            except:
                return 0.0
        try:
            return float(t)
        except:
            return 0.0
    
    def _convert_team_code_to_name(self, team_code: str) -> str:
        """KBO 팀 코드를 실제 팀명으로 변환"""
        team_code_map = {
            # KBO 공식 팀 코드 매핑
            "LT": "롯데",
            "HT": "한화", 
            "SS": "SSG",
            "NC": "NC",
            "KT": "KT",
            "WS": "키움",  # WS (구 우리)
            "WO": "키움",  # WO (키움 히어로즈 또는 우리)
            "LG": "LG",
            "OB": "두산",  # OB (구 OB베어스)
            "DS": "두산",  # DS (두산)
            "HH": "KIA",   # HH (구 해태)
            "KI": "KIA",   # KI (KIA)
            "SA": "삼성",  # SA (삼성)
            "SK": "SSG",   # SK (구 SK)
            # 추가 가능한 코드들
            "LO": "롯데",
            "HW": "한화"
        }
        
        converted_name = team_code_map.get(team_code, team_code)
        if converted_name == team_code:
            logger.warning(f"알 수 없는 팀 코드: {team_code}")
        logger.debug(f"팀 코드 변환: {team_code} → {converted_name}")
        return converted_name
    
    def get_review_stats(self, review_url: str, away_team: str = None, home_team: str = None) -> Dict:
        """
        리뷰 페이지에서 선수 기록을 파싱합니다.
        
        Args:
            review_url: KBO 리뷰 페이지 URL
            away_team: 원정팀 코드 (없으면 자동 파싱)
            home_team: 홈팀 코드 (없으면 자동 파싱)
            
        Returns:
            dict: {"pitchers": [...], "hitters": [...]}
        """
        logger.info(f"선수 기록 크롤링 시작: {review_url}")
        
        try:
            self.driver.get(review_url)
            
            # 페이지 로딩 대기
            self.wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table.tbl")))
            safe_sleep(3)  # 동적 콘텐츠 로딩 대기
            
            # 모든 .tbl 테이블 찾기
            tables = self.driver.find_elements(By.CSS_SELECTOR, "table.tbl")
            logger.info(f"발견된 테이블 수: {len(tables)}")
            
            # 테이블 유형별 인덱스 찾기
            lineup_idxs = []    # 라인업 테이블 (선수명 헤더)
            hitter_idxs = []    # 타자 기록 테이블 (타수, 안타 헤더)
            pitcher_idxs = []   # 투수 기록 테이블 (이닝, 자책 헤더)
            
            for idx, tbl in enumerate(tables):
                try:
                    hdrs = [th.text.strip() for th in tbl.find_elements(By.CSS_SELECTOR, "thead th")]
                    logger.debug(f"테이블 {idx} 헤더: {hdrs}")
                    
                    if "선수명" in hdrs:
                        lineup_idxs.append(idx)
                        logger.info(f"라인업 테이블 발견: {idx}")
                    if "타수" in hdrs and "안타" in hdrs:
                        hitter_idxs.append(idx)
                        logger.info(f"타자 기록 테이블 발견: {idx}")
                    if "이닝" in hdrs and "자책" in hdrs:
                        pitcher_idxs.append(idx)
                        logger.info(f"투수 기록 테이블 발견: {idx}")
                except Exception as e:
                    logger.warning(f"테이블 {idx} 헤더 파싱 오류: {e}")
                    continue
            
            # 팀 코드 설정 (URL에서 추출하거나 매개변수 사용)
            if away_team and home_team:
                team_codes = [away_team, home_team]
            else:
                # URL에서 game_id 추출하여 팀 코드 가져오기 (예: 20250601KTWS01)
                try:
                    game_id = review_url.split('gameId=')[1][:12]
                    team_codes = [game_id[8:10], game_id[10:12]]
                    logger.info(f"URL에서 팀 코드 추출: {team_codes}")
                except:
                    team_codes = ["AW", "HM"]  # 기본값
                    logger.warning("팀 코드 추출 실패, 기본값 사용")
            
            # 타자 기록 파싱
            hitters = []
            if len(lineup_idxs) >= 2 and len(hitter_idxs) >= 2:
                for team, li_idx, st_idx in zip(team_codes, lineup_idxs, hitter_idxs):
                    try:
                        # 라인업 테이블에서 선수명 컬럼 위치 찾기
                        name_hdrs = tables[li_idx].find_elements(By.CSS_SELECTOR, "thead th")
                        name_col = next(i for i, h in enumerate(name_hdrs) if h.text.strip() == "선수명")
                        
                        # 타자 기록 테이블에서 타수, 안타 컬럼 위치 찾기
                        stat_hdrs = [th.text.strip() for th in tables[st_idx].find_elements(By.CSS_SELECTOR, "thead th")]
                        atb_col = stat_hdrs.index("타수")
                        hit_col = stat_hdrs.index("안타")
                        
                        # 각 테이블의 행 데이터 매칭
                        rows_names = tables[li_idx].find_elements(By.CSS_SELECTOR, "tbody tr")
                        rows_stats = tables[st_idx].find_elements(By.CSS_SELECTOR, "tbody tr")
                        
                        for rn, rs in zip(rows_names, rows_stats):
                            try:
                                # 선수명 추출
                                cells_name = rn.find_elements(By.CSS_SELECTOR, "th, td")
                                if len(cells_name) <= name_col:
                                    continue
                                player = cells_name[name_col].text.strip()
                                if not player:
                                    continue
                                
                                # 타자 기록 추출
                                stats = rs.find_elements(By.TAG_NAME, "td")
                                if len(stats) <= max(atb_col, hit_col):
                                    continue
                                
                                atb_text = stats[atb_col].text.strip()
                                hit_text = stats[hit_col].text.strip()
                                
                                atb = int(atb_text) if atb_text.isdigit() else 0
                                hits = int(hit_text) if hit_text.isdigit() else 0
                                
                                hitter_data = {
                                    "team": self._convert_team_code_to_name(team),
                                    "playerName": player,
                                    "atBats": atb,
                                    "hits": hits
                                }
                                hitters.append(hitter_data)
                                logger.debug(f"타자 기록 추가: {hitter_data}")
                                
                            except Exception as e:
                                logger.error(f"타자 행 파싱 오류: {e}")
                                continue
                                
                    except Exception as e:
                        logger.error(f"팀 {team} 타자 기록 파싱 오류: {e}")
                        continue
            else:
                logger.warning("라인업 또는 타자 기록 테이블이 충분하지 않음")
            
            # 투수 기록 파싱
            pitchers = []
            if len(pitcher_idxs) >= 2:
                for team, st_idx in zip(team_codes, pitcher_idxs):
                    try:
                        rows = tables[st_idx].find_elements(By.CSS_SELECTOR, "tbody tr")
                        logger.info(f"팀 {team} 투수 테이블 행 수: {len(rows)}")
                        
                        for row in rows:
                            try:
                                cols = row.find_elements(By.TAG_NAME, "td")
                                if len(cols) < 16:  # 최소 16개 컬럼 필요
                                    continue
                                
                                player_name = cols[0].text.strip()
                                if not player_name:
                                    continue
                                
                                innings_text = cols[6].text.strip()  # 7번째 컬럼: 이닝
                                earned_text = cols[15].text.strip()  # 16번째 컬럼: 자책
                                
                                innings_float = self._parse_innings(innings_text)
                                earned_runs = int(earned_text) if earned_text.isdigit() else 0
                                
                                pitcher_data = {
                                    "team": self._convert_team_code_to_name(team),
                                    "playerName": player_name,
                                    "innings": str(innings_float),
                                    "earnedRuns": earned_runs
                                }
                                pitchers.append(pitcher_data)
                                logger.debug(f"투수 기록 추가: {pitcher_data}")
                                
                            except Exception as e:
                                logger.error(f"투수 행 파싱 오류: {e}")
                                continue
                                
                    except Exception as e:
                        logger.error(f"팀 {team} 투수 기록 파싱 오류: {e}")
                        continue
            else:
                logger.warning("투수 기록 테이블이 충분하지 않음")
            
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
        """투수 기록 행 파싱 (기존 방식 - 백업용)"""
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
        """타자 기록 행 파싱 (기존 방식 - 백업용)"""
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