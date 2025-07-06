import requests
import logging
from datetime import datetime
from typing import List, Dict, Optional
from bs4 import BeautifulSoup

from config import config

logger = logging.getLogger(__name__)

class KboTeamRankCrawler:
    """KBO 팀 순위 및 승률 크롤러 (정적 페이지용)"""
    
    def __init__(self):
        """크롤러 초기화"""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        
    def close(self):
        """세션 종료"""
        if hasattr(self, 'session'):
            self.session.close()
            logger.info("Team Rank Session 종료 완료")
    
    def get_team_rankings(self, target_date: Optional[str] = None) -> List[Dict]:
        """
        팀 순위 및 승률 정보 크롤링
        
        Args:
            target_date: 조회 날짜 (YYYY-MM-DD), None이면 현재 날짜
            
        Returns:
            List[Dict]: 팀 순위 정보 목록
        """
        team_rankings = []
        
        try:
            # 기본 URL
            base_url = "https://www.koreabaseball.com/Record/TeamRank/TeamRankDaily.aspx"
            
            # 날짜 파라미터가 있으면 추가
            if target_date:
                # YYYY-MM-DD 형식을 YYYY.MM.DD 형식으로 변환
                formatted_date = target_date.replace('-', '.')
                url = f"{base_url}?date={formatted_date}"
            else:
                url = base_url
            
            logger.info(f"팀 순위 크롤링 시작: {url}")
            
            # 페이지 요청
            response = self.session.get(url, timeout=10)
            response.encoding = "utf-8"
            
            if response.status_code != 200:
                logger.error(f"페이지 요청 실패: {response.status_code}")
                return team_rankings
            
            # HTML 파싱
            soup = BeautifulSoup(response.text, "html.parser")
            
            # 순위 테이블 찾기 (여러 가능한 셀렉터 시도)
            table_selectors = [
                "table.tData tbody",
                "table.tbl tbody", 
                ".tData tbody",
                ".tbl tbody"
            ]
            
            table_body = None
            for selector in table_selectors:
                table_body = soup.select_one(selector)
                if table_body:
                    logger.debug(f"테이블 발견: {selector}")
                    break
            
            if not table_body:
                logger.error("순위 테이블을 찾을 수 없음")
                return team_rankings
            
            # 테이블 행 파싱
            rows = table_body.find_all("tr")
            logger.info(f"테이블 행 수: {len(rows)}")
            
            for row_index, row in enumerate(rows):
                try:
                    team_data = self._parse_team_row(row)
                    if team_data:
                        team_rankings.append(team_data)
                        logger.debug(f"팀 데이터 파싱 완료: {team_data['teamName']} - 승률: {team_data['winRate']}")
                
                except Exception as e:
                    logger.debug(f"Row {row_index} 파싱 실패: {e}")
                    continue
            
            logger.info(f"팀 순위 크롤링 완료: 총 {len(team_rankings)}개 팀")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"네트워크 요청 실패: {e}")
        except Exception as e:
            logger.error(f"팀 순위 크롤링 실패: {e}")
        
        return team_rankings
    
    def _parse_team_row(self, row) -> Optional[Dict]:
        """팀 순위 행 파싱"""
        try:
            cells = row.find_all("td")
            
            if len(cells) < 7:  # 최소 필요한 컬럼 수 확인
                logger.debug(f"컬럼 수 부족: {len(cells)}")
                return None
            
            # 각 컬럼 데이터 추출
            rank = cells[0].get_text(strip=True)
            team_name = cells[1].get_text(strip=True)
            games_played = cells[2].get_text(strip=True)
            wins = cells[3].get_text(strip=True)
            losses = cells[4].get_text(strip=True)
            draws = cells[5].get_text(strip=True)
            win_rate = cells[6].get_text(strip=True)
            
            # 게임차는 8번째 컬럼에 있을 수 있음
            game_behind = cells[7].get_text(strip=True) if len(cells) > 7 else "0"
            
            # 숫자 값 검증 및 변환
            try:
                rank_num = int(rank) if rank.isdigit() else 0
                games_played_num = int(games_played) if games_played.isdigit() else 0
                wins_num = int(wins) if wins.isdigit() else 0
                losses_num = int(losses) if losses.isdigit() else 0
                draws_num = int(draws) if draws.isdigit() else 0
                
                # 승률 파싱 (소수점 형태)
                win_rate_float = float(win_rate) if self._is_valid_win_rate(win_rate) else 0.0
                
                # 게임차 파싱 (-, 숫자, 소수점 포함)
                game_behind_float = self._parse_game_behind(game_behind)
                
            except ValueError as e:
                logger.debug(f"숫자 변환 실패: {e}")
                return None
            
            # 팀명 검증
            if not team_name or team_name in ['합계', '계', 'Total']:
                return None
            
            result = {
                "rank": rank_num,
                "teamName": team_name,
                "gamesPlayed": games_played_num,
                "wins": wins_num,
                "losses": losses_num,
                "draws": draws_num,
                "winRate": win_rate_float,
                "gameBehind": game_behind_float,
                "crawledAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            return result
            
        except Exception as e:
            logger.debug(f"팀 행 파싱 실패: {e}")
            return None
    
    def _is_valid_win_rate(self, win_rate_str: str) -> bool:
        """승률 문자열 유효성 검증"""
        try:
            # 승률은 0.000 ~ 1.000 사이의 값
            rate = float(win_rate_str)
            return 0.0 <= rate <= 1.0
        except ValueError:
            return False
    
    def _parse_game_behind(self, game_behind_str: str) -> float:
        """게임차 파싱"""
        try:
            # '-'인 경우 (1위팀)
            if game_behind_str == '-' or game_behind_str == '':
                return 0.0
            
            # 숫자인 경우
            return float(game_behind_str)
            
        except ValueError:
            logger.debug(f"게임차 파싱 실패: {game_behind_str}")
            return 0.0
    
    def crawl_team_winrates(self, target_date: str) -> List[Dict]:
        """
        KBO 웹사이트에서 팀 승률만 크롤링 (간단 버전)
        
        Args:
            target_date: 조회 날짜 (YYYY-MM-DD)
            
        Returns:
            List[Dict]: 팀 승률 정보 목록
        """
        winrates = []
        
        try:
            # URL 구성
            base_url = "https://www.koreabaseball.com/Record/TeamRank/TeamRankDaily.aspx"
            
            if target_date:
                formatted_date = target_date.replace('-', '.')
                url = f"{base_url}?date={formatted_date}"
            else:
                url = base_url
            
            logger.info(f"팀 승률 크롤링 시작: {url}")
            
            # 페이지 요청
            response = self.session.get(url, timeout=10)
            response.encoding = "utf-8"
            
            if response.status_code != 200:
                logger.error(f"페이지 요청 실패: {response.status_code}")
                return winrates
            
            # HTML 파싱
            soup = BeautifulSoup(response.text, "html.parser")
            
            # 테이블 찾기
            table_selectors = [
                "table.tData tbody",
                "table.tbl tbody", 
                ".tData tbody",
                ".tbl tbody"
            ]
            
            table_body = None
            for selector in table_selectors:
                table_body = soup.select_one(selector)
                if table_body:
                    break
            
            if not table_body:
                logger.error("순위 테이블을 찾을 수 없음")
                return winrates
            
            rows = table_body.find_all("tr")
            
            for row in rows:
                cols = row.find_all("td")
                if len(cols) >= 7:
                    try:
                        team = cols[1].get_text(strip=True)
                        win_rate_str = cols[6].get_text(strip=True)
                        
                        # 팀명 검증
                        if not team or team in ['합계', '계', 'Total']:
                            continue
                            
                        # 승률 변환
                        win_rate = float(win_rate_str)
                        
                        winrates.append({
                            "team": team,
                            "winRate": win_rate,
                            "date": target_date
                        })
                        
                    except Exception as e:
                        logger.warning(f"파싱 오류: {e}")
                        continue
            
            logger.info(f"✅ {len(winrates)}개 팀 승률 수집 완료")
            
        except Exception as e:
            logger.error(f"팀 승률 크롤링 실패: {e}")
        
        return winrates
    
    def get_team_win_rates_only(self, target_date: Optional[str] = None) -> Dict[str, float]:
        """
        팀명과 승률만 추출하는 간단한 메서드
        
        Args:
            target_date: 조회 날짜 (YYYY-MM-DD)
            
        Returns:
            Dict[str, float]: {팀명: 승률} 딕셔너리
        """
        winrates_list = self.crawl_team_winrates(target_date or datetime.now().strftime("%Y-%m-%d"))
        return {item["team"]: item["winRate"] for item in winrates_list}
    
    def get_current_standings(self) -> List[Dict]:
        """현재 팀 순위 조회 (날짜 지정 없음)"""
        return self.get_team_rankings(None)
    
    def get_standings_by_date(self, date: str) -> List[Dict]:
        """특정 날짜 팀 순위 조회"""
        return self.get_team_rankings(date)