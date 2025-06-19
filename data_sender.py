import json
import logging
import requests
from typing import List, Dict
from config import config

logger = logging.getLogger(__name__)

class KboDataSender:
    def __init__(self, api_base_url: str = None):
        """
        데이터 전송기 초기화
        
        Args:
            api_base_url: Spring Boot API 베이스 URL
        """
        self.api_base_url = api_base_url or config.SPRING_API_BASE_URL
        
    def send_schedule_to_spring(self, games: List[Dict], game_date: str) -> bool:
        """
        경기 일정 데이터를 Spring Boot로 전송
        
        Args:
            games: Python 크롤러로 수집한 경기 목록
            game_date: 경기 날짜 (YYYY-MM-DD)
            
        Returns:
            bool: 전송 성공 여부
        """
        try:
            url = f"{self.api_base_url}/api/kbo/games"
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            
            # Python 데이터 형식을 Spring Boot 형식으로 변환
            spring_format_games = []
            for game in games:
                spring_game = {
                    "awayTeam": game["awayTeam"],          # 원정팀
                    "homeTeam": game["homeTeam"],          # 홈팀
                    "awayScore": game["awayScore"],        # 원정팀 점수
                    "homeScore": game["homeScore"],        # 홈팀 점수
                    "stadium": game["stadium"],            # 경기장
                    "gameDateTime": game["gameDateTime"],  # 경기 시간
                    "gameId": game["gameId"],              # 중요: gameId 포함
                    "boxscoreUrl": game["boxscoreUrl"]     # 박스스코어 URL
                }
                spring_format_games.append(spring_game)
            
            # 요청 데이터 준비
            request_data = {
                "games": spring_format_games,
                "gameDate": game_date
            }
            
            logger.info(f"Spring Boot로 경기 데이터 전송: {len(spring_format_games)}경기, 날짜: {game_date}")
            logger.debug(f"전송 URL: {url}")
            
            response = requests.post(
                url,
                data=json.dumps(request_data, ensure_ascii=False),
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                logger.info("경기 일정 전송 성공")
                return True
            else:
                logger.error(f"경기 일정 전송 실패: {response.status_code}")
                logger.error(f"응답 내용: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"경기 일정 전송 중 오류: {e}")
            return False
    
    def send_player_stats_to_spring(self, game_id: str, stats_data: Dict) -> bool:
        """
        선수 기록 데이터를 Spring Boot로 전송
        
        Args:
            game_id: 게임 ID
            stats_data: 선수 기록 데이터 {"pitchers": [...], "hitters": [...]}
            
        Returns:
            bool: 전송 성공 여부
        """
        try:
            url = f"{self.api_base_url}/api/kbo/player-stats"
            params = {"gameId": game_id}
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }
            
            logger.info(f"Spring Boot로 선수 기록 전송: gameId={game_id}")
            logger.info(f"데이터: 투수 {len(stats_data['pitchers'])}명, 타자 {len(stats_data['hitters'])}명")
            
            response = requests.post(
                url,
                params=params,
                data=json.dumps(stats_data, ensure_ascii=False),
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"선수 기록 전송 성공: {result}")
                return True
            else:
                logger.error(f"선수 기록 전송 실패: {response.status_code}")
                logger.error(f"응답 내용: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"선수 기록 전송 중 오류: {e}")
            return False
    
    def test_connection(self) -> bool:
        """
        Spring Boot 서버 연결 테스트
        
        Returns:
            bool: 연결 성공 여부
        """
        try:
            url = f"{self.api_base_url}/actuator/health"
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                logger.info("Spring Boot 서버 연결 성공")
                return True
            else:
                logger.warning(f"Spring Boot 서버 응답 이상: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Spring Boot 서버 연결 실패: {e}")
            return False