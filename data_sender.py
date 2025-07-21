import json
import logging
import requests
from datetime import datetime
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
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
        
    def send_monthly_schedule_to_spring(self, games: List[Dict], year_month: str) -> bool:
        """
        월별 경기 일정을 Spring Boot로 전송
        
        Args:
            games: 월별 경기 일정 목록 (점수=0, boxscoreUrl=null)
            year_month: 연월 (YYYY-MM)
            
        Returns:
            bool: 전송 성공 여부
        """
        try:
            url = f"{self.api_base_url}/api/kbo/games/schedule"
            
            # 요청 데이터 준비
            request_data = {
                "games": games,
                "yearMonth": year_month,
                "type": "SCHEDULE"
            }
            
            logger.info(f"Spring Boot로 월별 일정 전송: {len(games)}경기, 연월: {year_month}")
            logger.debug(f"전송 URL: {url}")
            
            response = requests.post(
                url,
                data=json.dumps(request_data, ensure_ascii=False),
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code == 200:
                logger.info("월별 경기 일정 전송 성공")
                return True
            else:
                logger.error(f"월별 일정 전송 실패: {response.status_code}")
                logger.error(f"응답 내용: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"월별 일정 전송 중 오류: {e}")
            return False
    
    def update_game_results_to_spring(self, games: List[Dict], game_date: str) -> bool:
        """
        경기 결과 업데이트를 Spring Boot로 전송
        
        Args:
            games: 경기 결과 목록 (점수, boxscoreUrl 포함)
            game_date: 경기 날짜 (YYYY-MM-DD)
            
        Returns:
            bool: 전송 성공 여부
        """
        try:
            url = f"{self.api_base_url}/api/kbo/games/results"
            
            # 요청 데이터 준비
            request_data = {
                "games": games,
                "gameDate": game_date,
                "type": "RESULTS"
            }
            
            logger.info(f"Spring Boot로 경기 결과 업데이트: {len(games)}경기, 날짜: {game_date}")
            logger.debug(f"전송 URL: {url}")
            
            response = requests.post(
                url,
                data=json.dumps(request_data, ensure_ascii=False),
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                logger.info(f"경기 결과 업데이트 성공: {result}")
                return True
            else:
                logger.error(f"경기 결과 업데이트 실패: {response.status_code}")
                logger.error(f"응답 내용: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"경기 결과 업데이트 중 오류: {e}")
            return False
    
    def send_player_stats_to_spring(self, game_id: str, stats_data: Dict) -> bool:
        """
        선수 기록 데이터를 Spring Boot로 전송 (기존 로직 유지)
        
        Args:
            game_id: 게임 ID
            stats_data: 선수 기록 데이터 {"pitchers": [...], "hitters": [...]}
            
        Returns:
            bool: 전송 성공 여부
        """
        try:
            url = f"{self.api_base_url}/api/kbo/player-stats"
            params = {"gameId": game_id}
            
            logger.info(f"Spring Boot로 선수 기록 전송: gameId={game_id}")
            logger.info(f"데이터: 투수 {len(stats_data['pitchers'])}명, 타자 {len(stats_data['hitters'])}명")
            
            response = requests.post(
                url,
                params=params,
                data=json.dumps(stats_data, ensure_ascii=False),
                headers=self.headers,
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
    
    def get_games_with_boxscore_urls(self, game_date: str) -> List[Dict]:
        """
        박스스코어 URL이 있는 경기 목록 조회
        
        Args:
            game_date: 경기 날짜 (YYYY-MM-DD)
            
        Returns:
            List[Dict]: 박스스코어 URL이 있는 경기 목록
        """
        try:
            url = f"{self.api_base_url}/api/kbo/games/with-boxscore"
            params = {"gameDate": game_date}
            
            response = requests.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                games = response.json()
                logger.info(f"박스스코어 URL 있는 경기 조회: {len(games)}경기")
                return games
            else:
                logger.error(f"경기 조회 실패: {response.status_code}")
                return []
                
        except Exception as e:
            logger.error(f"경기 조회 중 오류: {e}")
            return []
    
    def send_team_rankings_to_spring(self, team_rankings: List[Dict], target_date: str = None) -> bool:
        """
        팀 순위 데이터를 Spring Boot로 전송
        
        Args:
            team_rankings: 팀 순위 데이터 리스트
            target_date: 조회 날짜 (없으면 현재 날짜)
            
        Returns:
            bool: 전송 성공 여부
        """
        try:
            url = f"{self.api_base_url}/api/kbo/team-rankings"
            
            payload = {
                "date": target_date,
                "rankings": team_rankings,
                "totalTeams": len(team_rankings),
                "crawledAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            logger.info(f"팀 순위 데이터 전송 시작: {len(team_rankings)}개 팀")
            logger.debug(f"전송 데이터: {payload}")
            
            response = requests.post(
                url,
                data=json.dumps(payload, ensure_ascii=False),
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code == 200:
                logger.info("✅ 팀 순위 데이터 전송 성공")
                return True
            else:
                logger.error(f"❌ 팀 순위 데이터 전송 실패: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"팀 순위 데이터 전송 중 네트워크 오류: {e}")
            return False
        except Exception as e:
            logger.error(f"팀 순위 데이터 전송 중 오류: {e}")
            return False

    def send_team_winrates_to_spring(self, winrates: List[Dict], target_date: str = None) -> bool:
        """
        팀 승률 데이터만 Spring Boot로 전송 (빠른 버전)
        
        Args:
            winrates: 팀 승률 데이터 리스트 [{"team": "KIA", "winRate": 0.687, "date": "2025-07-06"}]
            target_date: 조회 날짜
            
        Returns:
            bool: 전송 성공 여부
        """
        try:
            url = f"{self.api_base_url}/api/kbo/team-rankings/win-rates"
            
            payload = {
                "date": target_date,
                "winRates": winrates,
                "totalTeams": len(winrates),
                "crawledAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
            logger.info(f"팀 승률 데이터 전송 시작: {len(winrates)}개 팀")
            logger.debug(f"전송 데이터: {payload}")
            
            response = requests.post(
                url,
                data=json.dumps(payload, ensure_ascii=False),
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code == 200:
                logger.info("✅ 팀 승률 데이터 전송 성공")
                return True
            else:
                logger.error(f"❌ 팀 승률 데이터 전송 실패: {response.status_code} - {response.text}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"팀 승률 데이터 전송 중 네트워크 오류: {e}")
            return False
        except Exception as e:
            logger.error(f"팀 승률 데이터 전송 중 오류: {e}")
            return False
        """
        Spring 서버에서 팀 승률 조회
        
        Args:
            target_date: 조회 날짜 (YYYY-MM-DD)
            
        Returns:
            Dict[str, float]: {팀명: 승률} 딕셔너리
        """
        try:
            url = f"{self.api_base_url}/api/kbo/team-rankings/win-rates"
            
            params = {}
            if target_date:
                params['date'] = target_date
            
            response = requests.get(
                url,
                params=params,
                headers=self.headers,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get('winRates', {})
            else:
                logger.error(f"팀 승률 조회 실패: {response.status_code} - {response.text}")
                return {}
                
        except requests.exceptions.RequestException as e:
            logger.error(f"팀 승률 조회 중 네트워크 오류: {e}")
            return {}
        except Exception as e:
            logger.error(f"팀 승률 조회 중 오류: {e}")
            return {}
    
    def test_connection(self) -> bool:
        """
        Spring Boot 서버 연결 테스트 (기존 로직 유지)
        
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