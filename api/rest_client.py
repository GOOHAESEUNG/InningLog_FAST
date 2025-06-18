# api/rest_client.py
import requests
import logging
from typing import List, Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class SpringRestClient:
    def __init__(self, base_url: str, timeout: int = 30):
        """
        Spring REST 서버 클라이언트 초기화
        
        Args:
            base_url: Spring 서버 기본 URL (예: http://localhost:8080)
            timeout: 요청 타임아웃 (초)
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
    
    def send_games_to_spring(self, games: List[Dict], game_date: str) -> bool:
        """
        크롤링한 게임 데이터를 Spring 서버로 전송
        
        Args:
            games: 게임 정보 리스트
            game_date: 게임 날짜 (YYYY-MM-DD)
        
        Returns:
            성공 여부
        """
        try:
            # Spring 서버 엔드포인트 (실제 엔드포인트에 맞게 수정 필요)
            endpoint = f"{self.base_url}/api/kbo/games"
            
            # 요청 데이터 구성
            payload = {
                "gameDate": game_date,
                "games": games
            }
            
            logger.info(f"Spring 서버로 {len(games)}개 게임 전송 시작: {game_date}")
            logger.debug(f"요청 데이터: {payload}")
            
            # POST 요청
            response = self.session.post(
                endpoint,
                json=payload,
                timeout=self.timeout
            )
            
            # 응답 확인
            response.raise_for_status()
            
            logger.info(f"게임 데이터 전송 성공: {response.status_code}")
            logger.debug(f"응답: {response.json() if response.content else 'No content'}")
            
            return True
            
        except requests.exceptions.Timeout:
            logger.error(f"요청 타임아웃: {self.timeout}초 초과")
            return False
            
        except requests.exceptions.ConnectionError:
            logger.error(f"Spring 서버 연결 실패: {self.base_url}")
            return False
            
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP 오류: {e.response.status_code} - {e.response.text}")
            return False
            
        except Exception as e:
            logger.error(f"예상치 못한 오류: {e}")
            return False
    
    def send_games_batch(self, games_by_date: Dict[str, List[Dict]]) -> Dict[str, bool]:
        """
        여러 날짜의 게임 데이터를 배치로 전송
        
        Args:
            games_by_date: {날짜: 게임리스트} 형태의 딕셔너리
        
        Returns:
            {날짜: 성공여부} 형태의 결과
        """
        results = {}
        
        for date, games in games_by_date.items():
            if games:  # 게임이 있는 날짜만 전송
                success = self.send_games_to_spring(games, date)
                results[date] = success
                
                # 연속 요청 시 서버 부하 방지
                import time
                time.sleep(0.5)
        
        return results
    
    def health_check(self) -> bool:
        """Spring 서버 상태 확인"""
        try:
            # 실제 health check 엔드포인트에 맞게 수정
            response = self.session.get(
                f"{self.base_url}/actuator/health",
                timeout=5
            )
            return response.status_code == 200
        except:
            return False
