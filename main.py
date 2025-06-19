import sys
import logging
import pymysql
from datetime import datetime
from typing import List, Dict

from config import config
from utils import setup_logging
from schedule_crawler import KboScheduleCrawler
from player_stats_crawler import KboPlayerStatsCrawler
from data_sender import KboDataSender

# 로깅 설정
setup_logging(config.LOG_LEVEL)
logger = logging.getLogger(__name__)

class KboMainProcessor:
    def __init__(self):
        """메인 프로세서 초기화"""
        self.schedule_crawler = None
        self.stats_crawler = None
        self.data_sender = KboDataSender()
        
    def __enter__(self):
        """컨텍스트 매니저 진입"""
        self.schedule_crawler = KboScheduleCrawler()
        self.stats_crawler = KboPlayerStatsCrawler()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """컨텍스트 매니저 종료 - 리소스 정리"""
        if self.schedule_crawler:
            self.schedule_crawler.close()
        if self.stats_crawler:
            self.stats_crawler.close()
    
    def process_schedule_only(self, date: str = None) -> bool:
        """
        경기 일정만 크롤링하고 저장
        
        Args:
            date: 크롤링할 날짜 (YYYY-MM-DD), None이면 오늘
            
        Returns:
            bool: 성공 여부
        """
        try:
            # 날짜 설정
            if not date:
                date = datetime.now().strftime("%Y-%m-%d")
            
            logger.info(f"=== 경기 일정 크롤링 시작: {date} ===")
            
            # 1. Spring Boot 서버 연결 확인
            if not self.data_sender.test_connection():
                logger.error("Spring Boot 서버에 연결할 수 없습니다")
                return False
            
            # 2. 경기 일정 크롤링
            games = self.schedule_crawler.get_games_by_date(date)
            
            if not games:
                logger.warning(f"{date} 날짜에 크롤링된 경기가 없습니다")
                return True  # 경기가 없는 것은 정상
            
            # 3. Spring Boot로 경기 일정 전송
            success = self.data_sender.send_schedule_to_spring(games, date)
            
            if success:
                logger.info(f"✅ 경기 일정 처리 완료: {len(games)}경기")
                return True
            else:
                logger.error("❌ 경기 일정 저장 실패")
                return False
                
        except Exception as e:
            logger.error(f"경기 일정 처리 중 오류: {e}")
            return False
    
    def process_player_stats_only(self, date: str = None) -> Dict:
        """
        선수 기록만 크롤링하고 저장
        
        Args:
            date: 처리할 날짜 (YYYY-MM-DD), None이면 오늘
            
        Returns:
            dict: 처리 결과 통계
        """
        try:
            # 날짜 설정
            if not date:
                date = datetime.now().strftime("%Y-%m-%d")
                
            logger.info(f"=== 선수 기록 크롤링 시작: {date} ===")
            
            # 1. DB에서 해당 날짜의 게임 조회
            games = self.get_games_from_db(date)
            
            if not games:
                logger.warning(f"{date} 날짜에 처리할 게임이 없습니다")
                return {"total": 0, "success": 0, "failed": 0}
            
            # 2. 각 게임의 선수 기록 처리
            success_count = 0
            failed_count = 0
            
            for game in games:
                if self.process_single_game_stats(game):
                    success_count += 1
                else:
                    failed_count += 1
            
            result = {
                "total": len(games),
                "success": success_count,
                "failed": failed_count
            }
            
            logger.info(f"✅ 선수 기록 처리 완료: {result}")
            return result
            
        except Exception as e:
            logger.error(f"선수 기록 처리 중 오류: {e}")
            return {"total": 0, "success": 0, "failed": 0}
    
    def process_full_workflow(self, date: str = None) -> bool:
        """
        전체 워크플로우: 경기 일정 → 선수 기록 순서로 처리
        
        Args:
            date: 처리할 날짜 (YYYY-MM-DD), None이면 오늘
            
        Returns:
            bool: 전체 성공 여부
        """
        try:
            # 날짜 설정
            if not date:
                date = datetime.now().strftime("%Y-%m-%d")
                
            logger.info(f"=== KBO 전체 워크플로우 시작: {date} ===")
            
            # 1단계: 경기 일정 크롤링 및 저장
            schedule_success = self.process_schedule_only(date)
            if not schedule_success:
                logger.error("경기 일정 처리 실패로 전체 워크플로우 중단")
                return False
            
            # 2단계: 선수 기록 크롤링 및 저장
            stats_result = self.process_player_stats_only(date)
            
            # 결과 요약
            logger.info(f"🎯 전체 워크플로우 완료")
            logger.info(f"   - 경기 일정: 성공")
            logger.info(f"   - 선수 기록: {stats_result['success']}/{stats_result['total']} 성공")
            
            return True
            
        except Exception as e:
            logger.error(f"전체 워크플로우 중 오류: {e}")
            return False
    
    def process_single_game_stats(self, game: Dict) -> bool:
        """
        단일 게임의 선수 기록 처리
        
        Args:
            game: 게임 정보 딕셔너리
            
        Returns:
            bool: 처리 성공 여부
        """
        game_id = game.get('game_id') or game.get('gameId')
        boxscore_url = game.get('boxscore_url') or game.get('boxscoreUrl')
        
        if not game_id or not boxscore_url:
            logger.warning(f"게임 정보 부족: gameId={game_id}, boxscoreUrl={boxscore_url}")
            return False
        
        try:
            logger.info(f"게임 {game_id} 선수 기록 처리 시작")
            
            # 1. 박스스코어 크롤링
            stats_data = self.stats_crawler.get_review_stats(boxscore_url)
            
            if not stats_data['pitchers'] and not stats_data['hitters']:
                logger.warning(f"게임 {game_id}: 크롤링된 선수 기록 없음")
                return False
            
            # 2. Spring Boot API로 전송
            success = self.data_sender.send_player_stats_to_spring(game_id, stats_data)
            
            if success:
                logger.info(f"게임 {game_id} 선수 기록 처리 완료")
                return True
            else:
                logger.error(f"게임 {game_id} 선수 기록 전송 실패")
                return False
                
        except Exception as e:
            logger.error(f"게임 {game_id} 처리 중 오류: {e}")
            return False
    
    def get_games_from_db(self, date: str = None) -> List[Dict]:
        """
        DB에서 게임 정보를 조회합니다.
        
        Args:
            date: 조회할 날짜 (YYYY-MM-DD), None이면 모든 게임
            
        Returns:
            list: 게임 정보 리스트
        """
        connection = None
        try:
            # MySQL 연결
            connection = pymysql.connect(**config.DB_CONFIG)
            
            with connection.cursor(pymysql.cursors.DictCursor) as cursor:
                if date:
                    # 특정 날짜의 게임만 조회 (boxscore_url이 있는 게임만)
                    sql = """
                    SELECT game_id, boxscore_url 
                    FROM game 
                    WHERE DATE(local_date_time) = %s 
                    AND boxscore_url IS NOT NULL 
                    AND boxscore_url != ''
                    """
                    cursor.execute(sql, (date,))
                else:
                    # boxscore_url이 있는 최근 게임들 조회
                    sql = """
                    SELECT game_id, boxscore_url 
                    FROM game 
                    WHERE boxscore_url IS NOT NULL 
                    AND boxscore_url != ''
                    ORDER BY local_date_time DESC 
                    LIMIT 10
                    """
                    cursor.execute(sql)
                
                games = cursor.fetchall()
                logger.info(f"DB에서 조회된 게임 수: {len(games)}")
                return games
                
        except Exception as e:
            logger.error(f"DB 조회 중 오류: {e}")
            return []
        finally:
            if connection:
                connection.close()

def main():
    """메인 함수"""
    
    # 명령행 인수 처리
    if len(sys.argv) < 2:
        print("사용법:")
        print("  python main.py schedule [날짜]     # 경기 일정만 크롤링")
        print("  python main.py stats [날짜]        # 선수 기록만 크롤링") 
        print("  python main.py full [날짜]         # 전체 워크플로우")
        print("  python main.py test               # 연결 테스트")
        print("")
        print("예시:")
        print("  python main.py schedule 2024-06-15")
        print("  python main.py stats")
        print("  python main.py full")
        return
    
    command = sys.argv[1].lower()
    date = sys.argv[2] if len(sys.argv) > 2 else None
    
    # 명령어별 실행
    if command == "test":
        # 연결 테스트
        sender = KboDataSender()
        if sender.test_connection():
            print("✅ Spring Boot 서버 연결 성공")
        else:
            print("❌ Spring Boot 서버 연결 실패")
            
    elif command == "schedule":
        # 경기 일정만 크롤링
        with KboMainProcessor() as processor:
            success = processor.process_schedule_only(date)
            if success:
                print("✅ 경기 일정 크롤링 완료")
            else:
                print("❌ 경기 일정 크롤링 실패")
                sys.exit(1)
                
    elif command == "stats":
        # 선수 기록만 크롤링
        with KboMainProcessor() as processor:
            result = processor.process_player_stats_only(date)
            if result['total'] > 0:
                print(f"✅ 선수 기록 크롤링 완료: {result['success']}/{result['total']} 성공")
            else:
                print("❌ 처리할 게임이 없습니다")
                
    elif command == "full":
        # 전체 워크플로우
        with KboMainProcessor() as processor:
            success = processor.process_full_workflow(date)
            if success:
                print("✅ 전체 워크플로우 완료")
            else:
                print("❌ 전체 워크플로우 실패")
                sys.exit(1)
                
    else:
        print(f"알 수 없는 명령어: {command}")
        sys.exit(1)

if __name__ == "__main__":
    main()