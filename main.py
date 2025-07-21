#!/usr/bin/env python3
"""
KBO 크롤링 메인 스크립트
- 월별 일정 크롤링
- 일일 경기 결과 업데이트  
- 선수 기록 크롤링
- 팀 순위 및 승률 크롤링
"""

import argparse
import logging
from datetime import datetime, timedelta

from utils import setup_logging
from monthly_schedule_crawler import KboMonthlyScheduleCrawler
from game_result_crawler import KboGameResultCrawler
from player_stats_crawler import KboPlayerStatsCrawler
from team_rank_crawler import KboTeamRankCrawler
from data_sender import KboDataSender


def main():
    parser = argparse.ArgumentParser(description="KBO 데이터 크롤링 시스템")
    subparsers = parser.add_subparsers(dest='command', help='실행할 명령어')
    
    # 1. 월별 일정 크롤링 명령어
    monthly_parser = subparsers.add_parser('monthly-schedule', help='월별 경기 일정 크롤링')
    monthly_parser.add_argument('year_month', help='연월 (YYYY-MM)')
    
    # 2. 일일 결과 업데이트 명령어  
    daily_parser = subparsers.add_parser('daily-update', help='일일 경기 결과 및 선수 기록 업데이트')
    daily_parser.add_argument('date', help='날짜 (YYYY-MM-DD)')
    
    # 3. 선수 기록만 크롤링 명령어
    stats_parser = subparsers.add_parser('player-stats', help='선수 기록만 크롤링')
    stats_parser.add_argument('date', help='날짜 (YYYY-MM-DD)')
    
    # 4. 팀 순위 크롤링 명령어
    rank_parser = subparsers.add_parser('team-rankings', help='팀 순위 및 승률 크롤링')
    rank_parser.add_argument('--date', help='날짜 (YYYY-MM-DD), 생략시 현재 날짜', default=None)
    
    # 5. 팀 승률만 크롤링 명령어 (빠른 버전)
    winrate_parser = subparsers.add_parser('team-winrates', help='팀 승률만 크롤링 (빠른 버전)')
    winrate_parser.add_argument('--date', help='날짜 (YYYY-MM-DD), 생략시 현재 날짜', default=None)
    
    # 6. 전체 파이프라인 (기존 호환성)
    full_parser = subparsers.add_parser('full', help='전체 파이프라인 (일정+결과+선수기록)')
    full_parser.add_argument('date', help='날짜 (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # 로깅 설정
    setup_logging("INFO")
    logger = logging.getLogger(__name__)
    
    if args.command == 'monthly-schedule':
        run_monthly_schedule_crawling(args.year_month)
    elif args.command == 'daily-update':
        run_daily_update(args.date)
    elif args.command == 'player-stats':
        run_player_stats_only(args.date)
    elif args.command == 'team-rankings':
        run_team_rankings_crawling(args.date)
    elif args.command == 'team-winrates':
        run_team_winrates_only(args.date)
    elif args.command == 'full':
        run_full_pipeline(args.date)
    else:
        parser.print_help()

def run_monthly_schedule_crawling(year_month: str):
    """월별 경기 일정 크롤링 실행"""
    logger = logging.getLogger(__name__)
    logger.info(f"=== 월별 일정 크롤링 시작: {year_month} ===")
    
    try:
        # 연월 파싱
        year, month = map(int, year_month.split('-'))
        
        # 월별 일정 크롤러 실행
        monthly_crawler = KboMonthlyScheduleCrawler()
        schedule_games = monthly_crawler.get_monthly_schedule(year, month)
        monthly_crawler.close()
        
        if not schedule_games:
            logger.warning("수집된 경기 일정이 없습니다")
            return
        
        # Spring으로 전송
        data_sender = KboDataSender()
        success = data_sender.send_monthly_schedule_to_spring(schedule_games, year_month)
        
        if success:
            logger.info(f"✅ 월별 일정 크롤링 완료: {len(schedule_games)}경기")
        else:
            logger.error("❌ 월별 일정 전송 실패")
            
    except Exception as e:
        logger.error(f"월별 일정 크롤링 중 오류: {e}")

def run_daily_update(date: str):
    """일일 경기 결과 업데이트 + 선수 기록 크롤링"""
    logger = logging.getLogger(__name__)
    logger.info(f"=== 일일 업데이트 시작: {date} ===")
    
    try:
        # 1단계: 경기 결과 업데이트
        result_crawler = KboGameResultCrawler()
        game_results = result_crawler.update_game_results(date)
        result_crawler.close()
        
        if not game_results:
            logger.warning(f"{date} 날짜에 완료된 경기가 없습니다")
            return
        
        # Spring으로 결과 업데이트 전송
        data_sender = KboDataSender()
        update_success = data_sender.update_game_results_to_spring(game_results, date)
        
        if not update_success:
            logger.error("경기 결과 업데이트 실패")
            return
        
        # 2단계: 박스스코어 URL이 있는 경기만 선수 기록 크롤링
        player_crawler = KboPlayerStatsCrawler()
        stats_success_count = 0
        
        for game in game_results:
            if game.get("boxscoreUrl"):
                logger.info(f"선수 기록 크롤링: {game['gameId']}")
                
                try:
                    # 팀 코드 추출
                    game_id = game["gameId"]
                    away_code = game_id[8:10] if len(game_id) >= 12 else None
                    home_code = game_id[10:12] if len(game_id) >= 12 else None
                    
                    # 선수 기록 크롤링
                    stats = player_crawler.get_review_stats(
                        game["boxscoreUrl"], 
                        away_code, 
                        home_code
                    )
                    
                    # Spring으로 선수 기록 전송
                    if stats["pitchers"] or stats["hitters"]:
                        stats_success = data_sender.send_player_stats_to_spring(game["gameId"], stats)
                        if stats_success:
                            stats_success_count += 1
                        
                except Exception as e:
                    logger.error(f"게임 {game['gameId']} 선수 기록 처리 실패: {e}")
                    continue
            else:
                logger.debug(f"게임 {game['gameId']}: 박스스코어 URL 없음")
        
        player_crawler.close()
        
        logger.info(f"✅ 일일 업데이트 완료: 경기결과 {len(game_results)}경기, 선수기록 {stats_success_count}경기")
        
    except Exception as e:
        logger.error(f"일일 업데이트 중 오류: {e}")

def run_player_stats_only(date: str):
    """선수 기록만 크롤링 (박스스코어 URL 기존 DB에서 조회)"""
    logger = logging.getLogger(__name__)
    logger.info(f"=== 선수 기록 크롤링 시작: {date} ===")
    
    try:
        # Spring에서 박스스코어 URL 있는 경기 조회
        data_sender = KboDataSender()
        games_with_boxscore = data_sender.get_games_with_boxscore_urls(date)
        
        if not games_with_boxscore:
            logger.warning(f"{date} 날짜에 박스스코어 URL이 있는 경기가 없습니다")
            return
        
        # 선수 기록 크롤링
        player_crawler = KboPlayerStatsCrawler()
        stats_success_count = 0
        
        for game in games_with_boxscore:
            logger.info(f"선수 기록 크롤링: {game['gameId']}")
            
            try:
                # 팀 코드 추출
                game_id = game["gameId"]
                away_code = game_id[8:10] if len(game_id) >= 12 else None
                home_code = game_id[10:12] if len(game_id) >= 12 else None
                
                # 선수 기록 크롤링
                stats = player_crawler.get_review_stats(
                    game["boxscoreUrl"], 
                    away_code, 
                    home_code
                )
                
                # Spring으로 선수 기록 전송
                if stats["pitchers"] or stats["hitters"]:
                    stats_success = data_sender.send_player_stats_to_spring(game["gameId"], stats)
                    if stats_success:
                        stats_success_count += 1
                        
            except Exception as e:
                logger.error(f"게임 {game['gameId']} 선수 기록 처리 실패: {e}")
                continue
        
        player_crawler.close()
        
        logger.info(f"✅ 선수 기록 크롤링 완료: {stats_success_count}/{len(games_with_boxscore)}경기")
        
    except Exception as e:
        logger.error(f"선수 기록 크롤링 중 오류: {e}")

def run_team_rankings_crawling(target_date: str = None):
    """팀 순위 및 승률 크롤링 실행"""
    logger = logging.getLogger(__name__)
    
    date_info = target_date if target_date else "현재"
    logger.info(f"=== 팀 순위 크롤링 시작: {date_info} ===")
    
    try:
        # 팀 순위 크롤러 실행
        rank_crawler = KboTeamRankCrawler()
        team_rankings = rank_crawler.get_team_rankings(target_date)
        rank_crawler.close()
        
        if not team_rankings:
            logger.warning("수집된 팀 순위 정보가 없습니다")
            return
        
        # 결과 출력
        logger.info(f"크롤링된 팀 순위:")
        for team in team_rankings:
            logger.info(f"  {team['rank']}위: {team['teamName']} - "
                       f"승률 {team['winRate']:.3f} ({team['wins']}승 {team['losses']}패 {team['draws']}무)")
        
        # Spring으로 전송
        data_sender = KboDataSender()
        success = data_sender.send_team_rankings_to_spring(team_rankings, target_date)
        
        if success:
            logger.info(f"✅ 팀 순위 크롤링 완료: {len(team_rankings)}개 팀")
        else:
            logger.error("❌ 팀 순위 전송 실패")
            
    except Exception as e:
        logger.error(f"팀 순위 크롤링 중 오류: {e}")

def run_team_winrates_only(target_date: str = None):
    """팀 승률만 크롤링 (빠른 버전)"""
    logger = logging.getLogger(__name__)
    
    date_info = target_date if target_date else "현재"
    logger.info(f"=== 팀 승률 크롤링 시작: {date_info} ===")
    
    try:
        # 팀 승률 크롤러 실행
        rank_crawler = KboTeamRankCrawler()
        winrates = rank_crawler.crawl_team_winrates(target_date or datetime.now().strftime("%Y-%m-%d"))
        rank_crawler.close()
        
        if not winrates:
            logger.warning("수집된 팀 승률 정보가 없습니다")
            return
        
        # 결과 출력
        logger.info(f"크롤링된 팀 승률:")
        for team_data in winrates:
            logger.info(f"  {team_data['team']}: {team_data['winRate']:.3f}")
        
        # Spring으로 전송 (승률만)
        data_sender = KboDataSender()
        success = data_sender.send_team_winrates_to_spring(winrates, target_date)
        
        if success:
            logger.info(f"✅ 팀 승률 크롤링 완료: {len(winrates)}개 팀")
        else:
            logger.error("❌ 팀 승률 전송 실패")
            
    except Exception as e:
        logger.error(f"팀 승률 크롤링 중 오류: {e}")

def run_full_pipeline(date: str):
    """전체 파이프라인 실행 (기존 호환성 유지)"""
    logger = logging.getLogger(__name__)
    logger.info(f"=== 전체 파이프라인 시작: {date} ===")
    
    # 일일 업데이트와 동일한 로직
    run_daily_update(date)

if __name__ == "__main__":
    main()