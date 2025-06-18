# api/schedule_api.py (FastAPI 엔드포인트 추가)
import sys
import os
from pathlib import Path

# 프로젝트 루트 경로를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import logging

# 크롤러 임포트
from crawler.schedule_selenium import KboScheduleCrawler
from api.rest_client import SpringRestClient

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,  # DEBUG로 변경하여 더 자세한 로그 확인
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="KBO Schedule Crawler API")

# Spring 서버 설정
SPRING_BASE_URL = os.getenv("SPRING_BASE_URL", "http://127.0.0.1:8080")
rest_client = SpringRestClient(SPRING_BASE_URL)


class CrawlRequest(BaseModel):
    date: Optional[str] = None  # YYYY-MM-DD format
    start_date: Optional[str] = None
    end_date: Optional[str] = None


class CrawlResponse(BaseModel):
    success: bool
    message: str
    games_count: int
    date: Optional[str] = None


@app.get("/")
async def root():
    """루트 엔드포인트"""
    return {
        "message": "KBO Schedule Crawler API",
        "endpoints": [
            "/crawl-and-save",
            "/health",
            "/test-connection",
            "/docs"
        ]
    }


@app.post("/crawl-and-save", response_model=CrawlResponse)
async def crawl_and_save_games(
    request: CrawlRequest,
    background_tasks: BackgroundTasks
):
    """
    KBO 경기 정보를 크롤링하고 Spring 서버로 전송
    """
    crawler = None
    
    try:
        # Spring 서버 상태 확인
        spring_connected = rest_client.health_check()
        if not spring_connected:
            logger.warning("Spring 서버가 응답하지 않습니다 - 크롤링은 계속 진행합니다")
        
        crawler = KboScheduleCrawler()
        
        # 단일 날짜 크롤링
        if request.date:
            logger.info(f"단일 날짜 크롤링 시작: {request.date}")
            games = crawler.get_games_by_date(request.date)
            
            if not games:
                return CrawlResponse(
                    success=True,
                    message="해당 날짜에 경기가 없습니다",
                    games_count=0,
                    date=request.date
                )
            
            # Spring 서버로 전송 (연결된 경우에만)
            if spring_connected:
                success = rest_client.send_games_to_spring(games, request.date)
                message = f"{len(games)}개 경기 정보를 {'성공적으로 저장했습니다' if success else '크롤링했습니다 (저장 실패)'}"
            else:
                success = True  # 크롤링은 성공
                message = f"{len(games)}개 경기 정보를 크롤링했습니다 (Spring 서버 미연결)"
            
            return CrawlResponse(
                success=success,
                message=message,
                games_count=len(games),
                date=request.date
            )
        
        # 날짜 범위 크롤링
        elif request.start_date and request.end_date:
            logger.info(f"날짜 범위 크롤링 요청: {request.start_date} ~ {request.end_date}")
            
            # 백그라운드 작업으로 처리
            background_tasks.add_task(
                crawl_date_range_task,
                request.start_date,
                request.end_date,
                rest_client
            )
            
            return CrawlResponse(
                success=True,
                message="날짜 범위 크롤링이 백그라운드에서 시작되었습니다",
                games_count=0
            )
        
        else:
            # 오늘 날짜 크롤링
            today = datetime.now().strftime("%Y-%m-%d")
            logger.info(f"오늘 날짜 크롤링 시작: {today}")
            
            games = crawler.get_games_by_date(today)
            
            if spring_connected and games:
                success = rest_client.send_games_to_spring(games, today)
                message = f"오늘({today}) {len(games)}개 경기 정보를 {'저장했습니다' if success else '크롤링했습니다'}"
            else:
                success = True
                message = f"오늘({today}) {len(games)}개 경기 정보를 크롤링했습니다 {'(Spring 서버 미연결)' if not spring_connected else ''}"
            
            return CrawlResponse(
                success=success,
                message=message,
                games_count=len(games),
                date=today
            )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"크롤링 중 오류 발생: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"크롤링 실패: {str(e)}"
        )
    finally:
        if crawler:
            crawler.close()


def crawl_date_range_task(
    start_date: str,
    end_date: str,
    rest_client: SpringRestClient
):
    """백그라운드에서 날짜 범위 크롤링 및 저장"""
    crawler = None
    
    try:
        from datetime import datetime, timedelta
        import time
        
        crawler = KboScheduleCrawler()
        
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        current = start
        total_games = 0
        failed_dates = []
        
        while current <= end:
            date_str = current.strftime("%Y-%m-%d")
            
            try:
                logger.info(f"크롤링 중: {date_str}")
                games = crawler.get_games_by_date(date_str)
                
                if games:
                    success = rest_client.send_games_to_spring(games, date_str)
                    
                    if success:
                        total_games += len(games)
                        logger.info(f"{date_str}: {len(games)}개 경기 저장 성공")
                    else:
                        failed_dates.append(date_str)
                        logger.error(f"{date_str}: 저장 실패")
                else:
                    logger.info(f"{date_str}: 경기 없음")
                
                current += timedelta(days=1)
                
                # 서버 부하 방지
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"{date_str} 처리 중 오류: {e}")
                failed_dates.append(date_str)
                current += timedelta(days=1)
        
        logger.info(f"날짜 범위 크롤링 완료: 총 {total_games}개 경기 저장")
        
        if failed_dates:
            logger.warning(f"실패한 날짜: {failed_dates}")
    
    except Exception as e:
        logger.error(f"날짜 범위 크롤링 실패: {e}", exc_info=True)
    finally:
        if crawler:
            crawler.close()


# 추가 유틸리티 엔드포인트
@app.get("/health")
async def health_check():
    """서버 상태 확인"""
    spring_status = rest_client.health_check()
    
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "spring_server": "connected" if spring_status else "disconnected",
        "spring_url": SPRING_BASE_URL
    }


@app.post("/test-connection")
async def test_spring_connection():
    """Spring 서버 연결 테스트"""
    try:
        # 상세한 연결 정보 제공
        is_connected = rest_client.health_check()
        
        return {
            "spring_url": SPRING_BASE_URL,
            "connected": is_connected,
            "status": "success" if is_connected else "warning",
            "message": "Spring 서버 연결 성공" if is_connected else "Spring 서버 연결 실패 - 크롤링은 가능하지만 데이터 저장이 안 됩니다"
        }
            
    except Exception as e:
        logger.error(f"연결 테스트 중 오류: {e}", exc_info=True)
        return {
            "spring_url": SPRING_BASE_URL,
            "connected": False,
            "status": "error",
            "message": f"연결 테스트 중 오류 발생: {str(e)}"
        }


# 사용 예시
if __name__ == "__main__":
    import uvicorn
    
    # FastAPI 서버 실행
    uvicorn.run(app, host="0.0.0.0", port=8000)