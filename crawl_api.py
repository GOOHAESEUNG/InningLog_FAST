import logging
from fastapi import FastAPI
from main import run_monthly_schedule_crawling, run_daily_update, run_team_winrates_only
from datetime import datetime, timedelta

# 로그 설정
logging.basicConfig(
    level=logging.INFO,
    format="📌 [%(asctime)s] %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

app = FastAPI()

@app.get("/")
def health():
    return {"status": "ok"}

@app.post("/crawl/monthly")
def crawl_monthly():
    today = datetime.today()
    year_month = today.strftime("%Y-%m")
    logging.info(f"📅 {year_month} 월간 경기 일정 크롤링을 시작합니다.")
    run_monthly_schedule_crawling(year_month)
    return {"message": f"{year_month} 월간 경기 일정 크롤링 시작됨"}

@app.post("/crawl/daily")
def crawl_daily():
    yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    logging.info(f"🗓️ {yesterday} 일자 경기 정보 업데이트 크롤링을 시작합니다.")
    run_daily_update(yesterday)
    return {"message": f"{yesterday} 일자 경기 정보 업데이트 크롤링 시작됨"}

@app.post("/crawl/winrates")
def crawl_winrates():
    date = datetime.today().strftime("%Y-%m-%d")
    logging.info(f"📊 {date} 기준 팀 승률 크롤링을 시작합니다.")
    run_team_winrates_only(date)
    return {"message": f"{date} 기준 팀 승률 크롤링 시작됨"}