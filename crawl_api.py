from fastapi import FastAPI
from main import run_monthly_schedule_crawling, run_daily_update, run_team_winrates_only
from datetime import datetime, timedelta

app = FastAPI()

@app.get("/")
def health():
    return {"status": "ok"}

@app.post("/crawl/monthly")
def crawl_monthly():
    today = datetime.today()
    year_month = today.strftime("%Y-%m")
    run_monthly_schedule_crawling(year_month)
    return {"message": f"Monthly schedule for {year_month} started"}

@app.post("/crawl/daily")
def crawl_daily():
    yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    run_daily_update(yesterday)
    return {"message": f"Daily update for {yesterday} started"}

@app.post("/crawl/winrates")
def crawl_winrates():
    date = datetime.today().strftime("%Y-%m-%d")
    run_team_winrates_only(date)
    return {"message": f"Winrate update for {date} started"}