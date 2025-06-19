#!/usr/bin/env python3
# debug_test.py - 실제 DB URL로 크롤링 테스트 + DTO 포맷 로그

import logging
import pymysql
import time
from dataclasses import dataclass
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

from utils import setup_logging
from config import config

# ──────────────────────────────────────────────────────────────────────────────
# DTO 정의
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class PitcherStatDto:
    team: str
    playerName: str
    innings: str
    earnedRuns: int

@dataclass
class HitterStatDto:
    team: str
    playerName: str
    atBats: int
    hits: int

# ──────────────────────────────────────────────────────────────────────────────
# DB 에서 boxscore_url 가져오기
# ──────────────────────────────────────────────────────────────────────────────
def get_real_boxscore_urls():
    logging.info("DB 접속 시도")
    conn = None
    try:
        conn = pymysql.connect(**config.DB_CONFIG)
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute("""
                SELECT game_id, boxscore_url 
                  FROM game 
                 WHERE DATE(local_date_time) = '2025-06-01' 
                   AND boxscore_url IS NOT NULL 
                   AND boxscore_url != ''
                 LIMIT 5
            """)
            rows = cur.fetchall()
            logging.info(f"DB에서 {len(rows)}개 URL 조회됨")
            return rows
    except Exception as e:
        logging.error(f"DB 조회 오류: {e}")
        return []
    finally:
        if conn:
            conn.close()
            logging.info("DB 연결 종료")

# ──────────────────────────────────────────────────────────────────────────────
# 크롤러 클래스
# ──────────────────────────────────────────────────────────────────────────────
class KboPlayerStatsCrawler:
    def __init__(self, headless: bool = True):
        opts = Options()
        if headless:
            opts.add_argument("--headless=new")
            opts.add_argument("--disable-gpu")
            opts.add_argument("--no-sandbox")
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=opts
        )
        logging.info("Chrome WebDriver 생성 완료")

    def close(self):
        if self.driver:
            self.driver.quit()
        logging.info("Player Stats WebDriver 종료 완료")

    def _parse_innings(self, text: str) -> float:
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

    def get_review_stats(self, url: str, away_team: str, home_team: str) -> dict:
        logging.info(f"선수 기록 크롤링 시작: {url}")
        self.driver.get(url)
        WebDriverWait(self.driver, 15).until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table.tbl"))
        )
        tables = self.driver.find_elements(By.CSS_SELECTOR, "table.tbl")
        logging.info(f"발견된 테이블 수: {len(tables)}")

        # 1) '선수명' 컬럼이 있는 라인업 테이블 인덱스 찾기
        lineup_idxs = []
        for idx, tbl in enumerate(tables):
            hdrs = [th.text.strip() for th in tbl.find_elements(By.CSS_SELECTOR, "thead th")]
            if "선수명" in hdrs:
                lineup_idxs.append(idx)

        # 2) '타수','안타' 헤더가 있는 타자 기록 테이블 인덱스 찾기
        hitter_idxs = []
        for idx, tbl in enumerate(tables):
            hdrs = [th.text.strip() for th in tbl.find_elements(By.CSS_SELECTOR, "thead th")]
            if "타수" in hdrs and "안타" in hdrs:
                hitter_idxs.append(idx)

        # 3) '이닝','자책' 헤더가 있는 투수 기록 테이블 인덱스 찾기
        pitcher_idxs = []
        for idx, tbl in enumerate(tables):
            hdrs = [th.text.strip() for th in tbl.find_elements(By.CSS_SELECTOR, "thead th")]
            if "이닝" in hdrs and "자책" in hdrs:
                pitcher_idxs.append(idx)

        # 팀 순서: lineup_idxs 순서대로 away→home 이라고 가정
        team_codes = [away_team, home_team]

        # 타자 파싱
        hitters = []
        for team, li_idx, st_idx in zip(team_codes, lineup_idxs, hitter_idxs):
            name_rows = tables[li_idx].find_elements(By.CSS_SELECTOR, "tbody tr")
            stat_rows = tables[st_idx].find_elements(By.CSS_SELECTOR, "tbody tr")
            for nr, sr in zip(name_rows, stat_rows):
                # 선수명 <th> 세 번째 칸
                ths = nr.find_elements(By.TAG_NAME, "th")
                player_name = ths[2].text.strip() if len(ths) >= 3 else ""
                cols = sr.find_elements(By.TAG_NAME, "td")
                hitters.append({
                    "team":        team,
                    "playerName":  player_name,
                    "atBats":      int(cols[0].text.strip() or 0),
                    "hits":        int(cols[1].text.strip() or 0),
                })

        # 투수 파싱 (이전과 동일)
        pitchers = []
        for team, st_idx in zip(team_codes, pitcher_idxs):
            for row in tables[st_idx].find_elements(By.CSS_SELECTOR, "tbody tr"):
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) < 16: continue
                pitchers.append({
                    "team":       team,
                    "playerName": cols[0].text.strip(),
                    "innings":    str(self._parse_innings(cols[6].text)),
                    "earnedRuns": int(cols[15].text.strip() or 0)
                })

        logging.info(f"크롤링 완료: 투수 {len(pitchers)}명, 타자 {len(hitters)}명")
        return {"pitchers": pitchers, "hitters": hitters}
# ──────────────────────────────────────────────────────────────────────────────
# 테스트 및 DTO 로깅
# ──────────────────────────────────────────────────────────────────────────────
def test_real_boxscore_urls():
    urls = get_real_boxscore_urls()
    if not urls:
        print("❌ DB에서 boxscore_url을 찾을 수 없습니다")
        return

    print(f"✅ DB에서 {len(urls)}개 게임의 boxscore_url을 찾았습니다\n")
    crawler = KboPlayerStatsCrawler(headless=False)

    for idx, row in enumerate(urls, start=1):
        game_id = row["game_id"]
        box_url = row["boxscore_url"]
        print(f"\n=== 테스트 {idx}: {game_id} ===")
        print(f"URL: {box_url}\n")

        # game_id 에서 팀 코드 추출 (YYYYMMDD|AW|HM|X)
        away_code = game_id[8:10]
        home_code = game_id[10:12]

        stats = crawler.get_review_stats(box_url, away_code, home_code)

        # DTO 변환
        pitcher_dtos = [
            PitcherStatDto(
                team=p["team"],
                playerName=p["playerName"],
                innings=str(p["innings"]),
                earnedRuns=p["earnedRuns"]
            )
            for p in stats["pitchers"]
        ]
        hitter_dtos  = [
            HitterStatDto(
                team=h["team"],
                playerName=h["playerName"],
                atBats=h["atBats"],
                hits=h["hits"]
            )
            for h in stats["hitters"]
        ]

        # 로그에 DTO 찍기
        logging.info("▶ PitcherStatDto 리스트:")
        for dto in pitcher_dtos:
            logging.info(dto)
        logging.info("▶ HitterStatDto 리스트:")
        for dto in hitter_dtos:
            logging.info(dto)

    crawler.close()
    print("\n🎉 테스트 완료")

if __name__ == "__main__":
    setup_logging("INFO")
    test_real_boxscore_urls()