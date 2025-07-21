import re
import logging
import pytz
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from utils import create_chrome_driver, generate_game_id_from_teams_date, to_kbo_date_format, safe_sleep
from config import config

logger = logging.getLogger(__name__)

class KboMonthlyScheduleCrawler:
    """월별 경기 일정 전용 크롤러 (점수, 박스스코어 URL 제외)"""

    def __init__(self):
        """크롤러 초기화"""
        self.driver = create_chrome_driver(headless=True, driver_path=config.CHROME_DRIVER_PATH)
        self.wait = WebDriverWait(self.driver, config.TIMEOUT)

    def __del__(self):
        """소멸자 - WebDriver 종료"""
        self.close()

    def close(self):
        """WebDriver 종료"""
        if hasattr(self, 'driver') and self.driver:
            self.driver.quit()
            logger.info("Monthly Schedule WebDriver 종료 완료")

    def get_monthly_schedule(self, year: int, month: int) -> List[Dict]:
        all_games = []
        try:
            start_date = datetime(year, month, 1)
            if month == 12:
                end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
            else:
                end_date = datetime(year, month + 1, 1) - timedelta(days=1)
            current_date = start_date

            logger.info(f"{year}년 {month}월 전체 일정 크롤링 시작 ({start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')})")

            while current_date <= end_date:
                date_str = current_date.strftime("%Y-%m-%d")
                daily_games = self._get_daily_schedule(date_str)
                all_games.extend(daily_games)
                safe_sleep(1)
                current_date += timedelta(days=1)

            logger.info(f"{year}년 {month}월 일정 크롤링 완료: 총 {len(all_games)}경기")
        except Exception as e:
            logger.error(f"월별 일정 크롤링 실패: {e}")
        return all_games

    def _get_daily_schedule(self, date_string: str) -> List[Dict]:
        games = []
        try:
            target_kbo_date = to_kbo_date_format(date_string)
            url = f"https://www.koreabaseball.com/Schedule/Schedule.aspx?date={date_string}"
            logger.debug(f"일정 크롤링: {date_string}, URL: {url}")

            self.driver.get(url)
            self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "table.tbl")))
            safe_sleep(1)

            rows = self.driver.find_elements(By.CSS_SELECTOR, "table.tbl tbody tr")
            current_date = ""
            is_target_date = False

            for row_index, row in enumerate(rows):
                try:
                    tds = row.find_elements(By.TAG_NAME, "td")
                    if len(tds) < 6:
                        continue

                    first_td = tds[0].text.strip()

                    if re.match(r"\d{2}\.\d{2}.*", first_td):
                        current_date = first_td[:5]
                        is_target_date = current_date == target_kbo_date
                        logger.debug(f"행 {row_index}: 날짜={current_date}, 목표={target_kbo_date}, 매치={is_target_date}")
                        if not is_target_date:
                            continue
                        game = self._parse_first_schedule_row(tds, current_date)
                        if game:
                            games.append(game)
                    elif re.match(r"\d{2}:\d{2}", first_td) and is_target_date:
                        game = self._parse_subsequent_schedule_row(tds, current_date)
                        if game:
                            games.append(game)
                except Exception as e:
                    logger.debug(f"Row {row_index} 파싱 실패: {e}")
                    continue
        except Exception as e:
            logger.debug(f"날짜 {date_string} 크롤링 실패: {e}")
        return games

    def _parse_first_schedule_row(self, tds: List, current_date: str) -> Optional[Dict]:
        try:
            if len(tds) < 8:
                return None
            time = tds[1].text.strip()
            match_info = tds[2].text.strip()
            stadium = tds[7].text.strip()
            logger.debug(f"[첫 경기 파싱] date={current_date}, time={time}, match_info={match_info}, stadium={stadium}")
            return self._parse_schedule_info(current_date, time, match_info, stadium)
        except Exception as e:
            logger.debug(f"첫 번째 일정 행 파싱 실패: {e}")
            return None

    def _parse_subsequent_schedule_row(self, tds: List, current_date: str) -> Optional[Dict]:
        try:
            if len(tds) < 7:
                return None
            time = tds[0].text.strip()
            match_info = tds[1].text.strip()
            stadium = tds[6].text.strip()
            logger.debug(f"[후속 경기 파싱] date={current_date}, time={time}, match_info={match_info}, stadium={stadium}")
            return self._parse_schedule_info(current_date, time, match_info, stadium)
        except Exception as e:
            logger.debug(f"후속 일정 행 파싱 실패: {e}")
            return None

    def _parse_schedule_info(self, date: str, time: str, match_info: str, stadium: str) -> Optional[Dict]:
        try:
            logger.debug(f"[일정 파싱 진입] date={date}, time={time}, match_info={match_info}, stadium={stadium}")
            if "vs" not in match_info:
                logger.debug("match_info에 'vs' 없음, 스킵")
                return None
            parts = match_info.split("vs")
            if len(parts) != 2:
                logger.debug("match_info split 실패, 스킵")
                return None
            away_team = re.sub(r'\d+', '', parts[0].strip())
            home_team = re.sub(r'\d+', '', parts[1].strip())
            logger.debug(f"away_team={away_team}, home_team={home_team}")

            if not away_team or not home_team:
                logger.debug("팀명 누락, 스킵")
                return None

            month, day = map(int, date.split('.'))
            hour, minute = map(int, time.split(':'))
            now_year = datetime.now().year
            naive_datetime = datetime(year=now_year, month=month, day=day, hour=hour, minute=minute)
            logger.debug(f"naive_datetime: {naive_datetime}")
            kst = pytz.timezone("Asia/Seoul")
            aware_datetime = kst.localize(naive_datetime)
            logger.debug(f"aware_datetime (KST): {aware_datetime}")
            game_datetime_str = aware_datetime.isoformat()

            game_id = generate_game_id_from_teams_date(date, away_team, home_team)

            result = {
                "awayTeam": away_team,
                "homeTeam": home_team,
                "awayScore": 0,
                "homeScore": 0,
                "stadium": stadium,
                "gameDateTime": game_datetime_str,
                "boxscoreUrl": None,
                "gameId": game_id,
                "status": "SCHEDULED"
            }
            logger.debug(f"[일정 파싱 완료] {away_team} vs {home_team} @ {game_datetime_str}")
            return result
        except Exception as e:
            logger.debug(f"[일정 파싱 실패] {match_info} -> {e}")
            return None