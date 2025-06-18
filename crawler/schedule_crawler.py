# crawler/schedule_crawler.py

import requests
from typing import List, Dict

class KboScheduleCrawler:
    API_URL = "https://www.koreabaseball.com/ws/Schedule.asmx/GetScheduleList"

    def __init__(self, timeout: int = 10):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; KboCrawler/1.0)"
        })
        self.timeout = timeout

    def fetch_schedule(self, target_date: str) -> List[Dict]:
        """
        target_date: 'YYYY-MM-DD'
        → API 호출 후 해당 일자에 해당하는 경기만 필터링해
          awayTeam, homeTeam, awayScore, homeScore, stadium,
          gameDateTime, boxscore_url 을 담은 dict 리스트 반환
        """
        year, month, day = target_date.split("-")
        payload = {
            "leId":      1,
            "srIdList":  "0,9,6",          # 정규시즌
            "seasonId":  year,             # ex. "2025"
            "gameMonth": str(int(month)),  # ex. "6"
            "teamId":    ""                # 전체 팀
        }

        resp = self.session.post(self.API_URL, data=payload,
                                 timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()

        # ASMX 응답 구조: data.d.rows 또는 data.rows
        rows = data.get("d", {}).get("rows") or data.get("rows") or []
        want_prefix = f"{year}-{month}-{day}"

        games = []
        for r in rows:
            # JSON 필드명은 실제 내려오는 키로 정확히 맞춰야 합니다.
            # 아래 예시는 자주 쓰이는 필드명을 가정한 것이니, 
            # rows[0].keys() 로 한 번 찍어보시고 실제 키로 바꿔주세요!
            gdate = r.get("GDATE") or r.get("PlayDate") or ""
            if not gdate.startswith(want_prefix):
                continue

            # 팀명, 점수
            away  = r.get("AT_NM")      or r.get("AwayName")
            home  = r.get("HT_NM")      or r.get("HomeName")
            a_sc  = int(r.get("AT_SR")  or r.get("AwayScore") or 0)
            h_sc  = int(r.get("HT_SR")  or r.get("HomeScore") or 0)

            # 시간, 구장
            time  = r.get("G_TM")       or r.get("GameTime")
            std   = r.get("ST_NM")      or r.get("StadiumName")

            # 박스 URL 단편/전체
            frag  = r.get("T1_URL")     or r.get("BoxScoreUrl") or ""
            if frag:
                url = (frag if frag.startswith("http")
                       else "https://www.koreabaseball.com" + frag)
                # REVIEW 섹션 강제 지정
                if "section=" not in url:
                    url += "&section=REVIEW"
            else:
                url = None

            if away and home:
                games.append({
                    "awayTeam":     away,
                    "homeTeam":     home,
                    "awayScore":    a_sc,
                    "homeScore":    h_sc,
                    "stadium":      std,
                    "gameDateTime": time,
                    "boxscore_url": url,
                })

        return games