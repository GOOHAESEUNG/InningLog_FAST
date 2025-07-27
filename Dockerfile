# ========== 1단계: 크롬 + 드라이버 설치만 따로 진행 ==========
FROM debian:bullseye-slim AS chrome-builder

# 필요한 패키지 설치
RUN apt-get update && apt-get install -y wget unzip curl gnupg ca-certificates libglib2.0-0 libnss3 libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 libasound2 libatk1.0-0 libatk-bridge2.0-0 libcups2 xdg-utils --no-install-recommends

# Chrome 설치 (126 버전)
RUN mkdir -p /opt/chrome && \
    wget -q https://storage.googleapis.com/chrome-for-testing-public/126.0.6478.114/linux64/chrome-linux64.zip && \
    unzip chrome-linux64.zip && \
    mv chrome-linux64 /opt/chrome/chrome126

# ChromeDriver 설치
RUN wget -q https://edgedl.me.gvt1.com/edgedl/chrome/chrome-for-testing/126.0.6478.114/linux64/chromedriver-linux64.zip && \
    unzip chromedriver-linux64.zip && \
    mv chromedriver-linux64/chromedriver /opt/chromedriver && \
    chmod +x /opt/chromedriver

# ========== 2단계: 실제 실행할 FastAPI 앱 + 크롬 복사 ==========
FROM python:3.10-slim

# 기본 패키지 설치
RUN apt-get update && apt-get install -y libglib2.0-0 libnss3 libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 libasound2 libatk1.0-0 libatk-bridge2.0-0 libcups2 xdg-utils curl && apt-get clean

# 크롬 & 드라이버 복사
COPY --from=chrome-builder /opt/chrome/chrome126 /opt/chrome/chrome126
COPY --from=chrome-builder /opt/chromedriver /usr/local/bin/chromedriver

# 심볼릭 링크 설정
RUN ln -sf /opt/chrome/chrome126/chrome /usr/bin/google-chrome

# 작업 디렉토리
WORKDIR /app

# 앱 파일 복사
COPY . .

# requirements 설치
RUN pip install --upgrade pip && pip install -r requirements.txt

# 환경변수 설정
ENV CHROME_BIN="/usr/bin/google-chrome"
ENV PATH="$PATH:/usr/local/bin"

# FastAPI 실행
CMD ["uvicorn", "crawl_api:app", "--host", "0.0.0.0", "--port", "8000"]