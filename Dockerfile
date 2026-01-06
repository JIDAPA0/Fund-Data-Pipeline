FROM mcr.microsoft.com/playwright/python:v1.40.0-jammy
ENV PYTHONUNBUFFERED=1
WORKDIR /app
RUN apt-get update && apt-get install -y \
    cron \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Bangkok

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN playwright install chromium

COPY . .

RUN chmod +x entrypoint.sh

CMD ["python", "src/05_db_synchronization/01_master_sync/07_master_sync_orchestrator.py"]