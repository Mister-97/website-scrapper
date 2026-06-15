# Playwright base image ships Chromium + all system deps
FROM mcr.microsoft.com/playwright/python:v1.60.0-noble

# App lives at /srv/app — separate from the Render disk mount at /app
WORKDIR /srv/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Persistent data (DB, config, previews) goes to /app which Render mounts as a disk
ENV DATA_DIR=/app

EXPOSE 8000
CMD ["python", "app.py"]
