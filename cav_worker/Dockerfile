FROM python:3.11-slim

# Install system deps for Playwright Chromium
# Note: ttf-unifont → fonts-unifont, ttf-ubuntu-font-family → fonts-ubuntu on Debian Trixie
# We skip `playwright install-deps` and manage deps manually to avoid obsolete package names
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    ca-certificates \
    fonts-liberation \
    fonts-unifont \
    libasound2t64 \
    libatk-bridge2.0-0t64 \
    libatk1.0-0t64 \
    libcups2t64 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0t64 \
    libnspr4 \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    xdg-utils \
    libxshmfence1 \
    libglu1-mesa \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright Chromium browser only (skip install-deps, managed above)
RUN playwright install chromium

COPY app.py .

EXPOSE 8090

CMD gunicorn app:app --bind 0.0.0.0:${PORT:-8090} --workers 2 --timeout 120
