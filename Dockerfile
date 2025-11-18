# Use slim Python base image
FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    wget gnupg ca-certificates unzip \
    fonts-liberation libasound2 libnspr4 libnss3 libx11-6 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libxkbcommon0 libdrm2 libgbm1 \
    libgtk-3-0 xdg-utils libxshmfence1 \
  && rm -rf /var/lib/apt/lists/*

RUN wget -qO- https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor > /usr/share/keyrings/google-linux.gpg \
  && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux.gpg] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
  && apt-get update && apt-get install -y google-chrome-stable \
  && rm -rf /var/lib/apt/lists/*
# Set the working directory inside the container
WORKDIR /app

# Directory to persist Chrome user data (mount a volume here when running)
ENV CHROME_PROFILE_DIR=/data/chrome-profile
ENV HTML_DUMP_DIR=/data/html-dumps
RUN mkdir -p ${CHROME_PROFILE_DIR} ${HTML_DUMP_DIR}
VOLUME ["/data/chrome-profile", "/data/html-dumps"]

# Copy requirements and install first (for caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Python application code
COPY app/ ./app

# Expose port 8000 (same as original CMD)
EXPOSE 8008

# Run with the same command as the old image
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8008"]

