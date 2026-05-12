FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends \
    libcairo2 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libgdk-pixbuf-2.0-0 \
    libffi8 \
    shared-mime-info \
    libpangoft2-1.0-0 \
    libharfbuzz0b \
    libjpeg62-turbo \
    libopenjp2-7 \
    libtiff6 \
    libwebp7 \
    libwebpdemux2 \
    libwebpmux3 \
    libx11-6 \
    libxcb1 \
    libxext6 \
    libxrender1 \
    libxrandr2 \
    libxss1 \
    libxtst6 \
    fonts-liberation \
    fontconfig \
 && rm -rf /var/lib/apt/lists/*
COPY detector/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY detector/detector.py .
COPY detector/templates ./templates
CMD ["python", "-u", "detector.py"]
