FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir elasticsearch pika
COPY detector.py .
CMD ["python", "-u", "detector.py"]
