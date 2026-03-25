FROM python:3.12-slim
WORKDIR /app
COPY sidecar.py .
RUN pip install pika
CMD ["python", "-u", "sidecar.py"]
