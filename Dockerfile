FROM python:3.12-slim

WORKDIR /monitoring

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/ ./scripts/
COPY config/ ./config/

CMD ["python", "./scripts/collect_metrics.py"]
