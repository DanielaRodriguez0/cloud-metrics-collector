FROM python:3.12-slim

WORKDIR /monitoring

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY scripts ./scripts
COPY config ./config
COPY utils ./utils
COPY models ./models
ENV PYTHONPATH="#{PYTHONPATH}:/monitoring"

CMD ["python", "./scripts/collect_metrics.py"]
