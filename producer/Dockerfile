FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .
COPY price_producer.py .
COPY start.sh .
RUN chmod +x start.sh

CMD ["./start.sh"]
