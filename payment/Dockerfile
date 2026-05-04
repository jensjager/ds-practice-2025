FROM python:3.12-slim

WORKDIR /app
COPY payment/requirements.txt /app/payment/requirements.txt
RUN pip install --no-cache-dir -r payment/requirements.txt

COPY utils/ /app/utils/
COPY payment/ /app/payment/

ENV PYTHONPATH=/app
ENV PAYMENT_PORT=50057
CMD ["python", "payment/src/app.py"]
