FROM python:3.11

WORKDIR /app

ENV PYTHONPATH=/app:$PYTHONPATH

COPY ./order_executor/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

CMD python utils/other/hotreload.py "order_executor/src/app.py"