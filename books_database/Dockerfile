FROM python:3.10-slim

WORKDIR /app

COPY books_database/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app:$PYTHONPATH

CMD ["python", "books_database/src/app.py"]
