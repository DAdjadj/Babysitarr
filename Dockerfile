FROM python:3.12-slim

WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends sqlite3 && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir requests
COPY babysitarr.py .

CMD ["python", "-u", "babysitarr.py"]
