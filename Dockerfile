
FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    postgresql-client \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p datasets && chmod 777 datasets

COPY start.sh /start.sh
RUN chmod +x /start.sh

COPY . .

RUN chmod -R 777 datasets/ && \
    chmod +x /start.sh

ENV DB_USER=postgres
ENV DB_PASSWORD=postgres
ENV DB_HOST=db
ENV DB_PORT=5432
ENV DB_NAME=consumos_db
ENV DATA_FOLDER=datasets
ENV BATCH_SIZE=1000

EXPOSE 5432

CMD ["/bin/bash", "/start.sh"] 