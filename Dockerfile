FROM python:3.11-slim

WORKDIR /app

# Dependências de sistema (ajuste se usar libs que precisem de compilação, ex: psycopg2)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copia e instala dependências primeiro (cache de build)
COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o restante do código
COPY app/ .

EXPOSE 8000

CMD ["uvicorn", "script:app", "--host", "0.0.0.0", "--port", "8000"]