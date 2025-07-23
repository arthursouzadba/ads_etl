FROM python:3.9-slim

WORKDIR /app

# Instala dependências do sistema
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev && \
    rm -rf /var/lib/apt/lists/*

# Copia requirements primeiro para cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o resto do código
COPY . .

# Configura healthcheck
HEALTHCHECK --interval=30s --timeout=3s \
    CMD ps aux | grep '[p]ython src/etl.py' || exit 1

# Comando de entrada
CMD ["python", "-u", "etl_drone.py"]