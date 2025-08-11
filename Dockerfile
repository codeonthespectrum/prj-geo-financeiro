# Use uma imagem e do Python
FROM python:3.11-slim

RUN apt-get update \
	&& apt-get install -y --no-install-recommends curl jq ca-certificates \
	&& rm -rf /var/lib/apt/lists/*

# Defina o diretório de trabalho no contêiner
WORKDIR /usr/src/app

# Copie o arquivo de dependências e instale-as
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copie o resto do código do seu projeto para o contêiner
COPY . .

# Comando padrão (pode ser sobrescrito pelo docker-compose)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]