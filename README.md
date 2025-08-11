# 📍 [Engenharia de Dados] Dados Geoespaciais e Socioeconomicos de São Paulo - Descubra quais regiões as estações e linhas de metrô mais influenciam no setor econômico
# MODELO MVP


[![Python](https://img.shields.io/badge/Python-3.10%2B-blue?logo=python)](https://www.python.org/)
[![PostGIS](https://img.shields.io/badge/PostGIS-Enabled-success?logo=postgresql)](https://postgis.net/)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)](https://www.docker.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

> Pipeline de processamento e análise geoespacial para integrar dados socioeconômicos e geográficos da cidade de São Paulo.

---

## ✨ Objetivo

O **prj-geo-financeiro** automatiza o **carregamento, transformação e análise** de dados geoespaciais e socioeconômicos no **PostGIS**, incluindo cálculo de **mediana sintética de renda** por setor censitário.

Com isso, cria-se uma base única para **análises espaciais**, **integrações de dados** e **visualizações**.

---

## 🗂 Estrutura do Projeto

```
prj-geo-financeiro/
├── docker-compose.yml        # Orquestração do ambiente (PostGIS + Python)
├── Dockerfile                 # Ambiente Python configurado para o pipeline
├── scripts/
│   ├── load_shapefiles.py     # Carrega setores censitários
│   ├── load_pois.py           # Importa pontos de interesse
│   ├── load_features.py       # Calcula distâncias
│   ├── load_census.py         # Importa dados de renda
│   ├── calculate_median.py    # Calcula mediana sintética
├── data/                      # Dados brutos (CSV, shapefiles)
└── index.html                 # Visualização (opcional)
```

---

## ⚙️ Tecnologias Utilizadas

* **Python 3.10+**
* **PostgreSQL + PostGIS**
* **Docker & Docker Compose**
* **Bibliotecas Python**:

  * `geopandas`
  * `psycopg2`
  * `pandas`
  * `shapely`
  * `sqlalchemy`

---

## 📥 Pré-requisitos

* [Docker](https://www.docker.com/get-started)
* [Docker Compose](https://docs.docker.com/compose/)
* Arquivos de dados organizados em `data/`
* Arquivo `.env` configurado com as variáveis corretas

Exemplo de `.env`:

```env
IBGE_YEAR=2010
MINIMUM_WAGE=510
CSV_PATH=data/census.csv
```

---

## 🚀 Como Executar

1️⃣ **Clone o repositório**

```bash
git clone https://github.com/codeonthespectrum/prj-geo-financeiro.git
cd prj-geo-financeiro
```

2️⃣ **Suba o ambiente**

```bash
docker compose up --build
```

3️⃣ **Execute os scripts na ordem**

```bash
docker compose run --rm app python scripts/load_shapefiles.py
docker compose run --rm app python scripts/load_pois.py
docker compose run --rm app python scripts/load_features.py
docker compose run --rm app python scripts/load_census.py
docker compose run --rm app python scripts/calculate_median.py
```

4️⃣ **Acesse o banco PostGIS**

```bash
docker compose exec db psql -U postgres -d geo_financeiro
```

---

## 📊 Exemplos de Consultas SQL

📍 **Top 5 setores com maior renda mediana**

```sql
SELECT setor_id, renda_mediana
FROM setores
ORDER BY renda_mediana DESC
LIMIT 5;
```

🚇 **Distância média de cada setor até o metrô**

```sql
SELECT setor_id, AVG(dist_metro) AS distancia_media
FROM distancias
GROUP BY setor_id
ORDER BY distancia_media ASC;
```

---

## 🔮 Próximos Passos

* [ ] Criar API REST para servir os dados processados
* [ ] Adicionar testes automatizados
* [ ] Melhorar tratamento de erros e logs
* [ ] Criar dashboard interativo (Power BI, Metabase ou Streamlit)
* [ ] Integrar novos POIs (escolas, hospitais, transporte público)

---

## 📄 Licença

Este projeto está sob a licença [MIT](LICENSE).

---

## 👤 Autor

Projeto criado por **[Gabrielly Gomes](https://github.com/codeonthespectrum)** 🐙💡

