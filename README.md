# Geo-Financeiro SP — API e Scripts

## Scripts relevantes

- scripts/load_geography.py: carrega setores (SP_setores_CD2022.shp) para PostGIS.
- scripts/load_pois.py: carrega estações de metrô (pois_metro_sp) para PostGIS.
- scripts/create_features.py: cria colunas de distância ao metrô (total e por linha).
- scripts/load_census.py: carrega renda (vl_renda) agregada (IBGE Agregados v3).
- scripts/load_census_sector_income.py: calcula mediana sintética por município e preenche vl_renda_setor.

## Mediana sintética (vl_renda_setor)

Entrada recomendada: CSV com distribuição de renda per capita por classes por município.

Variáveis de ambiente úteis:
- CENSO_CLASSES_CSV: caminho do CSV (default: data/censo_renda_percapita_classes_mun.csv)
- IBGE_PERIODOS: ano de referência (ex.: 2022)
- SALARIO_MIN_2022, SALARIO_MIN_2024, etc.: salário mínimo para conversão de classes em SM para R$

Execução via Docker Compose:

```sh
docker compose run --rm app python scripts/load_census_sector_income.py
```

Modo IBGE (experimental):
- USE_IBGE=1
- CLASSES_TABELA, CLASSES_VARIAVEL, CLASSES_CLASSIFICACAO

Exemplo:

```sh
USE_IBGE=1 CLASSES_TABELA=XXXX CLASSES_VARIAVEL=YYYY CLASSES_CLASSIFICACAO=ZZZZ[cat1,cat2,...] \
  docker compose run --rm app python scripts/load_census_sector_income.py
```

O script irá atualizar a coluna vl_renda_setor na tabela sp_setores via join por CD_MUN.
