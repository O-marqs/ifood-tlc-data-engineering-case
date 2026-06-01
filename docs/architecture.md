# Arquitetura

Este projeto usa uma arquitetura em camadas para processar os dados NYC TLC yellow taxi de janeiro a maio de 2023.

```text
landing -> bronze -> silver -> gold
```

O foco desta versão é exclusivamente `yellow`, porque as perguntas obrigatórias e as colunas exigidas pelo case pertencem diretamente a esse dataset. Green, FHV e FHVHV podem ser avaliados como extensões futuras, com contratos próprios.

## Landing

Camada de arquivos brutos. Os Parquets devem ser mantidos no layout:

```text
data/landing/tlc/yellow/year={yyyy}/month={mm}/yellow_tripdata_{yyyy}-{mm}.parquet
```

Também é possível baixar os arquivos primeiro em `data/landing/raw_downloads/` e organizá-los com:

```bash
python -m ifood_tlc.utils.organize_landing_files
```

## Bronze

Camada de ingestão com metadados técnicos:

- `service_type`
- `reference_year`
- `reference_month`
- `source_file`
- `ingestion_timestamp`
- `ingestion_run_id`

A bronze é escrita em Parquet particionado por `service_type`, `reference_year` e `reference_month`. O modo padrão é `overwrite` para manter reprocessamentos idempotentes no escopo processado. Na execução com todos os meses configurados, os meses são lidos, unidos e escritos uma única vez para evitar sobrescrita parcial mês a mês.

## Silver

Camada padronizada e enriquecida para yellow taxi. Ela mantém os registros suspeitos e não aplica filtros destrutivos.

Campos derivados:

- `vendor_id`
- `pickup_date`
- `pickup_hour`
- `trip_duration_minutes`

Antes da transformação, a silver valida explicitamente as colunas obrigatórias do yellow.

Na execução com todos os meses configurados, a silver transforma os meses disponíveis em memória lógica de Spark, une os DataFrames e escreve uma única vez. Isso preserva janeiro a maio em reprocessamentos completos.

## Gold

Camada de consumo analítico:

- `gold_yellow_trips`
- `gold_monthly_yellow_kpis`
- `gold_hourly_passenger_kpis`
- `quality/quality_yellow_trips_report`

A gold preserva as colunas obrigatórias do case em `gold_yellow_trips`. Colunas obrigatórias ausentes causam erro claro; colunas opcionais podem ser preenchidas como nulas com aviso.

## Configuração

A configuração central fica em `configs/pipeline.yml`. A raiz dos dados pode ser alterada com `DATA_DIR`. Se a variável não for definida, o padrão é `data/`.
