# Dicionário de dados

## Colunas obrigatórias do case

As colunas abaixo precisam estar disponíveis na camada de consumo:

- `VendorID`
- `passenger_count`
- `total_amount`
- `tpep_pickup_datetime`
- `tpep_dropoff_datetime`

## Silver yellow trips

`silver_yellow_trips` é gerada a partir da bronze yellow, sem filtros destrutivos.

Colunas derivadas:

- `vendor_id`: cópia padronizada de `VendorID`.
- `pickup_date`: data extraída de `tpep_pickup_datetime`.
- `pickup_hour`: hora extraída de `tpep_pickup_datetime`.
- `trip_duration_minutes`: diferença entre dropoff e pickup em minutos.

Se uma coluna obrigatória do case estiver ausente, a transformação falha com erro claro. Colunas opcionais ausentes podem ser criadas como nulas, com aviso, para manter o schema analítico estável.

## Gold datasets

`gold_yellow_trips` é o dataset de detalhe para consumo. Ele preserva:

- `VendorID`
- `passenger_count`
- `total_amount`
- `tpep_pickup_datetime`
- `tpep_dropoff_datetime`

Também inclui campos analíticos e de rastreabilidade quando disponíveis, como `vendor_id`, `pickup_date`, `pickup_hour`, `reference_year`, `reference_month`, `trip_duration_minutes`, `trip_distance`, `payment_type`, `fare_amount`, `tip_amount`, `source_file` e `ingestion_run_id`.

`gold_monthly_yellow_kpis` contém média de valor total, total de corridas e receita total por mês.

`gold_hourly_passenger_kpis` contém média de passageiros e total de corridas por hora de pickup.

No Spark, `AVG` ignora nulos na coluna agregada. `total_trips` conta todos os registros do grupo.
