# Qualidade de dados

O relatório de qualidade é gerado a partir de `silver_yellow_trips` e gravado em:

```text
data/gold/quality/quality_yellow_trips_report
```

Cada linha representa a avaliação de uma regra por `reference_year` e `reference_month`.

## Campos

- `run_id`: identificador da execução.
- `service_type`: atualmente `yellow`.
- `reference_year`: ano de referência.
- `reference_month`: mês de referência em dois dígitos.
- `rule_name`: regra avaliada.
- `total_records`: registros avaliados no período.
- `failed_records`: registros que violaram a regra.
- `failed_percentage`: percentual de falhas.
- `created_at`: timestamp de criação do relatório.

## Regras avaliadas

- pickup nulo;
- dropoff nulo;
- dropoff antes do pickup;
- valor total nulo, negativo ou zerado;
- passageiros nulos, negativos ou zerados;
- distância negativa;
- duração inválida;
- registros fora do mês de referência.

## Interpretação

O relatório é observacional. Ele não bloqueia o pipeline e não remove registros.

Para este case, a escrita do relatório usa `overwrite` por padrão. Essa decisão favorece reprodutibilidade durante reprocessamentos locais ou no Docker. O campo `run_id` permanece no dataset para identificar a execução que gerou o relatório mais recente.

Alguns valores suspeitos podem representar exceções de negócio, ajustes, reembolsos, cancelamentos ou dados incompletos reportados pela origem. Antes de aplicar filtros destrutivos em camadas analíticas, a decisão deve ser documentada.
