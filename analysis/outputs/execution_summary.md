# Resumo de execução

## Ambiente de execução

- Ambiente: Docker local
- Docker/Databricks/local: Docker Desktop + PySpark local
- Versão do Python: 3.11.15
- Versão do Java: OpenJDK 21.0.11
- Versão do PySpark: 3.5.8

## Data da execução

- Data: 31/05/2026
- Responsável: Lucas Marques

## Meses processados

- Ano: 2023
- Meses: 01, 02, 03, 04, 05
- Service type: yellow

## Status do pipeline

- Landing organizada: OK
- Bronze concluída: OK
- Silver concluída: OK
- Gold concluída: OK
- Quality report concluído: OK
- Export de outputs concluído: OK

## Contagens

- Bronze: 16.186.386 linhas
- Silver: 16.186.386 linhas
- Gold `gold_yellow_trips`: 16.186.386 linhas
- Gold `gold_monthly_yellow_kpis`: 5 linhas
- Gold `gold_hourly_passenger_kpis`: 120 linhas
- Quality report: 60 linhas

## Outputs das perguntas obrigatórias

- Pergunta 1 Markdown: `analysis/outputs/question_1_monthly_avg_total_amount.md`
- Pergunta 2 Markdown: `analysis/outputs/question_2_may_hourly_avg_passengers.md`
- Quality report summary: `analysis/outputs/quality_report_summary.md`

## Validações técnicas

- `python -m compileall src`: OK
- `pytest`: 9 passed

## Observações finais

O pipeline foi executado com Docker em modo PySpark local. Os dados brutos e processados ficam fora do GitHub, mas podem acompanhar o ZIP de avaliação.