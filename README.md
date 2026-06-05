# Case - NYC TLC

## Resumo executivo

Este projeto implementa uma solução de engenharia de dados para o case técnico, usando os dados públicos NYC TLC Trip Record Data de janeiro a maio de 2023.

A solução organiza arquivos Parquet brutos em uma landing zone, processa os dados com PySpark nas camadas bronze, silver e gold, gera métricas de qualidade e disponibiliza consultas SQL/PySpark para responder às perguntas obrigatórias do case.

O foco analítico é exclusivamente o dataset `yellow`, pois ele contém diretamente as colunas exigidas pelo enunciado: `VendorID`, `passenger_count`, `total_amount`, `tpep_pickup_datetime` e `tpep_dropoff_datetime`.

## Objetivo

Construir uma arquitetura simples, rastreável e explicável para:

- ingerir dados NYC TLC yellow taxi de janeiro a maio de 2023;
- usar PySpark em etapas relevantes do pipeline;
- disponibilizar dados para consumo analítico via SQL/PySpark;
- responder às perguntas obrigatórias:
  - Qual a média de valor total recebido em um mês considerando todos os yellow táxis da frota?
  - Qual a média de passageiros por cada hora do dia que pegaram táxi no mês de maio considerando todos os táxis da frota?

## Interpretação do escopo

Green, FHV e FHVHV não são implementados nesta versão. Eles podem ser considerados extensões futuras, com contratos próprios, porque não possuem o mesmo conjunto de colunas analíticas do yellow.

Essa decisão evita transformar artificialmente datasets diferentes apenas para encaixá-los nas perguntas obrigatórias.

## Arquitetura da solução

```text
landing -> bronze -> silver -> gold
```

- `landing`: arquivos Parquet brutos no layout oficial.
- `bronze`: ingestão com metadados técnicos e rastreabilidade.
- `silver`: padronização, tipagem e enriquecimento sem filtros destrutivos.
- `gold`: datasets de consumo, KPIs e relatório de qualidade.

## Estrutura do repositório

```text
configs/
  pipeline.yml
src/ifood_tlc/
  config.py
  paths.py
  spark.py
  ingestion/bronze.py
  transformations/silver_yellow.py
  transformations/gold_yellow.py
  quality/yellow_checks.py
  utils/organize_landing_files.py
  utils/inspect_landing_schemas.py
analysis/
  mandatory_questions.sql
  mandatory_questions_pyspark.py
  extra_insights.sql
  extra_insights_pyspark.py
  outputs/.gitkeep
notebooks/
  01_exploration.py
  02_run_pipeline_databricks.py
  03_case_analysis.py
  04_quality_report.py
docs/
  architecture.md
  data_dictionary.md
  data_quality.md
  delivery_checklist.md
tests/
```

A pasta `data/` e arquivos `.parquet` não devem ser versionados.

O repositório no GitHub não contém os dados brutos nem os Parquets processados. Para avaliação, um ZIP separado pode conter a pasta `data/` e os outputs pequenos de `analysis/outputs/`, caso seja necessário reproduzir ou evidenciar uma execução.

## Formas de avaliação

### 1. GitHub sem dados

O repositório no GitHub contém código, documentação, notebooks, testes e scripts de execução. Ele não contém `data/` nem arquivos Parquet.

Esse caminho é adequado para análise de arquitetura, organização do código, decisões técnicas, documentação e aderência ao case.

### 2. ZIP completo para o recrutador

Um ZIP de avaliação pode conter, além do código:

```text
data/landing/
data/bronze/
data/silver/
data/gold/
analysis/outputs/
```

Esse caminho facilita uma avaliação rápida sem exigir que o avaliador baixe novamente os dados da NYC TLC ou execute todo o pipeline do zero.

Os dados podem acompanhar o ZIP, mas continuam fora do versionamento Git. O GitHub permanece sem `data/` e sem Parquets.

Antes de publicar ou compactar a entrega, use o checklist prático em `docs/delivery_checklist.md`.

### 3. Execução com Docker

O Docker permite reproduzir o pipeline localmente sem configurar Java/JDK manualmente. Ele executa PySpark em modo local dentro de um único container; não é um cluster Spark distribuído.

Use este caminho quando o avaliador quiser reproduzir a execução localmente a partir dos Parquets posicionados em `data/landing/`.

### 4. Databricks Community

O Databricks Community continua disponível como alternativa para execução e demonstração técnica com Spark gerenciado. Os notebooks em `notebooks/` foram mantidos para esse fluxo.

## Tecnologias

- Python
- PySpark
- Spark SQL
- Parquet
- PyYAML
- pytest

## Preparação do ambiente local

Crie um ambiente virtual e instale o projeto em modo editável:

```bash
python -m venv .venv
.venv/Scripts/activate
pip install -r requirements.txt
pip install -e .
```

Alternativa sem instalação editável:

```bash
set PYTHONPATH=src
```

Para executar PySpark localmente, instale Java/JDK e configure `JAVA_HOME`. Em Databricks, o Spark já está disponível.

`DATA_DIR` é opcional. Se não for definido, o projeto usa `data/`.

```bash
set DATA_DIR=data
```

## Organização dos Parquets

Coloque os arquivos baixados inicialmente em:

```text
data/landing/raw_downloads/
```

Exemplos:

```text
data/landing/raw_downloads/yellow_tripdata_2023-01.parquet
data/landing/raw_downloads/yellow_tripdata_2023-02.parquet
data/landing/raw_downloads/yellow_tripdata_2023-03.parquet
data/landing/raw_downloads/yellow_tripdata_2023-04.parquet
data/landing/raw_downloads/yellow_tripdata_2023-05.parquet
```

Organize a landing:

```bash
python -m ifood_tlc.utils.organize_landing_files
```

Por segurança, o script copia por padrão. Para mover:

```bash
python -m ifood_tlc.utils.organize_landing_files --move
```

Layout final esperado:

```text
data/landing/tlc/yellow/year=2023/month=01/yellow_tripdata_2023-01.parquet
data/landing/tlc/yellow/year=2023/month=02/yellow_tripdata_2023-02.parquet
data/landing/tlc/yellow/year=2023/month=03/yellow_tripdata_2023-03.parquet
data/landing/tlc/yellow/year=2023/month=04/yellow_tripdata_2023-04.parquet
data/landing/tlc/yellow/year=2023/month=05/yellow_tripdata_2023-05.parquet
```

## Ordem final de execução

1. Organizar arquivos na landing.
2. Explorar schemas.
3. Ingerir bronze.
4. Transformar silver.
5. Construir gold.
6. Gerar quality report.
7. Executar perguntas obrigatórias.
8. Executar análises extras, se desejado.

## Exploração dos schemas

```bash
python -m ifood_tlc.utils.inspect_landing_schemas --service-types yellow
```

Saída esperada:

```text
analysis/landing_schema_summary.md
```

## Ingestão bronze

Janeiro:

```bash
python -m ifood_tlc.ingestion.bronze --service-type yellow --year 2023 --month 1
```

Janeiro a maio:

```bash
python -m ifood_tlc.ingestion.bronze --service-type yellow --year 2023 --all-config-months
```

Bronze usa `overwrite` por padrão para evitar duplicidade em reprocessamentos do mesmo escopo. A landing não é apagada.

Ao usar `--all-config-months`, o pipeline lê todos os meses configurados, une os DataFrames e escreve a bronze uma única vez. Isso evita que um mês sobrescreva os meses anteriores durante o reprocessamento completo.

## Transformação silver

Janeiro:

```bash
python -m ifood_tlc.transformations.silver_yellow --year 2023 --month 1
```

Janeiro a maio:

```bash
python -m ifood_tlc.transformations.silver_yellow --year 2023 --all-config-months
```

A silver valida as colunas obrigatórias do yellow antes da transformação e mantém registros suspeitos para auditoria.

Ao usar `--all-config-months`, a silver também transforma todos os meses configurados, une os DataFrames e escreve uma única vez, preservando janeiro a maio no mesmo reprocessamento.

## Construção gold

```bash
python -m ifood_tlc.transformations.gold_yellow --year 2023 --all-config-months
```

Saídas:

```text
data/gold/gold_yellow_trips
data/gold/gold_monthly_yellow_kpis
data/gold/gold_hourly_passenger_kpis
```

`gold_yellow_trips` preserva obrigatoriamente:

- `VendorID`
- `passenger_count`
- `total_amount`
- `tpep_pickup_datetime`
- `tpep_dropoff_datetime`

## Quality report

```bash
python -m ifood_tlc.quality.yellow_checks --year 2023 --all-config-months
```

Saída:

```text
data/gold/quality/quality_yellow_trips_report
```

O relatório mede problemas, mas não bloqueia o pipeline e não remove registros.

O modo padrão de escrita é `overwrite` para manter a reprodução do case determinística. O `run_id` continua registrado como metadado da execução.

## Perguntas obrigatórias

SQL:

```text
analysis/mandatory_questions.sql
```

PySpark:

```bash
python analysis/mandatory_questions_pyspark.py
```

O script imprime os resultados e, por padrão, grava outputs pequenos em:

```text
analysis/outputs/question_1_monthly_avg_total_amount/
analysis/outputs/question_1_monthly_avg_total_amount.md
analysis/outputs/question_2_may_hourly_avg_passengers/
analysis/outputs/question_2_may_hourly_avg_passengers.md
analysis/outputs/quality_report_summary/
analysis/outputs/quality_report_summary.md
```

Use `--skip-write` para apenas imprimir:

```bash
python analysis/mandatory_questions_pyspark.py --skip-write
```

Os resultados numéricos reais devem ser gerados após a execução do pipeline em Databricks ou ambiente local com Java/JDK.

`analysis/outputs/execution_summary.md` é um template para registrar ambiente, data, meses processados, contagens das camadas, status de qualidade e status de validações técnicas.

Outputs pequenos em CSV/Markdown podem ser versionados quando forem úteis como evidência. Parquets, dados de detalhe, caches e dados grandes devem ficar fora do GitHub.

## Smoke test

Depois de instalar o projeto e posicionar pelo menos o arquivo de janeiro, execute:

```bash
python -m ifood_tlc.utils.inspect_landing_schemas --service-types yellow --months 1
python -m ifood_tlc.ingestion.bronze --service-type yellow --year 2023 --month 1
python -m ifood_tlc.transformations.silver_yellow --year 2023 --month 1
python -m ifood_tlc.transformations.gold_yellow --year 2023 --month 1
python -m ifood_tlc.quality.yellow_checks --year 2023 --month 1
python analysis/mandatory_questions_pyspark.py --skip-write
```

Validações rápidas:

```python
spark.read.parquet("data/gold/gold_yellow_trips").printSchema()
spark.read.parquet("data/gold/gold_monthly_yellow_kpis").show()
spark.read.parquet("data/gold/quality/quality_yellow_trips_report").show()
```

## Databricks Community

O Databricks Community é uma alternativa para execução e demonstração técnica. Ele não é substituído pelo Docker; são caminhos complementares.

Use os notebooks em:

```text
notebooks/
```

Ordem recomendada:

1. `01_exploration.py`
2. `02_run_pipeline_databricks.py`
3. `03_case_analysis.py`
4. `04_quality_report.py`

Exemplo de layout DBFS:

```text
/dbfs/FileStore/ifood_tlc_case/project/src
/dbfs/FileStore/ifood_tlc_case/project/configs
/dbfs/FileStore/ifood_tlc_case/data/landing/tlc/yellow/year=2023/month=01/yellow_tripdata_2023-01.parquet
```

Nos notebooks:

```python
PROJECT_ROOT = "/dbfs/FileStore/ifood_tlc_case/project"
DATA_DIR = "/dbfs/FileStore/ifood_tlc_case/data"
```

Também é possível instalar o projeto no cluster:

```python
%pip install -e /dbfs/FileStore/ifood_tlc_case/project
```

Databricks Community tem recursos limitados. O foco demonstrável é yellow taxi de janeiro a maio.

## Execução com Docker

O Docker é uma facilidade de reprodução local para evitar configuração manual de Java/JDK. Ele executa PySpark local dentro de um único container; não é um cluster Spark.

Pré-requisito:

- Docker Desktop instalado.

Os arquivos Parquet devem estar no host em:

```text
data/landing/tlc/yellow/year=2023/month=01/yellow_tripdata_2023-01.parquet
data/landing/tlc/yellow/year=2023/month=02/yellow_tripdata_2023-02.parquet
data/landing/tlc/yellow/year=2023/month=03/yellow_tripdata_2023-03.parquet
data/landing/tlc/yellow/year=2023/month=04/yellow_tripdata_2023-04.parquet
data/landing/tlc/yellow/year=2023/month=05/yellow_tripdata_2023-05.parquet
```

Construa a imagem:

```bash
docker compose build
```

Valide o ambiente:

```bash
docker compose run --rm ifood-case bash scripts/check_environment.sh
```

Execute o pipeline completo:

```bash
docker compose run --rm ifood-case bash scripts/run_pipeline.sh
```

O compose monta o projeto em `/app` e a pasta local `data/` em `/app/data`. Os Parquets não são copiados para a imagem.

Resultados esperados:

```text
data/bronze/
data/silver/
data/gold/
analysis/outputs/
```

Para entrega via ZIP, a pasta `data/` pode acompanhar o pacote se o avaliador precisar reproduzir sem baixar os Parquets novamente. No GitHub, `data/` permanece ignorada.

O Databricks Community continua sendo uma alternativa documentada para execução com Spark gerenciado.

## Análises extras

Arquivos:

```text
analysis/extra_insights.sql
analysis/extra_insights_pyspark.py
```

Análises incluídas:

- receita média por hora;
- volume de corridas por dia;
- distribuição por forma de pagamento;
- distância versus valor por faixas;
- top horários por receita;
- duração média das corridas por mês.

## Qualidade de dados

O relatório mede:

- datas nulas;
- dropoff antes do pickup;
- valores negativos ou zerados;
- passageiros nulos, negativos ou zerados;
- distância negativa;
- duração inválida;
- registros fora do mês de referência.

Alguns valores suspeitos podem representar exceções de negócio, ajustes, reembolsos ou dados incompletos. A decisão de filtrar deve ser documentada em uma camada analítica.

## Decisões técnicas

- Foco exclusivo em yellow taxi nesta versão.
- PySpark para leitura, transformação, agregação e escrita.
- Bronze e silver idempotentes por padrão com `overwrite`.
- `reference_month` padronizado como string de dois dígitos.
- Gold falha se colunas obrigatórias estiverem ausentes.
- Quality report mede problemas sem bloquear pipeline.
- Notebooks são camada de orquestração, não duplicam o código principal.

## Testes

```bash
pytest
```

Testes unitários validam paths e formatação de mês. Testes com Spark usam DataFrames pequenos em memória. Se Java/JDK não estiver disponível localmente, os testes Spark são pulados com mensagem explícita.

## Limitações conhecidas

- Execução local com PySpark exige Java/JDK e `JAVA_HOME`.
- Resultados numéricos não são fixados no README.
- Green, FHV e FHVHV não são implementados nesta versão.
- Databricks Community pode ter limitação de memória e tempo.

## Próximos passos

- Executar o pipeline completo em ambiente Spark funcional.
- Versionar outputs pequenos de `analysis/outputs/` quando úteis como evidência.
- Adicionar validações formais de schema por versão.
- Avaliar extensão para outros service types com contratos separados.
