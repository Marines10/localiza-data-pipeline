# localiza-data-pipeline

## Objetivo do projeto

Implementação de um pipeline de dados com:
- ingestão de um CSV de transações de fraude
- transformação de dados em camadas `landing`, `raw`, `curated`
- geração de tabelas analíticas
- monitoramento de qualidade de dados
- orquestração com Apache Airflow
- execução via Docker

## Funcionalidades entregues

1. Importação do arquivo CSV para a camada `landing`
2. Gerenciamento de dados brutos (`raw`) em Delta Lake
3. Limpeza e transformação de dados para a camada `curated`
4. Geração de resultados analíticos:
   - média de `risk_score` por `location_region` ordenada em ordem decrescente
   - top 3 `receiving_address` com maior `amount` considerando apenas a transação mais recente com `transaction_type = sale`
5. Relatório de qualidade de dados com indicadores de registro e erros

## Estrutura do repositório

- `Dockerfile` - imagem do Airflow com Java, PySpark e Delta
- `docker-compose.yml` - define o container Airflow para orquestração local
- `requirements.txt` - dependências Python do projeto
- `dags/fraud_transactions_pipeline_dag.py` - DAG do Airflow que orquestra o pipeline
- `fraud_transactions/source_data/df_fraud_credit.csv` - arquivo de origem (não está no repositório por causa do tamanho)
- `fraud_transactions/jobs/ingestion/` - jobs de landing, raw e curated
- `fraud_transactions/jobs/analytics/` - jobs de analytics para os resultados exigidos
- `fraud_transactions/data_quality/` - job de qualidade de dados e geração de relatório
- `fraud_transactions/utils/paths.py` - define caminhos de arquivos e camadas
- `fraud_transactions/utils/spark_session.py` - configura Spark local com Delta Lake

## Pré-requisitos

### Recomendado

- Git
- Python 3.11
- Docker Desktop (ou Docker Engine)
- Docker Compose
- Visual Studio Code (opcional)

### Dependências Python

As dependências estão em `requirements.txt`:
- `pyspark`
- `delta-spark`
- `apache-airflow`

## Clonar o repositório

```bash
git clone https://github.com/Marines10/localiza-data-pipeline.git
cd localiza-data-pipeline
```

## Criar ambiente virtual e instalar dependências

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

> Se estiver usando Windows, ative o ambiente com:
>
> ```powershell
> .\.venv\Scripts\Activate.ps1
> ```

## Obter o arquivo de entrada

O arquivo CSV original não está no repositório por conta do limite de tamanho do GitHub.
Baixe o CSV a partir do link do desafio e copie para:

```bash
mkdir -p fraud_transactions/source_data
cp /caminho/para/df_fraud_credit.csv fraud_transactions/source_data/df_fraud_credit.csv
```

> Também funciona se você mover o arquivo direto para `fraud_transactions/source_data/df_fraud_credit.csv`.

## Visão geral das camadas

### `source_data`
Dados de entrada originais, sem alterações.

### `landing`
A cópia bruta do arquivo CSV original, organizada por data de execução.

### `raw`
Dados lidos do `landing`, transformados em Delta Lake, com normalização de nomes de coluna e adição da coluna `processing_date`.

### `curated`
Dados limpos e tipados:
- timestamp convertido para `timestamp`
- valores `amount` e `risk_score` convertidos para `double`
- strings limpas com `trim`
- filtros contra valores nulos ou inválidos

### `analytics`
Resultados finais:
- `avg_risk_score_by_region` (`location_region`, `avg_risk_score`)
- `top3_receiving_address_by_latest_sale_amount` (`receiving_address`, `amount`, `timestamp`)

### `quality`
Relatório JSON com métricas de qualidade como contagem de registros, erros e percentual de conformidade.

## Como rodar cada etapa manualmente

Execute sempre a partir da raiz do repositório.

### 1) Landing

```bash
source .venv/bin/activate
python -m fraud_transactions.jobs.ingestion.landing_job
```

### 2) Raw

```bash
python -m fraud_transactions.jobs.ingestion.raw_job
```

### 3) Curated

```bash
python -m fraud_transactions.jobs.ingestion.curated_job
```

### 4) Analytics - média de risco por região

```bash
python -m fraud_transactions.jobs.analytics.avg_risk_score_by_region_job
```

### 5) Analytics - top 3 receiving_address

```bash
python -m fraud_transactions.jobs.analytics.top3_receiving_address_job
```

### 6) Quality

```bash
python -m fraud_transactions.data_quality.data_quality_job
```

## Como rodar a pipeline completa com Docker + Airflow

### 1) Construir e iniciar o Airflow

```bash
docker compose up --build
```

ou em segundo plano:

```bash
docker compose up -d --build
```

### 2) Abrir o Airflow

Acesse:

```text
http://localhost:8080
```

Use o usuário e senha padrão:
- usuário: `airflow`
- senha: `airflow`

### 3) Ativar e executar a DAG

No Airflow UI:
- clique em `DAGs`
- ative a DAG `fraud_transactions_pipeline`
- clique em `Trigger DAG` para iniciar o fluxo

### 4) Ordem do fluxo

A DAG executa os jobs nesta sequência:

1. `landing`
2. `raw`
3. `curated`
4. `avg_risk_score_by_region`
5. `top3_receiving_address_by_latest_sale_amount`
6. `data_quality`

## Onde estão os arquivos de saída

- Landing: `fraud_transactions/sink_data/landing/fraud_transactions/<data>/df_fraud_credit.csv`
- Raw Delta: `fraud_transactions/sink_data/raw/fraud_transactions`
- Curated Delta: `fraud_transactions/sink_data/curated/fraud_transactions`
- Analytics Delta:
  - `fraud_transactions/sink_data/analytics/avg_risk_score_by_region`
  - `fraud_transactions/sink_data/analytics/top3_receiving_address_by_latest_sale_amount`
- Relatório de qualidade: `fraud_transactions/sink_data/quality/fraud_transactions_quality_report.json`

## Dicas para avaliação

- O arquivo `df_fraud_credit.csv` ficou fora do Git por causa do limite de 100 MB do GitHub.
- O código usa Delta Lake local e Python com PySpark, então o fluxo roda tanto localmente quanto dentro do container do Airflow.
- Se quiser testar apenas um job, execute o módulo Python direto a partir da raiz do projeto.
- Se quiser validar os dados, confira o arquivo JSON em `fraud_transactions/sink_data/quality/fraud_transactions_quality_report.json`.

## Recomendações de uso do Visual Studio Code

- Abra a pasta do projeto inteira em VS Code.
- Instale as extensões `Python` e `Docker`.
- Ative o terminal integrado e rode:

```bash
source .venv/bin/activate
```

- Use a árvore de arquivos para inspecionar o DAG e os jobs.

## Observações finais

Este projeto foi construído para atender ao desafio técnico com:
- ingestão e processamento em camadas
- orquestração com Airflow
- containerização com Docker
- data quality automatizada
