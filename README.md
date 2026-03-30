\# Pipeline de Dados com dbt + Dremio + Docker



\## Visão Geral



Este projeto implementa um pipeline de dados utilizando:



\* dbt para transformação de dados

\* Dremio como engine de consulta

\* Docker para containerização



O objetivo é padronizar, transformar e analisar dados fiscais, integrando informações de SPED e XML de NF-e.



\---



\## Arquitetura



Fontes de Dados (SPED / XML / Parquet)

↓

Dremio (Query Engine)

↓

dbt (Transformações)

↓

Camadas Analíticas (staging, fatos, auditoria)



\---



\## Estrutura do Projeto



```

dbt-dremio/

│── Dockerfile

│── docker-compose.yml

│── .gitignore

│

└── teste\_dremio\_dbt/

&#x20;   │── models/

&#x20;   │── profiles.yml

&#x20;   │── dbt\_project.yml

```



\---



\## Tecnologias Utilizadas



\* dbt (Data Build Tool)

\* Dremio OSS

\* Docker

\* SQL

\* Data Lake (Parquet / SPED / XML)



\---



\## Como Executar



\### Subir ambiente



```bash

docker compose up -d

```



\### Testar conexão



```bash

docker compose run dbt dbt debug --profiles-dir /usr/app

```



\### Executar transformações



```bash

docker compose run dbt dbt run --profiles-dir /usr/app

```



\---



\## Acesso ao Dremio



http://localhost:9047



\---



\## Camadas de Dados



\### Staging (`stg\_\*`)



Padronização dos dados brutos



\### Fato (`fct\_\*`)



Regras de negócio e consolidação



\### Auditoria (`diag\_\*`)



Validação e identificação de divergências



\---



\## Funcionalidades



\* Integração entre múltiplas fontes

\* Transformações SQL versionadas

\* Execução via containers

\* Criação de views analíticas no Dremio

\* Base para auditoria fiscal



\---



\## Testes



```bash

docker compose run dbt dbt test --profiles-dir /usr/app

```



\---



\## Próximos Passos



\* Integração com Airflow

\* Conexão com Google Cloud Storage

\* Modelos incrementais

\* Camada analítica final (BI)

\* CI/CD



\---



