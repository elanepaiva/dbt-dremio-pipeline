FROM python:3.9-slim
# Instalando curl para testes e dependências do dbt
RUN apt-get update && apt-get install -y curl git && rm -rf /var/lib/apt/lists/*
RUN pip install dbt-dremio
WORKDIR /usr/app