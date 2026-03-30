# 🛡️ Portal de Auditoria Fiscal Digital

## Visão Geral do Projeto

Este projeto dbt foi desenvolvido para automatizar e centralizar o **Confronto Fiscal** entre as operações emitidas pela empresa (armazenadas no **MySQL**) e as operações declaradas ao fisco (arquivos **SPED EFD ICMS/IPI** armazenados no **Google Cloud Platform - GCP**).

O objetivo principal é identificar divergências de valores e chaves de Notas Fiscais Eletrônicas (NF-e) de forma proativa, evitando multas e garantindo a integridade das declarações fiscais.

---

## 🏗️ Arquitetura Modern Data Lakehouse

Nossa solução utiliza uma arquitetura moderna que une a flexibilidade do Data Lake com a performance do Data Warehouse, sem a necessidade de migração física de dados.

| Componente | Tecnologia | Função |
| :--- | :--- | :--- |
| **Fontes de Dados (Local)** | **MySQL** | Armazena os XMLs das NF-e emitidas (Capa e Itens). |
| **Fontes de Dados (Nuvem)** | **GCP (Parquet)** | Armazena os arquivos SPED processados em formato colunar. |
| **Motor de Integração** | **Dremio** | Executa a Federação de Dados (JOIN em tempo real entre MySQL e GCP). |
| **Transformação e Regras** | **dbt (Data Build Tool)** | Define a lógica de negócio, limpa os dados e cria as visões de auditoria. |

---

## 📊 Estrutura de Modelagem de Dados (dbt)

A modelagem segue as melhores práticas do dbt, dividida em camadas semânticas:

### 1. Camada Staging (`models/staging`)
* **Limpeza e Padronização:** Renomeia colunas complexas do SPED (ex: `VL_ICMS` para `vICMS`), faz cast de tipos de dados e padroniza formatos de data.
* *Fontes:* `gcp_sped` (registros 0000, C100, C170, E110) e `mysql_xml` (prod, ide).

### 2. Camada Marts (`models/marts`)
* **Lógica de Negócio e Fatos:** Onde a auditoria acontece.
* **Modelo Principal:** `fct_confronto_xml_vs_sped`. Este modelo realiza o JOIN final entre o XML e o SPED usando a chave da NF-e e aponta as divergências.

---

## 🚀 Como Utilizar esta Documentação

Utilize o menu lateral para navegar entre as **Fontes de Dados (Sources)** e os **Modelos (Models)**.

**Dica Principal:** Clique no botão azul redondo no canto inferior direito (**Lineage Graph**) para ver o mapa visual de como os dados fluem desde o MySQL/GCP até o relatório final de auditoria.

---
### Contato
Para dúvidas sobre as regras de negócio ou a arquitetura, entre em contato com o time de **Suporte Fiscal**.