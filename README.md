# PT Frozen Foods — Lakehouse Data Platform

Projeto de dados com foco em:

- Data Engineering
- Lakehouse Architecture
- Azure Data Platform
- Databricks
- Analytics
- Machine Learning

## Objetivo

Construir uma plataforma moderna de dados, partindo de dados-fonte
simulados até camadas Bronze, Silver e Gold, prontas para BI e ML.

## Contexto

O projeto é tratado como um caso real de negócio e arquitetura.
O nome "PT Frozen Foods" é utilizado como nome substituto por motivos
de confidencialidade.

Os dados publicados neste repositório são sintéticos, gerados para fins
de portfólio e demonstração técnica, preservando sigilo e privacidade.
Apesar disso, a modelagem, a lógica de negócio e a arquitetura seguem
princípios realistas de implementação.

## Arquitetura alvo

- Fonte: SharePoint / ficheiros simulados / APIs
- Armazenamento: ADLS Gen2
- Orquestração: Azure Data Factory
- Transformação: Databricks
- Formato: Delta Lake
- Consumo: Power BI + ML

## Estrutura do projeto

- 01_docs
- 02_infra
- 03_data
- 04_notebooks
- 05_src
- 06_outputs
- 07_tests

## Estado atual

Estrutura inicial criada.
Próximo passo: geração de dados-fonte e implementação das camadas do Lakehouse.
