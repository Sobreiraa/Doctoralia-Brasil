# Doctoralia Brasil – Projeto de Engenharia de Dados

## Descrição

Este projeto consiste na construção de um pipeline de dados completo utilizando o dataset **Doctoralia Brasil**, com o objetivo de simular uma solução analítica através dos dados fornecidos.

O projeto inclui todas as etapas de um fluxo de engenharia de dados:

- ingestão de dados
- transformação
- modelagem dimensional
- estruturação de um Data Warehouse

Além disso, foram desenvolvidos modelos conceitual, lógico e físico para representar a estrutura dos dados.

---

## Objetivo do Projeto

Simular a construção de uma plataforma analítica capaz de responder perguntas de negócio como:

- distribuição de médicos por especialidade
- avaliação média de médicos
- tipos de consulta realizadas
- análise de consultas por período
- disponibilidade de médicos

A solução final permite gerar relatórios analíticos e alimentar dashboards.

OBS: Alguns dados são falsos, sendo gerados apenas para FINS DE ESTUDOS.

---

## Modelagem de Dados

O projeto utiliza **modelagem dimensional (Star Schema)**.

## Tecnologias Utilizadas

- Python
- Pandas
- Pandera (validação de dados)
- CSV
- Modelagem Dimensional
- Data Warehouse concepts

## Fluxo executado:

1. Extração dos dados
2. Transformação e limpeza
3. Criação das dimensões
4. Criação da tabela fato
5. Exportação para arquivos analíticos e também o insert dos dados no banco de dados criado no PostgreSQL

---

## Dataset

Fonte do dataset:

Kaggle – Doctoralia Brasil

Contém informações de médicos, especialidades, localização e características de atendimento, além de dados gerados aleatóriamente.

Projeto desenvolvido para estudo de **Engenharia de Dados**.
