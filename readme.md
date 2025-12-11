# Pipeline ETL - Preços de Combustíveis ANP 2024

Pipeline para extração, transformação e análise de dados de preços de combustíveis da Agência Nacional do Petróleo (ANP).

## Funcionalidades

- Extração automatizada de dados do portal de dados abertos da ANP
- Transformação e normalização de registros de preços
- Armazenamento em banco de dados SQLite
- Geração de relatórios analíticos

## Requisitos

```bash
pip install pandas sqlalchemy requests
```

## Execução

```bash
python vendas_combustivel.py
```

O processo executa as seguintes etapas:

1. Download do arquivo CSV da fonte oficial
2. Processamento em lotes de 5000 registros
3. Persistência no banco de dados `anp_2024.db`
4. Geração automática de relatórios no terminal

## Relatórios

O sistema gera três relatórios principais:

**Resumo de Preços por Produto**

- Quantidade de amostras
- Valores mínimo, médio e máximo por litro

**Análise Regional**

- Distribuição de postos pesquisados por região
- Preço médio regional por produto

**Ranking Estadual**

- Top 5 estados com maior preço médio de gasolina

## Estrutura do Banco de Dados

Tabela: `vendas_combustivel`

Campos principais:

- regiao, uf, municipio, bairro
- posto_nome, cnpj, bandeira
- produto, valor_venda, data_coleta

## Características Técnicas

- Processamento em streaming para otimização de memória
- Tratamento automático de linhas inconsistentes
- Conversão de formatos numéricos (vírgula para ponto decimal)
- Parsing de datas no formato brasileiro
- Substituição completa do banco a cada execução
