import pandas as pd
import requests
import os
from sqlalchemy import create_engine, text
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

URL_FONTE = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsan/2024/precos-gasolina-etanol-01.csv"
PLACEHOLDER = "temp_dados_anp.csv"
TABELA_DESTINO = "vendas_combustivel"


def get_db_engine():
    return create_engine(f'sqlite:///anp_2024.db')

def download_file():
    try:
        with requests.get(URL_FONTE, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(PLACEHOLDER, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    f.write(chunk)
        logging.info("Download concluído com sucesso!")
        return True
    except Exception as e:
        logging.error(f"Erro fatal no download: {e}")
        return False

def clean_and_transform(df_chunk):
    cols_map = {
        'Regiao - Sigla': 'regiao',
        'Estado - Sigla': 'uf',
        'Municipio': 'municipio',
        'Revenda': 'posto_nome',
        'CNPJ da Revenda': 'cnpj',
        'Bairro': 'bairro',
        'Produto': 'produto',
        'Data da Coleta': 'data_coleta',
        'Valor de Venda': 'valor_venda',
        'Bandeira': 'bandeira'
    }
    
    cols_existentes = [c for c in cols_map.keys() if c in df_chunk.columns]
    df_clean = df_chunk[cols_existentes].copy()
    df_clean.rename(columns=cols_map, inplace=True)
    
    if 'valor_venda' in df_clean.columns and df_clean['valor_venda'].dtype == 'object':
        df_clean['valor_venda'] = df_clean['valor_venda'].astype(str).str.replace(',', '.', regex=False)
        df_clean['valor_venda'] = pd.to_numeric(df_clean['valor_venda'], errors='coerce')

    if 'data_coleta' in df_clean.columns:
        df_clean['data_coleta'] = pd.to_datetime(df_clean['data_coleta'], format='%d/%m/%Y', errors='coerce')

    df_clean = df_clean.drop_duplicates()

    colunas_criticas = ['valor_venda', 'data_coleta', 'cnpj', 'produto']
    df_clean = df_clean.dropna(subset=colunas_criticas)

    df_clean = df_clean[df_clean['valor_venda'] > 0.01]

    cols_texto = ['regiao', 'uf', 'municipio', 'posto_nome', 'bairro', 'bandeira', 'produto']
    for col in cols_texto:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).str.strip().str.upper()

    return df_clean

def run_pipeline_db():
    logging.info("--- 2. PROCESSANDO ARQUIVO LOCAL PARA O BANCO ---")
    engine = get_db_engine()
    
    try:
        chunks = pd.read_csv(
            PLACEHOLDER, 
            sep=';', 
            encoding='utf-8',
            chunksize=5000,
            on_bad_lines='skip'
        )
        
        total_linhas = 0
        modo_escrita = 'replace' 
        
        for i, chunk in enumerate(chunks):
            df_limpo = clean_and_transform(chunk)
            
            if not df_limpo.empty:
                df_limpo.to_sql(TABELA_DESTINO, con=engine, if_exists=modo_escrita, index=False)
                modo_escrita = 'append'
                total_linhas += len(df_limpo)
            
            if i % 10 == 0:
                logging.info(f"Lote {i} processado. Linhas salvas: {total_linhas}")
                
        logging.info(f"--- SUCESSO! Total carregado: {total_linhas} registros. ---")
        return True

    except Exception as e:
        logging.error(f"Erro no processamento do banco: {e}")
        return False
    finally:
        if os.path.exists(PLACEHOLDER):
            os.remove(PLACEHOLDER)
            logging.info("Arquivo temporário limpo.")

def check_results_completo():
    logging.info("--- 3. GERANDO RELATÓRIO GERENCIAL ---")
    engine = get_db_engine()
    
    try:
        with engine.connect() as con:
            print("\n=== RESUMO DE PREÇOS (R$ / Litro) ===")
            query_geral = """
            SELECT 
                produto, 
                COUNT(*) as 'Qtd',
                ROUND(AVG(valor_venda), 2) as 'Média (R$)',
                ROUND(MIN(valor_venda), 2) as 'Min (R$)',
                ROUND(MAX(valor_venda), 2) as 'Max (R$)'
            FROM vendas_combustivel 
            GROUP BY produto
            """
            print(pd.read_sql(text(query_geral), con).to_string(index=False))

            print("\n=== TOP 5 ESTADOS MAIS CAROS (GASOLINA) ===")
            query_top = """
            SELECT uf, ROUND(AVG(valor_venda), 2) as 'Média R$'
            FROM vendas_combustivel WHERE produto = 'GASOLINA'
            GROUP BY uf ORDER BY "Média R$" DESC LIMIT 5
            """
            print(pd.read_sql(text(query_top), con).to_string(index=False))

    except Exception as e:
        print(f"Não foi possível gerar o relatório. Motivo: {e}")

if __name__ == "__main__":
    if download_file():
        if run_pipeline_db():
            check_results_completo()
        else:
            print("Falha na etapa de Banco de Dados.")
    else:
        print("Falha no Download.")