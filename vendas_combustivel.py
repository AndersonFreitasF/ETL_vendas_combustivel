import pandas as pd
from sqlalchemy import create_engine, text
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

URL_FONTE = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsan/2024/precos-gasolina-etanol-01.csv"
TABELA_DESTINO = "vendas_combustivel"

def get_db_engine():
    return create_engine(f'sqlite:///anp_2024.db')

def transform_chunk(df_chunk):
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

    return df_clean

def run_pipeline_stream():
    start_time = datetime.now()
    logging.info(f"--- INICIANDO PIPELINE ETL ---")
    
    engine = get_db_engine()
    storage_options = {'User-Agent': 'Mozilla/5.0'}
    
    try:
        chunks = pd.read_csv(
            URL_FONTE, 
            sep=';', 
            encoding='utf-8', 
            chunksize=5000,
            storage_options=storage_options,
            on_bad_lines='skip'
        )
        
        total_linhas = 0
        modo_escrita = 'replace' 
        
        for i, chunk in enumerate(chunks):
            df_limpo = transform_chunk(chunk)
            
            df_limpo.to_sql(TABELA_DESTINO, con=engine, if_exists=modo_escrita, index=False)
            
            modo_escrita = 'append'
            total_linhas += len(df_limpo)
            
            if i % 5 == 0:
                logging.info(f"Processando lote {i}... Linhas acumuladas: {total_linhas}")
                
    except Exception as e:
        logging.error(f"Erro: {e}")
        return

    logging.info(f"Total registros: {total_linhas}. Tempo: {datetime.now() - start_time} ---")

def check_results_completo():
    logging.info("Gerando Relatório Gerencial...")
    engine = get_db_engine()
    
    try:
        with engine.connect() as con:
            print("\n=======================================================")
            print("   RESUMO DE PREÇOS (R$ / Litro)")
            print("=======================================================")
            query_geral = """
            SELECT 
                produto, 
                COUNT(*) as 'Qtd Amostras',
                ROUND(MIN(valor_venda), 2) as 'Min (R$)',
                ROUND(AVG(valor_venda), 2) as 'Média (R$)',
                ROUND(MAX(valor_venda), 2) as 'Max (R$)'
            FROM vendas_combustivel 
            GROUP BY produto
            ORDER BY produto
            """
            df_geral = pd.read_sql(text(query_geral), con)
            print(df_geral.to_string(index=False))

            print("\n\n=======================================================")
            print("   ANÁLISE REGIONAL (Onde há mais postos pesquisados?)")
            print("=======================================================")
            query_regiao = """
            SELECT 
                regiao,
                produto,
                COUNT(*) as 'Qtd Postos',
                ROUND(AVG(valor_venda), 2) as 'Preço Médio Regional (R$)'
            FROM vendas_combustivel
            GROUP BY regiao, produto
            ORDER BY regiao, produto
            """
            df_regiao = pd.read_sql(text(query_regiao), con)
            print(df_regiao.to_string(index=False))

            print("\n\n=======================================================")
            print("   TOP 5 ESTADOS MAIS CAROS - GASOLINA")
            print("=======================================================")
            query_top = """
            SELECT 
                uf,
                ROUND(AVG(valor_venda), 2) as 'Média Gasolina (R$)'
            FROM vendas_combustivel
            WHERE produto = 'GASOLINA'
            GROUP BY uf
            ORDER BY "Média Gasolina (R$)" DESC
            LIMIT 5
            """
            df_top = pd.read_sql(text(query_top), con)
            print(df_top.to_string(index=False))

    except Exception as e:
        print("Erro na análise:", e)

if __name__ == "__main__":
    run_pipeline_stream()
    check_results_completo()