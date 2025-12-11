import pandas as pd
import requests
import os
from sqlalchemy import create_engine, text
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

URL_FONTE = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsan/2024/precos-gasolina-etanol-01.csv"
PLACEHOLDER = "temp_dados_anp.csv"
TABELA_DESTINO = "vendas_combustivel"
DB_NAME = "anp_2024.db"

def get_db_engine():
    logging.info("Conectando ao banco de dados SQLite...")
    engine = create_engine(f"sqlite:///{DB_NAME}")
    with engine.connect() as con:
        con.execute(text("PRAGMA journal_mode=WAL"))
        con.execute(text("PRAGMA synchronous=NORMAL"))
    return engine

def download_file():
    logging.info(f"Iniciando download da fonte: {URL_FONTE}")
    try:
        response = requests.get(URL_FONTE, stream=True, timeout=1024)
        response.raise_for_status()

        with open(PLACEHOLDER, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)

        tamanho_mb = os.path.getsize(PLACEHOLDER) / (1024 * 1024)
        logging.info(f"Download concluído com sucesso. Tamanho: {tamanho_mb:.2f} MB")
        return True

    except Exception as e:
        logging.error(f"Erro crítico no download: {e}")
        return False

def detectar_colunas(df_chunk):
    colmap = {}
    logging.info(f"Colunas brutas encontradas: {list(df_chunk.columns)}")

    for col in df_chunk.columns:
        c = col.lower().strip()

        if "regiao" in c:
            colmap[col] = "regiao"
        elif c in ["estado - sigla", "uf", "estado"] or "sigla" in c:
            colmap[col] = "uf"
        elif "municip" in c:
            colmap[col] = "municipio"
        elif "cnpj" in c:
            colmap[col] = "cnpj"
        elif "revenda" in c:
            colmap[col] = "posto_nome"
        elif "bairro" in c:
            colmap[col] = "bairro"
        elif "produto" in c:
            colmap[col] = "produto"
        elif "data" in c:
            colmap[col] = "data_coleta"
        elif "valor de venda" in c:
            colmap[col] = "valor_venda"
        elif "bandeira" in c:
            colmap[col] = "bandeira"

    logging.info(f"Mapeamento de colunas definido: {colmap}")
    return colmap

def clean_and_transform(df_chunk):
    if df_chunk.empty:
        logging.warning("Chunk vazio recebido.")
        return df_chunk

    qtd_inicial = len(df_chunk)
    logging.info(f"Iniciando limpeza de lote com {qtd_inicial} linhas.")

    colmap = detectar_colunas(df_chunk)

    if not colmap:
        logging.error("Nenhuma coluna válida identificada. Pulando chunk.")
        return pd.DataFrame()

    df = df_chunk[list(colmap.keys())].rename(columns=colmap).copy()

    texto_cols = ["regiao", "uf", "municipio", "posto_nome", "bairro", "bandeira", "produto"]
    for col in texto_cols:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.normalize("NFKD")
                .str.encode("ascii", errors="ignore")
                .str.decode("utf-8")
                .str.strip()
                .str.upper()
            )

    if "valor_venda" in df.columns:
        df["valor_venda"] = df["valor_venda"].astype(str).str.replace(",", ".", regex=False)
        df["valor_venda"] = pd.to_numeric(df["valor_venda"], errors="coerce")

    if "data_coleta" in df.columns:
        df["data_coleta"] = pd.to_datetime(df["data_coleta"], errors="coerce", dayfirst=True)

    if "cnpj" in df.columns:
        df["cnpj"] = df["cnpj"].astype(str).str.replace(r"\D", "", regex=True)

    criticas = ["valor_venda", "produto", "data_coleta"]
    df.dropna(subset=criticas, inplace=True)

    df = df[df["valor_venda"] > 0.01]
    df.drop_duplicates(inplace=True)

    qtd_final = len(df)
    removidas = qtd_inicial - qtd_final
    logging.info(f"Limpeza concluída. Linhas mantidas: {qtd_final} | Linhas removidas: {removidas}")
    
    return df

def run_pipeline_db():
    logging.info("Iniciando pipeline de carga no banco de dados.")
    engine = get_db_engine()

    try:
        logging.info("Lendo arquivo CSV em chunks...")
        with pd.read_csv(
            PLACEHOLDER,
            sep=";",
            encoding="utf-8",
            chunksize=5000,
            on_bad_lines="skip"
        ) as chunks:
            
            total = 0
            modo = "replace"

            for i, chunk in enumerate(chunks):
                logging.info(f"--- Processando Lote {i+1} ---")
                
                df_limpo = clean_and_transform(chunk)

                if df_limpo.empty:
                    logging.warning(f"Lote {i+1} resultou em DataFrame vazio após limpeza.")
                    continue

                logging.info(f"Escrevendo {len(df_limpo)} linhas no banco de dados...")
                df_limpo.to_sql(
                    TABELA_DESTINO,
                    con=engine,
                    index=False,
                    if_exists=modo
                )

                modo = "append"
                total += len(df_limpo)
                logging.info(f"Lote {i+1} gravado com sucesso.")

        logging.info(f"Carga finalizada. Total acumulado no banco: {total} linhas.")
        return True

    except Exception as e:
        logging.error(f"Erro fatal durante o processamento do pipeline: {e}")
        return False

    finally:
        if os.path.exists(PLACEHOLDER):
            try:
                os.remove(PLACEHOLDER)
                logging.info("Arquivo temporário limpo com sucesso.")
            except Exception as del_err:
                logging.error(f"Falha ao tentar remover arquivo temporário: {del_err}")

def check_results_completo(estado_filtro=None):
    logging.info("Gerando relatórios de validação...")
    engine = get_db_engine()

    filtro_sql = ""
    if estado_filtro:
        filtro_sql = f"WHERE uf = '{estado_filtro.upper()}'"
        logging.info(f"Aplicando filtro UF = {estado_filtro.upper()}")

    try:
        with engine.connect() as con:

            print("\n=== RESUMO POR PRODUTO ===")
            print(pd.read_sql(text(f"""
                SELECT produto,
                       COUNT(*) AS qtd,
                       ROUND(AVG(valor_venda), 2) AS media,
                       ROUND(MIN(valor_venda), 2) AS minimo,
                       ROUND(MAX(valor_venda), 2) AS maximo
                FROM vendas_combustivel
                {filtro_sql}
                GROUP BY produto;
            """), con).to_string(index=False))

            print("\n=== TOP 5 ESTADOS MAIS CAROS (GASOLINA) ===")
            print(pd.read_sql(text(f"""
                SELECT uf,
                       ROUND(AVG(valor_venda), 2) AS media
                FROM vendas_combustivel
                WHERE produto='GASOLINA'
                {f"AND uf='{estado_filtro.upper()}'" if estado_filtro else ""}
                GROUP BY uf
                ORDER BY media DESC
                LIMIT 5;
            """), con).to_string(index=False))

            print("\n=== MEDIANA POR PRODUTO ===")
            print(pd.read_sql(text(f"""
                WITH base AS (
                    SELECT produto, valor_venda
                    FROM vendas_combustivel
                    {filtro_sql}
                ),
                ordered AS (
                    SELECT produto, valor_venda,
                           ROW_NUMBER() OVER (PARTITION BY produto ORDER BY valor_venda) AS rn,
                           COUNT(*) OVER (PARTITION BY produto) AS total
                    FROM base
                )
                SELECT produto,
                       ROUND(AVG(valor_venda), 2) AS mediana
                FROM ordered
                WHERE rn IN (total/2, total/2 + 1)
                GROUP BY produto;
            """), con).to_string(index=False))

            print("\n=== VOLUME TOTAL POR PRODUTO ===")
            print(pd.read_sql(text(f"""
                SELECT produto,
                       COUNT(*) AS total_registros
                FROM vendas_combustivel
                {filtro_sql}
                GROUP BY produto
                ORDER BY total_registros DESC;
            """), con).to_string(index=False))

            print("\n=== DISTRIBUIÇÃO DE PREÇOS POR PRODUTO ===")
            print(pd.read_sql(text(f"""
                SELECT produto,
                       SUM(CASE WHEN valor_venda < 4 THEN 1 ELSE 0 END) AS faixa_ate_4,
                       SUM(CASE WHEN valor_venda >= 4 AND valor_venda < 5 THEN 1 ELSE 0 END) AS faixa_4_5,
                       SUM(CASE WHEN valor_venda >= 5 AND valor_venda < 6 THEN 1 ELSE 0 END) AS faixa_5_6,
                       SUM(CASE WHEN valor_venda >= 6 AND valor_venda < 7 THEN 1 ELSE 0 END) AS faixa_6_7,
                       SUM(CASE WHEN valor_venda >= 7 THEN 1 ELSE 0 END) AS faixa_acima_7
                FROM vendas_combustivel
                {filtro_sql}
                GROUP BY produto;
            """), con).to_string(index=False))

        logging.info("Relatórios gerados com sucesso.")

    except Exception as e:
        logging.error(f"Erro ao gerar relatórios: {e}")

if __name__ == "__main__":
    logging.info("=== INÍCIO DO PROCESSO ===")
    if download_file():
        if run_pipeline_db():
            check_results_completo(estado_filtro="AL")
        else:
            logging.error("Ocorreu um erro na etapa de carga do banco.")
    else:
        logging.error("Ocorreu um erro na etapa de download.")
    logging.info("=== PROCESSO FINALIZADO ===")
