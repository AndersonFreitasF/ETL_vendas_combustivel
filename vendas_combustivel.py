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
    engine = create_engine(f"sqlite:///{DB_NAME}")
    with engine.connect() as con:
        con.execute(text("PRAGMA journal_mode=WAL"))
        con.execute(text("PRAGMA synchronous=NORMAL"))
    return engine


def download_file():
    try:
        response = requests.get(URL_FONTE, stream=True, timeout=60)
        response.raise_for_status()

        with open(PLACEHOLDER, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    file.write(chunk)

        logging.info("Download concluído.")
        return True

    except Exception as e:
        logging.error(f"Erro no download: {e}")
        return False



def detectar_colunas(df_chunk):
    colmap = {}

    for col in df_chunk.columns:
        c = col.lower().strip()

        if "regiao" in c:
            colmap[col] = "regiao"
        elif c in ["estado - sigla", "uf", "estado"] or "sigla" in c:
            colmap[col] = "uf"
        elif "municip" in c:
            colmap[col] = "municipio"
        elif "revenda" in c:
            colmap[col] = "posto_nome"
        elif "cnpj" in c:
            colmap[col] = "cnpj"
        elif "bairro" in c:
            colmap[col] = "bairro"
        elif "produto" in c:
            colmap[col] = "produto"
        elif "data" in c:
            colmap[col] = "data_coleta"
        elif "valor" in c:
            colmap[col] = "valor_venda"
        elif "bandeira" in c:
            colmap[col] = "bandeira"

    logging.info(f"Colunas detectadas automaticamente: {colmap}")
    return colmap



def clean_and_transform(df_chunk):
    if df_chunk.empty:
        return df_chunk

    logging.info(f"Chunk bruto: {len(df_chunk)} linhas")

    colmap = detectar_colunas(df_chunk)

    if not colmap:
        logging.error("Nenhuma coluna válida encontrada neste chunk.")
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
        df["valor_venda"] = (
            df["valor_venda"]
            .astype(str)
            .str.replace(",", ".", regex=False)
        )
        df["valor_venda"] = pd.to_numeric(df["valor_venda"], errors="coerce")

    if "data_coleta" in df.columns:
        df["data_coleta"] = pd.to_datetime(df["data_coleta"], errors="coerce", dayfirst=True)


    if "cnpj" in df.columns:
        df["cnpj"] = df["cnpj"].astype(str).str.replace(r"\D", "", regex=True)


    criticas = ["valor_venda", "produto", "data_coleta"]
    criticas_presentes = [c for c in criticas if c in df.columns]
    df.dropna(subset=criticas_presentes, inplace=True)


    if "valor_venda" in df.columns:
        df = df[df["valor_venda"] > 0.01]

    df.drop_duplicates(inplace=True)

    logging.info(f"Chunk após limpeza: {len(df)} linhas")
    return df



def run_pipeline_db():
    logging.info("Iniciando carga no banco.")

    engine = get_db_engine()

    try:
        chunks = pd.read_csv(
            PLACEHOLDER,
            sep=";",
            encoding="utf-8",
            chunksize=5000,
            on_bad_lines="skip"
        )

        total = 0
        modo = "replace"

        for i, chunk in enumerate(chunks):
            df_limpo = clean_and_transform(chunk)

            if df_limpo.empty:
                continue

            df_limpo.to_sql(
                TABELA_DESTINO,
                con=engine,
                index=False,
                if_exists=modo
            )

            modo = "append"
            total += len(df_limpo)

            logging.info(f"Lote {i} salvo. Total até agora: {total}")

        logging.info(f"Finalizado. Total carregado: {total} linhas.")
        return True

    except Exception as e:
        logging.error(f"Erro no processamento: {e}")
        return False

    finally:
        if os.path.exists(PLACEHOLDER):
            os.remove(PLACEHOLDER)
            logging.info("Arquivo temporário removido.")



def check_results_completo():
    engine = get_db_engine()

    try:
        with engine.connect() as con:
            print("\n=== RESUMO POR PRODUTO ===")
            print(pd.read_sql(text("""
                SELECT produto,
                       COUNT(*) AS qtd,
                       ROUND(AVG(valor_venda), 2) AS media,
                       ROUND(MIN(valor_venda), 2) AS minimo,
                       ROUND(MAX(valor_venda), 2) AS maximo
                FROM vendas_combustivel
                GROUP BY produto;
            """), con).to_string(index=False))

            print("\n=== TOP 5 ESTADOS MAIS CAROS (GASOLINA) ===")
            print(pd.read_sql(text("""
                SELECT uf,
                       ROUND(AVG(valor_venda), 2) AS media
                FROM vendas_combustivel
                WHERE produto='GASOLINA'
                GROUP BY uf
                ORDER BY media DESC
                LIMIT 5;
            """), con).to_string(index=False))

    except Exception as e:
        logging.error(f"Erro no relatório: {e}")


if __name__ == "__main__":
    if download_file():
        if run_pipeline_db():
            check_results_completo()
        else:
            print("Erro na carga de dados.")
    else:
        print("Falha no download.")
