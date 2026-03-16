from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from minio import Minio
import os

# ─── Configuração do MinIO ───────────────────────────────────────────
# Aqui dizemos ao Python onde está o MinIO e como autenticar.
# "minio:9000" funciona porque ambos estão na mesma rede Docker.
MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "olist-raw"

# ─── Pasta onde estão os CSVs dentro do container ───────────────────
# Lembra que mapeamos ./data para /opt/airflow/data no docker-compose?
DATA_PATH = "/opt/airflow/data"


def upload_csvs_to_minio():
    """
    Conecta no MinIO e envia todos os CSVs da pasta /data.
    Cada arquivo vira um objeto dentro do bucket olist-raw.
    """
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # sem HTTPS no ambiente local
    )

    # Lista todos os arquivos .csv na pasta data
    arquivos = [f for f in os.listdir(DATA_PATH) if f.endswith(".csv")]

    if not arquivos:
        raise Exception("Nenhum CSV encontrado em /opt/airflow/data")

    for arquivo in arquivos:
        caminho_completo = os.path.join(DATA_PATH, arquivo)

        print(f"Enviando {arquivo} para o bucket {BUCKET_NAME}...")

        client.fput_object(
            bucket_name=BUCKET_NAME,
            object_name=f"raw/{arquivo}",   # ficará em olist-raw/raw/arquivo.csv
            file_path=caminho_completo
        )

        print(f"✓ {arquivo} enviado com sucesso!")


# ─── Definição da DAG ────────────────────────────────────────────────
# schedule_interval=None significa que só roda quando você disparar manualmente
with DAG(
    dag_id="dag_ingest_bronze",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["olist", "bronze", "ingestão"],
    doc_md="""
    ## DAG Bronze — Ingestão Raw
    Lê os CSVs do Olist da pasta /data e envia para o bucket `olist-raw` no MinIO.
    Simula a camada Bronze (raw zone) de uma arquitetura Medallion na AWS S3.
    """
) as dag:

    ingerir_dados = PythonOperator(
        task_id="upload_csvs_para_minio",
        python_callable=upload_csvs_to_minio
    )