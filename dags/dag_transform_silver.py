from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from minio import Minio
import pandas as pd
import io

# ─── Configuração ────────────────────────────────────────────────────
MINIO_ENDPOINT  = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME     = "olist-raw"

def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

# ─── Regras de limpeza por tabela ────────────────────────────────────
# Cada entrada define:
#   - colunas_data: quais colunas converter para datetime
#   - drop_nulls:   quais colunas não podem ter nulos (linha é removida)

REGRAS = {
    "olist_orders_dataset.csv": {
        "colunas_data": [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date"
        ],
        "drop_nulls": ["order_id", "customer_id", "order_status"]
    },
    "olist_customers_dataset.csv": {
        "colunas_data": [],
        "drop_nulls": ["customer_id", "customer_unique_id"]
    },
    "olist_order_items_dataset.csv": {
        "colunas_data": ["shipping_limit_date"],
        "drop_nulls": ["order_id", "product_id", "seller_id"]
    },
    "olist_products_dataset.csv": {
        "colunas_data": [],
        "drop_nulls": ["product_id"]
    },
    "olist_sellers_dataset.csv": {
        "colunas_data": [],
        "drop_nulls": ["seller_id"]
    },
    "olist_order_payments_dataset.csv": {
        "colunas_data": [],
        "drop_nulls": ["order_id", "payment_type"]
    },
    "olist_order_reviews_dataset.csv": {
        "colunas_data": ["review_creation_date", "review_answer_timestamp"],
        "drop_nulls": ["review_id", "order_id"]
    },
    "olist_geolocation_dataset.csv": {
        "colunas_data": [],
        "drop_nulls": ["geolocation_zip_code_prefix"]
    },
    "product_category_name_translation.csv": {
        "colunas_data": [],
        "drop_nulls": ["product_category_name"]
    }
}


def transformar_silver():
    """
    Para cada CSV na camada Bronze:
    1. Lê do MinIO como DataFrame
    2. Aplica regras de limpeza
    3. Salva como Parquet na camada silver/
    """
    client = get_minio_client()

    for arquivo, regras in REGRAS.items():

        print(f"\n{'='*50}")
        print(f"Processando: {arquivo}")

        # ── 1. Lê o CSV do MinIO direto para memória ──────────────────
        response = client.get_object(BUCKET_NAME, f"raw/{arquivo}")
        df = pd.read_csv(io.BytesIO(response.read()))

        print(f"  Linhas brutas: {len(df)}")

        # ── 2. Remove linhas com nulos em colunas críticas ─────────────
        if regras["drop_nulls"]:
            df = df.dropna(subset=regras["drop_nulls"])

        # ── 3. Converte colunas de data ────────────────────────────────
        for col in regras["colunas_data"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")

        print(f"  Linhas após limpeza: {len(df)}")

        # ── 4. Salva como Parquet em memória e envia pro MinIO ─────────
        # Por que em memória? Evita criar arquivos temporários no container
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        nome_parquet = arquivo.replace(".csv", ".parquet")

        client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=f"silver/{nome_parquet}",
            data=buffer,
            length=buffer.getbuffer().nbytes,
            content_type="application/octet-stream"
        )

        print(f"  ✓ Salvo em silver/{nome_parquet}")


# ─── Definição da DAG ────────────────────────────────────────────────
with DAG(
    dag_id="dag_transform_silver",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["olist", "silver", "transformação"],
    doc_md="""
    ## DAG Silver — Transformação
    Lê os CSVs brutos da camada Bronze (MinIO raw/),
    aplica limpeza e padronização, e salva como Parquet
    na camada Silver (MinIO silver/).
    """
) as dag:

    transformar_dados = PythonOperator(
        task_id="transformar_para_silver",
        python_callable=transformar_silver
    )