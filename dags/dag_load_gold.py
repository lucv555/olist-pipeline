from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from minio import Minio
import pandas as pd
import duckdb
import io
import os

# ─── Configuração ────────────────────────────────────────────────────
MINIO_ENDPOINT   = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME      = "olist-raw"

# DuckDB vai salvar o banco nesse arquivo dentro do container
DUCKDB_PATH = "/opt/airflow/data/gold.duckdb"

def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def ler_parquet(client, nome_arquivo):
    """Lê um Parquet do MinIO e retorna como DataFrame."""
    response = client.get_object(BUCKET_NAME, f"silver/{nome_arquivo}")
    return pd.read_parquet(io.BytesIO(response.read()))

def construir_gold():
    client = get_minio_client()

    # ── Lê todas as tabelas Silver ─────────────────────────────────
    print("Lendo tabelas Silver...")
    orders    = ler_parquet(client, "olist_orders_dataset.parquet")
    customers = ler_parquet(client, "olist_customers_dataset.parquet")
    items     = ler_parquet(client, "olist_order_items_dataset.parquet")
    products  = ler_parquet(client, "olist_products_dataset.parquet")
    sellers   = ler_parquet(client, "olist_sellers_dataset.parquet")
    payments  = ler_parquet(client, "olist_order_payments_dataset.parquet")
    translation = ler_parquet(client, "product_category_name_translation.parquet")

    # ── Conecta no DuckDB ──────────────────────────────────────────
    # DuckDB é um banco analítico que roda direto em arquivo — igual
    # ao Redshift mas local. Perfeito para portfólio.
    con = duckdb.connect(DUCKDB_PATH)

    # ── dim_cliente ────────────────────────────────────────────────
    print("Criando dim_cliente...")
    con.execute("DROP TABLE IF EXISTS dim_cliente")
    con.execute("""
        CREATE TABLE dim_cliente AS
        SELECT DISTINCT
            customer_id,
            customer_unique_id,
            customer_city,
            customer_state,
            customer_zip_code_prefix
        FROM customers
    """)

    # ── dim_produto ────────────────────────────────────────────────
    print("Criando dim_produto...")
    # Faz join com a tabela de tradução para ter o nome em inglês
    products_translated = products.merge(
        translation,
        on="product_category_name",
        how="left"
    )
    con.register("products_translated", products_translated)
    con.execute("DROP TABLE IF EXISTS dim_produto")
    con.execute("""
        CREATE TABLE dim_produto AS
        SELECT DISTINCT
            product_id,
            COALESCE(product_category_name_english, product_category_name) AS categoria,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm
        FROM products_translated
    """)

    # ── dim_vendedor ───────────────────────────────────────────────
    print("Criando dim_vendedor...")
    con.execute("DROP TABLE IF EXISTS dim_vendedor")
    con.execute("""
        CREATE TABLE dim_vendedor AS
        SELECT DISTINCT
            seller_id,
            seller_city,
            seller_state,
            seller_zip_code_prefix
        FROM sellers
    """)

    # ── dim_tempo ──────────────────────────────────────────────────
    # Extrai componentes de data do pedido para análises temporais
    print("Criando dim_tempo...")
    con.register("orders_reg", orders)
    con.execute("DROP TABLE IF EXISTS dim_tempo")
    con.execute("""
        CREATE TABLE dim_tempo AS
        SELECT DISTINCT
            CAST(order_purchase_timestamp AS DATE) AS data,
            YEAR(order_purchase_timestamp)         AS ano,
            MONTH(order_purchase_timestamp)        AS mes,
            DAY(order_purchase_timestamp)          AS dia,
            DAYOFWEEK(order_purchase_timestamp)    AS dia_semana,
            QUARTER(order_purchase_timestamp)      AS trimestre
        FROM orders_reg
        WHERE order_purchase_timestamp IS NOT NULL
    """)

    # ── fct_pedidos ────────────────────────────────────────────────
    # Tabela fato: une pedidos, itens e pagamentos
    print("Criando fct_pedidos...")
    con.register("orders_reg", orders)
    con.register("items_reg", items)
    con.register("payments_reg", payments)
    con.execute("DROP TABLE IF EXISTS fct_pedidos")
    con.execute("""
        CREATE TABLE fct_pedidos AS
        SELECT
            o.order_id,
            o.customer_id,
            i.product_id,
            i.seller_id,
            CAST(o.order_purchase_timestamp AS DATE) AS data_pedido,
            o.order_status                            AS status,
            i.price                                   AS valor_produto,
            i.freight_value                           AS valor_frete,
            (i.price + i.freight_value)               AS valor_total_item,
            p.payment_type                            AS tipo_pagamento,
            p.payment_installments                    AS parcelas,
            p.payment_value                           AS valor_pago
        FROM orders_reg o
        JOIN items_reg    i ON o.order_id = i.order_id
        LEFT JOIN payments_reg p ON o.order_id = p.order_id
            AND p.payment_sequential = 1
    """)

    # ── Valida os resultados ───────────────────────────────────────
    print("\n=== Resumo das tabelas Gold ===")
    for tabela in ["dim_cliente", "dim_produto", "dim_vendedor", "dim_tempo", "fct_pedidos"]:
        count = con.execute(f"SELECT COUNT(*) FROM {tabela}").fetchone()[0]
        print(f"  {tabela}: {count:,} linhas")

    con.close()
    print("\n✓ Camada Gold construída com sucesso!")


with DAG(
    dag_id="dag_load_gold",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["olist", "gold", "star schema"],
    doc_md="""
    ## DAG Gold — Star Schema
    Lê os Parquets da camada Silver e constrói o star schema
    no DuckDB (equivalente local ao Amazon Redshift).
    Tabelas: fct_pedidos, dim_cliente, dim_produto, dim_vendedor, dim_tempo.
    """
) as dag:

    carregar_gold = PythonOperator(
        task_id="construir_star_schema",
        python_callable=construir_gold
    )