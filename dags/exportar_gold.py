import duckdb
import pandas as pd

DUCKDB_PATH = r"C:\Users\lucas\olist-pipeline\data\gold.duckdb"
OUTPUT_PATH = r"C:\Users\lucas\olist-pipeline\data\gold_export"

import os
os.makedirs(OUTPUT_PATH, exist_ok=True)

con = duckdb.connect(DUCKDB_PATH, read_only=True)

tabelas = ["fct_pedidos", "dim_cliente", "dim_produto", "dim_vendedor", "dim_tempo"]

for tabela in tabelas:
    df = con.execute(f"SELECT * FROM {tabela}").df()
    caminho = os.path.join(OUTPUT_PATH, f"{tabela}.parquet")
    df.to_parquet(caminho, index=False)
    print(f"✓ {tabela} exportada — {len(df):,} linhas")

con.close()
print("\nPronto! Arquivos em:", OUTPUT_PATH)