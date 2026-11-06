"""
CIC406 Aula 07 - Laboratorio ZERO
O primeiro arquivo no bucket: o mesmo dado em CSV e em Parquet.

Uso:  python lab0_csv_vs_parquet.py <pasta-dos-csv> <pasta-de-saida>
"""
import sys, os, time
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

DADOS = sys.argv[1]
SAIDA = sys.argv[2]
os.makedirs(SAIDA, exist_ok=True)

# ---------------------------------------------------------------- 1. ler o CSV
origem = os.path.join(DADOS, "ocupacao.csv")
df = pd.read_csv(origem)
print(f"ocupacao.csv: {len(df):,} linhas, {len(df.columns)} colunas".replace(",", "."))
print("colunas:", list(df.columns))

# ------------------------------------------------- 2. gravar nos dois formatos
csv_out = os.path.join(SAIDA, "ocupacao.csv")
pq_out  = os.path.join(SAIDA, "ocupacao.parquet")
df.to_csv(csv_out, index=False)
pq.write_table(pa.Table.from_pandas(df, preserve_index=False), pq_out, compression="zstd")

tam_csv = os.path.getsize(csv_out)
tam_pq  = os.path.getsize(pq_out)
print(f"\nCSV     : {tam_csv:>10,} bytes".replace(",", "."))
print(f"Parquet : {tam_pq:>10,} bytes".replace(",", "."))
print(f"reducao : {tam_csv / tam_pq:.1f}x")

# ------------------------------------- 3. ler UMA coluna: o teste que convence
def cronometra(fn, repeticoes=5):
    melhor = float("inf")
    for _ in range(repeticoes):
        t = time.perf_counter()
        fn()
        melhor = min(melhor, time.perf_counter() - t)
    return melhor

t_csv = cronometra(lambda: pd.read_csv(csv_out, usecols=["nr_ocupantes"]))
t_pq  = cronometra(lambda: pq.read_table(pq_out, columns=["nr_ocupantes"]))
print(f"\nler so nr_ocupantes")
print(f"  do CSV     : {t_csv*1000:7.1f} ms")
print(f"  do Parquet : {t_pq*1000:7.1f} ms")
print(f"  ganho      : {t_csv / t_pq:.1f}x")

# ------------------------------------------ 4. o que o Parquet sabe do arquivo
meta = pq.ParquetFile(pq_out).metadata
print(f"\nmetadados que o CSV nao tem:")
print(f"  row groups        : {meta.num_row_groups}")
print(f"  linhas            : {meta.num_rows:,}".replace(",", "."))
print(f"  tamanho do schema : {meta.num_columns} colunas tipadas")
rg = meta.row_group(0)
for i in range(meta.num_columns):
    col = rg.column(i)
    est = col.statistics
    if est is not None:
        print(f"    {col.path_in_schema:<16} min={est.min}  max={est.max}")
