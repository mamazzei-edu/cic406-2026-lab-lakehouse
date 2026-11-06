"""
CIC406 Aula 07 - Laboratorio 1/3: ambiente, catalogo e camada BRONZE.

Cria o catalogo, os tres namespaces do Medallion e aterrissa os 12 CSVs na
camada bronze EXATAMENTE como vieram -- tudo como texto, defeitos inclusive.
Bronze nao limpa nada: ela registra o que o sistema de origem entregou.

Uso:  python lab1_catalogo.py <pasta-dos-csv>
"""
import sys, os
import pandas as pd
import pyarrow as pa
import conf

DADOS = sys.argv[1] if len(sys.argv) > 1 else "../dados"

ARQUIVOS = ["regiao", "estado", "cidade", "bairro", "cliente",
            "tipo_estabelecimento", "estabelecimento", "quarto",
            "tipo_servico", "servico", "ocupacao", "pedido"]

conf.descreve()
cat = conf.catalogo()
for camada in ("bronze", "silver", "gold"):
    cat.create_namespace_if_not_exists(camada)
print()

total = 0
for nome in ARQUIVOS:
    # dtype=str: bronze preserva o texto cru. "  MOEMA " continua com os espacos,
    # "128,00" continua com a virgula, o campo fim vazio continua vazio.
    df = pd.read_csv(os.path.join(DADOS, f"{nome}.csv"), dtype=str, keep_default_na=False)
    tbl = pa.Table.from_pandas(df, preserve_index=False)
    ident = f"bronze.{nome}"
    if cat.table_exists(ident):
        cat.drop_table(ident)
    t = cat.create_table(ident, schema=tbl.schema)
    t.append(tbl)
    total += len(df)
    print(f"  bronze.{nome:<22} {len(df):>7,} linhas".replace(",", "."))

print(f"\n  {'TOTAL':<29} {total:>7,} linhas".replace(",", "."))
print("""
As quatro camadas, nomeadas:
  1 object storage  -> o warehouse acima (MinIO ou disco)
  2 formato arquivo -> Parquet, escrito pelo PyIceberg
  3 formato tabela  -> Apache Iceberg (os .metadata.json e os .avro)
  4 catalogo        -> o SQLite acima; em producao, um catalogo REST

Abra o warehouse no explorador de arquivos (ou o console do MinIO) e procure,
dentro de bronze.db/pedido/, as pastas data/ e metadata/. O Parquet esta em
data/; o que transforma aquilo numa TABELA esta em metadata/.
""")
