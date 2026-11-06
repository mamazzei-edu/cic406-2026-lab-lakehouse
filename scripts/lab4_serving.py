"""
CIC406 Aula 07 - Laboratorio 4/4: a ultima milha (serving para BI).

A camada gold esta em Apache Iceberg. Nem o Power BI Desktop nem o Tableau
Desktop leem Iceberg. Este laboratorio publica a gold em dois formatos que
eles leem, e explica por que esse passo existe.

Uso:  python lab4_serving.py <pasta-de-saida>
"""
import sys, os, time
import duckdb
import pyarrow.parquet as pq
import conf

SAIDA = sys.argv[1] if len(sys.argv) > 1 else "../gold_bi"
os.makedirs(SAIDA, exist_ok=True)
cat = conf.catalogo()
N = lambda x: f"{x:,}".replace(",", ".")

TABELAS = ["dim_tempo", "dim_geografia", "dim_estabelecimento", "dim_servico",
           "fato_hospedagem", "fato_servico"]

# ======================================== 1. ler a gold pelo caminho correto
print("1. LENDO A GOLD PELO SNAPSHOT (nao pela pasta data/)\n")
dados = {}
for nome in TABELAS:
    t = cat.load_table(f"gold.{nome}")
    dados[nome] = t.scan().to_arrow()
    print(f"  gold.{nome:<22} {N(len(dados[nome])):>9} linhas"
          f"   snapshot {t.current_snapshot().snapshot_id}")

print("""
  O .scan() consulta o snapshot corrente e devolve SO os arquivos validos.
  E por isso que o serving passa pelo PyIceberg e nao por um glob na pasta.""")

# ============================================ 2. Parquet (Power BI Desktop)
print("\n2. PUBLICANDO EM PARQUET - para o Power BI Desktop\n")
dir_pq = os.path.join(SAIDA, "parquet")
os.makedirs(dir_pq, exist_ok=True)
for nome, arrow in dados.items():
    caminho = os.path.join(dir_pq, f"{nome}.parquet")
    pq.write_table(arrow, caminho, compression="snappy")
    print(f"  {nome + '.parquet':<30} {N(os.path.getsize(caminho)):>9} bytes")
print("""
  Power BI Desktop: Obter Dados > Parquet > caminho do arquivo.
  Um arquivo por tabela, um clique por tabela. Sem driver, sem admin.""")

# ============================= 3. DuckDB (Tableau Desktop e Power BI Desktop)
print("\n3. PUBLICANDO EM DUCKDB - para o Tableau Desktop\n")
db = os.path.join(SAIDA, "gold.duckdb")
if os.path.exists(db):
    os.remove(db)
con = duckdb.connect(db)
for nome, arrow in dados.items():
    con.register("tmp_arrow", arrow)
    con.execute(f"CREATE OR REPLACE TABLE {nome} AS SELECT * FROM tmp_arrow")
    con.unregister("tmp_arrow")
for (nome, n) in con.execute("""
        SELECT table_name, estimated_size FROM duckdb_tables() ORDER BY table_name
     """).fetchall():
    print(f"  {nome:<30} {N(int(n)):>9} linhas")

# ========================================= 4. CSV (plano B, sempre funciona)
print("\n4. PUBLICANDO EM CSV - plano B universal\n")
dir_csv = os.path.join(SAIDA, "csv")
os.makedirs(dir_csv, exist_ok=True)
for nome in TABELAS:
    caminho = os.path.join(dir_csv, f"{nome}.csv")
    con.execute(f"COPY {nome} TO '{caminho}' (HEADER, DELIMITER ',')")
    print(f"  {nome + '.csv':<30} {N(os.path.getsize(caminho)):>9} bytes")

# ============================ 5. o modelo estrela responde o desafio da Aula 04
print("\n5. TESTE DE ACEITE - as analises do desafio, sobre a gold publicada\n")
consultas = [
    ("Faturamento de servicos por regiao de origem do hospede", """
        SELECT g.nm_regiao, SUM(f.valor_servicos) AS total
          FROM fato_servico f
          JOIN dim_geografia g ON g.chave_geografia = f.chave_geografia_origem
         GROUP BY 1 ORDER BY 2 DESC"""),
    ("Gasto medio em servicos por numero de ocupantes do quarto", """
        SELECT f.nr_ocupantes,
               SUM(f.valor_servicos) AS total,
               ROUND(SUM(f.valor_servicos) / SUM(f.qtd_servicos), 2) AS ticket
          FROM fato_servico f GROUP BY 1 ORDER BY 1"""),
    ("Diarias e pessoas-dia por segmento da unidade", """
        SELECT e.segmento,
               SUM(h.qtd_ocupacoes) AS quartos_dia,
               SUM(h.qtd_hospedes)  AS pessoas_dia,
               SUM(h.valor_diarias) AS receita
          FROM fato_hospedagem h
          JOIN dim_estabelecimento e ON e.chave_estabelecimento = h.chave_estabelecimento
         GROUP BY 1 ORDER BY 4 DESC"""),
    ("Drill across: receita total por mes, somando os DOIS fatos", """
        WITH s AS (SELECT t.ano_mes, SUM(f.valor_servicos) v
                     FROM fato_servico f JOIN dim_tempo t ON t.data = f.data GROUP BY 1),
             d AS (SELECT t.ano_mes, SUM(h.valor_diarias) v
                     FROM fato_hospedagem h JOIN dim_tempo t ON t.data = h.data GROUP BY 1)
        SELECT COALESCE(s.ano_mes, d.ano_mes) AS mes,
               d.v AS diarias, s.v AS servicos, d.v + s.v AS total
          FROM d FULL JOIN s ON s.ano_mes = d.ano_mes
         ORDER BY 1 LIMIT 4"""),
]
for titulo, sql in consultas:
    t0 = time.perf_counter()
    df = con.execute(sql).df()
    ms = (time.perf_counter() - t0) * 1000
    print(f"  {titulo}  ({ms:.0f} ms)")
    print("    " + df.to_string(index=False).replace("\n", "\n    "))
    print()

con.close()
print(f"""Publicado em {os.path.abspath(SAIDA)}:
  parquet/   6 arquivos  -> Power BI Desktop (Obter Dados > Parquet)
  gold.duckdb            -> Tableau Desktop (JDBC) e Power BI (ODBC)
  csv/       6 arquivos  -> plano B, qualquer ferramenta

FECHE esta sessao antes de abrir o arquivo no BI: o DuckDB admite um unico
processo de escrita por arquivo.""")
