"""
CIC406 Aula 07 - Laboratorio 3/3: inspecao, time travel e evolucao de particao.

Reprocessa um mes da FATO_SERVICO e observa o que o Iceberg faz com os
arquivos. E aqui que a "tabela" se revela: uma arvore de arquivos imutaveis
mais um ponteiro para o snapshot corrente.

Uso:  python lab3_inspecao.py
"""
import os
from decimal import Decimal
import pandas as pd
import pyarrow as pa
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan
from pyiceberg.transforms import DayTransform
import conf

cat = conf.catalogo()
N = lambda x: f"{x:,}".replace(",", ".")
B = lambda x: f"{x:,.2f}".replace(",", "@").replace(".", ",").replace("@", ".")
fato = cat.load_table("gold.fato_servico")

# Este laboratorio ALTERA a tabela (reprocessa um mes e evolui a particao).
# Rodar duas vezes sobre o mesmo estado empilha commits e confunde os numeros.
if any(f.name == "dia_data" for f in fato.spec().fields):
    raise SystemExit(
        "Esta tabela já passou pelo laboratório 3 (a particao já é por dia).\n"
        "Rode novamente o lab2_silver_gold.py para recriar a gold e depois este.")
if len(fato.history()) > 1:
    raise SystemExit(
        "Esta tabela já tem mais de um commit.\n"
        "Rode novamente o lab2_silver_gold.py para recriar a gold e depois este.")


def arquivos_no_snapshot(t):
    return len(t.inspect.files())


def arquivos_no_disco(t):
    raiz = t.location().replace("file://", "")
    if not os.path.isdir(raiz):
        return None  # modo MinIO: conte pelo console
    return sum(len([f for f in arqs if f.endswith(".parquet")])
               for _, _, arqs in os.walk(os.path.join(raiz, "data")))


# ============================================ 1. o que o Iceberg gravou mesmo
print("1. O QUE ESTÁ GRAVADO\n")
arq = fato.inspect.files().to_pandas()
print(f"  arquivos de dados no snapshot corrente : {len(arq)}")
print(f"  linhas somadas nos arquivos            : {N(int(arq['record_count'].sum()))}")
print(f"  tamanho total                          : {N(int(arq['file_size_in_bytes'].sum()))} bytes")
print(f"  menor / maior arquivo                  : {N(int(arq['file_size_in_bytes'].min()))}"
      f" / {N(int(arq['file_size_in_bytes'].max()))} bytes")
print("\n  particoes (MonthTransform sobre data):")
part = fato.inspect.partitions().to_pandas()
print(f"    {len(part)} particoes, "
      f"{part['record_count'].min()} a {part['record_count'].max()} linhas cada")

# ================================================ 2. data skipping na pratica
print("\n2. DATA SKIPPING - o que substituiu o indice B-Tree\n")
filtro = And(GreaterThanOrEqual("data", "2026-04-01"), LessThan("data", "2026-05-01"))
scan = fato.scan(row_filter=filtro)
lidos = len(list(scan.plan_files()))
print(f"  arquivos existentes na tabela : {len(arq)}")
print(f"  arquivos que o scan vai abrir : {lidos}")
print(f"  linhas devolvidas             : {N(len(scan.to_arrow()))}")
print("  O filtro e por DATA; o pruning e por MES. Isso e hidden partitioning:")
print("  o usuario nao precisa saber como a tabela foi particionada.")

# ===================================== 3. reprocessar um mes (segunda carga)
print("\n3. SEGUNDA CARGA - reprocessando abril/2026\n")
antes_snap, antes_disco = arquivos_no_snapshot(fato), arquivos_no_disco(fato)
linhas_antes = len(fato.scan().to_arrow())

abril = fato.scan(row_filter=filtro).to_arrow()
print(f"  linhas de abril que serao reescritas : {N(len(abril))}")

fato.overwrite(df=abril, overwrite_filter=filtro)
fato = cat.load_table("gold.fato_servico")

depois_snap, depois_disco = arquivos_no_snapshot(fato), arquivos_no_disco(fato)
linhas_depois = len(fato.scan().to_arrow())
print(f"\n  {'':<34}{'antes':>8}{'depois':>9}")
print(f"  {'arquivos no snapshot corrente':<34}{antes_snap:>8}{depois_snap:>9}")
if antes_disco is not None:
    print(f"  {'arquivos .parquet no disco':<34}{antes_disco:>8}{depois_disco:>9}")
print(f"  {'linhas na tabela':<34}{N(linhas_antes):>8}{N(linhas_depois):>9}")
print("""
  A LICAO: ha mais arquivos no disco do que na tabela. O Parquet antigo de
  abril continua la -- e o que permite voltar no tempo. Quem apontar uma
  ferramenta de BI para a pasta data/ vai somar o mes de abril DUAS VEZES.
  O que diz quais arquivos valem é o snapshot, não o sistema de arquivos.""")

# ================================================== 4. snapshots e historico
print("\n4. SNAPSHOTS E TIME TRAVEL\n")
snaps = fato.inspect.snapshots().to_pandas()
for _, s in snaps.iterrows():
    print(f"  {str(s['committed_at'])[:19]}  {s['operation']:<9} id={s['snapshot_id']}")

primeiro = fato.history()[0].snapshot_id
antigo = fato.scan(snapshot_id=primeiro).to_arrow()
print(f"\n  tabela como estava no 1o commit : {N(len(antigo))} linhas")
print(f"  tabela agora                    : {N(len(fato.scan().to_arrow()))} linhas")
print("  Mesma contagem: o reprocessamento trocou os arquivos, não os dados.")

# ================================================ 5. evolucao de particao
print("\n5. PARTITION EVOLUTION - de mês para dia, sem reescrever nada\n")
print(f"  spec atual : {fato.spec()}")
with fato.update_spec() as us:
    us.remove_field("mes_data")
    us.add_field("data", DayTransform(), "dia_data")
fato = cat.load_table("gold.fato_servico")
print(f"  spec nova  : {fato.spec()}")
print(f"  arquivos apos a evolução : {arquivos_no_snapshot(fato)}  (nenhum foi reescrito)")
print(f"  linhas                   : {N(len(fato.scan().to_arrow()))}")
print("""
  Mudar a partição foi uma alteração de METADADO. Os arquivos antigos seguem
  particionados por mês; os próximos serão por dia; e a mesma consulta lê os
  dois. Em um DW clássico isso seria um ALTER TABLE com reorganização física
  e janela de indisponibilidade.""")

# ======================================================= 6. conferencia final
print("\n6. CONFERÊNCIA FINAL DA CAMADA GOLD\n")
esperado = {
    "gold.dim_geografia": 40, "gold.dim_estabelecimento": 21,
    "gold.dim_servico": 12, "gold.dim_tempo": 543,
    "gold.fato_servico": 29926, "gold.fato_hospedagem": 35774,
}
ok = True
for ident, alvo in esperado.items():
    n = len(cat.load_table(ident).scan().to_arrow())
    marca = "OK" if n == alvo else f"<<< esperado {N(alvo)}"
    ok &= (n == alvo)
    print(f"  {ident:<28}{N(n):>9}   {marca}")

fs = cat.load_table("gold.fato_servico").scan().to_arrow().to_pandas()
fh = cat.load_table("gold.fato_hospedagem").scan().to_arrow().to_pandas()
somas = [("soma de qtd_servicos", int(fs["qtd_servicos"].sum()), 34199, N),
         ("soma de valor_servicos", float(fs["valor_servicos"].sum()), 3827668.80, B),
         ("soma de qtd_hospedes", int(fh["qtd_hospedes"].sum()), 90935, N),
         ("soma de qtd_ocupacoes", int(fh["qtd_ocupacoes"].sum()), 37237, N),
         ("soma de valor_diarias", float(fh["valor_diarias"].sum()), 15380164.80, B)]
for rot, v, alvo, fmt in somas:
    bate = abs(v - alvo) < 0.005
    ok &= bate
    print(f"  {rot:<28}{fmt(v):>14}   {'OK' if bate else f'<<< esperado {fmt(alvo)}'}")

print(f"\n  {'CAMADA GOLD VALIDADA' if ok else 'HA DIVERGENCIAS'}")
