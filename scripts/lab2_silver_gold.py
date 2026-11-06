"""
CIC406 Aula 07 - Laboratorio 2/3: SILVER e GOLD.

Le a camada bronze, aplica a limpeza (silver) e materializa o modelo
dimensional do gabarito da Aula 04 como camada gold em Apache Iceberg:
quatro dimensoes conformadas e os dois fatos da constelacao.

Uso:  python lab2_silver_gold.py
"""
import hashlib
from decimal import Decimal
import pandas as pd
import pyarrow as pa

from pyiceberg.schema import Schema
from pyiceberg.types import (NestedField, StringType, IntegerType, LongType,
                             DateType, BooleanType, DecimalType)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import MonthTransform, IdentityTransform

import conf

cat = conf.catalogo()
N = lambda x: f"{x:,}".replace(",", ".")
B = lambda x: f"{x:,.2f}".replace(",", "@").replace(".", ",").replace("@", ".")


def ler(nome):
    return cat.load_table(f"bronze.{nome}").scan().to_pandas()


def chave64(*partes):
    """Chave substituta distribuivel: hash de 64 bits da chave natural.

    Substitui o SEQUENCE do DW e o no Rank do KNIME. Nao precisa de contador
    global, entao cada maquina de um cluster calcula a mesma chave sozinha.
    """
    s = "|".join(str(p) for p in partes)
    return int.from_bytes(hashlib.sha256(s.encode("utf-8")).digest()[:8], "big") >> 1


def grava(ident, schema, spec, arrow, rotulo):
    if cat.table_exists(ident):
        cat.drop_table(ident)
    t = cat.create_table(ident, schema=schema, partition_spec=spec)
    t.append(arrow)
    print(f"  {rotulo:<42} {N(len(arrow)):>9} linhas")
    return t


SEM_PARTICAO = PartitionSpec()

# ===================================================================== SILVER
print("SILVER - limpeza\n")

bairro = ler("bairro")
bairro["nm_bairro"] = bairro["nm_bairro"].str.strip().str.title()
bairro["id_bairro"] = bairro["id_bairro"].astype(int)
bairro["id_cidade"] = bairro["id_cidade"].astype(int)
print(f"  bairros distintos                          {N(bairro['nm_bairro'].nunique()):>9}")

cidade = ler("cidade"); cidade["id_cidade"] = cidade["id_cidade"].astype(int)
estado = ler("estado"); estado["id_regiao"] = estado["id_regiao"].astype(int)
regiao = ler("regiao"); regiao["id_regiao"] = regiao["id_regiao"].astype(int)

servico = ler("servico")
servico["codigo"] = servico["codigo"].astype(int)
servico["valor"] = servico["valor"].str.replace(",", ".", regex=False).astype(float)
servico["id_tp_servico"] = servico["id_tp_servico"].astype(int)
tipo_servico = ler("tipo_servico"); tipo_servico["id_tp_servico"] = tipo_servico["id_tp_servico"].astype(int)

cliente = ler("cliente")
cliente["cd_cliente"] = cliente["cd_cliente"].astype(int)
cliente["id_bairro"] = cliente["id_bairro"].astype(int)
cliente = cliente.sort_values("cd_cliente").drop_duplicates("rg", keep="first")
print(f"  clientes apos deduplicar por RG            {N(len(cliente)):>9}")

estab = ler("estabelecimento")
for c in ("id_estabelecimento", "id_bairro", "id_tp_estabelecimento"):
    estab[c] = estab[c].astype(int)
tipo_estab = ler("tipo_estabelecimento")
tipo_estab["id_tp_estabelecimento"] = tipo_estab["id_tp_estabelecimento"].astype(int)

quarto = ler("quarto")
for c in ("nr_quarto", "id_estabelecimento"):
    quarto[c] = quarto[c].astype(int)
quarto["preco"] = quarto["preco"].astype(float)

ocup_crua = ler("ocupacao")
for c in ("id_ocupacao", "cd_cliente", "nr_quarto", "id_estabelecimento"):
    ocup_crua[c] = ocup_crua[c].astype(int)
ocup_crua["nr_ocupantes"] = pd.to_numeric(ocup_crua["nr_ocupantes"], errors="coerce")
o = ocup_crua.copy()
o["inicio"] = pd.to_datetime(o["inicio"], errors="coerce")
o["fim"] = pd.to_datetime(o["fim"].replace("", None), errors="coerce")
ocup = o[o["nr_ocupantes"].between(1, 5) & o["fim"].notna() & (o["fim"] > o["inicio"])].copy()
ocup["nr_ocupantes"] = ocup["nr_ocupantes"].astype(int)
print(f"  ocupacoes validas (de {N(len(ocup_crua))})              {N(len(ocup)):>9}")

pedido = ler("pedido")
for c in ("id_pedido", "cd_servico", "id_ocupacao"):
    pedido[c] = pedido[c].astype(int)
pedido["data"] = pd.to_datetime(pedido["data"]).dt.date
orfaos = ~pedido["id_ocupacao"].isin(ocup_crua["id_ocupacao"])
pedido = pedido[~orfaos]
print(f"  pedidos apos remover {N(int(orfaos.sum()))} orfaos            {N(len(pedido)):>9}")

# ================================================================ DIMENSOES
print("\nGOLD - dimensoes conformadas\n")

geo = (bairro.merge(cidade, on="id_cidade").merge(estado, on="sigla_uf")
       .merge(regiao, on="id_regiao").sort_values("id_bairro").reset_index(drop=True))
geo["chave_geografia"] = geo["id_bairro"].map(lambda b: chave64("bairro", b))

esq_geo = Schema(
    NestedField(1, "chave_geografia", LongType(), required=True),
    NestedField(2, "id_bairro", IntegerType(), required=True),
    NestedField(3, "nm_bairro", StringType(), required=True),
    NestedField(4, "nm_cidade", StringType(), required=True),
    NestedField(5, "sigla_uf", StringType(), required=True),
    NestedField(6, "nm_estado", StringType(), required=True),
    NestedField(7, "nm_regiao", StringType(), required=True),
)
arrow_geo = pa.Table.from_pandas(
    geo[["chave_geografia", "id_bairro", "nm_bairro", "nm_cidade",
         "sigla_uf", "nm_estado", "nm_regiao"]],
    schema=esq_geo.as_arrow(), preserve_index=False)
grava("gold.dim_geografia", esq_geo, SEM_PARTICAO, arrow_geo, "gold.dim_geografia")

est = (estab.merge(tipo_estab, on="id_tp_estabelecimento")
       .merge(geo[["id_bairro", "nm_bairro", "nm_cidade", "sigla_uf"]], on="id_bairro"))
est["chave_estabelecimento"] = est["id_estabelecimento"]
esq_est = Schema(
    NestedField(1, "chave_estabelecimento", IntegerType(), required=True),
    NestedField(2, "nm_estabelecimento", StringType(), required=True),
    NestedField(3, "segmento", StringType(), required=True),
    NestedField(4, "publico_alvo", StringType(), required=True),
    NestedField(5, "nm_bairro", StringType(), required=True),
    NestedField(6, "nm_cidade", StringType(), required=True),
    NestedField(7, "sigla_uf", StringType(), required=True),
)
est_out = est.rename(columns={"descr_tp_estabelecimento": "segmento"})[
    ["chave_estabelecimento", "nm_estabelecimento", "segmento", "publico_alvo",
     "nm_bairro", "nm_cidade", "sigla_uf"]].sort_values("chave_estabelecimento")
grava("gold.dim_estabelecimento", esq_est, SEM_PARTICAO,
      pa.Table.from_pandas(est_out, schema=esq_est.as_arrow(), preserve_index=False),
      "gold.dim_estabelecimento")

srv = servico.merge(tipo_servico, on="id_tp_servico")
esq_srv = Schema(
    NestedField(1, "cd_servico", IntegerType(), required=True),
    NestedField(2, "descr_servico", StringType(), required=True),
    NestedField(3, "categoria", StringType(), required=True),
    NestedField(4, "marca", StringType(), required=True),
    NestedField(5, "valor_tabela", DecimalType(12, 2), required=True),
)
srv_out = srv.rename(columns={"codigo": "cd_servico",
                              "descr_tp_servico": "categoria"})[
    ["cd_servico", "descr_servico", "categoria", "marca", "valor"]].sort_values("cd_servico")
srv_arrow = pa.Table.from_arrays([
    pa.array(srv_out["cd_servico"], pa.int32()),
    pa.array(srv_out["descr_servico"], pa.string()),
    pa.array(srv_out["categoria"], pa.string()),
    pa.array(srv_out["marca"], pa.string()),
    pa.array([Decimal(f"{v:.2f}") for v in srv_out["valor"]], pa.decimal128(12, 2)),
], schema=esq_srv.as_arrow())
grava("gold.dim_servico", esq_srv, SEM_PARTICAO, srv_arrow, "gold.dim_servico")

# dim_tempo: calendario continuo cobrindo todo o periodo dos dois fatos
ini = min(pedido["data"].min(), ocup["inicio"].dt.date.min())
fim = max(pedido["data"].max(), (ocup["fim"] - pd.Timedelta(days=1)).dt.date.max())
cal = pd.DataFrame({"data": pd.date_range(ini, fim, freq="D")})
MES = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho",
       "agosto", "setembro", "outubro", "novembro", "dezembro"]
DIA = ["segunda", "terça", "quarta", "quinta", "sexta", "sábado", "domingo"]
cal["ano"] = cal["data"].dt.year
cal["trimestre"] = cal["data"].dt.quarter
cal["mes"] = cal["data"].dt.month
cal["nm_mes"] = cal["mes"].map(lambda m: MES[m - 1])
cal["ano_mes"] = cal["data"].dt.strftime("%Y-%m")
cal["dia"] = cal["data"].dt.day
cal["dia_semana"] = cal["data"].dt.dayofweek + 1
cal["nm_dia_semana"] = (cal["dia_semana"] - 1).map(lambda d: DIA[d])
cal["fim_de_semana"] = cal["dia_semana"] >= 6
cal["data"] = cal["data"].dt.date
esq_tempo = Schema(
    NestedField(1, "data", DateType(), required=True),
    NestedField(2, "ano", IntegerType(), required=True),
    NestedField(3, "trimestre", IntegerType(), required=True),
    NestedField(4, "mes", IntegerType(), required=True),
    NestedField(5, "nm_mes", StringType(), required=True),
    NestedField(6, "ano_mes", StringType(), required=True),
    NestedField(7, "dia", IntegerType(), required=True),
    NestedField(8, "dia_semana", IntegerType(), required=True),
    NestedField(9, "nm_dia_semana", StringType(), required=True),
    NestedField(10, "fim_de_semana", BooleanType(), required=True),
)
grava("gold.dim_tempo", esq_tempo, SEM_PARTICAO,
      pa.Table.from_pandas(cal[[f.name for f in esq_tempo.fields]],
                           schema=esq_tempo.as_arrow(), preserve_index=False),
      f"gold.dim_tempo ({ini} a {fim})")

mapa_geo = dict(zip(geo["id_bairro"], geo["chave_geografia"]))
bairro_do_estab = dict(zip(estab["id_estabelecimento"], estab["id_bairro"]))

# ============================================================= FATO_SERVICO
print("\nGOLD - fatos da constelacao\n")

fs = (pedido
      .merge(ocup[["id_ocupacao", "cd_cliente", "id_estabelecimento", "nr_ocupantes"]],
             on="id_ocupacao")
      .merge(servico[["codigo", "valor"]], left_on="cd_servico", right_on="codigo")
      .merge(cliente[["cd_cliente", "id_bairro"]], on="cd_cliente"))
fs["chave_geografia_origem"] = fs["id_bairro"].map(mapa_geo)
fs["chave_geografia_estab"] = fs["id_estabelecimento"].map(bairro_do_estab).map(mapa_geo)
print(f"  linhas apos os joins                       {N(len(fs)):>9}")

fato_s = (fs.groupby(["data", "chave_geografia_origem", "chave_geografia_estab",
                      "id_estabelecimento", "cd_servico", "nr_ocupantes"], as_index=False)
          .agg(qtd_servicos=("id_pedido", "count"), valor_servicos=("valor", "sum"))
          .rename(columns={"id_estabelecimento": "chave_estabelecimento"})
          .sort_values(["data", "chave_estabelecimento", "cd_servico"]))

esq_fs = Schema(
    NestedField(1, "data", DateType(), required=True),
    NestedField(2, "chave_geografia_origem", LongType(), required=True),
    NestedField(3, "chave_geografia_estab", LongType(), required=True),
    NestedField(4, "chave_estabelecimento", IntegerType(), required=True),
    NestedField(5, "cd_servico", IntegerType(), required=True),
    NestedField(6, "nr_ocupantes", IntegerType(), required=True),   # degenerada
    NestedField(7, "qtd_servicos", IntegerType(), required=True),
    NestedField(8, "valor_servicos", DecimalType(12, 2), required=True),
)
# particao pelo TEMPO -- nunca pelo bairro (ver slide 18)
spec_fs = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=MonthTransform(), name="mes_data"))
arrow_fs = pa.Table.from_arrays([
    pa.array(fato_s["data"], pa.date32()),
    pa.array(fato_s["chave_geografia_origem"], pa.int64()),
    pa.array(fato_s["chave_geografia_estab"], pa.int64()),
    pa.array(fato_s["chave_estabelecimento"], pa.int32()),
    pa.array(fato_s["cd_servico"], pa.int32()),
    pa.array(fato_s["nr_ocupantes"], pa.int32()),
    pa.array(fato_s["qtd_servicos"], pa.int32()),
    pa.array([Decimal(f"{v:.2f}") for v in fato_s["valor_servicos"]], pa.decimal128(12, 2)),
], schema=esq_fs.as_arrow())
grava("gold.fato_servico", esq_fs, spec_fs, arrow_fs, "gold.fato_servico")
print(f"    soma de qtd_servicos                     {N(int(fato_s['qtd_servicos'].sum())):>9}")
print(f"    soma de valor_servicos                R$ {B(float(fato_s['valor_servicos'].sum())):>12}")

# ========================================================== FATO_HOSPEDAGEM
h = ocup.copy()
h["noites"] = (h["fim"] - h["inicio"]).dt.days
print(f"\n  soma de noites                             {N(int(h['noites'].sum())):>9}")

exp = h.loc[h.index.repeat(h["noites"])].copy()
exp["n"] = exp.groupby("id_ocupacao").cumcount()
exp["data"] = (exp["inicio"] + pd.to_timedelta(exp["n"], unit="D")).dt.date
print(f"  linhas apos a explosao das diarias         {N(len(exp)):>9}")

exp = (exp.merge(quarto[["nr_quarto", "id_estabelecimento", "preco"]],
                 on=["nr_quarto", "id_estabelecimento"])
       .merge(cliente[["cd_cliente", "id_bairro"]], on="cd_cliente"))
exp["chave_geografia_origem"] = exp["id_bairro"].map(mapa_geo)
exp["chave_geografia_estab"] = exp["id_estabelecimento"].map(bairro_do_estab).map(mapa_geo)
print(f"  linhas apos o join com quarto              {N(len(exp)):>9}")

fato_h = (exp.groupby(["data", "chave_geografia_origem", "chave_geografia_estab",
                       "id_estabelecimento"], as_index=False)
          .agg(qtd_hospedes=("nr_ocupantes", "sum"),
               qtd_ocupacoes=("id_ocupacao", "nunique"),
               valor_diarias=("preco", "sum"))
          .rename(columns={"id_estabelecimento": "chave_estabelecimento"})
          .sort_values(["data", "chave_estabelecimento"]))

esq_fh = Schema(
    NestedField(1, "data", DateType(), required=True),
    NestedField(2, "chave_geografia_origem", LongType(), required=True),
    NestedField(3, "chave_geografia_estab", LongType(), required=True),
    NestedField(4, "chave_estabelecimento", IntegerType(), required=True),
    NestedField(5, "qtd_hospedes", IntegerType(), required=True),    # pessoas-dia
    NestedField(6, "qtd_ocupacoes", IntegerType(), required=True),   # quartos-dia
    NestedField(7, "valor_diarias", DecimalType(12, 2), required=True),
)
spec_fh = PartitionSpec(
    PartitionField(source_id=1, field_id=1000, transform=MonthTransform(), name="mes_data"))
arrow_fh = pa.Table.from_arrays([
    pa.array(fato_h["data"], pa.date32()),
    pa.array(fato_h["chave_geografia_origem"], pa.int64()),
    pa.array(fato_h["chave_geografia_estab"], pa.int64()),
    pa.array(fato_h["chave_estabelecimento"], pa.int32()),
    pa.array(fato_h["qtd_hospedes"], pa.int32()),
    pa.array(fato_h["qtd_ocupacoes"], pa.int32()),
    pa.array([Decimal(f"{v:.2f}") for v in fato_h["valor_diarias"]], pa.decimal128(12, 2)),
], schema=esq_fh.as_arrow())
grava("gold.fato_hospedagem", esq_fh, spec_fh, arrow_fh, "gold.fato_hospedagem")
print(f"    soma de qtd_hospedes                     {N(int(fato_h['qtd_hospedes'].sum())):>9}")
print(f"    soma de qtd_ocupacoes                    {N(int(fato_h['qtd_ocupacoes'].sum())):>9}")
print(f"    soma de valor_diarias                 R$ {B(float(fato_h['valor_diarias'].sum())):>12}")

print("\ntabelas na camada gold:")
for ns, nome in sorted(cat.list_tables("gold")):
    t = cat.load_table(f"{ns}.{nome}")
    print(f"  {ns}.{nome:<22} {N(len(t.scan().to_arrow())):>9} linhas")
