"""
CIC406 Aula 07 - configuracao comum aos laboratorios 1 a 4.

Um unico lugar decide se o warehouse fica no MinIO (s3://) ou no disco local
(file://). Os laboratorios nao mudam uma linha entre os dois modos.

    python lab1_catalogo.py ../dados                  -> ./wh no disco local
    python lab1_catalogo.py ../dados --modo minio     -> MinIO do docker-compose

O sinalizador --modo funciona igual em PowerShell, cmd, bash e zsh. A variavel
de ambiente MODO tambem e aceita, mas a sintaxe dela muda conforme o shell:

    bash / zsh    MODO=minio python lab1_catalogo.py ../dados
    PowerShell    $env:MODO = "minio"   (e depois o python, em outra linha)
    cmd.exe       set MODO=minio        (e depois o python, em outra linha)

Prefira --modo: e uma coisa a menos para dar errado na maquina do aluno.
"""
import os
import sys
from pyiceberg.catalog.sql import SqlCatalog

# Raiz do projeto = a pasta ACIMA de scripts/. Assim wh/ e o catalogo ficam
# ao lado de dados/ e gold_bi/, e nao dentro da pasta de codigo.
BASE = os.environ.get("LAB_BASE") or os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


def _modo_do_argv():
    """Le --modo <valor> ou --modo=<valor> e o remove de sys.argv.

    Removido, os argumentos posicionais dos laboratorios seguem funcionando.
    """
    for i, a in enumerate(list(sys.argv)):
        if a == "--modo" and i + 1 < len(sys.argv):
            valor = sys.argv[i + 1]
            del sys.argv[i:i + 2]
            return valor
        if a.startswith("--modo="):
            sys.argv.remove(a)
            return a.split("=", 1)[1]
    return None


MODO = (_modo_do_argv() or os.environ.get("MODO") or "local").strip().lower()
if MODO not in ("local", "minio"):
    raise SystemExit(
        f"Modo desconhecido: {MODO!r}. Use 'local' (padrao) ou 'minio'.\n"
        "Exemplo:  python lab1_catalogo.py ../dados --modo minio")

# Um catalogo por modo: os dois warehouses sao lugares diferentes, e misturar
# as duas coisas no mesmo catalogo deixa tabelas apontando para onde nao se
# esta olhando. Trocar de modo comeca um lakehouse novo, do lab 1 em diante.
CATALOGO_DB = os.path.join(
    BASE, "hotel_catalog.db" if MODO == "local" else "hotel_catalog_minio.db")


def propriedades():
    """Propriedades do catalogo conforme o modo escolhido."""
    props = {"uri": f"sqlite:///{CATALOGO_DB}"}
    if MODO == "minio":
        props.update({
            "warehouse": "s3://dw-hoteis/",
            "s3.endpoint": os.environ.get("S3_ENDPOINT", "http://localhost:9000"),
            "s3.access-key-id": os.environ.get("S3_KEY", "admin"),
            "s3.secret-access-key": os.environ.get("S3_SECRET", "admin12345"),
            "s3.path-style-access": "true",
        })
    else:
        wh = os.path.join(BASE, "wh")
        os.makedirs(wh, exist_ok=True)
        props["warehouse"] = f"file://{wh}"
    return props


def catalogo():
    return SqlCatalog("hotel", **propriedades())


def descreve():
    p = propriedades()
    print(f"modo      : {MODO}")
    print(f"catalogo  : {p['uri']}")
    print(f"warehouse : {p['warehouse']}")
