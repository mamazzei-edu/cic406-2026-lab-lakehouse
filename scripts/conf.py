"""
CIC406 Aula 07 - configuracao comum aos laboratorios 1 a 4.

Um unico lugar decide se o warehouse fica no MinIO (s3://) ou no disco local
(file://). Os laboratorios nao mudam uma linha entre os dois modos.

    MODO=minio  python lab1_catalogo.py ...     -> usa o MinIO do docker-compose
    python lab1_catalogo.py ...                 -> usa ./wh no disco local
"""
import os
from pyiceberg.catalog.sql import SqlCatalog

# Raiz do projeto = a pasta ACIMA de scripts/. Assim wh/ e o catalogo ficam
# ao lado de dados/ e gold_bi/, e nao dentro da pasta de codigo.
BASE = os.environ.get("LAB_BASE") or os.path.abspath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
MODO = os.environ.get("MODO", "local")

CATALOGO_DB = os.path.join(BASE, "hotel_catalog.db")


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
