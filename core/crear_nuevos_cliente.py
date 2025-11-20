import requests
import time
import sqlite3
import json
import os
from datetime import datetime, timedelta, timezone

# ==========================
# 🔧 CONFIGURACIÓN GENERAL
# ==========================
WISPRO_API_URL = "https://www.cloud.wispro.co/api/v1/clients"
WISPRO_TOKEN = "d000a623-df99-4bdb-9088-66b4d79e091e"

CONTIFICO_API_URL = "https://api.contifico.com/sistema/api/v1/persona/?pos=c8fa5542-5731-4c77-9085-80610c79230c"
CONTIFICO_TOKEN = "YGvucadm8aDmqTWzKox34gDGZZJ18FUNZh9ymo9nxDA"

# Ruta de base de datos dentro de /database
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQLITE_PATH = os.path.join(BASE_DIR, "..", "database", "integrador.db")

# Crear carpeta si no existe
os.makedirs(os.path.dirname(SQLITE_PATH), exist_ok=True)

PER_PAGE = 50
SLEEP_BETWEEN = 0.3


# ==========================
# 🧱 CONEXIÓN BASE DE DATOS
# ==========================
def db_conn():
    return sqlite3.connect(SQLITE_PATH, detect_types=sqlite3.PARSE_DECLTYPES)


def init_crear_nuevos_cliente_db():
    with db_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS crear_nuevos_cliente_importados (
                wispro_id TEXT PRIMARY KEY,
                razon_social TEXT,
                email TEXT,
                phone TEXT,
                phone_mobile TEXT,
                id_number TEXT UNIQUE,
                city TEXT,
                address TEXT,
                created_at TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS crear_nuevos_cliente_resultados (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                id_number TEXT,
                razon_social TEXT,
                status_code INTEGER,
                respuesta TEXT,
                exito INTEGER DEFAULT 0,
                mensaje_resultado TEXT,
                fecha_envio TEXT
            )
        """)
        conn.commit()


# ==========================
# 📥 CLIENTES WISPRO
# ==========================
def obtener_clientes_recientes():
    headers = {"accept": "application/json", "Authorization": WISPRO_TOKEN}
    nuevos = []
    page = 1
    fecha_limite = datetime.now(timezone.utc) - timedelta(days=3)

    while True:
        params = {"page": page, "per_page": PER_PAGE}
        resp = requests.get(WISPRO_API_URL, headers=headers, params=params)
        if resp.status_code != 200:
            print(f"Error Wispro {resp.status_code}")
            break

        data = resp.json()
        clientes = data.get("data", [])
        pag = data.get("meta", {}).get("pagination", {})
        total_pages = pag.get("total_pages", 1)
        current_page = pag.get("current_page", page)

        for c in clientes:
            fecha_creacion = datetime.fromisoformat(c["created_at"].replace("Z", "+00:00"))
            if fecha_creacion >= fecha_limite:
                nuevos.append(c)

        if current_page >= total_pages:
            break
        page += 1
        time.sleep(SLEEP_BETWEEN)
    return nuevos


# ==========================
# 🧩 MAPEO CONTIFICO
# ==========================
def mapear_a_contifico(cliente):
    identificacion = cliente.get("national_identification_number", "") or ""
    tipo = "J" if len(identificacion) == 13 else "N"
    return {
        "tipo": tipo,
        "personaasociada_id": "",
        "nombre_comercial": None,
        "telefonos": cliente.get("phone_mobile") or cliente.get("phone") or "",
        "ruc": identificacion if tipo == "J" else "",
        "razon_social": cliente.get("name", "CLIENTE SIN NOMBRE"),
        "direccion": cliente.get("address") or cliente.get("city") or "",
        "es_extranjero": False,
        "porcentaje_descuento": "",
        "es_cliente": True,
        "id": None,
        "es_empleado": False,
        "email": cliente.get("email", ""),
        "cedula": identificacion if tipo == "N" else "",
        "placa": "",
        "es_vendedor": False,
        "es_proveedor": False
    }


# ==========================
# 🚀 ENVÍO A CONTIFICO
# ==========================
def enviar_a_contifico(payload, wispro_id):
    headers = {
        "Authorization": CONTIFICO_TOKEN,
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        resp = requests.post(CONTIFICO_API_URL, headers=headers, data=json.dumps(payload))
        exito = 1 if resp.status_code in (200, 201) else 0
        mensaje = "Cliente creado correctamente" if exito else f"Error: {resp.text[:120]}"

        with db_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO crear_nuevos_cliente_resultados 
                (id_number, razon_social, status_code, respuesta, exito, mensaje_resultado, fecha_envio)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (payload.get("cedula") or payload.get("ruc"), payload.get("razon_social"),
                  resp.status_code, resp.text, exito, mensaje, fecha))
            conn.commit()

        return resp.status_code, mensaje
    except Exception as e:
        with db_conn() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO crear_nuevos_cliente_resultados 
                (id_number, razon_social, status_code, respuesta, exito, mensaje_resultado, fecha_envio)
                VALUES (?, ?, ?, ?, 0, ?, ?)
            """, (payload.get("cedula") or payload.get("ruc"), payload.get("razon_social"),
                  0, str(e), f"Error local: {e}", fecha))
            conn.commit()
        return 0, f"Error local: {e}"


# ==========================
# 🔁 PROCESO COMPLETO
# ==========================
def crear_nuevos_cliente():
    init_crear_nuevos_cliente_db()
    nuevos = obtener_clientes_recientes()

    with db_conn() as conn:
        for c in nuevos:
            conn.execute("""
                INSERT OR IGNORE INTO crear_nuevos_cliente_importados
                (wispro_id, razon_social, email, phone, phone_mobile, city, address, id_number, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                c.get("id"), c.get("name"), c.get("email"), c.get("phone"),
                c.get("phone_mobile"), c.get("city"), c.get("address"),
                c.get("national_identification_number"), c.get("created_at")
            ))
        conn.commit()

    for c in nuevos:
        payload = mapear_a_contifico(c)
        enviar_a_contifico(payload, c.get("id"))

    print("✅ Sincronización completada y registrada en BD.")
