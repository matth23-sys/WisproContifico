# core/contifico_client.py
# -*- coding: utf-8 -*-
"""
Sincroniza facturas de Contífico de forma INDEPENDIENTE:
Siempre desde 18/10/2025 hasta la fecha actual (hoy).
Guarda en facturas_contifico evitando duplicados por documento_id.
Registra fecha de última actualización automáticamente.
"""

import os
import sqlite3
import json
import requests
from datetime import datetime

DB_PATH = os.path.join(os.getcwd(), "database", "integrador.db")



# === CONFIGURACIÓN CONTIFICO ===
BASE_URL = "https://api.contifico.com/sistema/api/v1/registro/documento/"
API_KEY = os.getenv("CONTIFICO_API_KEY") or "YGVucadm8aDmqTWzKox34gDGZZJ18FUNZh9ymo9nxDA"
HEADERS = {
    "Authorization": API_KEY,
    "Accept": "application/json",
}

# Parámetros base comunes
PARAMS_BASE = {
    "tipo_registro": "CLI",  # Clientes
    "tipo": "FAC",           # Facturas
    "estado": "P",           # Pendientes
    "result_size": 200,
    "result_page": 1,
}


# =====================================================
# 📁 FUNCIONES DE BASE DE DATOS
# =====================================================
def _conn():
    return sqlite3.connect(DB_PATH)


def _ensure_table():
    """Garantiza que las tablas necesarias existan."""
    con = _conn(); cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS facturas_contifico (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fecha_emision TEXT,
        razon_social TEXT,
        documento_numero TEXT,
        documento_id TEXT UNIQUE,
        total REAL,
        saldo REAL,
        tipo_registro TEXT,
        tipo_documento TEXT,
        estado TEXT,
        data_json TEXT,
        creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS actualizaciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        origen TEXT,
        ultima_actualizacion TEXT
    )
    """)
    con.commit(); con.close()


# =====================================================
# 📦 DESCARGA DE FACTURAS DESDE CONTIFICO
# =====================================================
def _fetch_facturas_independiente():
    """Consulta independiente: desde 18/10/2025 hasta HOY."""
    inicio = "15/10/2025"
    fin = datetime.now().strftime("%d/%m/%Y")  # datetime.now().strftime("%d/%m/%Y")
    rango_texto = f"{inicio} → {fin}"

    print(f"📡 [Contífico] Consultando facturas {rango_texto}")

    params = PARAMS_BASE.copy()
    params["fecha_inicial"] = inicio
    params["fecha_final"] = fin

    documentos = []
    page = 1

    while True:
        params["result_page"] = page
        try:
            resp = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=40)
        except requests.RequestException as e:
            print(f"❌ Error de red Contífico: {e}")
            break

        if not resp.ok:
            print(f"⚠️ Error HTTP {resp.status_code}: {resp.text}")
            break

        try:
            data = resp.json()
        except ValueError:
            print("⚠️ Respuesta no es JSON válido.")
            break

        # Detectar si viene como lista o diccionario
        if isinstance(data, list):
            pagina = data
        elif isinstance(data, dict):
            pagina = data.get("results") or data.get("data") or []
        else:
            pagina = []

        print(f"📄 Página {page}: {len(pagina)} documentos")
        documentos.extend(pagina)

        if len(pagina) < params["result_size"]:
            break
        page += 1

    print(f"📦 Total recogidos: {len(documentos)}")
    return documentos, rango_texto


# =====================================================
# 💾 GUARDADO EN BASE DE DATOS
# =====================================================
def _guardar_facturas(documentos):
    """Guarda facturas en BD solo si no existen en MATCH, NO MATCH, PROCESADAS ni EXACTAS."""
    con = _conn()
    cur = con.cursor()
    nuevos, existentes, descartados = 0, 0, 0

    for d in documentos:
        persona = d.get("persona") or {}
        documento_numero = (d.get("documento") or "").strip().upper()

        # === VALIDAR DUPLICADOS EN OTRAS TABLAS ===
        cur.execute("SELECT 1 FROM comparacion_match WHERE documento_numero = ?", (documento_numero,))
        existe_match = cur.fetchone()

        cur.execute("SELECT 1 FROM comparacion_no_match WHERE documento_numero = ?", (documento_numero,))
        existe_no_match = cur.fetchone()

        cur.execute("SELECT 1 FROM facturas_procesadas WHERE documento_numero = ?", (documento_numero,))
        existe_procesada = cur.fetchone()

        cur.execute("SELECT 1 FROM facturas_exactas WHERE documento_numero = ?", (documento_numero,))
        existe_exacta = cur.fetchone()

        if existe_match or existe_no_match or existe_procesada or existe_exacta:
            descartados += 1
            continue  # Saltar si ya fue conciliada o está en otra tabla

        try:
            cur.execute("""
                INSERT INTO facturas_contifico (
                    fecha_emision, razon_social, documento_numero, documento_id,
                    total, saldo, tipo_registro, tipo_documento, estado, data_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                d.get("fecha_emision"),
                persona.get("razon_social"),
                documento_numero,
                d.get("id"),
                float(d.get("total") or 0),
                float(d.get("saldo") or 0),
                d.get("tipo_registro"),
                d.get("tipo_documento"),
                d.get("estado"),
                json.dumps(d, ensure_ascii=False),
            ))
            nuevos += 1
        except sqlite3.IntegrityError:
            existentes += 1
            continue

    con.commit()
    con.close()
    print(f"💾 Guardado Contífico — Nuevos: {nuevos}, Existentes: {existentes}, Desc. por duplicado: {descartados}")
    return {"nuevos": nuevos, "existentes": existentes, "descartados": descartados}


# =====================================================
# 🕒 REGISTRO DE ACTUALIZACIONES
# =====================================================
def registrar_actualizacion(origen):
    fecha = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    con = _conn(); cur = con.cursor()
    cur.execute(
        "INSERT INTO actualizaciones (origen, ultima_actualizacion) VALUES (?, ?)",
        (origen, fecha)
    )
    con.commit(); con.close()
    print(f"🕒 Última actualización registrada para {origen}: {fecha}")


# =====================================================
# 🔁 PROCESO COMPLETO DE SINCRONIZACIÓN
# =====================================================
def sincronizar_facturas_contifico():
    """Sincronización completa independiente."""
    _ensure_table()
    docs, rango = _fetch_facturas_independiente()
    if not docs:
        registrar_actualizacion("contifico")
        return {"nuevos": 0, "existentes": 0, "rango": rango}

    res = _guardar_facturas(docs)
    res["rango"] = rango
    registrar_actualizacion("contifico")
    print(f"✅ Contífico: nuevas={res['nuevos']} | existentes={res['existentes']} | rango={rango}")
    return res


# =====================================================
# 🧩 INTERFAZ COMPATIBLE CON app.py
# =====================================================
def obtener_facturas_desde_contifico(inicio=None, fin=None):
    """Alias compatible con el flujo de app.py"""
    print("🔁 Ejecutando sincronización independiente de Contífico (alias).")
    return sincronizar_facturas_contifico()


# =====================================================
# 📊 DASHBOARD LISTADO DE FACTURAS
# =====================================================
def listar_facturas_para_tabla():
    """Devuelve las facturas para el dashboard Contífico."""
    _ensure_table()
    con = _conn(); cur = con.cursor()
    cur.execute("""
        SELECT fecha_emision, razon_social, documento_numero,
               documento_id, total, saldo
        FROM facturas_contifico
        ORDER BY fecha_emision DESC
    """)
    rows = cur.fetchall()
    con.close()
    return rows



