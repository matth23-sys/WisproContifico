# -*- coding: utf-8 -*-
"""
Created on Sun Oct 26 11:33:03 2025

@author: Oscar
"""

import os
import re
import json
import sqlite3
import pandas as pd
import requests

# ============================================
# 🔧 CONFIGURACIÓN
# ============================================
DB_PATH = os.path.join(os.getcwd(), "database", "integrador.db")
WISPRO_URL = "https://www.cloud.wispro.co/api/v1/invoicing/payments"
WISPRO_TOKEN = "d000a623-df99-4bdb-9088-66b4d79e091e"
HEADERS = {"accept": "application/json", "Authorization": WISPRO_TOKEN}
PM_KIND = "PROCREDIT S.A. CTA 019037892134"


# ============================================
# 🧱 CONEXIÓN Y TABLAS
# ============================================

def _conn():
    return sqlite3.connect(DB_PATH)

def _ensure_schema_pcd():
    """Crea tablas necesarias para ProCredit"""
    with _conn() as con:
        cur = con.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS cb_banco_procredit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo TEXT UNIQUE,
            fecha TEXT,
            transaccion TEXT,
            valor REAL,
            saldo REAL,
            estado TEXT DEFAULT 'pendiente',
            origen TEXT DEFAULT 'solo banco'
        )""")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS cb_wispro_procredit (
            id TEXT PRIMARY KEY,
            public_id TEXT,
            client_name TEXT,
            payment_date TEXT,
            amount REAL,
            transaction_kind TEXT,
            transaction_code TEXT,
            data_json TEXT
        )""")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS cb_conciliados_procredit (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo TEXT,
            cliente TEXT,
            fecha_banco TEXT,
            monto_banco REAL,
            fecha_wispro TEXT,
            monto_wispro REAL,
            codigo_wispro TEXT,
            saldo REAL,
            estado TEXT,
            origen TEXT
        )""")
        con.commit()


# ============================================
# 📥 IMPORTAR EXCEL DEL BANCO PROCREDIT
# ============================================

def pcd_importar_excel(ruta_excel: str) -> int:
    """
    Importa TODOS los movimientos del Banco ProCredit.
    - Detecta la sección 'DETALLE DE MOVIMIENTOS' y el encabezado real (fila con 'Fecha').
    - Lee toda la tabla aunque haya filas en blanco intermedias.
    - Mapea columnas: Fecha, Ref., Valor, Descripción.
    - Guarda Descripción en el campo 'transaccion'.
    - Evita duplicados si ya existen (por 'codigo' / Ref).
    """
    _ensure_schema_pcd()

    if not os.path.exists(ruta_excel):
        raise FileNotFoundError(f"Archivo no encontrado: {ruta_excel}")

    # 1) Leer hoja completa sin asumir encabezado
    df_raw = pd.read_excel(ruta_excel, engine="openpyxl", header=None)

    # 2) Buscar la fila 'DETALLE DE MOVIMIENTOS'
    start_row = None
    for i, row in df_raw.iterrows():
        if any(isinstance(x, str) and "DETALLE DE MOVIMIENTOS" in x.upper() for x in row):
            start_row = i
            break
    if start_row is None:
        raise ValueError("❌ No se encontró 'DETALLE DE MOVIMIENTOS' en el Excel.")

    # 3) Buscar la fila de ENCABEZADO real (que contenga 'Fecha')
    header_row = None
    for j in range(start_row + 1, len(df_raw)):
        if any(isinstance(x, str) and "FECHA" in x.upper() for x in df_raw.iloc[j]):
            header_row = j
            break
    if header_row is None:
        raise ValueError("❌ No se encontró la fila de encabezados (con 'Fecha').")

    # 4) Leer a partir del encabezado real (toda la tabla completa)
    df = pd.read_excel(ruta_excel, engine="openpyxl", header=header_row)
    # No eliminamos filas de forma agresiva; solo descartamos completamente vacías
    df = df.dropna(how="all")

    # 5) Normalizar nombres de columnas (tildes, puntos, mayúsculas)
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(".", "", regex=False)
        .str.replace("á", "a").str.replace("é", "e").str.replace("í", "i")
        .str.replace("ó", "o").str.replace("ú", "u")
    )
    print("🧾 Encabezados detectados:", list(df.columns))

    # 6) Mapeo flexible (soporta variantes)
    col_fecha = next((c for c in df.columns if "fecha" in c), None)
    col_codigo = next((c for c in df.columns if c.startswith("ref") or "refer" in c), None)
    col_valor = next((c for c in df.columns if "valor" in c or "importe" in c), None)
    col_desc = next((c for c in df.columns if "desc" in c), None)

    mapping = {"fecha": col_fecha, "codigo": col_codigo, "valor": col_valor, "descripcion": col_desc}
    if not all(mapping.values()):
        raise ValueError(f"❌ Columnas requeridas no encontradas: {mapping}")

    df = df[[col_fecha, col_codigo, col_valor, col_desc]].copy()
    df.columns = ["fecha", "codigo", "valor", "transaccion"]

    total_archivo = len(df)

    # 7) Parseo de fecha (soporta 30/7/2025, datetime de Excel, etc.)
    def parse_fecha(x):
        try:
            return pd.to_datetime(x, errors="coerce", dayfirst=True)
        except Exception:
            return pd.NaT
    df["fecha"] = df["fecha"].apply(parse_fecha)

    # 8) Normalizar montos (admite $ 1.234,56 | 1,234.56 | 1234,56 | 1234.56)
    def limpiar_monto(x):
        if pd.isna(x):
            return 0.0
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        s = re.sub(r"[^\d,.\-]", "", s)  # quitar símbolos
        # Si tiene punto y coma, asumir '.' miles y ',' decimal
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        else:
            # Solo coma → decimal
            if "," in s:
                s = s.replace(",", ".")
        try:
            return float(s)
        except Exception:
            return 0.0
    df["valor"] = df["valor"].apply(limpiar_monto)

    # 9) Filtrado mínimo: requiere código y un valor numérico > 0
    df = df[df["codigo"].notna()]
    df["codigo"] = df["codigo"].astype(str).str.strip()
    df = df[df["codigo"] != ""]
    df = df[df["valor"] > 0]

    # 10) Insertar (evita duplicados por 'codigo' si la tabla lo marca UNIQUE)
    insertados = 0
    ya_existian = 0
    candidatos = len(df)

    with _conn() as con:
        cur = con.cursor()
        for _, r in df.iterrows():
            codigo = r["codigo"]
            valor = float(r["valor"])
            fecha_iso = r["fecha"].isoformat() if pd.notna(r["fecha"]) else None
            transaccion = (str(r["transaccion"]) if pd.notna(r["transaccion"]) else "").strip()

            # Evitar duplicados por 'codigo' (si quieres permitir duplicados, quita este SELECT)
            cur.execute("SELECT 1 FROM cb_banco_procredit WHERE codigo=?", (codigo,))
            if cur.fetchone():
                ya_existian += 1
                continue

            cur.execute("""
                INSERT INTO cb_banco_procredit (codigo, fecha, transaccion, valor, saldo)
                VALUES (?, ?, ?, ?, ?)
            """, (codigo, fecha_iso, transaccion, valor, 0.0))
            insertados += 1
        con.commit()

    print(f"📦 Total en archivo: {total_archivo} | 🧮 Candidatos válidos: {candidatos} | ✅ Insertados: {insertados} | ↺ Ya existían: {ya_existian}")
    return insertados




# ============================================
# ☁️ DESCARGAR PAGOS DESDE WISPRO
# ============================================

def pcd_descargar_wispro(fecha_inicio: str, fecha_fin: str) -> dict:
    _ensure_schema_pcd()
    params = {
        "state": "success",
        "created_at_after": f"{fecha_inicio}T00:00:00-05:00",
        "created_at_before": f"{fecha_fin}T23:59:59-05:00",
        "per_page": 2000
    }
    url = WISPRO_URL
    pagina, total, nuevos = 1, 0, 0

    con = _conn()
    cur = con.cursor()

    while True:
        resp = requests.get(url, headers=HEADERS, params=params if pagina == 1 else None)
        if resp.status_code != 200:
            break

        data = resp.json()
        pagos = data.get("data", [])
        total += len(pagos)

        for p in pagos:
            if (p.get("state") or "").lower() != "success":
                continue
            if PM_KIND not in (p.get("transaction_kind") or ""):
                continue

            pid = p.get("id")
            cur.execute("SELECT 1 FROM cb_wispro_procredit WHERE id=?", (pid,))
            if not cur.fetchone():
                cur.execute("""
                    INSERT INTO cb_wispro_procredit
                    (id, public_id, client_name, payment_date, amount, transaction_kind, transaction_code, data_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    pid,
                    p.get("public_id"),
                    p.get("client_name"),
                    p.get("payment_date"),
                    p.get("amount"),
                    p.get("transaction_kind"),
                    p.get("transaction_code"),
                    json.dumps(p)
                ))
                nuevos += 1

        con.commit()
        next_link = data.get("links", {}).get("next")
        if not next_link:
            break
        url = next_link
        params = None
        pagina += 1

    con.close()
    return {"total": total, "nuevos": nuevos}


# ============================================
# 🔄 CONCILIACIÓN AUTOMÁTICA
# ============================================


def pcd_conciliar(fi: str, ff: str):
    """Conciliación automática Banco ProCredit vs Wispro (solo limpia códigos de Wispro)"""
    _ensure_schema_pcd()

    with _conn() as con:
        dfb = pd.read_sql_query("""
            SELECT codigo, fecha AS fecha_banco, valor AS monto_banco, saldo, transaccion
            FROM cb_banco_procredit
            WHERE date(fecha) BETWEEN date(?) AND date(?)
        """, con, params=(fi, ff))

        dfw = pd.read_sql_query("""
            SELECT transaction_code AS codigo, client_name AS cliente,
                   payment_date AS fecha_wispro, amount AS monto_wispro,
                   transaction_code AS codigo_wispro
            FROM cb_wispro_procredit
            WHERE date(payment_date) BETWEEN date(?) AND date(?)
        """, con, params=(fi, ff))

    # 🧼 Solo normalizamos los códigos de WISPRO
    def limpiar_wispro(c):
        if not c:
            return ""
        return re.sub(r'[^0-9]', '', str(c)).strip()  # solo números

    dfb["codigo_norm"] = dfb["codigo"].astype(str).str.strip()
    dfw["codigo_norm"] = dfw["codigo"].apply(limpiar_wispro)

    conciliados, solo_banco, solo_wispro = [], [], []

    # 🔍 Conciliar por coincidencia exacta de código numérico
    for _, b in dfb.iterrows():
        match = dfw[dfw["codigo_norm"] == b["codigo_norm"]]
        if not match.empty:
            w = match.iloc[0]
            conciliados.append({
                "codigo": b["codigo"],
                "cliente": w["cliente"],
                "fecha_banco": b["fecha_banco"],
                "monto_banco": b["monto_banco"],
                "fecha_wispro": w["fecha_wispro"],
                "monto_wispro": w["monto_wispro"],
                "codigo_wispro": w["codigo_wispro"],
                "saldo": b["saldo"],
                "estado": "Conciliado",
                "origen": "ambos"
            })
        else:
            solo_banco.append({
                "codigo": b["codigo"],
                "cliente": None,
                "fecha_banco": b["fecha_banco"],
                "monto_banco": b["monto_banco"],
                "fecha_wispro": None,
                "monto_wispro": None,
                "saldo": b["saldo"],
                "estado": "No conciliado",
                "origen": "solo banco"
            })

    # 🧾 Registros que solo están en Wispro
    codigos_banco = set(dfb["codigo_norm"].tolist())
    for _, w in dfw.iterrows():
        if w["codigo_norm"] not in codigos_banco:
            solo_wispro.append({
                "codigo": w["codigo"],
                "cliente": w["cliente"],
                "fecha_banco": None,
                "monto_banco": None,
                "fecha_wispro": w["fecha_wispro"],
                "monto_wispro": w["monto_wispro"],
                "codigo_wispro": w["codigo_wispro"],
                "saldo": None,
                "estado": "No conciliado",
                "origen": "solo wispro"
            })

    # Consolidar
    df_final = pd.DataFrame(conciliados + solo_banco + solo_wispro)

    with _conn() as con:
        cur = con.cursor()
        cur.execute("DELETE FROM cb_conciliados_procredit")
        con.commit()
        
        if not df_final.empty:
            df_final.to_sql("cb_conciliados_procredit", con, if_exists="append", index=False, dtype={
                "codigo": "TEXT",
                "cliente": "TEXT",
                "fecha_banco": "TEXT",
                "monto_banco": "REAL",
                "fecha_wispro": "TEXT",
                "monto_wispro": "REAL",
                "codigo_wispro": "TEXT",
                "saldo": "REAL",
                "estado": "TEXT",
                "origen": "TEXT"
            })


    print(f"✅ Conciliados: {len(conciliados)} | Solo Banco: {len(solo_banco)} | Solo Wispro: {len(solo_wispro)}")
    return {
        "conciliados": len(conciliados),
        "solo_banco": len(solo_banco),
        "solo_wispro": len(solo_wispro)
    }




# ============================================
# 📄 CONSULTA Y EXPORTACIÓN
# ============================================

def pcd_registros(fi=None, ff=None):
    _ensure_schema_pcd()
    query = "SELECT * FROM cb_conciliados_procredit"
    params = ()
    if fi and ff:
        query += """
            WHERE (date(fecha_banco) BETWEEN date(?) AND date(?))
               OR (date(fecha_wispro) BETWEEN date(?) AND date(?))
        """
        params = (fi, ff, fi, ff)

    with _conn() as con:
        df = pd.read_sql_query(query, con, params=params)

    if df.empty:
        return []

    for col in ["fecha_banco", "fecha_wispro"]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%d/%m/%Y")

    df["monto_banco"] = df["monto_banco"].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "")
    df["monto_wispro"] = df["monto_wispro"].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "")
    df["saldo"] = df["saldo"].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "")

    return df.to_dict(orient="records")


def pcd_exportar_excel(fi, ff, ruta):
    con = sqlite3.connect(DB_PATH)
    try:
        query = """
            SELECT codigo, cliente, fecha_banco, monto_banco, fecha_wispro,
                   monto_wispro, codigo_wispro, saldo, estado, origen
            FROM cb_conciliados_procredit
            WHERE (fecha_banco BETWEEN ? AND ?) OR (fecha_wispro BETWEEN ? AND ?)
            ORDER BY estado DESC;
        """
        df = pd.read_sql_query(query, con, params=(fi, ff, fi, ff))
        if df.empty:
            return {"mensaje": "⚠️ No hay registros para exportar."}

        os.makedirs(os.path.dirname(ruta), exist_ok=True)
        with pd.ExcelWriter(ruta, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Conciliados")
        print(f"✅ Archivo Excel generado en: {ruta}")
        return {"mensaje": "✅ Reporte exportado correctamente."}
    except Exception as e:
        return {"mensaje": f"❌ Error al exportar: {e}"}
    finally:
        con.close()
