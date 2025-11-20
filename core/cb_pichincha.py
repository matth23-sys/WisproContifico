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
PC_KIND = "PICHINCHA CTA 3474862904"

# ============================================
# 🧱 FUNCIONES BASE DE BBDD
# ============================================

def _conn():
    """Conexión SQLite"""
    return sqlite3.connect(DB_PATH)

def _ensure_schema_pc():
    """Crea las tablas necesarias si no existen (PICHINCHA)"""
    with _conn() as con:
        cur = con.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS cb_banco_pichincha (
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
        CREATE TABLE IF NOT EXISTS cb_wispro_pichincha (
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
        CREATE TABLE IF NOT EXISTS cb_conciliados_pichincha (
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
# 📥 IMPORTAR EXCEL DEL BANCO PICHINCHA
# ============================================

def pc_importar_excel(ruta_excel: str) -> int:
    """Importa registros del Banco Pichincha tomando SOLO el valor de CRÉDITO."""
    _ensure_schema_pc()

    # === Detectar inicio de tabla ===
    df_raw = pd.read_excel(ruta_excel, engine="openpyxl", header=None)
    start_row = None
    for i, row in df_raw.iterrows():
        if any(isinstance(x, str) and "DETALLE DE MOVIMIENTOS" in x.upper() for x in row):
            start_row = i + 1
            break

    if start_row is None:
        raise ValueError("❌ No se encontró la sección 'DETALLE DE MOVIMIENTOS' en el archivo Excel.")

    # === Leer la tabla real ===
    df = pd.read_excel(ruta_excel, engine="openpyxl", header=start_row)
    df = df.dropna(how="all")
    df.columns = [str(c).strip().lower() for c in df.columns]

    # === Mapeo de columnas ===
    col_fecha = next((c for c in df.columns if "fecha" in c), None)
    col_codigo = next((c for c in df.columns if "doc" in c or "código" in c or "codigo" in c), None)
    col_trans = next((c for c in df.columns if "descr" in c or "detalle" in c or "trans" in c), None)
    col_saldo = next((c for c in df.columns if "saldo" in c), None)

    # Buscar CRÉDITO explícito
    col_credito = next((c for c in df.columns if "crédito" in c or "credito" in c or "haber" in c), None)

    # Si no hay columna "crédito", usar heurística: tomar la columna a la derecha de "débito"
    if not col_credito:
        col_debito = next((c for c in df.columns if "débito" in c or "debito" in c or "cargo" in c), None)
        if col_debito:
            idx_deb = df.columns.get_loc(col_debito)
            # Tomar la primera columna numérica a la derecha
            for j in range(idx_deb + 1, len(df.columns)):
                cand = df.columns[j]
                # probar si es numérica en la mayoría de filas
                sample = pd.to_numeric(df[cand].astype(str).str.replace(r"[^0-9,.\-]", "", regex=True), errors="coerce")
                if sample.notna().sum() > 0:
                    col_credito = cand
                    break

    # Validación estricta
    mapeo = {
        "fecha": col_fecha,
        "codigo": col_codigo,
        "transaccion": col_trans,
        "credito": col_credito,
        "saldo": col_saldo
    }
    if not all(mapeo.values()):
        raise ValueError(f"❌ Columnas requeridas no identificadas (se necesita CRÉDITO): {mapeo}")

    df = df[[mapeo["fecha"], mapeo["codigo"], mapeo["transaccion"], mapeo["credito"], mapeo["saldo"]]]
    df.columns = ["fecha", "codigo", "transaccion", "valor_credito", "saldo"]

    # === Fechas con meses en español ===
    meses_es_en = {
        "ene": "jan", "feb": "feb", "mar": "mar", "abr": "apr",
        "may": "may", "jun": "jun", "jul": "jul", "ago": "aug",
        "sep": "sep", "oct": "oct", "nov": "nov", "dic": "dec"
    }

    def convertir_fecha(valor):
        if isinstance(valor, str):
            v = valor.lower()
            for es, en in meses_es_en.items():
                v = v.replace(f"-{es}-", f"-{en}-")
            return pd.to_datetime(v, errors="coerce", dayfirst=True)
        return pd.to_datetime(valor, errors="coerce", dayfirst=True)

    df["fecha"] = df["fecha"].apply(convertir_fecha)

    # === Limpieza de montos (acepta $ 1.234,56 | 1,234.56 | 1234,56 | 1234.56) ===
    def limpiar_monto(x):
        if pd.isna(x):
            return 0.0
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip()
        # Quitar símbolos
        s = re.sub(r"[^\d,.\-]", "", s)

        # Casos con , y . (formato europeo habitual 1.234,56)
        if "," in s and "." in s:
            # asume '.' miles y ',' decimales
            s = s.replace(".", "").replace(",", ".")
        else:
            # Solo comas -> tratarlas como decimal
            if "," in s and "." not in s:
                s = s.replace(",", ".")
            # Solo puntos -> ya está bien

        try:
            return float(s)
        except Exception:
            return 0.0

    df["valor_credito"] = df["valor_credito"].apply(limpiar_monto)
    df["saldo"] = df["saldo"].apply(limpiar_monto)

    # Tomamos SOLO CRÉDITO como "valor"
    df["valor"] = df["valor_credito"]

    # Filtrar filas sin código o sin valor
    df = df[df["codigo"].astype(str).str.strip().ne("")]
    df = df[df["valor"] != 0]

    # === Insertar en DB ===
    insertados = 0
    with _conn() as con:
        cur = con.cursor()
        for _, r in df.iterrows():
            codigo = str(r["codigo"]).strip()
            cur.execute("SELECT 1 FROM cb_banco_pichincha WHERE codigo=?", (codigo,))
            if not cur.fetchone():
                cur.execute("""
                    INSERT INTO cb_banco_pichincha (codigo, fecha, transaccion, valor, saldo)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    codigo,
                    r["fecha"].isoformat() if pd.notna(r["fecha"]) else None,
                    str(r["transaccion"]) if pd.notna(r["transaccion"]) else "",
                    float(r["valor"]),
                    float(r["saldo"])
                ))
                insertados += 1
        con.commit()

    print(f"✅ Registros insertados (CRÉDITO): {insertados}")
    return insertados


# ============================================
# ☁️ DESCARGAR PAGOS WISPRO (PICHINCHA)
# ============================================

def pc_descargar_wispro(fecha_inicio: str, fecha_fin: str) -> dict:
    """Descarga pagos Wispro (solo Banco Pichincha)"""
    _ensure_schema_pc()
    params = {
        "state": "success",
        "payment_date_after": f"{fecha_inicio}T00:00:00-05:00",
        "payment_date_before": f"{fecha_fin}T23:59:59-05:00",
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
            if PC_KIND not in (p.get("transaction_kind") or ""):
                continue

            pid = p.get("id")
            cur.execute("SELECT 1 FROM cb_wispro_pichincha WHERE id=?", (pid,))
            if not cur.fetchone():
                cur.execute("""
                    INSERT INTO cb_wispro_pichincha
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
    print(f"✅ Pagos descargados: {nuevos}/{total}")
    return {"total": total, "nuevos": nuevos}

# ============================================
# 🧩 CONCILIACIÓN AUTOMÁTICA PICHINCHA
# ============================================
def pc_conciliar(fi: str, ff: str):
    """Conciliación automática Banco Pichincha vs Wispro (solo limpia códigos de Wispro)"""
    _ensure_schema_pc()

    with _conn() as con:
        dfb = pd.read_sql_query("""
            SELECT codigo, fecha AS fecha_banco, valor AS monto_banco, saldo
            FROM cb_banco_pichincha
            WHERE date(fecha) BETWEEN date(?) AND date(?)
        """, con, params=(fi, ff))

        dfw = pd.read_sql_query("""
            SELECT transaction_code AS codigo, client_name AS cliente,
                   payment_date AS fecha_wispro, amount AS monto_wispro, transaction_code AS codigo_wispro
            FROM cb_wispro_pichincha
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
        cur.execute("DELETE FROM cb_conciliados_pichincha")
        con.commit()
        if not df_final.empty:
            df_final.to_sql("cb_conciliados_pichincha", con, if_exists="append", index=False)

    print(f"✅ Conciliados: {len(conciliados)} | Solo Banco: {len(solo_banco)} | Solo Wispro: {len(solo_wispro)}")
    return {"conciliados": len(conciliados), "solo_banco": len(solo_banco), "solo_wispro": len(solo_wispro)}


# ============================================
# 📄 CONSULTAR Y EXPORTAR (PICHINCHA)
# ============================================

def pc_registros(fi=None, ff=None):
    _ensure_schema_pc()
    query = "SELECT * FROM cb_conciliados_pichincha"
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

def pc_exportar_excel(fi, ff, ruta):
    con = sqlite3.connect(DB_PATH)
    try:
        query = """
            SELECT codigo, cliente, fecha_banco, monto_banco,
                   fecha_wispro, monto_wispro, codigo_wispro, saldo, estado, origen
            FROM cb_conciliados_pichincha
            WHERE (fecha_banco BETWEEN ? AND ?) OR (fecha_wispro BETWEEN ? AND ?)
            ORDER BY estado DESC;
        """
        df = pd.read_sql_query(query, con, params=(fi, ff, fi, ff))

        if df.empty:
            return {"mensaje": "⚠️ No hay registros para exportar."}

        export_dir = os.path.dirname(ruta)
        if not os.path.exists(export_dir):
            os.makedirs(export_dir)

        with pd.ExcelWriter(ruta, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Conciliados_Pichincha")

            workbook = writer.book
            worksheet = writer.sheets["Conciliados_Pichincha"]

            header_format = workbook.add_format({
                "bold": True, "bg_color": "#1d3557", "font_color": "white",
                "align": "center", "valign": "vcenter", "border": 1
            })
            cell_format = workbook.add_format({"border": 1})
            money_format = workbook.add_format({"num_format": "#,##0.00", "border": 1})
            center_format = workbook.add_format({"align": "center", "border": 1})

            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
                worksheet.set_column(col_num, col_num, 18, cell_format)

            for col in ["monto_banco", "monto_wispro", "saldo"]:
                if col in df.columns:
                    col_idx = df.columns.get_loc(col)
                    worksheet.set_column(col_idx, col_idx, 14, money_format)

            if "estado" in df.columns:
                worksheet.set_column(df.columns.get_loc("estado"), df.columns.get_loc("estado"), 14, center_format)
            if "origen" in df.columns:
                worksheet.set_column(df.columns.get_loc("origen"), df.columns.get_loc("origen"), 14, center_format)

        print(f"✅ Archivo Excel generado en: {ruta}")
        return {"mensaje": "✅ Reporte exportado correctamente."}

    except Exception as e:
        print(f"❌ Error al exportar: {e}")
        return {"mensaje": f"❌ Error al exportar: {e}"}
    finally:
        con.close()
