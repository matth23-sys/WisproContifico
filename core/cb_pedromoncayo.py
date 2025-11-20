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
PM_KIND = "COOP. DE AHORRO Y CREDITO PEDRO MONCAYO LTDA. CTA 241701040196"

# ============================================
# 🧱 FUNCIONES BASE DE BBDD
# ============================================

def _conn():
    """Conexión SQLite"""
    return sqlite3.connect(DB_PATH)

def _ensure_schema():
    """Crea las tablas necesarias si no existen"""
    with _conn() as con:
        cur = con.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS cb_banco_pedromoncayo (
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
        CREATE TABLE IF NOT EXISTS cb_wispro_pedromoncayo (
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
        CREATE TABLE IF NOT EXISTS cb_conciliados_pedromoncayo (
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
# 📥 IMPORTAR EXCEL DEL BANCO
# ============================================

def pm_importar_excel(ruta_excel: str) -> int:
    """Importa registros bancarios desde Excel (únicos por código)"""
    _ensure_schema()
    df = pd.read_excel(ruta_excel)
    df.columns = [c.lower() for c in df.columns]

    mapping = {
        "fecha": next((c for c in df.columns if "fecha" in c), None),
        "codigo": next((c for c in df.columns if "doc" in c or "codigo" in c), None),
        "transaccion": next((c for c in df.columns if "trans" in c), None),
        "valor": next((c for c in df.columns if "valor" in c), None),
        "saldo": next((c for c in df.columns if "saldo" in c), None)
    }

    df = df[[mapping["fecha"], mapping["codigo"], mapping["transaccion"], mapping["valor"], mapping["saldo"]]]
    df.columns = ["fecha", "codigo", "transaccion", "valor", "saldo"]
    df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")

    insertados = 0
    with _conn() as con:
        cur = con.cursor()
        for _, r in df.iterrows():
            codigo = str(r["codigo"]).strip()
            cur.execute("SELECT 1 FROM cb_banco_pedromoncayo WHERE codigo=?", (codigo,))
            if not cur.fetchone():
                cur.execute("""
                    INSERT INTO cb_banco_pedromoncayo (codigo, fecha, transaccion, valor, saldo)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    codigo,
                    r["fecha"].isoformat() if pd.notna(r["fecha"]) else None,
                    str(r["transaccion"]),
                    float(r["valor"] or 0),
                    float(r["saldo"] or 0)
                ))
                insertados += 1
        con.commit()

    return insertados

# ============================================
# ☁️ DESCARGAR PAGOS DESDE WISPRO
# ============================================

def pm_descargar_wispro(fecha_inicio: str, fecha_fin: str) -> dict:
    """Descarga pagos Wispro (solo Coop. Pedro Moncayo)"""
    _ensure_schema()
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
            cur.execute("SELECT 1 FROM cb_wispro_pedromoncayo WHERE id=?", (pid,))
            if not cur.fetchone():
                cur.execute("""
                    INSERT INTO cb_wispro_pedromoncayo
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
# 🧩 CONCILIACIÓN AUTOMÁTICA
# ============================================

def _norm_code(c: str) -> str:
    if not c:
        return ""
    return re.sub(r'[^a-z0-9]', '', str(c).lower())

def pm_conciliar(fi: str, ff: str):
    """Conciliación automática Banco vs Wispro (solo Coop. Pedro Moncayo)"""
    _ensure_schema()

    with _conn() as con:
        dfb = pd.read_sql_query("""
            SELECT codigo, fecha AS fecha_banco, valor AS monto_banco, saldo
            FROM cb_banco_pedromoncayo
            WHERE date(fecha) BETWEEN date(?) AND date(?)
        """, con, params=(fi, ff))

        dfw = pd.read_sql_query("""
            SELECT transaction_code AS codigo, client_name AS cliente,
                   payment_date AS fecha_wispro, amount AS monto_wispro,transaction_code AS codigo_wispro
            FROM cb_wispro_pedromoncayo
            WHERE date(payment_date) BETWEEN date(?) AND date(?)
        """, con, params=(fi, ff))

    # 🧩 Normalizar códigos (solo letras y números, sin espacios)
    def norm_code(c):
        if not c:
            return ""
        c = re.sub(r'[^a-zA-Z0-9]', '', str(c)).lower()
        return c.strip()

    dfb["codigo_norm"] = dfb["codigo"].apply(norm_code)
    dfw["codigo_norm"] = dfw["codigo"].apply(norm_code)

    conciliados, solo_banco, solo_wispro = [], [], []

    # ✅ Coincidencias — compara solo los primeros 7 caracteres
    for _, b in dfb.iterrows():
        codigo_banco = b["codigo_norm"][:7]
        match = dfw[dfw["codigo_norm"].str.startswith(codigo_banco)]

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

    # ✅ Solo Wispro (no presente en banco)
    codigos_banco = [c[:7] for c in dfb["codigo_norm"].tolist()]

    for _, w in dfw.iterrows():
        codigo_wispro = w["codigo_norm"][:7]
        if not any(codigo_wispro == c for c in codigos_banco):
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

    # 🧾 Consolidar resultados
    df_final = pd.DataFrame(conciliados + solo_banco + solo_wispro)

    with _conn() as con:
        cur = con.cursor()
        cur.execute("DELETE FROM cb_conciliados_pedromoncayo")
        con.commit()
        if not df_final.empty:
            df_final.to_sql("cb_conciliados_pedromoncayo", con, if_exists="append", index=False)

    print(f"✅ Conciliados: {len(conciliados)} | Solo Banco: {len(solo_banco)} | Solo Wispro: {len(solo_wispro)}")

    return {
        "conciliados": len(conciliados),
        "solo_banco": len(solo_banco),
        "solo_wispro": len(solo_wispro)
    }


# ============================================
# 📄 CONSULTAR Y EXPORTAR
# ============================================

def pm_registros(fi=None, ff=None):
    """Devuelve los registros conciliados según rango de fechas."""
    _ensure_schema()
    query = "SELECT * FROM cb_conciliados_pedromoncayo"
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

    # ✅ Formatear fechas y valores
    for col in ["fecha_banco", "fecha_wispro"]:
        df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%d/%m/%Y")

    df["monto_banco"] = df["monto_banco"].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "")
    df["monto_wispro"] = df["monto_wispro"].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "")
    df["saldo"] = df["saldo"].apply(lambda x: f"${x:,.2f}" if pd.notna(x) else "")

    return df.to_dict(orient="records")


def pm_exportar_excel(fi, ff, ruta):
    """
    Exporta los registros conciliados de Pedro Moncayo a un archivo Excel.
    Incluye 'codigo_wispro' y usa una ruta absoluta garantizada.
    """
    con = sqlite3.connect(DB_PATH)
    try:
        query = """
            SELECT 
                codigo,
                cliente,
                fecha_banco,
                monto_banco,
                fecha_wispro,
                monto_wispro,
                codigo_wispro,
                saldo,
                estado,
                origen
            FROM cb_conciliados_pedromoncayo
            WHERE 
                (fecha_banco BETWEEN ? AND ?)
                OR 
                (fecha_wispro BETWEEN ? AND ?)
            ORDER BY estado DESC;
        """

        df = pd.read_sql_query(query, con, params=(fi, ff, fi, ff))

        if df.empty:
            return {"mensaje": "⚠️ No hay registros para exportar."}

        # Crear carpeta si no existe
        export_dir = os.path.dirname(ruta)
        if not os.path.exists(export_dir):
            os.makedirs(export_dir)

        # Guardar Excel con XlsxWriter
        with pd.ExcelWriter(ruta, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Conciliados")

            workbook = writer.book
            worksheet = writer.sheets["Conciliados"]

            # === Estilos básicos ===
            header_format = workbook.add_format({
                "bold": True,
                "bg_color": "#1d3557",
                "font_color": "white",
                "align": "center",
                "valign": "vcenter",
                "border": 1
            })
            cell_format = workbook.add_format({"border": 1})
            money_format = workbook.add_format({"num_format": "#,##0.00", "border": 1})
            center_format = workbook.add_format({"align": "center", "border": 1})

            # Encabezados
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
                worksheet.set_column(col_num, col_num, 18, cell_format)

            # Ajustar ancho y formato de columnas específicas
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



