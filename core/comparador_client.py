# -*- coding: utf-8 -*-
"""
Comparador Wispro ↔ Contífico (Versión Optimizada con facturas_pendientes)
---------------------------------------------------------------------------
1. Compara pagos ↔ facturas_contifico.
2. Registra MATCH / NO MATCH.
3. Inserta facturas conciliadas en facturas_pendientes.
4. Forma_cobro = 'TRA' por defecto.
5. codigo_mapeado según el banco:
    - PICHINCHA CTA 3474862904 → 91qdGwQNiLvEdN8j
    - COOP. DE AHORRO Y CREDITO PEDRO MONCAYO LTDA. CTA 241701040196 → gDGe7DYVFWD2an2x
    - RETENCIONES → HACER RETENCIONES
    - Cualquier otro → error.
Autor: Mateo Guerrón
"""

import os
import sqlite3
import pandas as pd
from datetime import datetime



# ===========================================================
# 🔹 CONFIGURACIÓN
# ===========================================================
DB_PATH = os.path.join(os.getcwd(), "database", "integrador.db")


def _conn():
    """Retorna conexión a la base de datos como diccionario (clave → valor)."""
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row  # ✅ ahora cada fila es accesible por nombre
    return con


# ===========================================================
# 🔹 ASEGURAR TABLA facturas_pendientes
# ===========================================================
def ensure_facturas_pendientes():
    con = _conn()
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS facturas_pendientes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            documento_id TEXT UNIQUE,
            documento_numero TEXT,
            forma_cobro TEXT DEFAULT 'TRA',
            total_contifico REAL,
            total_wispro REAL,
            banco TEXT,
            codigo_mapeado TEXT,
            transaccion TEXT,
            fecha_pago TEXT,
            fecha_emision TEXT,
            creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    con.commit()
    con.close()


# ===========================================================
# 🔹 INSERCIÓN CONTROLADA EN facturas_pendientes
# ===========================================================
def insertar_factura_pendiente(cur, documento_id, doc, total_contifico, total_wispro, banco, codigo, fecha_pago, fecha_emision):
    """
    Inserta una factura en facturas_pendientes con forma_cobro fija ('TRA')
    y codigo_mapeado dependiente del banco.
    """

    # 🔹 Mapeo de bancos
    codigo_map = {
        "PICHINCHA CTA 3474862904": "91qdGwQNiLvEdN8j",
        "COOP. DE AHORRO Y CREDITO PEDRO MONCAYO LTDA. CTA 241701040196": "gDGe7DYVFWD2an2x",
        "RETENCIONES": "HACER RETENCIONES"
    }

    # 🔹 Normalizar nombre del banco
    banco_norm = str(banco).strip().upper()
    codigo_mapeado = None

    for key, val in codigo_map.items():
        if key.strip().upper() == banco_norm:
            codigo_mapeado = val
            break

    if codigo_mapeado is None:
        raise ValueError(f"❌ Banco no reconocido: '{banco}'. Solo se permiten los 3 definidos.")

    # 🔹 Insertar
    cur.execute("""
        INSERT OR REPLACE INTO facturas_pendientes (
            documento_id, documento_numero, forma_cobro,
            total_contifico, total_wispro, banco, codigo_mapeado,
            transaccion, fecha_pago, fecha_emision, creado_en
        )
        VALUES (?, ?, 'TRA', ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        documento_id, doc, total_contifico, total_wispro, banco,
        codigo_mapeado, codigo, fecha_pago, fecha_emision,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ))


# ===========================================================
# 🔹 FUNCIÓN PRINCIPAL DE COMPARACIÓN
# ===========================================================

def ejecutar_comparacion():
    """
    Compara pagos (desglose_pagos) con facturas_contifico.
    Clasifica las facturas procesadas en:
      - facturas_exactas → abs(total_contifico - total_wispro) <= 0.01
      - facturas_parciales → total_wispro
      - facturas_parciales_pendientes → (total_contifico - total_wispro)
        y vuelve a insertar ese valor (la diferencia) en facturas_contifico.total,
        eliminando luego esas facturas de facturas_parciales_pendientes.
      - Además guarda nombre_cliente (razon_social) en todas las tablas relacionadas.
    """
    import pandas as pd
    from datetime import datetime
    import sqlite3, os

    DB_PATH = os.path.join(os.getcwd(), "database", "integrador.db")

    def _conn():
        return sqlite3.connect(DB_PATH)

    def _mapear_codigo(banco: str):
        if not isinstance(banco, str):
            return None
        b = banco.upper()
        if "PICHINCHA CTA 3474862904" in b:
            return "91qdGwQNiLvEdN8j"
        if "PEDRO MONCAYO" in b:
            return "gDGe7DYVFWD2an2x"
        if "RETENCIONES" in b:
            return "HACER RETENCIONES"
        return None

    def _norm_fecha(val, dayfirst=False):
        try:
            dt = pd.to_datetime(val, errors="coerce", dayfirst=dayfirst, utc=True)
            if pd.isna(dt):
                return None, ""
            dt_naive = dt.tz_convert(None) if hasattr(dt, "tz") and dt.tz is not None else dt
            dt_naive = pd.to_datetime(dt_naive)
            return dt_naive, dt_naive.strftime("%d/%m/%Y")  # formato dd/mm/yyyy
        except Exception:
            return None, ""

    def _existe_en(cur, tabla, doc):
        cur.execute(f"SELECT 1 FROM {tabla} WHERE documento_numero = ?", (doc,))
        return cur.fetchone() is not None

    print("🔁 Iniciando comparación Wispro ↔ Contífico...")
    con = _conn()
    cur = con.cursor()

    # === Cargar datos base ===
    pagos = pd.read_sql_query("""
        SELECT client_name, transaction_code, transaction_kind,
               transaccion_amount AS total_wispro,
               transaccion_invoice_number AS documento_numero,
               updated_at AS fecha_pago
        FROM desglose_pagos
    """, con)

    facturas_contifico = pd.read_sql_query("""
        SELECT documento_numero, documento_id, total AS total_contifico,
               razon_social, fecha_emision
        FROM facturas_contifico
    """, con)

    existentes_no_match = pd.read_sql_query("""
        SELECT DISTINCT documento_numero
        FROM comparacion_no_match
    """, con)

    for df, col in [(pagos, "documento_numero"),
                    (facturas_contifico, "documento_numero"),
                    (existentes_no_match, "documento_numero")]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

    facturas_dict = {row["documento_numero"]: row for _, row in facturas_contifico.iterrows()}
    no_match_set = set(existentes_no_match["documento_numero"].tolist())

    nuevos_match = 0
    nuevos_no_match = 0
    procesadas = 0
    mov_exactas = 0
    mov_parciales = 0
    mov_parc_pend = 0
    errores = 0

    # === 1️⃣ Comparación principal ===
    for _, pago in pagos.iterrows():
        try:
            doc = str(pago["documento_numero"]).strip().upper()
            if not doc or doc == "NAN":
                continue
            
            
            try:
                monto_wispro = float(pago["total_wispro"]) if pago["total_wispro"] not in (None, "", "NULL") else 0.0
            except Exception:
                monto_wispro = 0.0

            monto_wispro = float(pago["total_wispro"] or 0)
            codigo = pago["transaction_code"]
            banco = pago["transaction_kind"]
            fecha_pago_raw = pago["fecha_pago"]

            if _existe_en(cur, "facturas_procesadas", doc) or \
               _existe_en(cur, "facturas_exactas", doc) or \
               _existe_en(cur, "facturas_parciales", doc) or \
               _existe_en(cur, "facturas_parciales_pendientes", doc):
                cur.execute("DELETE FROM comparacion_no_match WHERE documento_numero = ?", (doc,))
                continue

            if doc in facturas_dict:
                fc = facturas_dict[doc]
                total_factura = float(fc.get("total_contifico", 0))
                fecha_emision_raw = fc.get("fecha_emision", None)
                documento_id = fc.get("documento_id", None)
                nombre_cliente = fc.get("razon_social", None)
                codigo_mapeado = _mapear_codigo(banco)
                if not codigo_mapeado:
                    continue

                cur.execute("""
                    INSERT OR REPLACE INTO facturas_procesadas (
                        documento_id, documento_numero, forma_cobro,
                        total_contifico, total_wispro, banco,
                        codigo_mapeado, transaccion, fecha_pago,
                        fecha_emision, nombre_cliente, creado_en
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    documento_id, doc, "TRA",
                    total_factura, monto_wispro,
                    banco, codigo_mapeado, codigo,
                    fecha_pago_raw, fecha_emision_raw,
                    nombre_cliente,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ))

                # 🔹 Eliminar de desglose_pagos cuando hay match
                cur.execute("DELETE FROM desglose_pagos WHERE transaccion_invoice_number = ?", (doc,))

                cur.execute("DELETE FROM facturas_contifico WHERE documento_numero = ?", (doc,))
                cur.execute("DELETE FROM comparacion_no_match WHERE documento_numero = ?", (doc,))
                nuevos_match += 1
                procesadas += 1
            else:
                # 🔸 NO MATCH → guardar con el esquema exacto de tu tabla
                if not _existe_en(cur, "comparacion_no_match", doc):
                    cur.execute("""
                        INSERT INTO comparacion_no_match (
                            documento_numero,
                            documento_id,
                            total_contifico,
                            transaction_kind,
                            transaction_code,
                            fecha_emision,
                            estado_final,
                            estado_match,
                            creado_en,
                            total_wispro,
                            created_at,
                            nombre_cliente
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        doc,                     # documento_numero
                        None,                    # documento_id
                        0,                       # total_contifico (no hay en Contífico)
                        banco,                   # transaction_kind
                        codigo,                  # transaction_code
                        None,                    # fecha_emision
                        "CUADRAR",               # estado_final
                        "NO ENCONTRADO",         # estado_match
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # creado_en
                        monto_wispro,            # total_wispro (desde Wispro)
                        fecha_pago_raw,          # created_at (fecha pago Wispro)
                        pago["client_name"]      # nombre_cliente (desde desglose_pagos)
                    ))
                    nuevos_no_match += 1
        except Exception as e:
            errores += 1
            print("⚠️ Error procesando registro:", e)

    # === 2️⃣ Clasificación de facturas_procesadas ===
    facturas_proc = pd.read_sql_query("""
        SELECT documento_id, documento_numero, forma_cobro,
               total_contifico, total_wispro, banco, codigo_mapeado,
               transaccion, fecha_pago, fecha_emision, nombre_cliente, creado_en
        FROM facturas_procesadas
    """, con)

    for _, r in facturas_proc.iterrows():
        try:
            doc = str(r["documento_numero"]).strip().upper()
            if _existe_en(cur, "facturas_exactas", doc) or \
               _existe_en(cur, "facturas_parciales", doc) or \
               _existe_en(cur, "facturas_parciales_pendientes", doc):
                cur.execute("DELETE FROM facturas_procesadas WHERE documento_numero = ?", (doc,))
                continue

            total_c = float(r["total_contifico"] or 0)
            total_w = float(r["total_wispro"] or 0)
            dif_c_w = abs(round(total_c - total_w, 2))
            nombre_cliente = r["nombre_cliente"]

            dt_pago, pago_txt = _norm_fecha(r["fecha_pago"], dayfirst=False)
            dt_emis, emis_txt = _norm_fecha(r["fecha_emision"], dayfirst=True)

            if dt_pago and dt_emis and dt_pago >= dt_emis:
                fecha_envio, estado_fecha = pago_txt, "SIN CAMBIOS"
            elif dt_emis:
                fecha_envio, estado_fecha = emis_txt, "CUADRAR"
            else:
                fecha_envio, estado_fecha = "", "PENDIENTE"

            if abs(total_c - total_w) <= 0.01:
                # === FACTURAS EXACTAS ===
                valor_a_enviar = total_c
                cur.execute("""
                    INSERT OR REPLACE INTO facturas_exactas (
                        documento_id, documento_numero, forma_cobro,
                        valor_a_enviar, banco, codigo_mapeado, transaccion,
                        fecha_envio, estado_fecha, nombre_cliente, creado_en
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    r["documento_id"], doc, r["forma_cobro"], valor_a_enviar,
                    r["banco"], r["codigo_mapeado"], r["transaccion"],
                    fecha_envio, estado_fecha, nombre_cliente,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ))
                mov_exactas += 1
            else:
                # === FACTURAS PARCIALES ===
                cur.execute("""
                    INSERT OR REPLACE INTO facturas_parciales (
                        documento_id, documento_numero, forma_cobro, valor_a_enviar,
                        banco, codigo_mapeado, transaccion, fecha_envio, estado_fecha,
                        nombre_cliente, creado_en
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    r["documento_id"], doc, r["forma_cobro"], total_w,
                    r["banco"], r["codigo_mapeado"], r["transaccion"],
                    fecha_envio, estado_fecha, nombre_cliente,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ))
                mov_parciales += 1

                # === FACTURAS PARCIALES PENDIENTES ===
                cur.execute("""
                    INSERT OR REPLACE INTO facturas_parciales_pendientes (
                        documento_id, documento_numero, forma_cobro, valor_a_enviar,
                        banco, codigo_mapeado, transaccion, fecha_envio, estado_fecha,
                        nombre_cliente, creado_en
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    r["documento_id"], doc, r["forma_cobro"], dif_c_w,
                    r["banco"], r["codigo_mapeado"], r["transaccion"],
                    fecha_envio, estado_fecha, nombre_cliente,
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ))
                mov_parc_pend += 1

                # === Solo reenviar las parciales pendientes a Contífico ===
                cur.execute("""
                    INSERT OR REPLACE INTO facturas_contifico (
                        documento_id, documento_numero, total, razon_social, fecha_emision
                    ) VALUES (?, ?, ?, ?, ?)
                """, (
                    r["documento_id"], doc, dif_c_w, nombre_cliente, r["fecha_emision"]
                ))

                # Eliminar de parciales_pendientes después de reenviar
                cur.execute("DELETE FROM facturas_parciales_pendientes WHERE documento_numero = ?", (doc,))

            # Limpiar la tabla intermedia
            cur.execute("DELETE FROM facturas_procesadas WHERE documento_numero = ?", (doc,))

        except Exception as e:
            errores += 1
            print(f"⚠️ Error clasificando {r.get('documento_numero')}: {e}")

    con.commit()
    con.close()

    print(f"✅ Fin — MATCH: {nuevos_match}, NO MATCH: {nuevos_no_match}, "
          f"Exactas: {mov_exactas}, Parciales: {mov_parciales}, Pendientes: {mov_parc_pend}, "
          f"Procesadas: {procesadas}, Errores: {errores}")

    return {
        "match": nuevos_match,
        "no_match": nuevos_no_match,
        "exactas": mov_exactas,
        "parciales": mov_parciales,
        "pendientes": mov_parc_pend,
        "procesadas": procesadas,
        "errores": errores
    }


# ===========================================================
# 🔹 DASHBOARD HELPERS
# ===========================================================
def listar_match(limit: int = 1000):
    """Devuelve registros MATCH (historial completo)."""
    try:
        con = _conn()
        df = pd.read_sql_query(f"""
            SELECT documento_numero, documento_id, total_wispro, total_contifico,
                   transaction_kind, transaction_code, created_at,
                   fecha_emision, estado_match, creado_en
            FROM comparacion_match
            ORDER BY creado_en DESC
            LIMIT {int(limit)}
        """, con)
        con.close()
        return df.to_dict(orient="records")
    except Exception as e:
        print("❌ Error al listar MATCH:", e)
        return []



def listar_no_match(limit: int = 1000):
    """Devuelve registros NO MATCH (pendientes de revisión)."""
    try:
        con = _conn()
        df = pd.read_sql_query(f"""
            SELECT
                documento_numero,
                transaction_kind,
                transaction_code,
                total_contifico,
                fecha_emision,
                estado_match,
                creado_en,
                total_wispro,
                created_at,
                nombre_cliente
            FROM comparacion_no_match
            ORDER BY creado_en DESC
            LIMIT {int(limit)}
        """, con)
        con.close()
        return df.to_dict(orient="records")
    except Exception as e:
        print("❌ Error al listar NO MATCH:", e)
        return []



