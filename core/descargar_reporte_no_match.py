# -*- coding: utf-8 -*-
"""
Core: descargar_reporte_no_match.py
-----------------------------------
Maneja el filtrado y exportación del reporte de pagos NO MATCH.
Compatible con el template no_match_dashboard.html
"""

import os
import sqlite3
import pandas as pd
from datetime import datetime
from flask import send_file, make_response

DB_PATH = os.path.join(os.getcwd(), "database", "integrador.db")


def _conn():
    return sqlite3.connect(DB_PATH)


def filtrar_nomatch(fecha_inicio=None, fecha_fin=None):
    """
    Devuelve registros NO MATCH filtrados por FECHA (YYYY-MM-DD) usando SQL.
    'created_at' está almacenado como texto ISO con zona; usamos SUBSTR(created_at,1,10).
    """
    con = _conn()
    cur = con.cursor()

    if fecha_inicio and fecha_fin:
        cur.execute("""
            SELECT
                documento_numero,
                total_wispro,
                transaction_code,
                created_at,
                estado_match
            FROM comparacion_no_match
            WHERE SUBSTR(created_at,1,10) BETWEEN ? AND ?
            ORDER BY SUBSTR(created_at,1,19) DESC
        """, (fecha_inicio, fecha_fin))
    else:
        cur.execute("""
            SELECT
                documento_numero,
                total_wispro,
                transaction_code,
                created_at,
                estado_match
            FROM comparacion_no_match
            ORDER BY SUBSTR(created_at,1,19) DESC
        """)

    rows = cur.fetchall()
    cols = [c[0] for c in cur.description]
    con.close()
    # devolver como lista de dicts para Jinja
    return [dict(zip(cols, r)) for r in rows]

def exportar_nomatch_excel(fecha_inicio=None, fecha_fin=None):
    """
    Genera un Excel con los NO MATCH filtrados por FECHA (YYYY-MM-DD) usando SQL.
    """
    con = _conn()

    if fecha_inicio and fecha_fin:
        query = """
            SELECT
                documento_numero AS 'Documento',
                total_wispro     AS 'Total Wispro',
                transaction_code AS 'Código Transacción',
                created_at       AS 'Fecha',
                estado_match     AS 'Estado Match'
            FROM comparacion_no_match
            WHERE SUBSTR(created_at,1,10) BETWEEN ? AND ?
            ORDER BY SUBSTR(created_at,1,19) DESC
        """
        df = pd.read_sql_query(query, con, params=(fecha_inicio, fecha_fin))
    else:
        query = """
            SELECT
                documento_numero AS 'Documento',
                total_wispro     AS 'Total Wispro',
                transaction_code AS 'Código Transacción',
                created_at       AS 'Fecha',
                estado_match     AS 'Estado Match'
            FROM comparacion_no_match
            ORDER BY SUBSTR(created_at,1,19) DESC
        """
        df = pd.read_sql_query(query, con)

    con.close()

    if df.empty:
        from flask import make_response
        resp = make_response("⚠️ No se encontraron registros NO MATCH para ese rango de fechas.")
        resp.mimetype = "text/plain"
        return resp

    # crear Excel temporal
    nombre = f"Reporte_NO_MATCH_{(fecha_inicio or 'todo')}_a_{(fecha_fin or 'todo')}.xlsx"
    ruta = os.path.join(os.getcwd(), nombre)

    with pd.ExcelWriter(ruta, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="NO MATCH")
        hoja = w.sheets["NO MATCH"]
        for col in hoja.columns:
            hoja.column_dimensions[col[0].column_letter].width = 20

    from flask import send_file
    return send_file(
        ruta,
        as_attachment=True,
        download_name=nombre,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


def exportar_asientos_excel(fecha_inicio=None, fecha_fin=None):
    """
    Genera un archivo Excel con formato contable (asientos)
    a partir de los registros NO MATCH filtrados por fechas.
    """
    from flask import send_file, make_response

    con = _conn()
    if fecha_inicio and fecha_fin:
        query = """
            SELECT
                transaction_code,
                transaction_kind,
                total_wispro,
                created_at,
                nombre_cliente
            FROM comparacion_no_match
            WHERE SUBSTR(created_at,1,10) BETWEEN ? AND ?
            ORDER BY SUBSTR(created_at,1,19) DESC
        """
        df = pd.read_sql_query(query, con, params=(fecha_inicio, fecha_fin))
    else:
        query = """
            SELECT
                transaction_code,
                transaction_kind,
                total_wispro,
                created_at,
                nombre_cliente
            FROM comparacion_no_match
            ORDER BY SUBSTR(created_at,1,19) DESC
        """
        df = pd.read_sql_query(query, con)
    con.close()

    if df.empty:
        resp = make_response("⚠️ No se encontraron registros para generar asientos.")
        resp.mimetype = "text/plain"
        return resp

    # =====================================================
    # 🔹 FORMATEO DE DATOS
    # =====================================================
    filas = []
    for _, row in df.iterrows():
        fecha_obj = pd.to_datetime(row["created_at"], errors="coerce")
        fecha_texto = fecha_obj.strftime("%d/%m/%Y") if not pd.isna(fecha_obj) else ""

        glosa = f"WISPRO {row['transaction_code']} {row['nombre_cliente']} APP DR"

        # Mapeo de cuentas según banco
        banco = row["transaction_kind"]
        if "PICHINCHA CTA 3474862904" in banco.upper():
            cuenta2 = "Banco Pichincha Cta. Cte. #3474862904"
        elif "PEDRO MONCAYO" in banco.upper():
            cuenta2 = "Coop. Pedro Moncayo Cta. Ahorros. #241701040196"
        elif "PROCREDIT" in banco.upper():
            cuenta2 = "Banco Procredit Cta Cte #019037892134"
        else:
            cuenta2 = banco or "Cuenta No Identificada"

        valor = str(row["total_wispro"]).replace(".", ",")  # valor con coma

        # === Fila 1 ===
        filas.append({
            "FECHA": fecha_texto,
            "GLOSA": glosa,
            "CUENTA": "",
            "DEBE": "",
            "HABER": "",
            "CENTRO COSTO": ""
        })

        # === Fila 2 ===
        filas.append({
            "FECHA": "",
            "GLOSA": "",
            "CUENTA": "Clientes Locales No Relacionados",
            "DEBE": "",
            "HABER": valor,
            "CENTRO COSTO": ""
        })

        # === Fila 3 ===
        filas.append({
            "FECHA": "",
            "GLOSA": "",
            "CUENTA": cuenta2,
            "DEBE": valor,
            "HABER": "",
            "CENTRO COSTO": ""
        })

    # =====================================================
    # 🔹 GENERAR EXCEL
    # =====================================================
    df_out = pd.DataFrame(filas)
    nombre_archivo = f"Asientos_NO_MATCH_{(fecha_inicio or 'todo')}_a_{(fecha_fin or 'todo')}.xlsx"
    ruta_salida = os.path.join(os.getcwd(), nombre_archivo)

    with pd.ExcelWriter(ruta_salida, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, sheet_name="ASIENTOS")
        hoja = writer.sheets["ASIENTOS"]
        for col in hoja.columns:
            hoja.column_dimensions[col[0].column_letter].width = 25

    return send_file(
        ruta_salida,
        as_attachment=True,
        download_name=nombre_archivo,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
