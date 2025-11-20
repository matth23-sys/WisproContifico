
# -*- coding: utf-8 -*-
"""
Core: descargar_reporte_enviadas.py
----------------------------------
Maneja el filtrado y la exportación de facturas enviadas a Contífico.
Incluye:
 - Filtro por rango de fechas (campo 'fecha' en formato dd/mm/aaaa)
 - Generación de archivo Excel con los resultados filtrados
"""

import os
import sqlite3
import pandas as pd
from datetime import datetime
from flask import send_file, make_response

DB_PATH = os.path.join(os.getcwd(), "database", "integrador.db")


def _conn():
    """Conexión con la base de datos."""
    return sqlite3.connect(DB_PATH)


# =====================================================
# 🔹 FUNCIÓN: OBTENER FACTURAS ENVIADAS FILTRADAS
# =====================================================
def obtener_facturas_enviadas(fecha_inicio, fecha_fin):
    """
    Obtiene las facturas enviadas filtradas por rango de fechas.
    Las fechas están almacenadas como texto en formato dd/mm/aaaa.
    """
    con = _conn()
    cur = con.cursor()

    cur.execute("""
        SELECT documento_numero, forma_cobro, monto, cuenta_bancaria_id,
               numero_comprobante, fecha, status_envio, nombre_cliente, creado_en
        FROM facturas_enviadas
        ORDER BY creado_en DESC
    """)
    filas = cur.fetchall()
    columnas = [desc[0] for desc in cur.description]
    con.close()

    df = pd.DataFrame(filas, columns=columnas)

    # Si no hay fechas, devolvemos todo
    if not fecha_inicio or not fecha_fin:
        return df.to_dict(orient="records")

    # Convertir formato de entrada (aaaa-mm-dd → dd/mm/aaaa)
    def convertir(fecha_str):
        try:
            return datetime.strptime(fecha_str, "%Y-%m-%d").strftime("%d/%m/%Y")
        except:
            return fecha_str

    inicio = convertir(fecha_inicio)
    fin = convertir(fecha_fin)

    # Filtrar en rango (texto dd/mm/aaaa)
    df_filtrado = df[df["fecha"].between(inicio, fin)]
    return df_filtrado.to_dict(orient="records")


# =====================================================
# 🔹 FUNCIÓN: GENERAR ARCHIVO EXCEL CON RESULTADOS
# =====================================================
def generar_reporte_enviadas(fecha_inicio, fecha_fin):
    """
    Genera y descarga un archivo Excel con las facturas filtradas.
    Si no hay resultados, devuelve un mensaje de advertencia.
    """
    con = _conn()
    cur = con.cursor()

    cur.execute("""
        SELECT documento_numero AS 'Número Factura',
               forma_cobro AS 'Forma de Cobro',
               monto AS 'Monto',
               cuenta_bancaria_id AS 'Cuenta Bancaria',
               numero_comprobante AS 'Comprobante',
               fecha AS 'Fecha',
               status_envio AS 'Estado',
               nombre_cliente AS 'Nombre Cliente',
               creado_en AS 'Fecha Registro'
        FROM facturas_enviadas
        ORDER BY creado_en DESC
    """)
    filas = cur.fetchall()
    columnas = [desc[0] for desc in cur.description]
    con.close()

    df = pd.DataFrame(filas, columns=columnas)

    # Filtro de fechas
    if fecha_inicio and fecha_fin:
        def convertir(fecha_str):
            try:
                return datetime.strptime(fecha_str, "%Y-%m-%d").strftime("%d/%m/%Y")
            except:
                return fecha_str

        inicio = convertir(fecha_inicio)
        fin = convertir(fecha_fin)
        df = df[df["Fecha"].between(inicio, fin)]

    if df.empty:
        resp = make_response("⚠️ No se encontraron registros en ese rango de fechas.")
        resp.mimetype = "text/plain"
        return resp

    # Crear archivo Excel temporal
    nombre_archivo = f"Reporte_Facturas_Enviadas_{fecha_inicio}_a_{fecha_fin}.xlsx"
    ruta_salida = os.path.join(os.getcwd(), nombre_archivo)

    with pd.ExcelWriter(ruta_salida, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Facturas Enviadas")
        hoja = writer.sheets["Facturas Enviadas"]
        for columna in hoja.columns:
            hoja.column_dimensions[columna[0].column_letter].width = 20

    return send_file(
        ruta_salida,
        as_attachment=True,
        download_name=nombre_archivo,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
