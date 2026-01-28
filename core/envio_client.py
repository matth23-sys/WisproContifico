# -*- coding: utf-8 -*-
"""
Envía facturas (exactas y parciales) a Contífico mediante su API.
Incluye nombre_cliente en cada registro guardado.
"""

import os
import sqlite3
import requests
from datetime import datetime

DB_PATH = os.path.join(os.getcwd(), "database", "integrador.db")

API_KEY = os.getenv("CONTIFICO_API_KEY") or "YGVucadm8aDmqTWzKox34gDGZZJ18FUNZh9ymo9nxDA"
BASE_URL = "https://api.contifico.com/sistema/api/v1"
HEADERS = {
    "Authorization": API_KEY,
    "Content-Type": "application/json",
    "Accept": "application/json",
}


def _conn():
    return sqlite3.connect(DB_PATH)


def _registrar_envio(cur, tabla, data: dict):
    """Registra un envío o error, incluyendo el nombre_cliente."""
    cur.execute(f"""
        INSERT INTO {tabla} (
            documento_id, documento_numero, forma_cobro, monto,
            cuenta_bancaria_id, numero_comprobante, fecha,
            status_envio, detalle_envio, nombre_cliente, creado_en
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data.get("documento_id"),
        data.get("documento_numero"),
        data.get("forma_cobro"),
        data.get("monto"),
        data.get("cuenta_bancaria_id"),
        data.get("numero_comprobante"),
        data.get("fecha"),
        data.get("status_envio"),
        data.get("detalle_envio"),
        data.get("nombre_cliente"),
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ))


def enviar_facturas_a_contifico():
    """
    Envía facturas en lotes de 5 para evitar timeouts.
    """
    BATCH_SIZE = 5
    con = _conn()
    cur = con.cursor()

    cur.execute("""
        SELECT 'facturas_exactas' AS origen, documento_id, documento_numero, forma_cobro,
               valor_a_enviar, codigo_mapeado, transaccion, fecha_envio, nombre_cliente
        FROM facturas_exactas
        UNION ALL
        SELECT 'facturas_parciales' AS origen, documento_id, documento_numero, forma_cobro,
               valor_a_enviar, codigo_mapeado, transaccion, fecha_envio, nombre_cliente
        FROM facturas_parciales
    """)
    facturas = cur.fetchall()

    total_envios, exitosos, errores = 0, 0, 0

    # 🔹 Procesar en bloques
    for i in range(0, len(facturas), BATCH_SIZE):
        lote = facturas[i:i + BATCH_SIZE]
        print(f"🚀 Enviando lote {i//BATCH_SIZE + 1} ({len(lote)} facturas)")

        # Nueva sesión por lote = nueva conexión
        with requests.Session() as session:
            session.headers.update(HEADERS)

            for f in lote:
                origen, documento_id, doc_num, forma_cobro, monto, cuenta_id, comprobante, fecha_envio, nombre_cliente = f
                total_envios += 1
                url = f"{BASE_URL}/documento/{documento_id}/cobro/"
                payload = {
                    "forma_cobro": forma_cobro or "TRA",
                    "monto": float(monto or 0),
                    "cuenta_bancaria_id": cuenta_id,
                    "numero_comprobante": comprobante,
                }
                if fecha_envio:
                    payload["fecha"] = fecha_envio

                try:
                    resp = session.post(url, json=payload, timeout=30)

                    try:
                        detalle = resp.json()
                    except Exception:
                        detalle = resp.text

                    if resp.status_code == 201:
                        _registrar_envio(cur, "facturas_enviadas", {
                            "documento_id": documento_id,
                            "documento_numero": doc_num,
                            "forma_cobro": forma_cobro,
                            "monto": monto,
                            "cuenta_bancaria_id": cuenta_id,
                            "numero_comprobante": comprobante,
                            "fecha": fecha_envio,
                            "status_envio": "✅ Enviado",
                            "detalle_envio": str(detalle),
                            "nombre_cliente": nombre_cliente
                        })
                        exitosos += 1
                        cur.execute(f"DELETE FROM {origen} WHERE documento_numero = ?", (doc_num,))
                        cur.execute("DELETE FROM facturas_errores WHERE documento_numero = ?", (doc_num,))
                    else:
                        _registrar_envio(cur, "facturas_errores", {
                            "documento_id": documento_id,
                            "documento_numero": doc_num,
                            "forma_cobro": forma_cobro,
                            "monto": monto,
                            "cuenta_bancaria_id": cuenta_id,
                            "numero_comprobante": comprobante,
                            "fecha": fecha_envio,
                            "status_envio": f"❌ Error {resp.status_code}",
                            "detalle_envio": str(detalle),
                            "nombre_cliente": nombre_cliente
                        })
                        errores += 1

                except Exception as e:
                    errores += 1
                    _registrar_envio(cur, "facturas_errores", {
                        "documento_id": documento_id,
                        "documento_numero": doc_num,
                        "forma_cobro": forma_cobro,
                        "monto": monto,
                        "cuenta_bancaria_id": cuenta_id,
                        "numero_comprobante": comprobante,
                        "fecha": fecha_envio,
                        "status_envio": "❌ Error interno",
                        "detalle_envio": str(e),
                        "nombre_cliente": nombre_cliente
                    })

        # 🔹 Guardar después de cada lote
        con.commit()
        print("💾 Lote guardado en DB")

    con.close()
    print(f"💸 ENVÍO FINALIZADO → Total: {total_envios}, Éxitos: {exitosos}, Errores: {errores}")
    return {"total": total_envios, "exitosos": exitosos, "errores": errores}



def listar_facturas_enviadas():
    """Devuelve las facturas enviadas correctamente, incluyendo nombre_cliente."""
    con = _conn()
    cur = con.cursor()
    cur.execute("""
        SELECT documento_numero, forma_cobro, monto, cuenta_bancaria_id,
               numero_comprobante, fecha, status_envio, detalle_envio,
               nombre_cliente, creado_en
        FROM facturas_enviadas
        ORDER BY creado_en DESC
    """)
    filas = cur.fetchall()
    columnas = [desc[0] for desc in cur.description]
    con.close()
    return [dict(zip(columnas, fila)) for fila in filas]


def listar_facturas_errores():
    """Devuelve las facturas con error de envío, incluyendo nombre_cliente."""
    con = _conn()
    cur = con.cursor()
    cur.execute("""
        SELECT documento_numero, forma_cobro, monto, cuenta_bancaria_id,
               numero_comprobante, fecha, status_envio, detalle_envio,
               nombre_cliente, creado_en
        FROM facturas_errores
        ORDER BY creado_en DESC
    """)
    filas = cur.fetchall()
    columnas = [desc[0] for desc in cur.description]
    con.close()
    return [dict(zip(columnas, fila)) for fila in filas]

