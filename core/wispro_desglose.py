# -*- coding: utf-8 -*-
"""
Módulo Wispro → Desglose de Pagos
----------------------------------
Desglosa solo los pagos marcados como 'pendiente' en pagos_wispro
y los marca como 'procesado' al finalizar correctamente.

Autor: Mateo Guerrón
"""

import requests
import sqlite3
import os
import json

DB_PATH = os.path.join(os.getcwd(), "database", "integrador.db")
WISPRO_URL = "https://www.cloud.wispro.co/api/v1/invoicing/payments"
WISPRO_TOKEN = "d000a623-df99-4bdb-9088-66b4d79e091e"

HEADERS = {
    "accept": "application/json",
    "Authorization": WISPRO_TOKEN
}


def _conn():
    return sqlite3.connect(DB_PATH)


# ===========================================================
# 🔹 DESGLOSE SOLO DE PAGOS PENDIENTES
# ===========================================================
def procesar_desglose():
    """
    Solo procesa los pagos marcados como 'pendiente' en pagos_wispro.
    Luego los marca como 'procesado' para no repetirlos.
    """
    con = _conn()
    cur = con.cursor()

    # 🔍 Seleccionar solo los pagos pendientes
    cur.execute("""
        SELECT id, client_name, updated_at
        FROM pagos_wispro
        WHERE state='success' AND estado_desglose='pendiente'
    """)
    pagos = cur.fetchall()

    nuevos, errores = 0, 0
    print(f"🔄 Pagos pendientes encontrados: {len(pagos)}")

    for pid, cliente, fecha in pagos:
        url = f"{WISPRO_URL}/{pid}"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=60)
            if resp.status_code != 200:
                print(f"⚠️ Error {resp.status_code} en pago {pid}")
                errores += 1
                continue

            data = resp.json().get("data", {})
            transacciones = data.get("payment_transactions", [])

            for t in transacciones:
                inv_num = t.get("invoice_number")
                cur.execute("""
                    INSERT INTO desglose_pagos (
                        updated_at, client_name, transaction_code, transaction_kind,
                        transaccion_amount, transaccion_invoice_number, documentos_asociados, color
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    fecha,
                    cliente,
                    data.get("transaction_code"),
                    data.get("transaction_kind"),
                    t.get("amount", 0),
                    inv_num,
                    len(transacciones),
                    "🟢"
                ))
                nuevos += 1

            # ✅ Marcar el pago como procesado
            cur.execute("""
                UPDATE pagos_wispro
                SET estado_desglose='procesado'
                WHERE id=?
            """, (pid,))

        except Exception as e:
            print(f"⚠️ Error al procesar pago {pid}: {e}")
            errores += 1

    con.commit()
    con.close()
    print(f"✅ Desglose finalizado: nuevos {nuevos}, errores {errores}")
    return {"nuevos_desgloses": nuevos, "errores": errores}


# ===========================================================
# 🔹 LISTAR DESGLOSE PARA DASHBOARD
# ===========================================================
def listar_desglose_para_tabla():
    """
    Devuelve los registros de desglose listos para el dashboard Flask.
    """
    con = _conn()
    cur = con.cursor()

    cur.execute("""
        SELECT 
            id,
            updated_at,
            client_name,
            transaction_code,
            transaction_kind,
            transaccion_amount,
            transaccion_invoice_number,
            documentos_asociados,
            color
        FROM desglose_pagos
        ORDER BY datetime(updated_at) DESC
    """)

    rows = cur.fetchall()
    con.close()
    return rows


