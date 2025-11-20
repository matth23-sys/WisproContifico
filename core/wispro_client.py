# -*- coding: utf-8 -*-
"""
Cliente Wispro → Descarga de Pagos
----------------------------------
Descarga los pagos exitosos desde Wispro, los guarda en la base local
y automáticamente marca los nuevos pagos como 'pendiente' para luego desglosarlos.

Autor: Mateo Guerrón
"""

import requests
import sqlite3
import os
import json
from core import wispro_desglose  # Importa el módulo de desglose



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
# 🔹 DESCARGAR PAGOS Y DESGLOSAR AUTOMÁTICAMENTE LOS NUEVOS
# ===========================================================
def descargar_pagos(fecha_inicio: str, fecha_fin: str):
    params = {
        "state": "success",
        "created_at_after": f"{fecha_inicio}T00:00:00-05:00",
        "created_at_before": f"{fecha_fin}T23:59:59-05:00",
        "per_page": 2000
    }

    url = WISPRO_URL
    pagina = 1
    total_api = 0
    nuevos_pagos = 0
    nuevas_trans = 0
    monto_total = 0.0

    con = _conn()
    cur = con.cursor()

    while True:
        print(f"🔄 Descargando página {pagina}…")
        resp = requests.get(url, headers=HEADERS, params=params if pagina == 1 else None, timeout=60)
        if resp.status_code != 200:
            raise RuntimeError(f"Error {resp.status_code}: {resp.text}")

        data = resp.json()
        pagos = data.get("data", [])
        total_api += len(pagos)

        for p in pagos:
            if (p.get("state") or "").lower() != "success":
                continue

            pid = p.get("id")
            monto = float(p.get("amount") or 0)
            monto_total += monto

            # Verificar duplicado
            cur.execute("SELECT 1 FROM pagos_wispro WHERE id=?", (pid,))
            if not cur.fetchone():
                cur.execute("""
                    INSERT INTO pagos_wispro (
                        id, public_id, created_at, updated_at, state, amount, comment,
                        name_user, email_user, client_id, client_name, client_public_id,
                        payment_date, credit_amount, name_collector, email_collector,
                        transaction_kind, transaction_code, data_json, estado_desglose
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    p.get("id"), p.get("public_id"), p.get("created_at"), p.get("updated_at"),
                    p.get("state"), p.get("amount"), p.get("comment"), p.get("name_user"),
                    p.get("email_user"), p.get("client_id"), p.get("client_name"),
                    p.get("client_public_id"), p.get("payment_date"), p.get("credit_amount"),
                    p.get("name_collector"), p.get("email_collector"),
                    p.get("transaction_kind"), p.get("transaction_code"),
                    json.dumps(p),
                    "pendiente"   # 🔹 nuevo campo de control
                ))
                nuevos_pagos += 1

            # Guardar transacciones
            for t in (p.get("payment_transactions") or []):
                tid = t.get("id")
                cur.execute("SELECT 1 FROM pago_transacciones WHERE id=?", (tid,))
                if not cur.fetchone():
                    cur.execute("""
                        INSERT INTO pago_transacciones (
                            id, payment_id, invoice_id, invoice_number, amount, data_json
                        ) VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        t.get("id"), pid, t.get("invoice_id"), t.get("invoice_number"),
                        t.get("amount"), json.dumps(t)
                    ))
                    nuevas_trans += 1

        con.commit()

        next_link = data.get("links", {}).get("next")
        if not next_link:
            break
        url = next_link
        pagina += 1
        params = None

    con.close()
    print(f"✅ Descargados {total_api} pagos, {nuevos_pagos} nuevos, {nuevas_trans} transacciones nuevas, total ${monto_total:,.2f}")

    # 🚀 Si hay pagos nuevos, los desglosa automáticamente
    if nuevos_pagos > 0:
        print(f"🔍 Desglosando {nuevos_pagos} pagos nuevos...")
        resultado_desglose = wispro_desglose.procesar_desglose()
        print(f"✅ Desglose completado: {resultado_desglose}")
    else:
        print("⚠️ No hay pagos nuevos para desglosar.")

    return {
        "rango": f"{fecha_inicio} → {fecha_fin}",
        "pagos_totales_en_api": total_api,
        "pagos_nuevos": nuevos_pagos,
        "transacciones_nuevas": nuevas_trans,
        "monto_total_api": monto_total
    }


# ===========================================================
# 🔹 LISTAR PAGOS PARA DASHBOARD
# ===========================================================
def listar_pagos_para_tabla():
    con = _conn()
    cur = con.cursor()
    cur.execute("""
        SELECT
            p.id,
            p.state,
            p.amount,
            COALESCE(p.client_name, '') AS client_name,
            p.payment_date,
            COALESCE(p.transaction_code, '') AS transaction_code,
            COALESCE(p.transaction_kind, '') AS transaction_kind,
            p.estado_desglose,
            COALESCE(GROUP_CONCAT(pt.invoice_number, ', '), '') AS invoice_numbers
        FROM pagos_wispro p
        LEFT JOIN pago_transacciones pt ON pt.payment_id = p.id
        WHERE LOWER(p.state) = 'success'
        GROUP BY 
            p.id, p.state, p.amount, p.client_name, p.payment_date,
            p.transaction_code, p.transaction_kind, p.estado_desglose
        ORDER BY p.payment_date DESC
    """)
    rows = cur.fetchall()
    con.close()
    return rows


