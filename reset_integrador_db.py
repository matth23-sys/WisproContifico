# -*- coding: utf-8 -*-
"""
Reinicia completamente la base de datos integrador.db
------------------------------------------------------
Elimina las tablas existentes y las vuelve a crear desde cero, incluyendo:

  - pagos_wispro
  - pago_transacciones
  - desglose_pagos
  - facturas_contifico
  - actualizaciones
  - comparacion_match
  - comparacion_no_match
  - resultados_match
  - facturas_procesadas
  - facturas_exactas
  - facturas_parciales
  - facturas_parciales_pendientes
  - facturas_enviadas
  - facturas_errores

Autor: Mateo Guerrón
"""

import sqlite3
import os

DB_PATH = os.path.join(os.getcwd(), "database", "integrador.db")


def reset_database():
    """Elimina y recrea todas las tablas del integrador."""
    db_dir = os.path.dirname(DB_PATH)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    print("⚠️  Limpiando base de datos existente...")

    tablas = [
        "pagos_wispro",
        "pago_transacciones",
        "desglose_pagos",
        "facturas_contifico",
        "actualizaciones",
        "comparacion_match",
        "comparacion_no_match",
        "resultados_match",
        "facturas_procesadas",
        "facturas_exactas", 
        "facturas_totales", 
        "facturas_parciales",
        "facturas_parciales_pendientes",
        "facturas_enviadas",
        "facturas_errores"
    ]
    for t in tablas:
        cur.execute(f"DROP TABLE IF EXISTS {t};")

    # ===========================
    # 🔹 TABLA 1: PAGOS DE WISPRO
    # ===========================
    cur.execute("""
    CREATE TABLE pagos_wispro (
        id TEXT PRIMARY KEY,
        public_id TEXT,
        created_at TEXT,
        updated_at TEXT,
        state TEXT,
        amount REAL,
        comment TEXT,
        name_user TEXT,
        email_user TEXT,
        client_id TEXT,
        client_name TEXT,
        client_public_id TEXT,
        payment_date TEXT,
        credit_amount REAL,
        name_collector TEXT,
        email_collector TEXT,
        transaction_kind TEXT,
        transaction_code TEXT,
        estado_desglose TEXT DEFAULT 'pendiente',
        data_json TEXT
    );
    """)

    # ===========================
    # 🔹 TABLA 2: TRANSACCIONES ASOCIADAS
    # ===========================
    cur.execute("""
    CREATE TABLE pago_transacciones (
        id TEXT PRIMARY KEY,
        payment_id TEXT,
        invoice_id TEXT,
        invoice_number TEXT,
        amount REAL,
        data_json TEXT
    );
    """)

    # ===========================
    # 🔹 TABLA 3: DESGLOSE DE PAGOS
    # ===========================
    cur.execute("""
    CREATE TABLE desglose_pagos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        updated_at TEXT,
        client_name TEXT,
        transaction_code TEXT,
        transaction_kind TEXT,
        transaccion_amount REAL,
        transaccion_invoice_number TEXT,
        documentos_asociados INTEGER,
        color TEXT
    );
    """)

    # ===========================
    # 🔹 TABLA 4: FACTURAS CONTIFICO
    # ===========================
    cur.execute("""
    CREATE TABLE facturas_contifico (
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
    );
    """)

    # ===========================
    # 🔹 TABLA 5: REGISTRO DE ACTUALIZACIONES
    # ===========================
    cur.execute("""
    CREATE TABLE actualizaciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        origen TEXT NOT NULL,
        ultima_actualizacion TEXT NOT NULL,
        creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # ===========================
    # 🔹 TABLA 6: COMPARACIÓN MATCH
    # ===========================
    cur.execute("""
    CREATE TABLE comparacion_match (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        documento_numero TEXT UNIQUE,
        documento_id TEXT,
        total_contifico REAL,
        total_wispro REAL,
        transaction_kind TEXT,
        transaction_code TEXT,
        created_at TEXT,
        fecha_emision TEXT,
        estado_final TEXT,
        estado_match TEXT,
        nombre_cliente TEXT,
        creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # ===========================
    # 🔹 TABLA 7: COMPARACIÓN NO MATCH
    # ===========================
    cur.execute("""
                CREATE TABLE IF NOT EXISTS comparacion_no_match (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    documento_numero TEXT UNIQUE,
    documento_id TEXT,
    total_contifico REAL,
    transaction_kind TEXT,
    transaction_code TEXT,
    fecha_emision TEXT,
    estado_final TEXT,
    estado_match TEXT,
    creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_wispro REAL,
    created_at TEXT,
    nombre_cliente TEXT
    );
    """)

    # ===========================
    # 🔹 TABLA 8: RESULTADOS DE MATCH
    # ===========================
    cur.execute("""
    CREATE TABLE resultados_match (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        id_match TEXT UNIQUE,
        fecha_proceso TEXT,
        client_name TEXT,
        documento_numero TEXT,
        monto_pago REAL,
        monto_factura REAL,
        estado_match TEXT,
        observacion TEXT,
        nombre_cliente TEXT,
        creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # ===========================
    # 🔹 TABLA 9: FACTURAS PROCESADAS
    # ===========================
    cur.execute("""
    CREATE TABLE facturas_procesadas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        documento_id TEXT UNIQUE,
        documento_numero TEXT,
        forma_cobro TEXT,
        total_contifico REAL,
        total_wispro REAL,
        banco TEXT,
        codigo_mapeado TEXT,
        transaccion TEXT,
        fecha_pago TEXT,
        fecha_emision TEXT,
        nombre_cliente TEXT,
        creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # ===========================
    # 🔹 TABLA 10: FACTURAS EXACTAS
    # ===========================
    cur.execute("""
    CREATE TABLE facturas_exactas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        documento_id TEXT,
        documento_numero TEXT UNIQUE,
        forma_cobro TEXT,
        valor_a_enviar REAL,
        banco TEXT,
        codigo_mapeado TEXT,
        transaccion TEXT,
        fecha_envio TEXT,
        estado_fecha TEXT,
        nombre_cliente TEXT,
        creado_en TEXT
    );
    """)

    # ===========================
    # 🔹 TABLA 11: FACTURAS PARCIALES PENDIENTES
    # ===========================
    cur.execute("""
    CREATE TABLE facturas_parciales_pendientes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        documento_id TEXT,
        documento_numero TEXT UNIQUE,
        forma_cobro TEXT,
        valor_a_enviar REAL,
        banco TEXT,
        codigo_mapeado TEXT,
        transaccion TEXT,
        fecha_envio TEXT,
        estado_fecha TEXT,
        nombre_cliente TEXT,
        creado_en TEXT
    );
    """)

    # ===========================
    # 🔹 TABLA 12: FACTURAS PARCIALES
    # ===========================
    cur.execute("""
    CREATE TABLE facturas_parciales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        documento_id TEXT,
        documento_numero TEXT UNIQUE,
        forma_cobro TEXT,
        valor_a_enviar REAL,
        banco TEXT,
        codigo_mapeado TEXT,
        transaccion TEXT,
        fecha_envio TEXT,
        estado_fecha TEXT,
        nombre_cliente TEXT,
        creado_en TEXT
    );
    """)

    # ===========================
    # 🔹 TABLA 13: FACTURAS ENVIADAS
    # ===========================
    cur.execute("""
    CREATE TABLE facturas_enviadas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        documento_id TEXT,
        documento_numero TEXT,
        forma_cobro TEXT,
        monto REAL,
        cuenta_bancaria_id TEXT,
        numero_comprobante TEXT,
        fecha TEXT,
        status_envio TEXT,
        detalle_envio TEXT,
        nombre_cliente TEXT,
        creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # ===========================
    # 🔹 TABLA 14: FACTURAS ERRORES
    # ===========================
    cur.execute("""
    CREATE TABLE facturas_errores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        documento_id TEXT,
        documento_numero TEXT,
        forma_cobro TEXT,
        monto REAL,
        cuenta_bancaria_id TEXT,
        numero_comprobante TEXT,
        fecha TEXT,
        status_envio TEXT,
        detalle_envio TEXT,
        nombre_cliente TEXT,
        creado_en TEXT
    );
    """)

    con.commit()
    con.close()

    print("\n✅ Base de datos reiniciada correctamente.")
    print("📂 Ubicación:", DB_PATH)
    print("📋 Tablas creadas con el nuevo campo 'nombre_cliente' en todas las tablas principales.")


if __name__ == "__main__":
    reset_database()
