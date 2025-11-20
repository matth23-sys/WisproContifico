# -*- coding: utf-8 -*-
# routes_modulo.py
# ======================================================
# 📦 MÓDULO DE RUTAS PRINCIPALES - DINAMICRED
# ======================================================

from flask import Blueprint, render_template, request, jsonify, send_file, Response
import threading, os, sqlite3, pandas as pd, time, json
from datetime import datetime, timedelta

# ===== MÓDULOS CORE =====
from core import (
    wispro_client,
    wispro_desglose,
    contifico_client,
    comparador_client,
    descargar_reporte_no_match
)
from core.envio_client import enviar_facturas_a_contifico, listar_facturas_enviadas, listar_facturas_errores
from core.descargar_reporte_enviadas import obtener_facturas_enviadas, generar_reporte_enviadas
from core.cb_pedromoncayo import pm_importar_excel, pm_descargar_wispro, pm_conciliar, pm_exportar_excel
from core.crear_nuevos_cliente import crear_nuevos_cliente, db_conn

# ======================================================
# 🧱 CONFIGURACIÓN DEL BLUEPRINT
# ======================================================
bp = Blueprint("rutas", __name__)

DB_PATH = os.path.join(os.getcwd(), "database", "integrador.db")
estado_proceso = {"fase": "", "detalle": ""}

# ======================================================
# 🔹 TODAS LAS RUTAS PRINCIPALES
# ======================================================

# ---------- HOME ----------
@bp.route('/')
def home():
    return render_template('index.html')

# ---------- PROCESADOR GLOBAL ----------
@bp.route('/procesar_comparador')
def procesar_comparador():
    info = comparador_client.ejecutar_comparacion(validar_monto=True, tolerancia=0.05)
    return render_template("proceso_en_curso.html", info=info)

@bp.route('/procesar_todo', methods=['POST'])
def procesar_todo():
    inicio = request.form.get('inicio')
    fin = request.form.get('fin')
    if not inicio or not fin:
        return "⚠️ Debes ingresar ambas fechas."

    estado_proceso["fase"] = "Iniciando proceso..."
    estado_proceso["detalle"] = f"Rango seleccionado: {inicio} → {fin}"

    def ejecutar():
        try:
            estado_proceso["fase"] = "Descargando pagos desde Wispro..."
            wispro_client.descargar_pagos(inicio, fin)
            estado_proceso["fase"] = "Procesando desglose..."
            wispro_desglose.procesar_desglose()
            estado_proceso["fase"] = "Sincronizando facturas desde Contífico..."
            contifico_client.sincronizar_facturas_contifico()
            estado_proceso["fase"] = "✅ Proceso completado"
        except Exception as e:
            estado_proceso["fase"] = "❌ Error en proceso"
            estado_proceso["detalle"] = str(e)

    threading.Thread(target=ejecutar).start()
    return render_template('proceso_en_curso.html')

@bp.route('/estado_proceso')
def estado_proceso_api():
    return jsonify(estado_proceso)

# ---------- PAGOS Y FACTURAS ----------
@bp.route('/pagos')
def pagos():
    rows = wispro_client.listar_pagos_para_tabla()
    return render_template('pagos_dashboard.html', rows=rows)

@bp.route('/desgloses')
def desgloses():
    rows = wispro_desglose.listar_desglose_para_tabla()
    return render_template('pagos_desglose.html', rows=rows)

@bp.route('/facturas')
def facturas():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        SELECT fecha_emision, razon_social, documento_numero, documento_id, total, saldo
        FROM facturas_contifico ORDER BY fecha_emision DESC
    """)
    rows = cur.fetchall()
    cur.execute("""
        SELECT ultima_actualizacion FROM actualizaciones
        WHERE origen='contifico' ORDER BY id DESC LIMIT 1
    """)
    ultima = cur.fetchone()
    ultima_actualizacion = ultima[0] if ultima else "Sin registros aún"
    con.close()
    return render_template("facturas_dashboard.html", rows=rows, ultima_actualizacion=ultima_actualizacion)


# ---------- COMPARADOR ----------
@bp.route('/match')
def ver_match():
    data = comparador_client.listar_match()
    return render_template("match_dashboard.html", data=data)

@bp.route('/no_match')
def ver_no_match():
    fecha_inicio = (request.args.get('fecha_inicio') or '').strip()
    fecha_fin = (request.args.get('fecha_fin') or '').strip()
    data = (descargar_reporte_no_match.filtrar_nomatch(fecha_inicio, fecha_fin)
            if fecha_inicio and fecha_fin else comparador_client.listar_no_match())
    return render_template("no_match_dashboard.html", data=data,
                           fecha_inicio=fecha_inicio, fecha_fin=fecha_fin)

@bp.route('/no_match/exportar')
def exportar_no_match():
    fecha_inicio = request.args.get("fecha_inicio", "")
    fecha_fin = request.args.get("fecha_fin", "")
    return descargar_reporte_no_match.exportar_nomatch_excel(fecha_inicio, fecha_fin)

# ---------- REPORTES ----------
@bp.route('/reporte_match')
def reporte_match():
    con = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("""
        SELECT documento_id, documento_numero, forma_cobro, total_contifico, total_wispro,
               banco, codigo_mapeado, transaccion, fecha_pago, fecha_emision
        FROM facturas_procesadas ORDER BY fecha_emision DESC
    """, con)
    con.close()
    return render_template("reporte_match.html", facturas=df.to_dict(orient="records"))

@bp.route('/descargar_reporte_match')
def descargar_reporte_match():
    output = os.path.join(os.getcwd(), "reportes", "REPORTE_MATCH.xlsx")
    if not os.path.exists(output):
        pd.DataFrame(columns=["documento_id","documento_numero","total_contifico","total_wispro","banco","codigo_mapeado","fecha_pago","fecha_emision","validacion"]).to_excel(output, index=False)
    return send_file(output, as_attachment=True)

# ---------- ENVÍOS ----------
@bp.route("/enviar_a_pagar")
def route_enviar_a_pagar():
    resultado = enviar_facturas_a_contifico()
    return f"""
    <h3>💸 Envío completado</h3>
    <p>Total procesadas: {resultado['total']}</p>
    <p>✅ Enviadas correctamente: {resultado['exitosos']}</p>
    <p>❌ Errores: {resultado['errores']}</p>
    <a href="/reporte_enviadas" class="btn btn-success mt-3">Ver enviadas</a>
    <a href="/reporte_errores" class="btn btn-danger mt-3">Ver errores</a>
    <a href="/" class="btn btn-outline-secondary mt-3">Volver</a>
    """

@bp.route("/reporte_enviadas")
def route_reporte_enviadas():
    facturas = listar_facturas_enviadas()
    return render_template("reporte_enviadas.html", facturas=facturas)

@bp.route("/reporte_errores")
def route_reporte_errores():
    facturas = listar_facturas_errores()
    return render_template("reporte_errores.html", facturas=facturas)


# ---------- CONCILIACIÓN PEDRO MONCAYO ----------
@bp.route("/conciliacion-bancaria/pedromoncayo")
def cb_pm_dashboard():
    return render_template("cb_reporte_pedromoncayo.html")

@bp.route("/conciliacion-bancaria/pedromoncayo/cargar", methods=["POST"])
def cb_pm_cargar():
    f = request.files.get("archivo")
    if not f:
        return jsonify({"mensaje": "⚠️ Sube un archivo Excel."}), 400
    uploads = os.path.join(os.getcwd(), "uploads")
    os.makedirs(uploads, exist_ok=True)
    ruta = os.path.join(uploads, f.filename)
    f.save(ruta)
    n = pm_importar_excel(ruta)
    return jsonify({"mensaje": f"✅ {n} registros importados."})

@bp.route("/conciliacion-bancaria/pedromoncayo/exportar")
def cb_pm_exportar():
    fi = request.args.get("inicio")
    ff = request.args.get("fin")
    ruta = os.path.join("exports", f"PEDROMONCAYO_{fi}_a_{ff}.xlsx")
    resultado = pm_exportar_excel(fi, ff, ruta)
    if "Error" in resultado["mensaje"]:
        return jsonify(resultado), 500
    return send_file(ruta, as_attachment=True)

# ---------- CREAR CLIENTES ----------
@bp.route("/generar_clientes")
def generar_clientes():
    return render_template("generar_clientes.html")

@bp.route("/sync/run-stream")
def sync_run_stream():
    def generar():
        crear_nuevos_cliente()
        with db_conn() as conn:
            cur = conn.execute("""SELECT razon_social,email,COALESCE(phone,phone_mobile),
                                  id_number,city,address,IFNULL(status_code,'Pendiente'),
                                  IFNULL(mensaje_resultado,'Sin respuesta'),IFNULL(exito,0)
                                  FROM crear_nuevos_cliente_importados
                                  LEFT JOIN crear_nuevos_cliente_resultados USING(id_number)
                                  ORDER BY created_at DESC""")
            registros = cur.fetchall()
        for r in registros:
            yield json.dumps({"razon_social": r[0], "email": r[1], "telefono": r[2], "id": r[3], "exito": bool(r[8])}) + "\n"
            time.sleep(0.2)
    return Response(generar(), mimetype="text/plain")

