# app.py
from flask import Flask, render_template, request, jsonify, redirect, session, url_for, flash
from werkzeug.serving import run_simple
import threading
from datetime import datetime, timedelta
import os, time, sqlite3, pandas as pd
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
from core.comparador_client import ejecutar_comparacion, _conn

# ====== MÓDULOS CORE ======
from core import (
    wispro_client,
    wispro_desglose,
    contifico_client,
    comparador_client
)
from core.envio_client import enviar_facturas_a_contifico, listar_facturas_enviadas, listar_facturas_errores
from core.descargar_reporte_enviadas import obtener_facturas_enviadas, generar_reporte_enviadas



# ======================================
# 🚀 INICIALIZAR FLASK
# ======================================
app = Flask(__name__, template_folder='templates', static_folder='static')
app.secret_key = "dinamicred2025"  # cámbiala por algo único en producción

# ======================================================
# 👤 LOGIN Y LOGOUT
# ======================================================
USUARIO_POR_DEFECTO = {
    "username": "admin@dinamicred.com",
    "password_plano": "dinamicred2025nr"
}

def _conn():
    return sqlite3.connect(DB_PATH)

def _ensure_user_table():
    """Crea tabla usuarios y usuario por defecto si no existen."""
    con = _conn()
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    con.commit()

    # Verificar usuario por defecto
    cur.execute("SELECT username FROM usuarios WHERE username=?", (USUARIO_POR_DEFECTO["username"],))
    existe = cur.fetchone()
    if not existe:
        hash_pw = generate_password_hash(USUARIO_POR_DEFECTO["password_plano"], method="pbkdf2:sha256", salt_length=16)
        cur.execute("INSERT INTO usuarios (username, password_hash) VALUES (?, ?)",
                    (USUARIO_POR_DEFECTO["username"], hash_pw))
        con.commit()
        print("✅ Usuario por defecto creado:", USUARIO_POR_DEFECTO["username"])
    con.close()


# ======================================================
# 🔐 RUTAS DE LOGIN / LOGOUT
# ======================================================
@app.route("/login", methods=["GET", "POST"])
def login():
    _ensure_user_table()

    if request.method == "POST":
        username = request.form.get("username").strip()
        password = request.form.get("password").strip()

        con = _conn()
        cur = con.cursor()
        cur.execute("SELECT id, password_hash FROM usuarios WHERE username=?", (username,))
        user = cur.fetchone()
        con.close()

        if not user or not check_password_hash(user[1], password):
            flash("❌ Usuario o contraseña incorrectos.", "danger")
            return render_template("login.html")

        session.clear()
        session["user_id"] = user[0]
        session["username"] = username
        flash("✅ Sesión iniciada correctamente.", "success")
        return redirect(url_for("rutas.home"))  # 👈 Ajustado para tu blueprint

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    flash("Sesión cerrada correctamente.", "info")
    return redirect(url_for("login"))


# ======================================================
# 🧱 MIDDLEWARE GLOBAL DE PROTECCIÓN
# ======================================================
@app.before_request
def proteger_rutas():
    """Protege todas las rutas excepto login, logout y archivos estáticos."""
    rutas_publicas = ["/login", "/logout", "/static", "/favicon.ico"]

    # Permitir rutas públicas
    if any(request.path.startswith(r) for r in rutas_publicas):
        return None

    # Evitar que usuario logueado vuelva a /login
    if session.get("user_id") and request.path == "/login":
        return redirect(url_for("rutas.home"))

    # Si no hay sesión → redirigir a login
    if not session.get("user_id"):
        return redirect(url_for("login"))


# ======================================================
# 📦 IMPORTAR Y REGISTRAR TUS RUTAS GRANDES
# ======================================================
from routes_modulo import bp as rutas_bp
app.register_blueprint(rutas_bp)

















# ======================================================
# 🔹 PROCESAR COMPARADOR MANUAL
# ======================================================
@app.route('/procesar_comparador')
def procesar_comparador():
    """Ejecuta la comparación global Contífico ↔ Wispro (versión manual)."""
    info = comparador_client.ejecutar_comparacion(validar_monto=True, tolerancia=0.05)
    return render_template("proceso_en_curso.html", info=info)


@app.route('/')
def home():
    return render_template('index.html')


@app.route('/pagos')
def pagos():
    rows = wispro_client.listar_pagos_para_tabla()
    return render_template('pagos_dashboard.html', rows=rows)


@app.route('/desgloses')
def desgloses():
    rows = wispro_desglose.listar_desglose_para_tabla()
    return render_template('pagos_desglose.html', rows=rows)


@app.route('/facturas')
def facturas():
    db_path = os.path.join(os.getcwd(), "database", "integrador.db")
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    cur.execute("""
        SELECT fecha_emision, razon_social, documento_numero, documento_id, total, saldo
        FROM facturas_contifico
        ORDER BY fecha_emision DESC
    """)
    rows = cur.fetchall()

    cur.execute("""
        SELECT ultima_actualizacion
        FROM actualizaciones
        WHERE origen='contifico'
        ORDER BY id DESC
        LIMIT 1
    """)
    ultima = cur.fetchone()
    ultima_actualizacion = ultima[0] if ultima else "Sin registros aún"

    con.close()
    return render_template("facturas_dashboard.html", rows=rows, ultima_actualizacion=ultima_actualizacion)


@app.route('/estado_proceso')
def estado_proceso_api():
    return jsonify(estado_proceso)


def ejecutar_proceso_completo(inicio, fin):
    """Ejecuta Wispro + Desglose + Contífico"""
    global estado_proceso
    try:
        # ===== 1️⃣ WISPRO =====
        estado_proceso["fase"] = "Descargando pagos desde Wispro..."
        estado_proceso["detalle"] = f"Consultando API del {inicio} al {fin}"
        resumen = wispro_client.descargar_pagos(inicio, fin)

        pagos_nuevos = resumen.get("pagos_nuevos", 0)
        if pagos_nuevos == 0:
            estado_proceso["fase"] = "🟡 Proceso detenido"
            estado_proceso["detalle"] = "No se encontraron pagos nuevos."
            print("⚠️ No se encontraron pagos nuevos en Wispro, proceso detenido.")
            return

        # ===== 2️⃣ DESGLOSE =====
        estado_proceso["fase"] = "Procesando desglose de pagos..."
        estado_proceso["detalle"] = "Analizando facturas asociadas a cada pago..."
        desglose_info = wispro_desglose.procesar_desglose()

        # ===== 3️⃣ CONTÍFICO =====
        estado_proceso["fase"] = "Sincronizando facturas desde Contífico..."
        estado_proceso["detalle"] = "Consulta independiente: 18/10/2025 → hoy"
        contifico_info = contifico_client.sincronizar_facturas_contifico()

        # ===== ✅ FIN =====
        estado_proceso["fase"] = "✅ Proceso completado"
        estado_proceso["detalle"] = (
            f"Pagos nuevos: {pagos_nuevos} | "
            f"Desgloses nuevos: {desglose_info.get('nuevos', 0)} | "
            f"Facturas nuevas: {contifico_info.get('nuevos', 0)} | "
            f"Existentes (Contífico): {contifico_info.get('existentes', 0)}"
        )

        print("✅ Proceso completo ejecutado correctamente.")
        print(f"📊 {estado_proceso['detalle']}")

    except Exception as e:
        estado_proceso["fase"] = "❌ Error en el proceso"
        estado_proceso["detalle"] = str(e)
        print("❌ Error durante la ejecución:", e)


@app.route('/procesar_todo', methods=['POST'])
def procesar_todo():
    """Ejecuta Wispro + Desglose + Contífico (manual)."""
    inicio = request.form.get('inicio')
    fin = request.form.get('fin')
    if not inicio or not fin:
        return "⚠️ Debes ingresar ambas fechas."

    global estado_proceso
    estado_proceso["fase"] = "Iniciando proceso..."
    estado_proceso["detalle"] = f"Rango seleccionado: {inicio} → {fin}"

    hilo = threading.Thread(target=ejecutar_proceso_completo, args=(inicio, fin))
    hilo.start()

    return render_template('proceso_en_curso.html')


# ======================================================
# 🔁 SINCRONIZACIÓN AUTOMÁTICA CONTIFICO (CADA 1 MINUTO)
# ======================================================
def sincronizacion_automatica_contifico():
    """Ejecuta la sincronización automática con Contífico cada 1 minuto."""
    while True:
        try:
            hoy = datetime.now()
            inicio = (hoy - timedelta(days=3)).strftime("%d/%m/%Y")
            fin = hoy.strftime("%d/%m/%Y")

            print(f"🔁 [Contífico] Consultando documentos del {inicio} al {fin} ...")
            info = contifico_client.obtener_facturas_desde_contifico(inicio, fin)

            print(f"✅ [Contífico] Sincronización completada — Nuevas: {info.get('nuevos', 0)}, Existentes: {info.get('existentes', 0)}")

            db_path = os.path.join(os.getcwd(), "database", "integrador.db")
            con = sqlite3.connect(db_path)
            cur = con.cursor()
            cur.execute("""
                INSERT INTO actualizaciones (origen, ultima_actualizacion)
                VALUES (?, ?)
            """, ("contifico", datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            con.commit()
            con.close()

        except Exception as e:
            print("❌ Error en sincronización Contífico:", e)

        time.sleep(60)


# ======================================================
# 🔹 RUTAS DE COMPARACIÓN (MATCH / NO MATCH)
# ======================================================
@app.route('/procesar_match')

def procesar_match():
    try:
        info = comparador_client.ejecutar_comparacion()
        return render_template("comparacion_proceso.html", info=info)
    except Exception as e:
        print(f"❌ Error en comparación: {e}")
        return f"❌ Error en comparación: {str(e)}"



@app.route('/match')
def ver_match():
    data = comparador_client.listar_match()
    return render_template("match_dashboard.html", data=data)



@app.route('/no_match', methods=['GET'])
def ver_no_match():
    """
    Página principal de NO MATCH con filtro por fechas.
    Si no se envían fechas, lista todo.
    """
    fecha_inicio = (request.args.get('fecha_inicio') or '').strip()
    fecha_fin = (request.args.get('fecha_fin') or '').strip()

    # Mostrar en consola para confirmar
    print(f"[NO_MATCH] Filtro recibido: inicio={fecha_inicio}, fin={fecha_fin}")

    if fecha_inicio and fecha_fin:
        # 🔹 Filtra por fechas (usa SUBSTR(created_at,1,10) en SQL)
        data = descargar_reporte_no_match.filtrar_nomatch(fecha_inicio, fecha_fin)
    else:
        # 🔹 Sin fechas: muestra todo
        data = comparador_client.listar_no_match()

    return render_template(
        "no_match_dashboard.html",
        data=data,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin
    )


@app.route('/actualizaciones')
def ver_actualizaciones():
    """Muestra el historial de actualizaciones registradas."""
    con = sqlite3.connect(os.path.join(os.getcwd(), "database", "integrador.db"))
    cur = con.cursor()
    cur.execute("SELECT origen, ultima_actualizacion FROM actualizaciones ORDER BY id DESC LIMIT 50")
    data = cur.fetchall()
    con.close()
    return render_template("actualizaciones_dashboard.html", data=data)


# ======================================================
# 📊 DASHBOARD DE FACTURAS PROCESADAS (VALIDACIÓN FINAL)
# ======================================================
@app.route('/reporte_match')
def reporte_match():
    try:
        import sqlite3, os, pandas as pd
        DB_PATH = os.path.join(os.getcwd(), "database", "integrador.db")
        con = sqlite3.connect(DB_PATH)

        # ✅ Ajustado: ya no se incluyen los campos eliminados
        df = pd.read_sql_query("""
            SELECT documento_id, documento_numero, forma_cobro,
                   total_contifico, total_wispro, banco,
                   codigo_mapeado, transaccion, fecha_pago, fecha_emision
            FROM facturas_procesadas
            ORDER BY fecha_emision DESC
        """, con)

        con.close()
        facturas = df.to_dict(orient="records")
        return render_template("reporte_match.html", facturas=facturas)

    except Exception as e:
        print(f"❌ Error en reporte_match: {e}")
        return f"❌ Error al generar el reporte_match: {str(e)}"








# ======================================================
# 📥 DESCARGAR EXCEL DE REPORTE
# ======================================================
@app.route('/descargar_reporte_match')
def descargar_reporte_match():
    """Permite descargar el Excel del reporte."""
    from flask import send_file
    OUTPUT_FILE = os.path.join(os.getcwd(), "reportes", "REPORTE_MATCH.xlsx")

    # Si el archivo no existe, crear uno temporal
    if not os.path.exists(OUTPUT_FILE):
        import pandas as pd
        df_vacio = pd.DataFrame(columns=[
            "documento_id", "documento_numero", "total_contifico", "total_wispro",
            "banco", "codigo_mapeado", "fecha_pago", "fecha_emision", "validacion"
        ])
        df_vacio.to_excel(OUTPUT_FILE, index=False)

    return send_file(OUTPUT_FILE, as_attachment=True)



@app.route('/reporte_exactas')
def reporte_exactas():
    """Muestra el dashboard con las facturas exactas."""
    try:
        con = _conn()
        facturas = pd.read_sql_query("""
            SELECT documento_id, documento_numero, forma_cobro,
                   valor_a_enviar, banco, codigo_mapeado, transaccion,
                   fecha_envio, estado_fecha, creado_en
            FROM facturas_exactas
            ORDER BY creado_en DESC
        """, con)
        con.close()
        return render_template('reporte_exactas.html', facturas=facturas.to_dict(orient='records'))
    except Exception as e:
        print("❌ Error al mostrar reporte_exactas:", e)
        return f"Error al cargar reporte_exactas: {e}"


@app.route("/reporte_parciales")
def reporte_parciales():
    """Muestra el dashboard de facturas parciales (diferencias positivas o negativas)."""
    try:
        con = _conn()
        cur = con.cursor()

        cur.execute("""
            SELECT 
                documento_id, documento_numero, forma_cobro,
                valor_a_enviar, banco, codigo_mapeado, transaccion,
                fecha_envio, estado_fecha, creado_en
            FROM facturas_parciales
            ORDER BY creado_en DESC
        """)
        filas = cur.fetchall()
        columnas = [desc[0] for desc in cur.description]
        facturas = [dict(zip(columnas, fila)) for fila in filas]

        con.close()
        return render_template("reporte_parciales.html", facturas=facturas)

    except Exception as e:
        print(f"⚠️ Error cargando reporte parciales: {e}")
        return f"<h3>Error cargando el reporte: {e}</h3>"
    
    


@app.route("/reporte_parciales_pendientes", methods=["GET"])
def reporte_parciales_pendientes():
    """Dashboard de facturas parciales pendientes (valor a enviar = diferencia)."""
    try:
        con = _conn()
        cur = con.cursor()
        cur.execute("""
            SELECT 
                documento_id, documento_numero, forma_cobro,
                valor_a_enviar, banco, codigo_mapeado, transaccion,
                fecha_envio, estado_fecha, creado_en
            FROM facturas_parciales_pendientes
            ORDER BY creado_en DESC
        """)
        filas = cur.fetchall()
        columnas = [c[0] for c in cur.description]
        facturas = [dict(zip(columnas, f)) for f in filas]
        con.close()
        return render_template("reporte_parciales_pendientes.html", facturas=facturas)
    except Exception as e:
        print(f"⚠️ Error cargando reporte_parciales_pendientes: {e}")
        return f"<h3>Error cargando el reporte: {e}</h3>"


# ===========================================================
# 💸 RUTAS DE ENVÍO DE FACTURAS Y ERRORES
# ===========================================================
from core.envio_client import enviar_facturas_a_contifico, listar_facturas_enviadas, listar_facturas_errores

@app.route("/enviar_a_pagar")
def route_enviar_a_pagar():
    """Ejecuta el envío de facturas exactas y parciales."""
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

@app.route("/reporte_enviadas")
def route_reporte_enviadas():
    """Dashboard con las facturas enviadas a Contífico."""
    facturas = listar_facturas_enviadas()
    return render_template("reporte_enviadas.html", facturas=facturas)

@app.route("/reporte_errores")
def route_reporte_errores():
    """Dashboard con las facturas con errores de envío."""
    facturas = listar_facturas_errores()
    return render_template("reporte_errores.html", facturas=facturas)





# ============================================================
# ============================================================
# 📅 FACTURAS ENVIADAS - FILTRO Y DESCARGA DE REPORTE
# ============================================================
from core.descargar_reporte_enviadas import obtener_facturas_enviadas, generar_reporte_enviadas

@app.route("/facturas_enviadas")
def facturas_enviadas():
    """Renderiza la vista de facturas enviadas con filtro por fechas."""
    fecha_inicio = request.args.get("fecha_inicio", "")
    fecha_fin = request.args.get("fecha_fin", "")
    facturas = obtener_facturas_enviadas(fecha_inicio, fecha_fin)

    return render_template(
        "reporte_enviadas.html",
        facturas=facturas,
        fecha_inicio=fecha_inicio,
        fecha_fin=fecha_fin
    )


@app.route("/facturas_enviadas/exportar")
def exportar_facturas_enviadas():
    """Genera el archivo Excel con el mismo filtro de fechas."""
    fecha_inicio = request.args.get("fecha_inicio", "")
    fecha_fin = request.args.get("fecha_fin", "")
    return generar_reporte_enviadas(fecha_inicio, fecha_fin)



# ==========================================================
# 🔹 RUTA: EXPORTAR A EXCEL DESDE EL TEMPLATE
# ==========================================================
from core import descargar_reporte_no_match


@app.route('/no_match/exportar', methods=['GET'])
def exportar_no_match():
    """
    Descarga un Excel con los registros NO MATCH filtrados por rango de fechas.
    Se activa desde el botón del template.
    """
    fecha_inicio = request.args.get("fecha_inicio", "")
    fecha_fin = request.args.get("fecha_fin", "")
    return descargar_reporte_no_match.exportar_nomatch_excel(fecha_inicio, fecha_fin)

@app.route('/no_match/asientos', methods=['GET'])
def exportar_asientos():
    """
    Genera el Excel de Asientos contables con el rango de fechas filtrado.
    """
    fecha_inicio = (request.args.get("fecha_inicio") or "").strip()
    fecha_fin = (request.args.get("fecha_fin") or "").strip()
    return descargar_reporte_no_match.exportar_asientos_excel(fecha_inicio, fecha_fin)





















from core import cb_pedromoncayo

from flask import Flask, request, jsonify, render_template, send_file
from core.cb_pedromoncayo import (
    pm_importar_excel, pm_descargar_wispro,
    pm_conciliar, pm_registros, pm_exportar_excel
)


# Dashboard principal
@app.route("/conciliacion_bancaria")
def home_conciliacion():
    return render_template("cb_dashboard.html")

# Página Pedro Moncayo
@app.route("/conciliacion-bancaria/pedromoncayo")
def cb_pm_dashboard():
    return render_template("cb_reporte_pedromoncayo.html")

# Importar Excel del banco
@app.route("/conciliacion-bancaria/pedromoncayo/cargar", methods=["POST"])
def cb_pm_cargar():
    f = request.files.get("archivo")
    if not f:
        return jsonify({"mensaje": "⚠️ Sube un archivo Excel."}), 400
    uploads = os.path.join(os.getcwd(), "uploads")
    os.makedirs(uploads, exist_ok=True)
    ruta = os.path.join(uploads, f.filename)
    f.save(ruta)
    n = pm_importar_excel(ruta)
    return jsonify({"mensaje": f"✅ {n} registros importados desde Excel."})

# Descargar pagos Wispro
@app.route("/conciliacion-bancaria/pedromoncayo/descargar", methods=["POST"])
def cb_pm_descargar():
    data = request.get_json()
    fi, ff = data.get("inicio"), data.get("fin")
    if not fi or not ff:
        return jsonify({"mensaje": "⚠️ Falta rango de fechas."}), 400
    r = pm_descargar_wispro(fi, ff)
    return jsonify(r)

# Ejecutar conciliación
@app.route("/conciliacion-bancaria/pedromoncayo/conciliar", methods=["POST"])
def cb_pm_conciliar():
    data = request.get_json()
    fi, ff = data.get("inicio"), data.get("fin")
    if not fi or not ff:
        return jsonify({"mensaje": "⚠️ Falta rango de fechas."}), 400
    r = pm_conciliar(fi, ff)
    return jsonify({
        "mensaje": "✅ Conciliación completada correctamente",
        "conciliados": r["conciliados"],
        "solo_banco": r["solo_banco"],
        "solo_wispro": r["solo_wispro"]
    })

# 📊 Ver registros conciliados
# ===============================
# 📘 Ver registros conciliados (Pedro Moncayo)
# ===============================

from flask import jsonify, request

# 🔹 Ruta de tu base de datos (ajústala si está en otra carpeta)
DB_PATH = os.path.join(os.path.dirname(__file__), "database", "integrador.db")

@app.route("/conciliacion-bancaria/pedromoncayo/registros")
def pm_registros_route():
    fi = request.args.get("inicio")
    ff = request.args.get("fin")

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

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
        cur.execute(query, (fi, ff, fi, ff))
        datos = [dict(
            codigo=row[0],
            cliente=row[1],
            fecha_banco=row[2],
            monto_banco=row[3],
            fecha_wispro=row[4],
            monto_wispro=row[5],
            codigo_wispro=row[6],
            saldo=row[7],
            estado=row[8],
            origen=row[9]
        ) for row in cur.fetchall()]
    except Exception as e:
        return jsonify({"mensaje": f"❌ Error al consultar registros: {e}"}), 500
    finally:
        con.close()

    return jsonify(datos)



# 📤 Exportar reporte Excel
@app.route("/conciliacion-bancaria/pedromoncayo/exportar")
def pm_exportar_route():
    fi = request.args.get("inicio")
    ff = request.args.get("fin")
    ruta = os.path.join("exports", f"PEDROMONCAYO_{fi}_a_{ff}.xlsx")
    try:
        resultado = pm_exportar_excel(fi, ff, ruta)
        if "Error" in resultado["mensaje"]:
            return jsonify(resultado), 500
        return send_file(ruta, as_attachment=True)
    except Exception as e:
        return jsonify({"mensaje": f"❌ Error al exportar: {str(e)}"}), 500





























import os
import sqlite3
from flask import Flask, request, jsonify, render_template, send_file
from core.cb_pichincha import (
    pc_importar_excel, pc_descargar_wispro,
    pc_conciliar, pc_registros, pc_exportar_excel
)



# ============================================================
# 🏦 BANCO PICHINCHA — DASHBOARD
# ============================================================

@app.route("/conciliacion-bancaria/pichincha")
def cb_pichincha_dashboard():
    return render_template("cb_reporte_pichincha.html")


# ============================================================
# 📥 IMPORTAR EXCEL DEL BANCO
# ============================================================

@app.route("/conciliacion-bancaria/pichincha/cargar", methods=["POST"])
def cb_pichincha_cargar():
    f = request.files.get("archivo")
    if not f:
        return jsonify({"mensaje": "⚠️ Sube un archivo Excel."}), 400

    uploads = os.path.join(os.getcwd(), "uploads")
    os.makedirs(uploads, exist_ok=True)
    ruta = os.path.join(uploads, f.filename)
    f.save(ruta)

    try:
        n = pc_importar_excel(ruta)
        return jsonify({"mensaje": f"✅ {n} registros importados desde Excel."})
    except Exception as e:
        return jsonify({"mensaje": f"❌ Error al importar Excel: {e}"}), 500


# ============================================================
# ☁️ DESCARGAR PAGOS DESDE WISPRO
# ============================================================

@app.route("/conciliacion-bancaria/pichincha/descargar", methods=["POST"])
def cb_pichincha_descargar():
    data = request.get_json()
    fi, ff = data.get("inicio"), data.get("fin")

    if not fi or not ff:
        return jsonify({"mensaje": "⚠️ Falta rango de fechas."}), 400

    try:
        r = pc_descargar_wispro(fi, ff)
        return jsonify(r)
    except Exception as e:
        return jsonify({"mensaje": f"❌ Error al descargar pagos: {e}"}), 500


# ============================================================
# ⚙️ EJECUTAR CONCILIACIÓN AUTOMÁTICA
# ============================================================

@app.route("/conciliacion-bancaria/pichincha/conciliar", methods=["POST"])
def cb_pichincha_conciliar():
    data = request.get_json()
    fi, ff = data.get("inicio"), data.get("fin")

    if not fi or not ff:
        return jsonify({"mensaje": "⚠️ Falta rango de fechas."}), 400

    try:
        r = pc_conciliar(fi, ff)
        return jsonify({
            "mensaje": "✅ Conciliación completada correctamente",
            "conciliados": r["conciliados"],
            "solo_banco": r["solo_banco"],
            "solo_wispro": r["solo_wispro"]
        })
    except Exception as e:
        return jsonify({"mensaje": f"❌ Error en conciliación: {e}"}), 500


# ============================================================
# 📊 VER REGISTROS CONCILIADOS (BANCO PICHINCHA)
# ============================================================

DB_PATH = os.path.join(os.getcwd(), "database", "integrador.db")

@app.route("/conciliacion-bancaria/pichincha/registros")
def cb_pichincha_registros():
    fi = request.args.get("inicio")
    ff = request.args.get("fin")

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

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
            FROM cb_conciliados_pichincha
            WHERE 
                (fecha_banco BETWEEN ? AND ?)
                OR 
                (fecha_wispro BETWEEN ? AND ?)
            ORDER BY estado DESC;
        """
        cur.execute(query, (fi, ff, fi, ff))
        datos = [dict(
            codigo=row[0],
            cliente=row[1],
            fecha_banco=row[2],
            monto_banco=row[3],
            fecha_wispro=row[4],
            monto_wispro=row[5],
            codigo_wispro=row[6],
            saldo=row[7],
            estado=row[8],
            origen=row[9]
        ) for row in cur.fetchall()]
    except Exception as e:
        return jsonify({"mensaje": f"❌ Error al consultar registros: {e}"}), 500
    finally:
        con.close()

    return jsonify(datos)


# ============================================================
# 📤 EXPORTAR REPORTE A EXCEL
# ============================================================

@app.route("/conciliacion-bancaria/pichincha/exportar")
def cb_pichincha_exportar():
    fi = request.args.get("inicio")
    ff = request.args.get("fin")
    ruta = os.path.join("exports", f"PICHINCHA_{fi}_a_{ff}.xlsx")

    try:
        resultado = pc_exportar_excel(fi, ff, ruta)
        if "Error" in resultado["mensaje"]:
            return jsonify(resultado), 500
        return send_file(ruta, as_attachment=True)
    except Exception as e:
        return jsonify({"mensaje": f"❌ Error al exportar: {str(e)}"}), 500















from core.cb_procredit import (
    pcd_importar_excel, pcd_descargar_wispro,
    pcd_conciliar, pcd_registros, pcd_exportar_excel
)

@app.route("/conciliacion-bancaria/procredit")
def cb_procredit_dashboard():
    return render_template("cb_reporte_procredit.html")

@app.route("/conciliacion-bancaria/procredit/cargar", methods=["POST"])
def cb_procredit_cargar():
    f = request.files.get("archivo")
    if not f:
        return jsonify({"mensaje": "⚠️ Sube un archivo Excel."}), 400
    uploads = os.path.join(os.getcwd(), "uploads")
    os.makedirs(uploads, exist_ok=True)
    ruta = os.path.join(uploads, f.filename)
    f.save(ruta)
    n = pcd_importar_excel(ruta)
    return jsonify({"mensaje": f"✅ {n} registros importados correctamente."})

@app.route("/conciliacion-bancaria/procredit/descargar", methods=["POST"])
def cb_procredit_descargar():
    data = request.get_json()
    fi, ff = data.get("inicio"), data.get("fin")
    r = pcd_descargar_wispro(fi, ff)
    return jsonify(r)

@app.route("/conciliacion-bancaria/procredit/conciliar", methods=["POST"])
def cb_procredit_conciliar():
    data = request.get_json()
    fi, ff = data.get("inicio"), data.get("fin")
    r = pcd_conciliar(fi, ff)
    return jsonify(r)

DB_PATH = os.path.join(os.getcwd(), "database", "integrador.db")

@app.route("/conciliacion-bancaria/procredit/registros")
def cb_procredit_registros():
    fi = request.args.get("inicio")
    ff = request.args.get("fin")

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

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
            FROM cb_conciliados_procredit
            WHERE 
                (fecha_banco IS NOT NULL AND date(fecha_banco) BETWEEN date(?) AND date(?))
                OR 
                (fecha_wispro IS NOT NULL AND date(fecha_wispro) BETWEEN date(?) AND date(?))
            ORDER BY estado DESC, 
                     COALESCE(date(fecha_banco), date(fecha_wispro)) DESC;
        """
        cur.execute(query, (fi, ff, fi, ff))
        rows = cur.fetchall()

        datos = []
        for row in rows:
            datos.append({
                "codigo": row[0] or "",
                "cliente": row[1] or "",
                "fecha_banco": row[2] or "",
                "monto_banco": f"${float(row[3]):,.2f}" if row[3] else "",
                "fecha_wispro": row[4] or "",
                "monto_wispro": f"${float(row[5]):,.2f}" if row[5] else "",
                "codigo_wispro": row[6] or "",
                "saldo": f"${float(row[7]):,.2f}" if row[7] else "",
                "estado": row[8] or "",
                "origen": row[9] or ""
            })

    except Exception as e:
        return jsonify({"mensaje": f"❌ Error al consultar registros: {e}"}), 500
    finally:
        con.close()

    return jsonify(datos)


@app.route("/conciliacion-bancaria/procredit/exportar")
def cb_procredit_exportar():
    fi = request.args.get("inicio")
    ff = request.args.get("fin")
    ruta = os.path.join("exports", f"PROCREDIT_{fi}_a_{ff}.xlsx")
    return send_file(ruta, as_attachment=True) if os.path.exists(ruta) else jsonify(pcd_exportar_excel(fi, ff, ruta))













from flask import Flask, render_template, Response
from core.crear_nuevos_cliente import crear_nuevos_cliente, db_conn
import json, time


@app.route("/")
def dashboard():
    return render_template("dashboard_clientes.html")

@app.route("/sync/run-stream")
def sync_run_stream():
    def generar():
        crear_nuevos_cliente()

        with db_conn() as conn:
            cur = conn.execute("""
                SELECT 
                    i.razon_social, i.email, COALESCE(i.phone, i.phone_mobile) AS telefono,
                    i.id_number, i.city, i.address,
                    IFNULL(r.status_code, 'Pendiente') AS status_code,
                    IFNULL(r.mensaje_resultado, 'Sin respuesta') AS mensaje_resultado,
                    IFNULL(r.exito, 0) AS exito
                FROM crear_nuevos_cliente_importados i
                LEFT JOIN crear_nuevos_cliente_resultados r 
                ON i.id_number = r.id_number
                ORDER BY i.created_at DESC
            """)
            registros = cur.fetchall()

        total = len(registros)
        exito = len([r for r in registros if r[-1] == 1])
        error = total - exito

        for r in registros:
            data = {
                "razon_social": r[0],
                "email": r[1],
                "telefono": r[2],
                "identificacion": r[3],
                "ciudad": r[4],
                "direccion": r[5],
                "status": r[6],
                "mensaje": r[7],
                "exito": bool(r[8]),
                "total_importados": total,
                "total_envios": total,
                "exito_total": exito,
                "error_total": error,
            }
            yield json.dumps({"type": "update", "data": data}) + "\n"
            time.sleep(0.2)

        yield json.dumps({"type": "end"}) + "\n"

    return Response(generar(), mimetype="text/plain")



@app.route("/sync/historial")
def sync_historial():
    """Devuelve los registros actuales del historial sin ejecutar sincronización."""
    from flask import jsonify
    with db_conn() as conn:
        cur = conn.execute("""
            SELECT 
                i.razon_social, i.email, COALESCE(i.phone, i.phone_mobile) AS telefono,
                i.id_number, i.city, i.address,
                IFNULL(r.status_code, 'Pendiente') AS status_code,
                IFNULL(r.mensaje_resultado, 'Sin respuesta') AS mensaje_resultado,
                IFNULL(r.exito, 0) AS exito
            FROM crear_nuevos_cliente_importados i
            LEFT JOIN crear_nuevos_cliente_resultados r 
            ON i.id_number = r.id_number
            ORDER BY i.created_at DESC
        """)
        registros = cur.fetchall()

    data = []
    total = len(registros)
    exito = len([r for r in registros if r[-1] == 1])
    error = total - exito

    for r in registros:
        data.append({
            "razon_social": r[0],
            "email": r[1],
            "telefono": r[2],
            "identificacion": r[3],
            "ciudad": r[4],
            "direccion": r[5],
            "status": r[6],
            "mensaje": r[7],
            "exito": bool(r[8])
        })

    return jsonify({
        "total_importados": total,
        "total_envios": total,
        "exito_total": exito,
        "error_total": error,
        "clientes": data
    })

# 🔹 RUTA RESTAURADA
@app.route("/generar_clientes")
def generar_clientes():
    try:
        return render_template("generar_clientes.html")
    except:
        return "<h3 style='font-family:sans-serif; color:#1d72b8;'>Página de generación de clientes pendiente de implementar.</h3>"







# ======================================================
# 🔁 SINCRONIZACIÓN AUTOMÁTICA COMPARADOR (cada 1 hora)
# ======================================================
def sincronizacion_automatica_comparador():
    """
    Ejecuta el proceso completo de comparación Wispro ↔ Contífico cada 1 hora.
    Registra la ejecución en la tabla 'actualizaciones'.
    """
    db_path = os.path.join(os.getcwd(), "database", "integrador.db")

    while True:
        try:
            print("\n🔁 [Comparador] Iniciando sincronización automática Wispro ↔ Contífico...")
            resultado = comparador_client.ejecutar_comparacion()

            # ✅ Registro en base de datos
            con = sqlite3.connect(db_path)
            cur = con.cursor()
            cur.execute("""
                INSERT INTO actualizaciones (origen, ultima_actualizacion)
                VALUES (?, ?)
            """, (
                "comparador",
                datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            ))
            con.commit()
            con.close()

            # ✅ Logs informativos
            print(f"✅ [Comparador] Proceso finalizado — "
                  f"MATCH: {resultado.get('match', 0)}, "
                  f"NO MATCH: {resultado.get('no_match', 0)}, "
                  f"Exactas: {resultado.get('exactas', 0)}, "
                  f"Parciales: {resultado.get('parciales', 0)}, "
                  f"Pendientes: {resultado.get('pendientes', 0)}, "
                  f"Errores: {resultado.get('errores', 0)}")

        except Exception as e:
            print("❌ [Comparador] Error durante la sincronización automática:", e)

        # Esperar 1 hora (3600 segundos)
        print("⏳ Próxima ejecución del comparador en 1 hora...")
        time.sleep(20)















# ======================================================
# 🔁 CREACIÓN DE HILOS (DISPONIBLES PARA DEV Y PRODUCCIÓN)
# ======================================================
hilo_contifico = threading.Thread(target=sincronizacion_automatica_contifico, daemon=True)
hilo_comparador = threading.Thread(target=sincronizacion_automatica_comparador, daemon=True)

# ======================================================
# 🚀 EJECUCIÓN PRINCIPAL DE FLASK (SOLO DESARROLLO)
# ======================================================
if __name__ == '__main__':
    os.environ["FLASK_ENV"] = "development"

    # Activar hilos automáticos solo en modo desarrollo
    hilo_contifico.start()
    hilo_comparador.start()

    print("🚀 Sincronización automática de Contífico y Comparador activa (cada 1 minuto).")

    # ❗❗ YA NO USAR localhost NI 127.0.0.1 EN PRODUCCIÓN ❗❗
    # Solo se usa en tu máquina local:
    run_simple('0.0.0.0', 5000, app, use_reloader=False, use_debugger=True)
