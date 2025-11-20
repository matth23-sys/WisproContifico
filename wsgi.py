from app import app, hilo_contifico, hilo_comparador

# 🔁 Iniciar hilos automáticos en producción
hilo_contifico.start()
hilo_comparador.start()

# Entrada para Gunicorn
application = app
