import psycopg2
import os
import csv
from datetime import datetime

# Configuración de conexión (modificar a conveniencia)
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'dbname': 'Novedades_V1_Municipios',
    'user': 'postgres',
    'password': '1234jcgg'
}

TABLAS_OBJETIVO = ['zhf_urbana', 'zhf_rural']
LOG_DETALLADO = "reporte_detallado.log"
LOG_RESUMEN = "reporte_resumen.csv"

# Listas de clasificación
sin_registros = {'zhf_urbana': [], 'zhf_rural': []}
mayoria_ceros = {'zhf_urbana': [], 'zhf_rural': []}
mayoria_completos = {'zhf_urbana': [], 'zhf_rural': []}

# Inicializa logs
with open(LOG_DETALLADO, 'w') as f:
    f.write(f"Análisis iniciado: {datetime.now()}\n\n")

with open(LOG_RESUMEN, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["Esquema", "Tabla", "Total Registros", "Filas con mayoría ceros/nulos", "Filas con mayoría de datos", "Clasificación"])

def log(msg):
    print(msg)
    with open(LOG_DETALLADO, 'a') as f:
        f.write(msg + '\n')

def analizar_tabla(cur, esquema, tabla):
    cur.execute(f'SET search_path TO {esquema}')
    cur.execute(f"SELECT * FROM {tabla} LIMIT 1")
    colnames = [desc[0] for desc in cur.description]

    cur.execute(f"SELECT COUNT(*) FROM {tabla}")
    total = cur.fetchone()[0]

    if total == 0:
        sin_registros[tabla].append(esquema)
        log(f"[{esquema}.{tabla}] Tabla sin registros.")
        return ('sin_datos', 0, 0)

    cur.execute(f"SELECT * FROM {tabla}")
    rows = cur.fetchall()

    filas_incongruentes = 0
    filas_completas = 0

    for row in rows:
        valores = list(row)
        n_total = len(valores)

        n_incongruentes = 0
        for v in valores:
            if v is None:
                n_incongruentes += 1
            elif isinstance(v, str) and v.strip() == '':
                n_incongruentes += 1
            else:
                try:
                    if isinstance(v, (int, float, str)):
                        num = float(v)
                        if -9 <= num <= 9:
                            n_incongruentes += 1
                except (ValueError, TypeError):
                    pass  # Ignora valores que no se pueden convertir a float

        n_validos = n_total - n_incongruentes

        if n_incongruentes >= 4:
            filas_incongruentes += 1
        elif n_validos > n_incongruentes:
            filas_completas += 1

    if filas_incongruentes > filas_completas:
        mayoria_ceros[tabla].append(esquema)
        clasificacion = 'mayoría ceros/nulos/valores pequeños'
    else:
        mayoria_completos[tabla].append(esquema)
        clasificacion = 'mayoría datos completos'

    log(f"[{esquema}.{tabla}] Total: {total}, Incongruentes: {filas_incongruentes}, Completas: {filas_completas}, Clasificación: {clasificacion}")

    with open(LOG_RESUMEN, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([esquema, tabla, total, filas_incongruentes, filas_completas, clasificacion])

    return (clasificacion, filas_incongruentes, filas_completas)

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    log("Obteniendo esquemas que comienzan con 'cun'...")
    cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name ~ '^cun[0-9]{5}$'")
    esquemas = [row[0] for row in cur.fetchall()]

    for esquema in esquemas:
        log(f"\nAnalizando esquema: {esquema}")
        for tabla in TABLAS_OBJETIVO:
            try:
                cur.execute(f"SELECT to_regclass('{esquema}.{tabla}')")
                if cur.fetchone()[0]:
                    analizar_tabla(cur, esquema, tabla)
                else:
                    log(f"[{esquema}.{tabla}] Tabla no existe.")
            except Exception as e:
                log(f"Error analizando {esquema}.{tabla}: {e}")

    # Guardar listas finales
    with open("esquemas_sin_datos.txt", 'w') as f:
        for tipo in sin_registros:
            f.write(f"\n{tipo.upper()} SIN DATOS:\n")
            f.writelines(f"{e}\n" for e in sin_registros[tipo])

    with open("esquemas_mayoria_ceros.txt", 'w') as f:
        for tipo in mayoria_ceros:
            f.write(f"\n{tipo.upper()} CON MAYORÍA CEROS/NULOS:\n")
            f.writelines(f"{e}\n" for e in mayoria_ceros[tipo])

    with open("esquemas_datos_completos.txt", 'w') as f:
        for tipo in mayoria_completos:
            f.write(f"\n{tipo.upper()} CON DATOS COMPLETOS:\n")
            f.writelines(f"{e}\n" for e in mayoria_completos[tipo])

    log("\nAnálisis completado.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
