# ESTE SCRIPT TIENE EL FIN DE RECOPILAR LOS DICCIONARIOS Y EQUIVALENCIAS A CADA ZHF DE LOS MPIOS ACTUALIZADOS, BASANDONOS EN EL DOCUMENTO OFICIAL ENTREGADO POR EL OPERADOR
# -*- coding: utf-8 -*-
import psycopg2
import os
import datetime

# --------- CONFIGURACIÓN GENERAL ---------

# Lista de códigos DIVIPOLA de municipios a procesar
municipios = ['25035']  # Agrega más según necesidad

# Diccionarios de decodificación por municipio
# Aquí tú completas con las codificaciones específicas de cada municipio
diccionarios_mpios = {
    '25035': {
        'orden_variables': ['topografia', 'influencia_vial', 'servicio_publico', 'uso_actual_suelo', 'norma_uso_suelo', 'tipificacion_construccion'],
        'longitudes': [1, 1, 1, 1, 3, 2],
        'topografia': {'1': 'Plano', '2': 'Inclinado'},
        'influencia_vial': {'1': 'Pavimentadas', '2': 'Sin_Pavimentar', '3': 'Peatonales'},
        'servicio_publico': {'1': 'Servicios_Basicos_Y_Complementarios'},
        'uso_actual_suelo': {'1': 'Residencial', '2': 'Comercial', '3': 'Institucional'},
        'norma_uso_suelo': {'01': 'Mixto'}, #FALTA ESTE
        'tipificacion_construccion': {'01': 'Tipo 1', '02': 'Tipo 2'}   #FALTA ESTE
    },

# ESPECIAL PORQUE TIPIFICACION CONSTRUCCION ES CONDICIONAL
    '25040': {
        'orden_variables': ['topografia', 'influencia_vial', 'servicio_publico', 'uso_actual_suelo', 'norma_uso_suelo', 'tipificacion_construccion'],
        'longitudes': [1, 1, 1, 1, 2, 1],
        'topografia': {'1': 'Plano', '2': 'Inclinado', '3': 'Empinado'},
        'influencia_vial': {'1': 'Pavimentadas', '2': 'Sin_Pavimentar', '3': 'Peatonales', '4': 'Sin_Vias'},
        'servicio_publico': {'1': 'Sin_Servicios', '2': 'Básicos_Incompletos', '3': 'Básicos_Completos ', '4': 'Básicos_Y_Complementarios'},
        'uso_actual_suelo': {'1': 'Residencial', '2': 'Comercial', '3': 'Industrial', '4': 'Institucional', '5': 'Lote'},
        'norma_uso_suelo': {'111': 'Protegido, Multiple, Desarrollo', '112': 'Protegido, Multiple, Consolidación', '113': 'Protegido, Multiple, Conservación', '121': 'Protegido, Residencial, Desarrollo', '122': 'Protegido, Residencial, Consolidación', '123': 'Protegido, Residencial, Conservación', '211': 'No protegido, Multiple, Desarrollo', '212': 'No protegido, Multiple, Consolidación', '213': 'No protegido, Multiple, Conservación', '221': 'No protegido, Residencial, Desarrollo', '222': 'No protegido, Residencial, Consolidación', '223': 'No protegido, Residencial, Conservación'}, 
        'tipificacion_construccion': {
            'Residencial': {
                '2': 'Residencial_2_Bajo',
                '3': 'Residencial_3_Medio_Bajo',
                '4': 'Residencial_4_Medio',
                '5': 'Residencial_5_Medio_Alto',
                '6': 'Residencial_6_Alto',
                '0': 'NULL'
            },
            'Comercial': {
                '1': 'Comercial_Barrial',
                '2': 'Comercial_Sectorial',
                '0': 'NULL'
            }
    }},
}

# Parámetros de conexión a la base de datos
conexion_db = {
    'dbname': 'nombre_bd',
    'user': 'usuario',
    'password': 'clave',
    'host': 'localhost',
    'port': '5432'
}

# Ruta para guardar los reportes
ruta_reportes = 'reportes_zhf'
if not os.path.exists(ruta_reportes):
    os.makedirs(ruta_reportes)

# --------- FUNCIONES ---------

def conectar_db():
    """Establece conexión con la base de datos PostgreSQL."""
    try:
        conn = psycopg2.connect(**conexion_db)
        return conn
    except Exception as e:
        print("Error conectando a la base de datos:", e)
        return None

def decodificar_codigo(mpio, codigo):
    """Decodifica el código ZHF de acuerdo al diccionario del municipio."""
    try:
        dicc = diccionarios_mpios[mpio]
        orden = dicc['orden_variables']
        longitudes = dicc['longitudes']

        resultado = {}
        pos = 0

        for idx, (var, lon) in enumerate(zip(orden, longitudes)):
            parte = codigo[pos:pos+lon]

            # --- CASO CONDICIONAL PARA 25040 ---
            # !! DESCOMENTAR ESTA SECCIÓN SOLO CUANDO mpio == '25040'
            # if mpio == '25040' and var == 'tipificacion_construccion':
            #     uso_actual = resultado.get('uso_actual_suelo', '0')  # por defecto '0'
            #     significado = dicc[var].get(uso_actual, {}).get(parte, 'Desconocido')
            # else:
            #     significado = dicc.get(var, {}).get(parte, 'Desconocido')
            # -----------------------------------

            # --- CASO GENERAL PARA TODOS LOS DEMÁS MUNICIPIOS (DICCIONARIOS PLANOS) ---
            significado = dicc.get(var, {}).get(parte, 'Desconocido')
            #  !! COMENTAR ESTA LÍNEA SI VAS A USAR LA LÓGICA CONDICIONAL PARA 25040
            # -----------------------------------------------------------------------------

            resultado[var] = significado
            pos += lon

        return resultado
    except Exception as e:
        return {'error': 'Error en decodificación: ' + str(e)}

    
def verificar_campos_existentes(conn, mpio):
    """Verifica si los campos decodificados existen en la tabla del municipio."""
    esquema = 'cun' + mpio
    tabla = 'zhf_urbana'
    tabla_completa = esquema + '.' + tabla

    try:
        with conn.cursor() as cur:
            # Obtener columnas existentes
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
            """, (esquema, tabla))
            columnas = [row[0] for row in cur.fetchall()]

            campos_esperados = diccionarios_mpios[mpio]['orden_variables']
            faltantes = [c for c in campos_esperados if c not in columnas]

            if faltantes:
                print("⚠️ Faltan los siguientes campos en {}: {}".format(tabla_completa, ', '.join(faltantes)))
                return False, faltantes
            else:
                print("✓ Todos los campos existen en la tabla {}.".format(tabla_completa))
                return True, []

    except Exception as e:
        print("⚠️ Error verificando columnas para {}: {}".format(tabla_completa, e))
        return False, ['Error consultando columnas']

def procesar_municipio(conn, mpio):
    """Procesa y actualiza los registros de un municipio específico."""
    esquema = 'cun' + mpio
    tabla = esquema + '.zhf_urbana'
    resultados = []
    errores = 0
    procesados = 0

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, codigo_zona_fisica FROM {}".format(tabla))
            rows = cur.fetchall()

            for row in rows:
                id_registro, codigo = row
                procesados += 1
                decodificado = decodificar_codigo(mpio, codigo)

                if 'error' in decodificado:
                    errores += 1
                    resultados.append({
                        'id': id_registro,
                        'codigo': codigo,
                        'decodificado': decodificado
                    })
                    continue  # salta a la siguiente iteración

                # Preparar UPDATE dinámico con los campos decodificados
                set_values = []
                valores = []
                for campo, valor in decodificado.items():
                    set_values.append('{} = %s'.format(campo))
                    valores.append(valor)

                valores.append(id_registro)  # para el WHERE

                sql_update = "UPDATE {} SET {} WHERE id = %s".format(
                    tabla,
                    ", ".join(set_values)
                )

                try:
                    cur.execute(sql_update, valores)
                except Exception as e:
                    errores += 1
                    resultados.append({
                        'id': id_registro,
                        'codigo': codigo,
                        'decodificado': {'error': 'Error actualizando registro: ' + str(e)}
                    })
                    continue

                resultados.append({
                    'id': id_registro,
                    'codigo': codigo,
                    'decodificado': decodificado
                })

        conn.commit()
        print("→ Cambios guardados para municipio {}".format(mpio))

    except Exception as e:
        print("⚠️ Error procesando municipio {}: {}".format(mpio, e))

    return resultados, procesados, errores


def generar_reporte(mpio, resultados, procesados, errores, campos_faltantes):
    """Genera un archivo de texto con los resultados de la decodificación."""
    nombre_archivo = os.path.join(ruta_reportes, 'reporte_' + mpio + '.txt')
    with open(nombre_archivo, 'w') as f:
        f.write("REPORTE DECODIFICACIÓN ZHF - Municipio {}\n".format(mpio))
        f.write("Fecha: {}\n".format(datetime.datetime.now()))
        f.write("Total registros procesados: {}\n".format(procesados))
        f.write("Errores: {}\n".format(errores))

        if campos_faltantes:
            f.write("\n⚠️ CAMPOS FALTANTES EN LA TABLA:\n")
            for campo in campos_faltantes:
                f.write(" - {}\n".format(campo))
        else:
            f.write("\n✓ Todos los campos requeridos existen en la tabla.\n")

        f.write("\nDETALLE DE REGISTROS:\n\n")
        for item in resultados:
            f.write("ID: {}\n".format(item['id']))
            f.write("Código: {}\n".format(item['codigo']))
            for var, val in item['decodificado'].items():
                f.write(" - {}: {}\n".format(var, val))
            f.write("\n")

    print("📝 Reporte generado para municipio {} en {}\n".format(mpio, nombre_archivo))

# --------- EJECUCIÓN PRINCIPAL ---------

def main():
    conn = conectar_db()
    if not conn:
        print("No se pudo establecer la conexión. Terminando ejecución.")
        return

    for mpio in municipios:
        print("\n🔷 Procesando municipio:", mpio)

        campos_ok, campos_faltantes = verificar_campos_existentes(conn, mpio)

        resultados, procesados, errores = [], 0, 0
        if campos_ok:
            resultados, procesados, errores = procesar_municipio(conn, mpio)

        generar_reporte(mpio, resultados, procesados, errores, campos_faltantes)

    conn.close()
    print("\n✅ Proceso completado.")
