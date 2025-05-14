import psycopg2

# -------------------------------
# Diccionarios de referencia URBANA
# -------------------------------
topografia = {
    'Plano': '1',
    'Inclinado': '2',
    'Empinado': '3'
}

vias = {
    'Pavimentadas': '1',
    'Sin Pavimentar': '2',
    'Peatonales': '3',
    'Sin Vias': '4'
}

servicios = {
    'Basicos mas Complementarios': '1',
    'Basicos Completos': '2',
    'Basicos Incompletos': '3',
    'Sin Servicios': '4'
}


uso_suelo = {
    'Comercial': '1',
    'Industrial': '2',
    'Residencial': '3',
    'Lote': '4',
    'Institucional': '5'
    }

norma_uso = {
    'CONSOLIDACION': '01',
    'CONSOLIDACION DOTACIONAL': '02',
    'DESARROLLO': '03',
    'DOTACIONAL': '04',
    'PROTECCION ZMPA': '05'
}

tipificacion = {
    'No presenta': '00',
    'Comercial Especializado': '01',
    'Comercial sectorial': '02',
    'Comercial barrial': '03',
    'Residencial 6 – (Alto)': '04',
    'Residencial 5 – (Medio-Alto)': '05',
    'Residencial 4 – (Medio)': '06',
    'Residencial 3 – (Medio-Bajo)': '07',
    'Residencial 2 – (Bajo)': '08',
    'Residencial 1 – (Bajo-Bajo)': '09',
    'Industria Liviana': '10',
    'Industria mediana': '11',
    'Industria Pesada': '12',
    'Institucional': '13',    
}

# -------------------------------
# Funciones de codificación / decodificación ZHF URBANA
# -------------------------------


# Función para codificar ZHF urbana
def codificar_zhf(var_topo, var_vias, var_serv, var_norma, var_uso, var_tipif, var_extra):
    try:
        codigo = (
            topografia[var_topo.lower()] +
            vias[var_vias.lower()] +
            servicios[var_serv.lower()] +
            uso_suelo[var_norma.lower()] +
            uso_suelo[var_uso.lower()] +
            tipificacion[var_tipif.lower()] +
            var_extra.zfill(2)
        )
        return codigo
    except KeyError as e:
        return "Error: Variable inválida -> " + str(e)

# Función para decodificar código ZHF urbana
def decodificar_zhf(codigo):
    resultado = {}

    secciones = {
        'topografia': (0, 2),
        'vias': (2, 4),
        'servicios': (4, 6),
        'norma_uso': (6, 8),
        'uso_actual': (8, 10),
        'tipificacion': (10, 12),
        'extra': (12, 14)
    }

    # Inversión de diccionarios
    inv_topografia = {v: k for k, v in topografia.items()}
    inv_vias = {v: k for k, v in vias.items()}
    inv_servicios = {v: k for k, v in servicios.items()}
    inv_uso = {v: k for k, v in uso_suelo.items()}
    inv_tipif = {v: k for k, v in tipificacion.items()}

    resultado['topografia'] = inv_topografia.get(codigo[0:2], 'Desconocido')
    resultado['vias'] = inv_vias.get(codigo[2:4], 'Desconocido')
    resultado['servicios'] = inv_servicios.get(codigo[4:6], 'Desconocido')
    resultado['norma_uso'] = inv_uso.get(codigo[6:8], 'Desconocido')
    resultado['uso_actual'] = inv_uso.get(codigo[8:10], 'Desconocido')
    resultado['tipificacion'] = inv_tipif.get(codigo[10:12], 'Desconocido')
    resultado['extra'] = codigo[12:14]

    return resultado

# Codificación
codigo = codificar_zhf('Media', 'Pavimentadas', 'Completos', 'Residencial', 'Comercial', 'Tipo 2', '07')
print("Código generado:", codigo)
# Código generado: 03010101020207

# Decodificación
detalles = decodificar_zhf("03010101020207")
print("Detalles decodificados:")
for k, v in detalles.items():
    print(" -", k, ":", v)


# -------------------------------
# Base para ZHF RURAL (a completar según tu modelo)
# -------------------------------
# Puedes definir nuevos diccionarios si los valores cambian en lo rural


# -------------------------------
# Conexión y procesamiento
# -------------------------------
try:
    conn = psycopg2.connect(
        dbname="basededatos",
        user="usuario",
        password="clave",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    cur.execute("SELECT id, pendiente, vias, servicios, normas, uso, tipif, extra FROM alturas")
    rows = cur.fetchall()

    for row in rows:
        id_registro = row[0]
        try:
            # Extraer valores por columna
            pendiente = row[1]
            via = row[2]
            servicio = row[3]
            norma = row[4]
            uso = row[5]
            tipif = row[6]
            extra = row[7]

            # Codificar
            codigo = codificar_zhf(pendiente, via, servicio, norma, uso, tipif, extra)

            if "Error" not in codigo:
                # Decodificar
                detalles = decodificar_zhf(codigo)
                print("ID {} -> Código: {}, Detalles: {}".format(id_registro, codigo, detalles))
            else:
                print("ID {} -> {}".format(id_registro, codigo))

        except Exception as err:
            print("ID {} -> Error en el procesamiento del registro: {}".format(id_registro, err))

    cur.close()
    conn.close()

except psycopg2.Error as e:
    print("Error de conexión a PostgreSQL:", e)
