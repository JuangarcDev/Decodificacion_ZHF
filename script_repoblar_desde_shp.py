import os
import geopandas as gpd
from sqlalchemy import create_engine, text
from datetime import datetime

# -----------------------------
# Configuración de la conexión
# -----------------------------
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'dbname': 'Novedades_V1_Municipios',
    'user': 'postgres',
    'password': '1234jcgg'
}

# ---------------------------------
# Parámetros de ejecución del script
# ---------------------------------
MUNICIPIOS = ['25645', '25035']

# Rutas de los SHP por municipio
RUTAS_SHP = {
    '25645': {
        'rural': [r'C:\ACC\Zonas_Homogeneas_Decodificacion\Insumos\ARCHIVOS_NAS\Go\25645_SAN_ANTONIO\ZHF_RURALES\Zonas Homogéneas Físicas Rurales\Zonas Homogeneas Fisicas Rurales.shp'],
        'urbana': [r'C:\ACC\Zonas_Homogeneas_Decodificacion\Insumos\ARCHIVOS_NAS\Go\25645_SAN_ANTONIO\ZHF_URBANAS\Zonas Homogéneas Físicas Urbanas\Zonas Homogeneas Fisicas Centros Poblados.shp', r'C:\ACC\Zonas_Homogeneas_Decodificacion\Insumos\ARCHIVOS_NAS\Go\25645_SAN_ANTONIO\ZHF_URBANAS\Zonas Homogéneas Físicas Urbanas\Zonas Homogeneas Fisicas Urbanas.shp'],
    },
    
    '25035': {
       'rural': [r'C:\ACC\Zonas_Homogeneas_Decodificacion\Insumos\ARCHIVOS_NAS\Go\25035_ANAPOIMA\ZHF_RURALES\Zonas Homogéneas Físicas Rurales\Zonas Homogeneas Fisicas Rurales.shp'], 
       'urbana': []  # Sin información urbana
    },
#    '25333': {
#        'rural': [r'ruta/a/NOMBRE_SHP1.shp', r'ruta/a/NOMBRE_SHP2.shp'],
#        'urbana': [r'ruta/a/NOMBRE_25333_1.shp', r'ruta/a/NOMBRE_25333_2.shp']
#    }
}

# -------------------------
# Mapeo de campos por múltiples nombres posibles
# -------------------------
MAPEO_CAMPOS = {
    'zhf_rural': {
        'codigo': ['ID_ZHFR', 'ID', 'COD_ZHF'],
        'codigo_zona_fisica': ['ZHFR', 'ZHF_R'],
        'area_homogenea_tierra': ['AHT', 'uso_suelo'],
        'disponibilidad_agua': ['Disp_Aguas', 'Disponibili'],
        'influencia_vial': ['Inf_Vial', 'Influencia'],
        'uso_actual_suelo': ['Uso_Actual', 'USO_ACT'],
        'norma_uso_suelo': ['Uso', 'norma_uso']
    },
    'zhf_urbana': {
        'codigo': ['ID_ZHFCP', 'ID_ZHFU'],
        'codigo_zona_fisica': ['ZHF', 'ZONA_FISICA'],
        'topografia': ['Topografia', 'TOPOGRAF'],
        'influencia_vial': ['Influencia', 'INFLUENCIA_V'],
        'servicio_publico': ['Servicios_', 'SERV_PUB'],
        'uso_actual_suelo': ['Uso', 'USO_ACTUAL'],
        'norma_uso_suelo_categoria': ['Categoria', 'CAT_USO'],
        'norma_uso_suelo_area_activ': ['Area_Activ', 'AREA_ACT'],
        'norma_uso_suelo_tratamiento': ['Tratamient', 'TRAT_USO'],
        'tipificacion_construccion': ['Tipificaci', 'TIPIF_CONS']
    }
}

# -------------------------
# Configuración personalizada por municipio (si aplica)
# -------------------------
CONFIG_PERSONALIZADA = {
    # Ejemplo:
    # '25111': {
    #     'zhf_rural': {
    #         'Uso': 'norma_uso_suelo_personalizado'
    #     }
    # }
}

# -------------------------
# Utilidad: encontrar columna existente
# -------------------------
def encontrar_campo(gdf, posibles_nombres):
    for nombre in posibles_nombres:
        if nombre in gdf.columns:
            return nombre
    return None

# -------------------------
# Función para establecer la conexión a la base de datos
# -------------------------
def conectar_bd(config):
    cadena_conexion = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
    return create_engine(cadena_conexion)

# -------------------------
# Función para procesar los SHP y actualizar la base de datos
# -------------------------
def procesar_shp(engine, municipio, tipo, rutas_shp, mapeo_campos, log_lines):
    esquema = f"cun{municipio}"
    tabla = f"zhf_{tipo}"
    mapeo = mapeo_campos.get(tabla, {})

    if not rutas_shp:
        log_lines.append(f"❌ Municipio {municipio} - Sin archivos SHP para {tipo}.")
        return

    try:
        with engine.begin() as conn:
            conn.execute(text(f'DELETE FROM "{esquema}"."{tabla}";'))
            log_lines.append(f"🧹 Municipio {municipio} - Registros eliminados de {tabla}.")
    except Exception as e:
        log_lines.append(f"❌ Error al limpiar la tabla {esquema}.{tabla}: {e}")
        return

    id_base = int(municipio) * 1000
    contador = 1

    for ruta in rutas_shp:
        try:
            if not os.path.exists(ruta):
                log_lines.append(f"❌ Archivo no encontrado: {ruta}")
                continue

            gdf = gpd.read_file(ruta)
            gdf = gdf.to_crs(epsg=9377)

            registros = []
            for _, fila in gdf.iterrows():
                campos = {}
                for campo_objetivo, nombres_posibles in mapeo.items():
                    campos[campo_objetivo] = str(fila.get(encontrar_campo(gdf, nombres_posibles), '')).strip()

                registro = {
                    'id': id_base + contador,
                    'codigo': campos.get('codigo'),
                    'codigo_zona_fisica': campos.get('codigo_zona_fisica'),
                    'vigencia': datetime(2023, 1, 1),
                    'geometria': fila.geometry
                }

                if tipo == 'rural':
                    registro.update({
                        'area_homogenea_tierra': campos.get('area_homogenea_tierra'),
                        'disponibilidad_agua': campos.get('disponibilidad_agua'),
                        'influencia_vial': campos.get('influencia_vial'),
                        'uso_actual_suelo': campos.get('uso_actual_suelo'),
                        'norma_uso_suelo': campos.get('norma_uso_suelo')
                    })
                else:  # urbano
                    norma_uso = (campos.get('norma_uso_suelo_categoria') or '') + ', ' + \
                                (campos.get('norma_uso_suelo_area_activ') or '') + ', ' + \
                                (campos.get('norma_uso_suelo_tratamiento') or '')
                    registro.update({
                        'topografia': campos.get('topografia'),
                        'influencia_vial': campos.get('influencia_vial'),
                        'servicio_publico': campos.get('servicio_publico'),
                        'uso_actual_suelo': campos.get('uso_actual_suelo'),
                        'norma_uso_suelo': norma_uso,
                        'tipificacion_construccion': campos.get('tipificacion_construccion')
                    })

                registros.append(registro)
                contador += 1

            if registros:
                df_insertar = gpd.GeoDataFrame(registros, geometry='geometria', crs='EPSG:9377')
                df_insertar.to_postgis(name=tabla, con=engine, schema=esquema, if_exists='append', index=False)
                log_lines.append(f"✅ Municipio {municipio} - {len(registros)} registros insertados en {tabla}.")
            else:
                log_lines.append(f"⚠️ Municipio {municipio} - No se generaron registros desde {ruta}.")

        except Exception as e:
            log_lines.append(f"❌ Error procesando archivo {ruta} del municipio {municipio}: {e}")

# -------------------------
# Función principal
# -------------------------
def main():
    log_lines = []
    try:
        engine = conectar_bd(DB_CONFIG)
        for municipio in MUNICIPIOS:
            rutas = RUTAS_SHP.get(municipio, {})
            for tipo in ['rural', 'urbana']:
                rutas_tipo = rutas.get(tipo, [])
                procesar_shp(engine, municipio, tipo, rutas_tipo, MAPEO_CAMPOS, log_lines)
        engine.dispose()
    except Exception as e:
        log_lines.append(f"❌ Error general en el proceso: {e}")
    finally:
        with open("reporte_proceso.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(log_lines))
        print("✅ Proceso finalizado. Revisa 'reporte_proceso.txt' para más detalles.")

if __name__ == "__main__":
    main()
