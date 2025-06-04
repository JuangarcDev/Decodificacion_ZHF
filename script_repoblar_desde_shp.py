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
MUNICIPIOS = ['25645', '25035', '25040', '25599', '25326', '25596', '25769', '25805', '25807', '25815']

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
    '25040': {
        'rural': [r'C:\ACC\Zonas_Homogeneas_Decodificacion\Insumos\ARCHIVOS_NAS\Go\25040_ANOLAIMA\ZHF_RURALES\Zonas Homogéneas Físicas Rurales\Zonas_Homogeneas_Fisicas_Rurales.shp'],
        'urbana': []    # Sin información urbana
    },
    '25599': {
        'rural': [r'C:\ACC\Zonas_Homogeneas_Decodificacion\Insumos\ARCHIVOS_NAS\Go\25599_APULO\ZHF_RURALES\Zonas Homogéneas Físicas Rurales\Zonas_Homogeneas_Fisicas_Rurales.shp'],
        'urbana': []    # Sin información urbana
    },
    '25326': {
        'rural': [r'C:\ACC\Zonas_Homogeneas_Decodificacion\Insumos\ARCHIVOS_NAS\Go\25326_GUATAVITA\ZHF_RURALES\Zonas Homogéneas Físicas Rurales\Zonas_Homogeneas_Fisicas_Rurales.shp'],
        'urbana': []    # Sin información urbana
    },
    '25596': {
        'rural': [r'C:\ACC\Zonas_Homogeneas_Decodificacion\Insumos\ARCHIVOS_NAS\Go\25596_QUIPILE\ZHF_RURALES\Zonas Homogéneas Físicas Rurales\Zonas Homogeneas Fisicas Rurales.shp'],
        'urbana': []    # Sin información urbana
    },
    '25769': {
        'rural': [r'C:\ACC\Zonas_Homogeneas_Decodificacion\Insumos\ARCHIVOS_NAS\Go\25769_SUBACHOQUE\ZHF_RURALES\Zonas Homogéneas Físicas Rurales\Zonas_Homogeneas_Fisicas_Rurales.shp'],
        'urbana': []    # Sin información urbana
    },
    '25805' : {
        'rural': [r'C:\ACC\Zonas_Homogeneas_Decodificacion\Insumos\ARCHIVOS_NAS\Go\25805_TIBACUY\ZHF_RURALES\Zonas Homogéneas Físicas Rurales\Zonas_Homogeneas_Fisicas_Rurales.shp'],
        'urbana': []    # Sin información urbana
    },
    '25807': {
        'rural': [r'C:\ACC\Zonas_Homogeneas_Decodificacion\Insumos\ARCHIVOS_NAS\Go\25807_TIBIRITA\ZHF_RURALES\Zonas Homogéneas Físicas Rurales\ZONA HOMOGENEA FISICA RURAL.shp'],
        'urbana': []    # Sin información urbana
    },
    '25815': {
        'rural': [r'C:\ACC\Zonas_Homogeneas_Decodificacion\Insumos\ARCHIVOS_NAS\Go\25815_TOCAIMA\ZHF_RURALES\Zonas Homogéneas Físicas Rurales\Zonas Homogeneas Fisicas Rurales.shp'],
        'urbana': []    # Sin información urbana
    },
}

# -------------------------
# Mapeo de campos por defecto (genérico)
# -------------------------
MAPEO_CAMPOS_GENERAL = {
    'zhf_rural': {
        'codigo': ['ID_ZHFR', 'COD_ZHF'],
        'codigo_zona_fisica': ['ZHFR', 'ZHF_R', 'codigo_1', 'COD_ZHFR_1'],
        'area_homogenea_tierra': ['AHT', 'a_hom_tier', 'VALOR_PO_1','uso_suelo'],
        'disponibilidad_agua': ['Disp_Aguas', 'Disponibil', 'AV_disponi', 'AV_Disponi', 'aguas'],
        'influencia_vial': ['Inf_Vial', 'Influencia', 'AV_Incluen', 'AV_Influen', 'Av_Influen', 'vias', 'AV_Influ_V'],
        'uso_actual_suelo': ['Uso_Actual', 'AV_uso_Act', 'AV_Uso_Act', 'Av_Uso_Act', 'uso_suel_1', 'AV_UsoSuel'],
        'norma_uso_suelo': ['Uso', 'norma_uso', 'clasi','Usos', 'norma_us_1', 'norma_uso_', 'CLASIFICAC', 'Grupo'],
        'vigencia': datetime(2024, 1, 1)     
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
        'tipificacion_construccion': ['Tipificaci', 'TIPIF_CONS'],
        'vigencia': datetime(2024, 1, 1)      
    }
}

# -------------------------
# Configuración personalizada por municipio (si aplica)
# -------------------------
MAPEO_CAMPOS_PERSONALIZADO = {
    '25645': {
        'zhf_urbana': {
            'vigencia': datetime(2023, 1, 1),  # si se usa uno solo ya concatenado
        },
        'zhf_rural': {
            'vigencia': datetime(2023, 1, 1),
        }
    },
    # ... otros municipios
    '25035': {
        'zhf_rural': {
            'vigencia': datetime(2024, 1, 1),
        }
    },
    '25040': {
        'zhf_rural': {
            'vigencia': datetime(2024, 1, 1),
        }
    },
    '25599': {
        'zhf_rural': {
            'vigencia': ['vigencia'],
        }
    },
    '25596': {
        'zhf_rural': {
            'vigencia': ['vigencia'],
        }
    },
    '25769':
    {
        'zhf_rural': {
            'norma_uso_suelo': ['norma_uso_'],
        }
    },
    '25805': 
    {
        'zhf_rural': {
            'norma_uso_suelo': ['CLASIFICAC'],
        }
    },
    '25807': 
    {
        'zhf_rural': {
            'norma_uso_suelo': ['norma_AV'],
        }
    },
    '25815': 
    {
        'zhf_rural': {
            'norma_uso_suelo': ['Grupo'],
            'vigencia': ['vigencia'],
        }
    },

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
# Obtener mapeo efectivo para municipio y tipo
# -------------------------
def obtener_mapeo(municipio, tipo):
    mapeo_def = MAPEO_CAMPOS_GENERAL.get(f"zhf_{tipo}", {})
    mapeo_mpio = MAPEO_CAMPOS_PERSONALIZADO.get(municipio, {}).get(f"zhf_{tipo}", {})
    # fusionar dando prioridad a mapeo_mpio
    return {**mapeo_def, **mapeo_mpio}

# -------------------------
# Función para establecer la conexión a la base de datos
# -------------------------
def conectar_bd(config):
    cadena_conexion = f"postgresql://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
    return create_engine(cadena_conexion)

# -------------------------
# Función para procesar los SHP y actualizar la base de datos
# -------------------------
def procesar_shp(engine, municipio, tipo, rutas_shp, log_lines):
    esquema = f"cun{municipio}"
    tabla = f"zhf_{tipo}"
    mapeo = obtener_mapeo(municipio, tipo)

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
                    if isinstance(nombres_posibles, list):
                        campos[campo_objetivo] = str(fila.get(encontrar_campo(gdf, nombres_posibles), '')).strip()
                    else:
                        campos[campo_objetivo] = nombres_posibles
                registro = {
                    'id': id_base + contador,
                    'codigo': campos.get('codigo'),
                    'codigo_zona_fisica': campos.get('codigo_zona_fisica'),
                    'geometria': fila.geometry
                }

                if tipo == 'rural':
                    registro.update({
                        'area_homogenea_tierra': campos.get('area_homogenea_tierra'),
                        'disponibilidad_agua': campos.get('disponibilidad_agua'),
                        'influencia_vial': campos.get('influencia_vial'),
                        'uso_actual_suelo': campos.get('uso_actual_suelo'),
                        'norma_uso_suelo': campos.get('norma_uso_suelo'),
                        'vigencia': campos.get('vigencia')
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
                        'tipificacion_construccion': campos.get('tipificacion_construccion'),
                        'vigencia': campos.get('vigencia')                        
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
                # ✅ Solo pasas engine, municipio, tipo, rutas_tipo, log_lines
                procesar_shp(engine, municipio, tipo, rutas_tipo, log_lines)

        engine.dispose()
    except Exception as e:
        log_lines.append(f"❌ Error general en el proceso: {e}")
    finally:
        with open("reporte_proceso.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(log_lines))
        print("✅ Proceso finalizado. Revisa 'reporte_proceso.txt' para más detalles.")

if __name__ == "__main__":
    main()
