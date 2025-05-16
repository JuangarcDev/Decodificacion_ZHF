# -*- coding: utf-8 -*-
import psycopg2
from lxml import etree
from shapely.geometry import Polygon, MultiPolygon
from geoalchemy2.shape import from_shape
from datetime import datetime
from shapely.wkt import dumps as shapely_to_wkt

# Conexión a la base de datos
def get_connection():
    try:
        conn = psycopg2.connect(
            port='5432',
            host="localhost",
            database="Novedades_V1_Municipios",
            user="postgres",
            password="1234jcgg"
        )
        print("[INFO] Conexión a la base de datos exitosa.")
        return conn
    except Exception as e:
        print("[ERROR] Fallo en la conexión a la base de datos:", e)
        raise

# Procesa un bloque de XML y devuelve una lista de diccionarios con atributos y geometría
def procesar_registros_xml(xml_bloque):
    print("[INFO] Iniciando procesamiento de XML.")
    try:
        root = etree.fromstring("<root>" + xml_bloque + "</root>")
    except Exception as e:
        print("[ERROR] No se pudo parsear el XML:", e)
        return []

    print("[DEBUG] Nodo raíz parseado:", root.tag)
    print("[DEBUG] Listado de todos los nodos (tags):")
    for elem in root.iter():
        print("  -", elem.tag)

    registros = []

    # Buscar nodos ignorando namespaces con comodín {*}
    for i, subelem in enumerate(root.iter()):
        if subelem.tag.endswith('AV_ZonaHomogeneaFisicaUrbana'):
            print("[INFO] Procesando registro #{}".format(i + 1))

            def get_text(tag):
                # Buscar hijo con el tag ignorando namespace
                el = subelem.find('.//{{*}}{}'.format(tag))
                return el.text.strip() if el is not None and el.text else None

            atributos = {
                'codigo': get_text('Codigo'),
                'codigo_zona_fisica': get_text('Codigo_Zona_Fisica'),
                'topografia': get_text('Topografia'),
                'influencia_vial': get_text('Influencia_Vial'),
                'servicio_publico': get_text('Servicio_Publico'),
                'uso_actual_suelo': get_text('Uso_Actual_Suelo'),
                'norma_uso_suelo': get_text('Norma_Uso_Suelo'),
                'tipificacion_construccion': get_text('Tipificacion_Construccion'),
                'vigencia': None
            }

            vigencia_text = get_text('Vigencia')
            if vigencia_text:
                try:
                    atributos['vigencia'] = datetime.strptime(vigencia_text, '%Y-%m-%d').date()
                except Exception as e:
                    print("[WARN] Fecha 'Vigencia' no pudo convertirse:", e)

            print("[INFO] Atributos extraídos:", atributos)

            coords = []
            # Buscar coordenadas ignorando namespace también
            for coord in subelem.findall('.//{*}COORD'):
                try:
                    x_text = coord.find('./{*}C1')
                    y_text = coord.find('./{*}C2')
                    z_text = coord.find('./{*}C3')

                    x = float(x_text.text) if x_text is not None and x_text.text else None
                    y = float(y_text.text) if y_text is not None and y_text.text else None
                    z = float(z_text.text) if z_text is not None and z_text.text else None

                    if None not in (x, y, z):
                        coords.append((x, y, z))
                    else:
                        print("[WARN] Coordenada con valores faltantes, se omite:", (x, y, z))
                except Exception as e:
                    print("[ERROR] Error al extraer coordenadas:", e)

            print("[INFO] Total coordenadas extraídas:", len(coords))

            if coords and coords[0] != coords[-1]:
                coords.append(coords[0])

            try:
                poligono = Polygon(coords)
                multi = MultiPolygon([poligono])
                atributos['geometria_wkt'] = shapely_to_wkt(multi)
                print("[INFO] Geometría procesada correctamente.")
            except Exception as e:
                print("[ERROR] Error al generar geometría:", e)
                continue  # salta este registro

            registros.append(atributos)

    print("[INFO] Total registros procesados del XML:", len(registros))
    return registros


# El resto de funciones no cambia, pero te las dejo aquí para referencia:
def insertar_registros_por_municipio(codmpio, registros):
    if not registros:
        print("[ADVERTENCIA] No hay registros para insertar para el municipio", codmpio)
        return

    schema = 'cun' + codmpio
    tabla = 'zhf_urbana'

    try:
        conn = get_connection()
        cursor = conn.cursor()

        print("[INFO] Borrando registros existentes del municipio:", codmpio)
        cursor.execute("DELETE FROM {}.{}".format(schema, tabla))

        print("[INFO] Insertando {} nuevos registros...".format(len(registros)))

        insert_sql = """
            INSERT INTO {}.{} (
                geometria, codigo, codigo_zona_fisica, topografia, influencia_vial,
                servicio_publico, uso_actual_suelo, norma_uso_suelo,
                tipificacion_construccion, vigencia
            ) VALUES (
                ST_GeomFromText(%s, 9377), %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """.format(schema, tabla)

        for i, r in enumerate(registros):
            try:
                valores = [
                    r['geometria_wkt'], r['codigo'], r['codigo_zona_fisica'], r['topografia'],
                    r['influencia_vial'], r['servicio_publico'], r['uso_actual_suelo'],
                    r['norma_uso_suelo'], r['tipificacion_construccion'], r['vigencia']
                ]
                cursor.execute(insert_sql, valores)
                print("[INFO] Registro #{} insertado.".format(i + 1))
            except Exception as e:
                print("[ERROR] Error al insertar registro #{}:".format(i + 1), e)

        conn.commit()
        print("[INFO] Inserción completada y cambios guardados.")
        cursor.close()
        conn.close()

    except Exception as e:
        print("[ERROR] Error durante la inserción de registros:", e)



def procesar_varios_municipios(xml_por_municipio):
    for codmpio, xml_str in xml_por_municipio.items():
        print("\n=== Procesando municipio:", codmpio, "===")
        registros = procesar_registros_xml(xml_str)
        insertar_registros_por_municipio(codmpio, registros)
    print("\n[INFO] Proceso completado.")

# USO DEL SCRIPT:
xml_por_municipio = {
            '25053': '''<Submodelo_Avaluos_V1_2.Avaluos.AV_ZonaHomogeneaFisicaUrbana TID="bc55aab5-3330-4614-bbfd-5b40c82019a4"><Codigo>005</Codigo><Codigo_Zona_Fisica>12150513</Codigo_Zona_Fisica><Topografia>Plano</Topografia><Influencia_Vial>Sin_Pavimentar</Influencia_Vial><Servicio_Publico>Sin_Servicios</Servicio_Publico><Uso_Actual_Suelo>Lote</Uso_Actual_Suelo><Norma_Uso_Suelo>RESIDENCIAL DENSIDAD BAJA</Norma_Uso_Suelo><Tipificacion_Construccion>Comercial_Sectorial</Tipificacion_Construccion><Vigencia>2023-01-01</Vigencia><Geometria><ISO19107_PLANAS_V3_0.GM_MultiSurface3D><geometry><ISO19107_PLANAS_V3_0.GM_Surface3DListValue><value><SURFACE><BOUNDARY><POLYLINE><COORD><C1>4843406.700</C1><C2>2030581.200</C2><C3>0.000</C3></COORD><COORD><C1>4843418.385</C1><C2>2030589.856</C2><C3>0.000</C3></COORD><COORD><C1>4843428.441</C1><C2>2030597.107</C2><C3>0.000</C3></COORD><COORD><C1>4843433.929</C1><C2>2030601.348</C2><C3>0.000</C3></COORD><COORD><C1>4843438.219</C1><C2>2030604.681</C2><C3>0.000</C3></COORD><COORD><C1>4843447.334</C1><C2>2030611.871</C2><C3>0.000</C3></COORD><COORD><C1>4843448.100</C1><C2>2030612.400</C2><C3>0.000</C3></COORD><COORD><C1>4843453.062</C1><C2>2030616.465</C2><C3>0.000</C3></COORD><COORD><C1>4843455.359</C1><C2>2030618.311</C2><C3>0.000</C3></COORD><COORD><C1>4843458.000</C1><C2>2030620.500</C2><C3>0.000</C3></COORD><COORD><C1>4843463.554</C1><C2>2030624.481</C2><C3>0.000</C3></COORD><COORD><C1>4843468.430</C1><C2>2030628.032</C2><C3>0.000</C3></COORD><COORD><C1>4843470.428</C1><C2>2030629.487</C2><C3>0.000</C3></COORD><COORD><C1>4843476.137</C1><C2>2030633.719</C2><C3>0.000</C3></COORD><COORD><C1>4843483.614</C1><C2>2030639.339</C2><C3>0.000</C3></COORD><COORD><C1>4843486.466</C1><C2>2030641.482</C2><C3>0.000</C3></COORD><COORD><C1>4843493.100</C1><C2>2030646.300</C2><C3>0.000</C3></COORD><COORD><C1>4843501.475</C1><C2>2030651.786</C2><C3>0.000</C3></COORD><COORD><C1>4843509.077</C1><C2>2030656.314</C2><C3>0.000</C3></COORD><COORD><C1>4843515.200</C1><C2>2030659.800</C2><C3>0.000</C3></COORD><COORD><C1>4843522.400</C1><C2>2030663.800</C2><C3>0.000</C3></COORD><COORD><C1>4843531.400</C1><C2>2030668.900</C2><C3>0.000</C3></COORD><COORD><C1>4843535.900</C1><C2>2030671.400</C2><C3>0.000</C3></COORD><COORD><C1>4843541.200</C1><C2>2030674.000</C2><C3>0.000</C3></COORD><COORD><C1>4843547.300</C1><C2>2030674.900</C2><C3>0.000</C3></COORD><COORD><C1>4843575.356</C1><C2>2030682.214</C2><C3>0.000</C3></COORD><COORD><C1>4843618.200</C1><C2>2030688.200</C2><C3>0.000</C3></COORD><COORD><C1>4843629.300</C1><C2>2030674.200</C2><C3>0.000</C3></COORD><COORD><C1>4843632.800</C1><C2>2030647.400</C2><C3>0.000</C3></COORD><COORD><C1>4843640.600</C1><C2>2030639.000</C2><C3>0.000</C3></COORD><COORD><C1>4843663.800</C1><C2>2030634.200</C2><C3>0.000</C3></COORD><COORD><C1>4843677.500</C1><C2>2030630.600</C2><C3>0.000</C3></COORD><COORD><C1>4843675.700</C1><C2>2030622.300</C2><C3>0.000</C3></COORD><COORD><C1>4843648.300</C1><C2>2030602.600</C2><C3>0.000</C3></COORD><COORD><C1>4843644.500</C1><C2>2030600.400</C2><C3>0.000</C3></COORD><COORD><C1>4843631.000</C1><C2>2030592.500</C2><C3>0.000</C3></COORD><COORD><C1>4843623.800</C1><C2>2030567.400</C2><C3>0.000</C3></COORD><COORD><C1>4843630.300</C1><C2>2030540.000</C2><C3>0.000</C3></COORD><COORD><C1>4843644.000</C1><C2>2030519.100</C2><C3>0.000</C3></COORD><COORD><C1>4843644.600</C1><C2>2030500.500</C2><C3>0.000</C3></COORD><COORD><C1>4843645.100</C1><C2>2030485.700</C2><C3>0.000</C3></COORD><COORD><C1>4843658.700</C1><C2>2030471.500</C2><C3>0.000</C3></COORD><COORD><C1>4843566.700</C1><C2>2030387.700</C2><C3>0.000</C3></COORD><COORD><C1>4843505.800</C1><C2>2030444.500</C2><C3>0.000</C3></COORD><COORD><C1>4843485.400</C1><C2>2030520.500</C2><C3>0.000</C3></COORD><COORD><C1>4843448.000</C1><C2>2030534.400</C2><C3>0.000</C3></COORD><COORD><C1>4843407.500</C1><C2>2030549.900</C2><C3>0.000</C3></COORD><COORD><C1>4843377.600</C1><C2>2030560.600</C2><C3>0.000</C3></COORD><COORD><C1>4843406.700</C1><C2>2030581.200</C2><C3>0.000</C3></COORD></POLYLINE></BOUNDARY></SURFACE></value></ISO19107_PLANAS_V3_0.GM_Surface3DListValue></geometry></ISO19107_PLANAS_V3_0.GM_MultiSurface3D></Geometria></Submodelo_Avaluos_V1_2.Avaluos.AV_ZonaHomogeneaFisicaUrbana>
            <Submodelo_Avaluos_V1_2.Avaluos.AV_ZonaHomogeneaFisicaUrbana TID="bb5a57c8-5ea5-47c9-8263-ea89b5412372"><Codigo>002</Codigo><Codigo_Zona_Fisica>11130507</Codigo_Zona_Fisica><Topografia>Plano</Topografia><Influencia_Vial>Pavimentadas</Influencia_Vial><Servicio_Publico>Sin_Servicios</Servicio_Publico><Uso_Actual_Suelo>Industrial</Uso_Actual_Suelo><Norma_Uso_Suelo>RESIDENCIAL DENSIDAD BAJA</Norma_Uso_Suelo><Tipificacion_Construccion>Comercial_Barrial</Tipificacion_Construccion><Vigencia>2023-01-01</Vigencia><Geometria><ISO19107_PLANAS_V3_0.GM_MultiSurface3D><geometry><ISO19107_PLANAS_V3_0.GM_Surface3DListValue><value><SURFACE><BOUNDARY><POLYLINE><COORD><C1>4843570.697</C1><C2>2030690.600</C2><C3>0.000</C3></COORD><COORD><C1>4843576.497</C1><C2>2030691.100</C2><C3>0.000</C3></COORD><COORD><C1>4843581.697</C1><C2>2030695.500</C2><C3>0.000</C3></COORD><COORD><C1>4843589.097</C1><C2>2030703.800</C2><C3>0.000</C3></COORD><COORD><C1>4843593.137</C1><C2>2030708.782</C2><C3>0.000</C3></COORD><COORD><C1>4843599.397</C1><C2>2030716.500</C2><C3>0.000</C3></COORD><COORD><C1>4843618.200</C1><C2>2030688.200</C2><C3>0.000</C3></COORD><COORD><C1>4843575.356</C1><C2>2030682.214</C2><C3>0.000</C3></COORD><COORD><C1>4843547.300</C1><C2>2030674.900</C2><C3>0.000</C3></COORD><COORD><C1>4843541.200</C1><C2>2030674.000</C2><C3>0.000</C3></COORD><COORD><C1>4843535.900</C1><C2>2030671.400</C2><C3>0.000</C3></COORD><COORD><C1>4843531.400</C1><C2>2030668.900</C2><C3>0.000</C3></COORD><COORD><C1>4843522.400</C1><C2>2030663.800</C2><C3>0.000</C3></COORD><COORD><C1>4843515.200</C1><C2>2030659.800</C2><C3>0.000</C3></COORD><COORD><C1>4843509.077</C1><C2>2030656.314</C2><C3>0.000</C3></COORD><COORD><C1>4843501.475</C1><C2>2030651.786</C2><C3>0.000</C3></COORD><COORD><C1>4843493.100</C1><C2>2030646.300</C2><C3>0.000</C3></COORD><COORD><C1>4843486.466</C1><C2>2030641.482</C2><C3>0.000</C3></COORD><COORD><C1>4843483.614</C1><C2>2030639.339</C2><C3>0.000</C3></COORD><COORD><C1>4843476.137</C1><C2>2030633.719</C2><C3>0.000</C3></COORD><COORD><C1>4843470.428</C1><C2>2030629.487</C2><C3>0.000</C3></COORD><COORD><C1>4843468.430</C1><C2>2030628.032</C2><C3>0.000</C3></COORD><COORD><C1>4843463.554</C1><C2>2030624.481</C2><C3>0.000</C3></COORD><COORD><C1>4843458.000</C1><C2>2030620.500</C2><C3>0.000</C3></COORD><COORD><C1>4843455.359</C1><C2>2030618.311</C2><C3>0.000</C3></COORD><COORD><C1>4843453.062</C1><C2>2030616.465</C2><C3>0.000</C3></COORD><COORD><C1>4843448.100</C1><C2>2030612.400</C2><C3>0.000</C3></COORD><COORD><C1>4843447.334</C1><C2>2030611.871</C2><C3>0.000</C3></COORD><COORD><C1>4843438.219</C1><C2>2030604.681</C2><C3>0.000</C3></COORD><COORD><C1>4843433.929</C1><C2>2030601.348</C2><C3>0.000</C3></COORD><COORD><C1>4843428.441</C1><C2>2030597.107</C2><C3>0.000</C3></COORD><COORD><C1>4843418.385</C1><C2>2030589.856</C2><C3>0.000</C3></COORD><COORD><C1>4843406.700</C1><C2>2030581.200</C2><C3>0.000</C3></COORD><COORD><C1>4843401.800</C1><C2>2030585.500</C2><C3>0.000</C3></COORD><COORD><C1>4843402.025</C1><C2>2030585.736</C2><C3>0.000</C3></COORD><COORD><C1>4843404.490</C1><C2>2030588.318</C2><C3>0.000</C3></COORD><COORD><C1>4843411.906</C1><C2>2030596.084</C2><C3>0.000</C3></COORD><COORD><C1>4843421.098</C1><C2>2030605.382</C2><C3>0.000</C3></COORD><COORD><C1>4843426.600</C1><C2>2030609.900</C2><C3>0.000</C3></COORD><COORD><C1>4843426.958</C1><C2>2030609.486</C2><C3>0.000</C3></COORD><COORD><C1>4843427.045</C1><C2>2030609.385</C2><C3>0.000</C3></COORD><COORD><C1>4843431.376</C1><C2>2030613.240</C2><C3>0.000</C3></COORD><COORD><C1>4843439.606</C1><C2>2030620.404</C2><C3>0.000</C3></COORD><COORD><C1>4843440.223</C1><C2>2030620.941</C2><C3>0.000</C3></COORD><COORD><C1>4843441.858</C1><C2>2030622.195</C2><C3>0.000</C3></COORD><COORD><C1>4843446.220</C1><C2>2030625.542</C2><C3>0.000</C3></COORD><COORD><C1>4843451.414</C1><C2>2030629.527</C2><C3>0.000</C3></COORD><COORD><C1>4843456.737</C1><C2>2030633.621</C2><C3>0.000</C3></COORD><COORD><C1>4843463.461</C1><C2>2030638.793</C2><C3>0.000</C3></COORD><COORD><C1>4843463.033</C1><C2>2030639.327</C2><C3>0.000</C3></COORD><COORD><C1>4843468.365</C1><C2>2030643.833</C2><C3>0.000</C3></COORD><COORD><C1>4843474.900</C1><C2>2030649.100</C2><C3>0.000</C3></COORD><COORD><C1>4843477.538</C1><C2>2030651.446</C2><C3>0.000</C3></COORD><COORD><C1>4843477.843</C1><C2>2030651.698</C2><C3>0.000</C3></COORD><COORD><C1>4843480.876</C1><C2>2030654.309</C2><C3>0.000</C3></COORD><COORD><C1>4843484.200</C1><C2>2030657.000</C2><C3>0.000</C3></COORD><COORD><C1>4843492.497</C1><C2>2030663.706</C2><C3>0.000</C3></COORD><COORD><C1>4843501.628</C1><C2>2030671.376</C2><C3>0.000</C3></COORD><COORD><C1>4843506.800</C1><C2>2030675.400</C2><C3>0.000</C3></COORD><COORD><C1>4843512.600</C1><C2>2030680.000</C2><C3>0.000</C3></COORD><COORD><C1>4843516.600</C1><C2>2030683.200</C2><C3>0.000</C3></COORD><COORD><C1>4843521.700</C1><C2>2030687.100</C2><C3>0.000</C3></COORD><COORD><C1>4843525.700</C1><C2>2030690.200</C2><C3>0.000</C3></COORD><COORD><C1>4843526.500</C1><C2>2030690.500</C2><C3>0.000</C3></COORD><COORD><C1>4843531.400</C1><C2>2030692.200</C2><C3>0.000</C3></COORD><COORD><C1>4843533.500</C1><C2>2030692.900</C2><C3>0.000</C3></COORD><COORD><C1>4843536.700</C1><C2>2030693.500</C2><C3>0.000</C3></COORD><COORD><C1>4843549.197</C1><C2>2030693.200</C2><C3>0.000</C3></COORD><COORD><C1>4843556.597</C1><C2>2030691.900</C2><C3>0.000</C3></COORD><COORD><C1>4843564.097</C1><C2>2030691.300</C2><C3>0.000</C3></COORD><COORD><C1>4843570.697</C1><C2>2030690.600</C2><C3>0.000</C3></COORD></POLYLINE></BOUNDARY></SURFACE></value></ISO19107_PLANAS_V3_0.GM_Surface3DListValue></geometry></ISO19107_PLANAS_V3_0.GM_MultiSurface3D></Geometria></Submodelo_Avaluos_V1_2.Avaluos.AV_ZonaHomogeneaFisicaUrbana>
'''#,
    #'002': '''<Submodelo_Avaluos_V1_2.Avaluos.AV_ZonaHomogeneaFisicaUrbana ... </Submodelo_Avaluos_V1_2.Avaluos.AV_ZonaHomogeneaFisicaUrbana>...'''
}

procesar_varios_municipios(xml_por_municipio)