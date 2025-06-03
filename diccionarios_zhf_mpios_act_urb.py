# -*- coding: utf-8 -*-
import psycopg2
import os
import datetime

# ESTE SCRIPT TIENE EL FIN DE RECOPILAR LOS DICCIONARIOS Y EQUIVALENCIAS A CADA ZHF DE LOS MPIOS ACTUALIZADOS, BASANDONOS EN EL DOCUMENTO OFICIAL ENTREGADO POR EL OPERADOR, PRINCIPALMENTE PARA LA PARTE URBANA

# --------- CONFIGURACIÓN GENERAL ---------

# Lista de códigos DIVIPOLA de municipios a procesar
# Municipios con lógica condicional (requieren lógica especial para ciertas variables)
municipios_condicionales = ['25035', '25040', '25599', '25269', '25326', '25596', '25769', '25805', '25807', '25815'] # COMENTADA 25645 HASTA VERIFICAR SI TIENE VALORES EN SUS ZHF

# Municipios con lógica de diccionario plano (sin condicionales)
municipios_planos = []  # Agrega aquí los demás códigos

# Diccionarios de decodificación por municipio
# Aquí tú completas con las codificaciones específicas de cada municipio
diccionarios_mpios = {
    # FALTAN VARIAS CODIFICACIONES DE VARIABLES, ADEMAS SE EVIDENCIAN MAS ZHF QUE EN EL DOCUMENTO BASE. NO LISTO PARA EJECUTAR
                #               ---------           MPIOS GO CATASTRAL                  --------------
    '25035': {
        'orden_variables': ['topografia', 'influencia_vial', 'servicio_publico', 'uso_actual_suelo', 'norma_uso_suelo', 'tipificacion_construccion'],
        'longitudes': [1, 1, 1, 1, 3, 1],
        'topografia': {'1': 'Plano', '2': 'Inclinado', '3': 'Empinado'},
        'influencia_vial': {'1': 'Pavimentadas', '2': 'Sin_Pavimentar', '3': 'Peatonales', '4': 'Sin_Vias'},
        'servicio_publico': {'1': 'Sin_Servicios', '2': 'Básicos_Incompletos', '3': 'Básicos_Completos ', '4': 'Básicos_Y_Complementarios'},
        'uso_actual_suelo': {'1': 'Residencial', '2': 'Comercial', '3': 'Industrial', '4': 'Institucional', '5': 'Lote', '6': 'Espacio Publico'},
        'norma_uso_suelo': {
            '110': 'Protegido, Múltiple, Sin Tratamiento', '111': 'Protegido, Múltiple, Conservación', '112': 'Protegido, Múltiple, Recuperación', '113': 'Protegido, Múltiple, Conservacion Urbanistica',
            '114': 'Protegido, Múltiple, Consolidacion', '116': 'Protegido, Múltiple, Desarrollo de areas sin urbanizar', '117': 'Protegido, Múltiple, Mejoramiento Integral', '118': 'Protegido, Múltiple, Desarrollo de areas urbanizadas',
            '119': 'Protegido, Múltiple, Desarrollo por urbanizaciones', '120': 'Protegido, Residencial, Sin Tratamiento', '121': 'Protegido, Residencial, Conservación', '122': 'Protegido, Residencial, Recuperación',
            '123': 'Protegido, Residencial, Conservacion Urbanistica', '124': 'Protegido, Residencial, Consolidacion', '126': 'Protegido, Residencial, Desarrollo de areas sin urbanizar', '127': 'Protegido, Residencial, Mejoramiento Integral',
            '128': 'Protegido, Residencial, Desarrollo de areas urbanizadas', '129': 'Protegido, Residencial, Desarrollo por urbanizaciones', '130': 'Protegido, Residencial Desarrollo, Sin Tratamiento', '131': 'Protegido, Residencial Desarrollo, Conservación',
            '132': 'Protegido, Residencial Desarrollo, Recuperación', '133': 'Protegido, Residencial Desarrollo, Conservacion Urbanistica', '134': 'Protegido, Residencial Desarrollo, Consolidacion', '136': 'Protegido, Residencial Desarrollo, Desarrollo de areas sin urbanizar',
            '137': 'Protegido, Residencial Desarrollo, Mejoramiento Integral', '138': 'Protegido, Residencial Desarrollo, Desarrollo de areas urbanizadas', '139': 'Protegido, Residencial Desarrollo, Desarrollo por urbanizaciones', '140': 'Protegido, Residencial Sin Desarrollo, Sin Tratamiento',
            '141': 'Protegido, Residencial Sin Desarrollo, Conservación', '142': 'Protegido, Residencial Sin Desarrollo, Recuperación', '143': 'Protegido, Residencial Sin Desarrollo, Conservacion Urbanistica', '144': 'Protegido, Residencial Sin Desarrollo, Consolidacion',
            '146': 'Protegido, Residencial Sin Desarrollo, Desarrollo de areas sin urbanizar', '147': 'Protegido, Residencial Sin Desarrollo, Mejoramiento Integral', '148': 'Protegido, Residencial Sin Desarrollo, Desarrollo de areas urbanizadas', '149': 'Protegido, Residencial Sin Desarrollo, Desarrollo por urbanizaciones',
            '150': 'Protegido, Comercial, Sin Tratamiento', '151': 'Protegido, Comercial, Conservación', '152': 'Protegido, Comercial, Recuperación', '153': 'Protegido, Comercial, Conservacion Urbanistica',
            '154': 'Protegido, Comercial, Consolidacion', '156': 'Protegido, Comercial, Desarrollo de areas sin urbanizar', '157': 'Protegido, Comercial, Mejoramiento Integral', '158': 'Protegido, Comercial, Desarrollo de areas urbanizadas',
            '159': 'Protegido, Comercial, Desarrollo por urbanizaciones', '160': 'Protegido, Industrial, Sin Tratamiento', '161': 'Protegido, Industrial, Conservación', '162': 'Protegido, Industrial, Recuperación',
            '163': 'Protegido, Industrial, Conservacion Urbanistica', '164': 'Protegido, Industrial, Consolidacion', '166': 'Protegido, Industrial, Desarrollo de areas sin urbanizar', '167': 'Protegido, Industrial, Mejoramiento Integral', '168': 'Protegido, Industrial, Desarrollo de areas urbanizadas',
            '169': 'Protegido, Industrial, Desarrollo por urbanizaciones', '170': 'Protegido, Dotacional, Sin Tratamiento', '171': 'Protegido, Dotacional, Conservación', '172': 'Protegido, Dotacional, Recuperación',
            '173': 'Protegido, Dotacional, Conservacion Urbanistica', '174': 'Protegido, Dotacional, Consolidacion', '176': 'Protegido, Dotacional, Desarrollo de areas sin urbanizar', '177': 'Protegido, Dotacional, Mejoramiento Integral', '178': 'Protegido, Dotacional, Desarrollo de areas urbanizadas',
            '179': 'Protegido, Dotacional, Desarrollo por urbanizaciones', '180': 'Protegido, Senderos Ecologicos, Sin Tratamiento', '181': 'Protegido, Senderos Ecologicos, Conservación', '182': 'Protegido, Senderos Ecologicos, Recuperación',
            '183': 'Protegido, Senderos Ecologicos, Conservacion Urbanistica', '184': 'Protegido, Senderos Ecologicos, Consolidacion', '186': 'Protegido, Senderos Ecologicos, Desarrollo de areas sin urbanizar', '187': 'Protegido, Senderos Ecologicos, Mejoramiento Integral', '188': 'Protegido, Senderos Ecologicos, Desarrollo de areas urbanizadas',
            '189': 'Protegido, Senderos Ecologicos, Desarrollo por urbanizaciones', '210': 'No protegido, Múltiple, Sin Tratamiento', '211': 'No protegido, Múltiple, Conservación', '212': 'No protegido, Múltiple, Recuperación',
            '213': 'No protegido, Múltiple, Conservacion Urbanistica', '214': 'No protegido, Múltiple, Consolidacion', '216': 'No protegido, Múltiple, Desarrollo de areas sin urbanizar', '217': 'No protegido, Múltiple, Mejoramiento Integral', '218': 'No protegido, Múltiple, Desarrollo de areas urbanizadas',
            '219': 'No protegido, Múltiple, Desarrollo por urbanizaciones', '220': 'No protegido, Residencial, Sin Tratamiento', '221': 'No protegido, Residencial, Conservación', '222': 'No protegido, Residencial, Recuperación',
            '223': 'No protegido, Residencial, Conservacion Urbanistica', '224': 'No protegido, Residencial, Consolidacion', '226': 'No protegido, Residencial, Desarrollo de areas sin urbanizar', '227': 'No protegido, Residencial, Mejoramiento Integral', '228': 'No protegido, Residencial, Desarrollo de areas urbanizadas',
            '229': 'No protegido, Residencial, Desarrollo por urbanizaciones', '230': 'No protegido, Residencial Desarrollo, Sin Tratamiento', '231': 'No protegido, Residencial Desarrollo, Conservación', '232': 'No protegido, Residencial Desarrollo, Recuperación',
            '233': 'No protegido, Residencial Desarrollo, Conservacion Urbanistica', '234': 'No protegido, Residencial Desarrollo, Consolidacion', '236': 'No protegido, Residencial Desarrollo, Desarrollo de areas sin urbanizar', '237': 'No protegido, Residencial Desarrollo, Mejoramiento Integral', '238': 'No protegido, Residencial Desarrollo, Desarrollo de areas urbanizadas',
            '239': 'No protegido, Residencial Desarrollo, Desarrollo por urbanizaciones', '240': 'No protegido, Residencial Sin Desarrollo, Sin Tratamiento', '241': 'No protegido, Residencial Sin Desarrollo, Conservación', '242': 'No protegido, Residencial Sin Desarrollo, Recuperación',
            '243': 'No protegido, Residencial Sin Desarrollo, Conservacion Urbanistica', '244': 'No protegido, Residencial Sin Desarrollo, Consolidacion', '246': 'No protegido, Residencial Sin Desarrollo, Desarrollo de areas sin urbanizar', '247': 'No protegido, Residencial Sin Desarrollo, Mejoramiento Integral', '248': 'No protegido, Residencial Sin Desarrollo, Desarrollo de areas urbanizadas',
            '249': 'No protegido, Residencial Sin Desarrollo, Desarrollo por urbanizaciones', '250': 'No protegido, Comercial, Sin Tratamiento', '251': 'No protegido, Comercial, Conservación', '252': 'No protegido, Comercial, Recuperación',
            '253': 'No protegido, Comercial, Conservacion Urbanistica', '254': 'No protegido, Comercial, Consolidacion', '256': 'No protegido, Comercial, Desarrollo de areas sin urbanizar', '257': 'No protegido, Comercial, Mejoramiento Integral', '258': 'No protegido, Comercial, Desarrollo de areas urbanizadas',
            '259': 'No protegido, Comercial, Desarrollo por urbanizaciones', '260': 'No protegido, Industrial, Sin Tratamiento', '261': 'No protegido, Industrial, Conservación', '262': 'No protegido, Industrial, Recuperación',
            '263': 'No protegido, Industrial, Conservacion Urbanistica', '264': 'No protegido, Industrial, Consolidacion', '266': 'No protegido, Industrial, Desarrollo de areas sin urbanizar', '267': 'No protegido, Industrial, Mejoramiento Integral', '268': 'No protegido, Industrial, Desarrollo de areas urbanizadas',
            '269': 'No protegido, Industrial, Desarrollo por urbanizaciones', '270': 'No protegido, Dotacional, Sin Tratamiento', '271': 'No protegido, Dotacional, Conservación', '272': 'No protegido, Dotacional, Recuperación',
            '273': 'No protegido, Dotacional, Conservacion Urbanistica', '274': 'No protegido, Dotacional, Consolidacion', '276': 'No protegido, Dotacional, Desarrollo de areas sin urbanizar', '277': 'No protegido, Dotacional, Mejoramiento Integral', '278': 'No protegido, Dotacional, Desarrollo de areas urbanizadas',
            '279': 'No protegido, Dotacional, Desarrollo por urbanizaciones', '280': 'No protegido, Senderos Ecologicos, Sin Tratamiento', '281': 'No protegido, Senderos Ecologicos, Conservación', '282': 'No protegido, Senderos Ecologicos, Recuperación',
            '283': 'No protegido, Senderos Ecologicos, Conservacion Urbanistica', '284': 'No protegido, Senderos Ecologicos, Consolidacion', '286': 'No protegido, Senderos Ecologicos, Desarrollo de areas sin urbanizar', '287': 'No protegido, Senderos Ecologicos, Mejoramiento Integral', '288': 'No protegido, Senderos Ecologicos, Desarrollo de areas urbanizadas',
            '289': 'No protegido, Senderos Ecologicos, Desarrollo por urbanizaciones'
        }, #FALTA ESTE
        'tipificacion_construccion': {
            'Residencial': {
                '1': 'Residencial_1_Bajo_Bajo',
                '2': 'Residencial_2_Bajo',
                '3': 'Residencial_3_Medio_Bajo',
                '4': 'Residencial_4_Medio',
                '5': 'Residencial_5_Medio_Alto',
                '6': 'Residencial_6_Alto',
                '0': ''
            },
            'Comercial': {
                '1': 'Comercial_Barrial',
                '2': 'Comercial_Sectorial',
                '0': ''
            }
    }},

    # ESPECIAL PORQUE TIPIFICACION CONSTRUCCION ES CONDICIONAL. OK PARA EJECUTAR
    '25040': {
        'orden_variables': ['topografia', 'influencia_vial', 'servicio_publico', 'uso_actual_suelo', 'norma_uso_suelo', 'tipificacion_construccion'],
        'longitudes': [1, 1, 1, 1, 3, 1],
        'topografia': {'1': 'Plano', '2': 'Inclinado', '3': 'Empinado'},
        'influencia_vial': {'1': 'Pavimentadas', '2': 'Sin_Pavimentar', '3': 'Peatonales', '4': 'Sin_Vias'},
        'servicio_publico': {'1': 'Sin_Servicios', '2': 'Básicos_Incompletos', '3': 'Básicos_Completos ', '4': 'Básicos_Y_Complementarios'},
        'uso_actual_suelo': {'1': 'Residencial', '2': 'Comercial', '3': 'Industrial', '4': 'Institucional', '5': 'Lote', '6': 'Espacio Publico'},
        'norma_uso_suelo': {
            '111': 'Protegido, Multiple, Desarrollo', '112': 'Protegido, Multiple, Consolidación', '113': 'Protegido, Multiple, Conservación', 
            '121': 'Protegido, Residencial, Desarrollo', '122': 'Protegido, Residencial, Consolidación', '123': 'Protegido, Residencial, Conservación', 
            '211': 'No protegido, Multiple, Desarrollo', '212': 'No protegido, Multiple, Consolidación', '213': 'No protegido, Multiple, Conservación', 
            '221': 'No protegido, Residencial, Desarrollo', '222': 'No protegido, Residencial, Consolidación', '223': 'No protegido, Residencial, Conservación'
            }, 
        'tipificacion_construccion': {
            'Residencial': {
                '1': 'Residencial_1_Bajo_Bajo',
                '2': 'Residencial_2_Bajo',
                '3': 'Residencial_3_Medio_Bajo',
                '4': 'Residencial_4_Medio',
                '5': 'Residencial_5_Medio_Alto',
                '6': 'Residencial_6_Alto',
                '0': ''
            },
            'Comercial': {
                '1': 'Comercial_Barrial',
                '2': 'Comercial_Sectorial',
                '0': ''
            }
    }},

    # ESPECIAL PORQUE TIPIFICACION CONSTRUCCION ES CONDICIONAL. OK PARA EJECUTAR
    '25599': {
        'orden_variables': ['topografia', 'influencia_vial', 'servicio_publico', 'uso_actual_suelo', 'norma_uso_suelo', 'tipificacion_construccion'],
        'longitudes': [1, 1, 1, 1, 3, 1],
        'topografia': {'1': 'Plano', '2': 'Inclinado', '3': 'Empinado'},
        'influencia_vial': {'1': 'Pavimentadas', '2': 'Sin_Pavimentar', '3': 'Peatonales', '4': 'Sin_Vias'},
        'servicio_publico': {'1': 'Sin_Servicios', '2': 'Básicos_Incompletos', '3': 'Básicos_Completos ', '4': 'Básicos_Y_Complementarios'},
        'uso_actual_suelo': {'1': 'Residencial', '2': 'Comercial', '3': 'Industrial', '4': 'Institucional', '5': 'Lote'},
        'norma_uso_suelo': {
            '110': 'Protegido, Multiple, Sin Tratamiento', '111': 'Protegido, Multiple, Desarrollo', '112': 'Protegido, Multiple, Consolidación', 
            '120': 'Protegido, Residencial, Sin Tratamiento', '121': 'Protegido, Residencial, Desarrollo', '122': 'Protegido, Residencial, Consolidación', 
            '170': 'Protegido, Zona de Protección, Sin Tratamiento', '171': 'Protegido, Zona de Protección, Desarrollo', '172': 'Protegido, Zona de Protección, Consolidación', 
            '210': 'No Protegido, Multiple, Sin Tratamiento', '211': 'No Protegido, Multiple, Desarrollo', '212': 'No Protegido, Multiple, Consolidación', 
            '220': 'No Protegido, Residencial, Sin Tratamiento', '221': 'No Protegido, Residencial, Desarrollo', '222': 'No Protegido, Residencial, Consolidación', 
            '270': 'No Protegido, Zona de Protección, Sin Tratamiento', '271': 'No Protegido, Zona de Protección, Desarrollo', '272': 'No Protegido, Zona de Protección, Consolidación'
            },
        'tipificacion_construccion': {
            'Residencial': {
                '1': 'Residencial_1_Bajo_Bajo',
                '2': 'Residencial_2_Bajo',
                '3': 'Residencial_3_Medio_Bajo',
                '4': 'Residencial_4_Medio',
                '5': 'Residencial_5_Medio_Alto',
                '6': 'Residencial_6_Alto',
                '0': ''
            },
            'Comercial': {
                '1': 'Comercial_Barrial',
                '2': 'Comercial_Sectorial',
                '0': ''
            }
    }},

    # ESPECIAL PORQUE TIPIFICACION CONSTRUCCION ES CONDICIONAL. OK PARA EJECUTAR
    '25269': {
        'orden_variables': ['topografia', 'influencia_vial', 'servicio_publico', 'uso_actual_suelo', 'norma_uso_suelo', 'tipificacion_construccion'],
        'longitudes': [1, 1, 1, 1, 3, 1],
        'topografia': {'1': 'Plano', '2': 'Inclinado', '3': 'Empinado'},
        'influencia_vial': {'1': 'Pavimentadas', '2': 'Sin_Pavimentar', '3': 'Peatonales', '4': 'Sin_Vias'},
        'servicio_publico': {'1': 'Sin_Servicios', '2': 'Básicos_Incompletos', '3': 'Básicos_Completos ', '4': 'Básicos_Y_Complementarios'},
        'uso_actual_suelo': {'1': 'Residencial', '2': 'Comercial', '3': 'Industrial', '4': 'Institucional', '5': 'Lote', '6': 'Espacio Publico'},
        'norma_uso_suelo': {
            '110': 'Protegido, Múltiple, Sin Tratamiento', '111': 'Protegido, Múltiple, Desarrollo', '112': 'Protegido, Múltiple',
            '113': 'Protegido, Múltiple, Conservación', '114': 'Protegido, Múltiple, Mejoramiento Integral', '115': 'Protegido, Múltiple, Renovación Urbana',
            '120': 'Protegido, Residencial Neto, Sin Tratamiento', '121': 'Protegido, Residencial Neto, Desarrollo', '122': 'Protegido, Residencial Neto, Consolidación',
            '123': 'Protegido, Residencial Neto, Conservación', '124': 'Protegido, Residencial Neto, Mejoramiento Integral', '125': 'Protegido, Residencial Neto, Renovación Urbana',
            '130': 'Protegido, Residencial Mixto, Sin Tratamiento', '131': 'Protegido, Residencial Mixto, Desarrollo', '132': 'Protegido, Residencial Mixto, Consolidación',
            '133': 'Protegido, Residencial Mixto, Conservación', '134': 'Protegido, Residencial Mixto, Mejoramiento Integral', '135': 'Protegido, Residencial Mixto, Renovación Urbana',
            '140': 'Protegido, Industrial, Sin Tratamiento', '141': 'Protegido, Industrial, Desarrollo', '142': 'Protegido, Industrial, Consolidación',
            '143': 'Protegido, Industrial, Conservación', '144': 'Protegido, Industrial, Mejoramiento Integral', '145': 'Protegido, Industrial, Renovación Urbana',
            '150': 'Protegido, Dotacional, Sin Tratamiento', '151': 'Protegido, Dotacional, Desarrollo', '152': 'Protegido, Dotacional, Consolidación',
            '153': 'Protegido, Dotacional, Conservación', '154': 'Protegido, Dotacional, Mejoramiento Integral', '155': 'Protegido, Dotacional, Renovación Urbana',
            '160': 'Protegido, Parques, Cultura, Recreación, Sin Tratamiento', '161': 'Protegido, Parques, Cultura, Recreación, Desarrollo', '162': 'Protegido, Parques, Cultura, Recreación, Consolidación',
            '163': 'Protegido, Parques, Cultura, Recreación, Conservación', '164': 'Protegido, Parques, Cultura, Recreación, Mejoramiento Integral', '165': 'Protegido, Parques, Cultura, Recreación, Renovación Urbana',
            '170': 'Protegido, Suelos de Protección, Sin Tratamiento', '171': 'Protegido, Suelos de Protección, Desarrollo', '172': 'Protegido, Suelos de Protección, Consolidación',
            '173': 'Protegido, Suelos de Protección, Conservación', '174': 'Protegido, Suelos de Protección, Mejoramiento Integral', '175': 'Protegido, Suelos de Protección, Renovación Urbana',
            '180': 'Protegido, Residencial Rural, Sin Tratamiento', '181': 'Protegido, Residencial Rural, Desarrollo', '182': 'Protegido, Residencial Rural, Consolidación',
            '183': 'Protegido, Residencial Rural, Conservación', '184': 'Protegido, Residencial Rural, Mejoramiento Integral', '185': 'Protegido, Residencial Rural, Renovación Urbana',
            '190': 'Protegido, Industrial Rural, Sin Tratamiento', '191': 'Protegido, Industrial Rural, Desarrollo', '192': 'Protegido, Industrial Rural, Consolidación',
            '193': 'Protegido, Industrial Rural, Conservación', '194': 'Protegido, Industrial Rural, Mejoramiento Integral', '195': 'Protegido, Industrial Rural, Renovación Urbana', 
            '210': 'No Protegido, Múltiple, Sin Tratamiento', '211': 'No Protegido, Múltiple, Desarrollo', '212': 'No Protegido, Múltiple, Consolidación',
            '213': 'No Protegido, Múltiple, Conservación', '214': 'No Protegido, Múltiple, Mejoramiento Integral', '215': 'No Protegido, Múltiple, Renovación Urbana',
            '220': 'No Protegido, Residencial Neto, Sin Tratamiento', '221': 'No Protegido, Residencial Neto, Desarrollo', '222': 'No Protegido, Residencial Neto, Consolidación',
            '223': 'No Protegido, Residencial Neto, Conservación', '224': 'No Protegido, Residencial Neto, Mejoramiento Integral', '225': 'No Protegido, Residencial Neto, Renovación Urbana',
            '230': 'No Protegido, Residencial Mixto, Sin Tratamiento', '231': 'No Protegido, Residencial Mixto, Desarrollo', '232': 'No Protegido, Residencial Mixto, Consolidación',
            '233': 'No Protegido, Residencial Mixto, Conservación', '234': 'No Protegido, Residencial Mixto, Mejoramiento Integral', '235': 'No Protegido, Residencial Mixto, Renovación Urbana',
            '240': 'No Protegido, Industrial, Sin Tratamiento', '241': 'No Protegido, Industrial, Desarrollo', '242': 'No Protegido, Industrial, Consolidación',
            '243': 'No Protegido, Industrial, Conservación', '244': 'No Protegido, Industrial, Mejoramiento Integral', '245': 'No Protegido, Industrial, Renovación Urbana',
            '250': 'No Protegido, Dotacional, Sin Tratamiento', '251': 'No Protegido, Dotacional, Desarrollo', '252': 'No Protegido, Dotacional, Consolidación',
            '253': 'No Protegido, Dotacional, Conservación', '254': 'No Protegido, Dotacional, Mejoramiento Integral', '255': 'No Protegido, Dotacional, Renovación Urbana',
            '260': 'No Protegido, Parques, Cultura, Recreación, Sin Tratamiento', '261': 'No Protegido, Parques, Cultura, Recreación, Desarrollo', '262': 'No Protegido, Parques, Cultura, Recreación, Consolidación',
            '263': 'No Protegido, Parques, Cultura, Recreación, Conservación', '264': 'No Protegido, Parques, Cultura, Recreación, Mejoramiento Integral', '265': 'No Protegido, Parques, Cultura, Recreación, Renovación Urbana',
            '270': 'No Protegido, Suelos de Protección, Sin Tratamiento', '271': 'No Protegido, Suelos de Protección, Desarrollo', '272': 'No Protegido, Suelos de Protección, Consolidación',
            '273': 'No Protegido, Suelos de Protección, Conservación', '274': 'No Protegido, Suelos de Protección, Mejoramiento Integral', '275': 'No Protegido, Suelos de Protección, Renovación Urbana',
            '280': 'No Protegido, Residencial Rural, Sin Tratamiento', '281': 'No Protegido, Residencial Rural, Desarrollo', '282': 'No Protegido, Residencial Rural, Consolidación',
            '283': 'No Protegido, Residencial Rural, Conservación', '284': 'No Protegido, Residencial Rural, Mejoramiento Integral', '285': 'No Protegido, Residencial Rural, Renovación Urbana',
            '290': 'No Protegido, Industrial Rural, Sin Tratamiento', '291': 'No Protegido, Industrial Rural, Desarrollo', '292': 'No Protegido, Industrial Rural, Consolidación',
            '293': 'No Protegido, Industrial Rural, Conservación', '294': 'No Protegido, Industrial Rural, Mejoramiento Integral', '295': 'No Protegido, Industrial Rural, Renovación Urbana',
            }, 
        'tipificacion_construccion': {
            'Residencial': {
                '1': 'Residencial_1_Bajo_Bajo',
                '2': 'Residencial_2_Bajo',
                '3': 'Residencial_3_Medio_Bajo',
                '4': 'Residencial_4_Medio',
                '5': 'Residencial_5_Medio_Alto',
                '6': 'Residencial_6_Alto',
                '0': ''
            },
            'Comercial': {
                '1': 'Comercial_Barrial',
                '2': 'Comercial_Sectorial',
                '0': ''
            }}
        },

    # ESPECIAL PORQUE TIPIFICACION CONSTRUCCION ES CONDICIONAL. OK PARA EJECUTAR MISMO QUE 25269
    '25326': {
        'orden_variables': ['topografia', 'influencia_vial', 'servicio_publico', 'uso_actual_suelo', 'norma_uso_suelo', 'tipificacion_construccion'],
        'longitudes': [1, 1, 1, 1, 3, 1],
        'topografia': {'1': 'Plano', '2': 'Inclinado', '3': 'Empinado'},
        'influencia_vial': {'1': 'Pavimentadas', '2': 'Sin_Pavimentar', '3': 'Peatonales', '4': 'Sin_Vias'},
        'servicio_publico': {'1': 'Sin_Servicios', '2': 'Básicos_Incompletos', '3': 'Básicos_Completos ', '4': 'Básicos_Y_Complementarios'},
        'uso_actual_suelo': {'1': 'Residencial', '2': 'Comercial', '3': 'Industrial', '4': 'Institucional', '5': 'Lote', '6': 'Espacio Publico'},
        'norma_uso_suelo': {
            '110': 'Protegido, Múltiple, Sin Tratamiento', '111': 'Protegido, Múltiple, Desarrollo', '112': 'Protegido, Múltiple',
            '113': 'Protegido, Múltiple, Conservación', '114': 'Protegido, Múltiple, Mejoramiento Integral', '115': 'Protegido, Múltiple, Renovación Urbana',
            '120': 'Protegido, Residencial Neto, Sin Tratamiento', '121': 'Protegido, Residencial Neto, Desarrollo', '122': 'Protegido, Residencial Neto, Consolidación',
            '123': 'Protegido, Residencial Neto, Conservación', '124': 'Protegido, Residencial Neto, Mejoramiento Integral', '125': 'Protegido, Residencial Neto, Renovación Urbana',
            '130': 'Protegido, Residencial Mixto, Sin Tratamiento', '131': 'Protegido, Residencial Mixto, Desarrollo', '132': 'Protegido, Residencial Mixto, Consolidación',
            '133': 'Protegido, Residencial Mixto, Conservación', '134': 'Protegido, Residencial Mixto, Mejoramiento Integral', '135': 'Protegido, Residencial Mixto, Renovación Urbana',
            '140': 'Protegido, Industrial, Sin Tratamiento', '141': 'Protegido, Industrial, Desarrollo', '142': 'Protegido, Industrial, Consolidación',
            '143': 'Protegido, Industrial, Conservación', '144': 'Protegido, Industrial, Mejoramiento Integral', '145': 'Protegido, Industrial, Renovación Urbana',
            '150': 'Protegido, Dotacional, Sin Tratamiento', '151': 'Protegido, Dotacional, Desarrollo', '152': 'Protegido, Dotacional, Consolidación',
            '153': 'Protegido, Dotacional, Conservación', '154': 'Protegido, Dotacional, Mejoramiento Integral', '155': 'Protegido, Dotacional, Renovación Urbana',
            '160': 'Protegido, Parques, Cultura, Recreación, Sin Tratamiento', '161': 'Protegido, Parques, Cultura, Recreación, Desarrollo', '162': 'Protegido, Parques, Cultura, Recreación, Consolidación',
            '163': 'Protegido, Parques, Cultura, Recreación, Conservación', '164': 'Protegido, Parques, Cultura, Recreación, Mejoramiento Integral', '165': 'Protegido, Parques, Cultura, Recreación, Renovación Urbana',
            '170': 'Protegido, Suelos de Protección, Sin Tratamiento', '171': 'Protegido, Suelos de Protección, Desarrollo', '172': 'Protegido, Suelos de Protección, Consolidación',
            '173': 'Protegido, Suelos de Protección, Conservación', '174': 'Protegido, Suelos de Protección, Mejoramiento Integral', '175': 'Protegido, Suelos de Protección, Renovación Urbana',
            '180': 'Protegido, Residencial Rural, Sin Tratamiento', '181': 'Protegido, Residencial Rural, Desarrollo', '182': 'Protegido, Residencial Rural, Consolidación',
            '183': 'Protegido, Residencial Rural, Conservación', '184': 'Protegido, Residencial Rural, Mejoramiento Integral', '185': 'Protegido, Residencial Rural, Renovación Urbana',
            '190': 'Protegido, Industrial Rural, Sin Tratamiento', '191': 'Protegido, Industrial Rural, Desarrollo', '192': 'Protegido, Industrial Rural, Consolidación',
            '193': 'Protegido, Industrial Rural, Conservación', '194': 'Protegido, Industrial Rural, Mejoramiento Integral', '195': 'Protegido, Industrial Rural, Renovación Urbana', 
            '210': 'No Protegido, Múltiple, Sin Tratamiento', '211': 'No Protegido, Múltiple, Desarrollo', '212': 'No Protegido, Múltiple, Consolidación',
            '213': 'No Protegido, Múltiple, Conservación', '214': 'No Protegido, Múltiple, Mejoramiento Integral', '215': 'No Protegido, Múltiple, Renovación Urbana',
            '220': 'No Protegido, Residencial Neto, Sin Tratamiento', '221': 'No Protegido, Residencial Neto, Desarrollo', '222': 'No Protegido, Residencial Neto, Consolidación',
            '223': 'No Protegido, Residencial Neto, Conservación', '224': 'No Protegido, Residencial Neto, Mejoramiento Integral', '225': 'No Protegido, Residencial Neto, Renovación Urbana',
            '230': 'No Protegido, Residencial Mixto, Sin Tratamiento', '231': 'No Protegido, Residencial Mixto, Desarrollo', '232': 'No Protegido, Residencial Mixto, Consolidación',
            '233': 'No Protegido, Residencial Mixto, Conservación', '234': 'No Protegido, Residencial Mixto, Mejoramiento Integral', '235': 'No Protegido, Residencial Mixto, Renovación Urbana',
            '240': 'No Protegido, Industrial, Sin Tratamiento', '241': 'No Protegido, Industrial, Desarrollo', '242': 'No Protegido, Industrial, Consolidación',
            '243': 'No Protegido, Industrial, Conservación', '244': 'No Protegido, Industrial, Mejoramiento Integral', '245': 'No Protegido, Industrial, Renovación Urbana',
            '250': 'No Protegido, Dotacional, Sin Tratamiento', '251': 'No Protegido, Dotacional, Desarrollo', '252': 'No Protegido, Dotacional, Consolidación',
            '253': 'No Protegido, Dotacional, Conservación', '254': 'No Protegido, Dotacional, Mejoramiento Integral', '255': 'No Protegido, Dotacional, Renovación Urbana',
            '260': 'No Protegido, Parques, Cultura, Recreación, Sin Tratamiento', '261': 'No Protegido, Parques, Cultura, Recreación, Desarrollo', '262': 'No Protegido, Parques, Cultura, Recreación, Consolidación',
            '263': 'No Protegido, Parques, Cultura, Recreación, Conservación', '264': 'No Protegido, Parques, Cultura, Recreación, Mejoramiento Integral', '265': 'No Protegido, Parques, Cultura, Recreación, Renovación Urbana',
            '270': 'No Protegido, Suelos de Protección, Sin Tratamiento', '271': 'No Protegido, Suelos de Protección, Desarrollo', '272': 'No Protegido, Suelos de Protección, Consolidación',
            '273': 'No Protegido, Suelos de Protección, Conservación', '274': 'No Protegido, Suelos de Protección, Mejoramiento Integral', '275': 'No Protegido, Suelos de Protección, Renovación Urbana',
            '280': 'No Protegido, Residencial Rural, Sin Tratamiento', '281': 'No Protegido, Residencial Rural, Desarrollo', '282': 'No Protegido, Residencial Rural, Consolidación',
            '283': 'No Protegido, Residencial Rural, Conservación', '284': 'No Protegido, Residencial Rural, Mejoramiento Integral', '285': 'No Protegido, Residencial Rural, Renovación Urbana',
            '290': 'No Protegido, Industrial Rural, Sin Tratamiento', '291': 'No Protegido, Industrial Rural, Desarrollo', '292': 'No Protegido, Industrial Rural, Consolidación',
            '293': 'No Protegido, Industrial Rural, Conservación', '294': 'No Protegido, Industrial Rural, Mejoramiento Integral', '295': 'No Protegido, Industrial Rural, Renovación Urbana',
            }, 
        'tipificacion_construccion': {
            'Residencial': {
                '1': 'Residencial_1_Bajo_Bajo',
                '2': 'Residencial_2_Bajo',
                '3': 'Residencial_3_Medio_Bajo',
                '4': 'Residencial_4_Medio',
                '5': 'Residencial_5_Medio_Alto',
                '6': 'Residencial_6_Alto',
                '0': ''
            },
            'Comercial': {
                '1': 'Comercial_Barrial',
                '2': 'Comercial_Sectorial',
                '0': ''
            }}
        },
# ESPECIAL PORQUE TIPIFICACION CONSTRUCCION ES CONDICIONAL. OK PARA EJECUTAR
    '25596': {
        'orden_variables': ['topografia', 'influencia_vial', 'servicio_publico', 'uso_actual_suelo', 'norma_uso_suelo', 'tipificacion_construccion'],
        'longitudes': [1, 1, 1, 1, 3, 1],
        'topografia': {'1': 'Plano', '2': 'Inclinado', '3': 'Empinado'},
        'influencia_vial': {'1': 'Pavimentadas', '2': 'Sin_Pavimentar', '3': 'Peatonales', '4': 'Sin_Vias'},
        'servicio_publico': {'1': 'Sin_Servicios', '2': 'Básicos_Incompletos', '3': 'Básicos_Completos ', '4': 'Básicos_Y_Complementarios'},
        'uso_actual_suelo': {'1': 'Residencial', '2': 'Comercial', '3': 'Industrial', '4': 'Institucional', '5': 'Lote'},
        'norma_uso_suelo': {
            '200': 'No Protegido, Sin Norma, Sin Norma'},
        'tipificacion_construccion': {
            'Residencial': {
                '1': 'Residencial_1_Bajo_Bajo',
                '2': 'Residencial_2_Bajo',
                '3': 'Residencial_3_Medio_Bajo',
                '4': 'Residencial_4_Medio',
                '5': 'Residencial_5_Medio_Alto',
                '6': 'Residencial_6_Alto',
                '0': ''
            },
            'Comercial': {
                '1': 'Comercial_Barrial',
                '2': 'Comercial_Sectorial',
                '0': ''
            }
    }},

# ESPECIAL PORQUE TIPIFICACION CONSTRUCCION ES CONDICIONAL. REVISAR NO TIENE CODIGO DE ZHF
    '25645': {
        'orden_variables': ['topografia', 'influencia_vial', 'servicio_publico', 'uso_actual_suelo', 'norma_uso_suelo', 'tipificacion_construccion'],
        'longitudes': [1, 1, 1, 1, 3, 1],
        'topografia': {'1': 'Plano', '2': 'Inclinado', '3': 'Empinado'},
        'influencia_vial': {'1': 'Pavimentadas', '2': 'Sin_Pavimentar', '3': 'Peatonales', '4': 'Sin_Vias'},
        'servicio_publico': {'1': 'Sin_Servicios', '2': 'Básicos_Incompletos', '3': 'Básicos_Completos ', '4': 'Básicos_Y_Complementarios'},
        'uso_actual_suelo': {'1': 'Residencial', '2': 'Comercial', '3': 'Industrial', '4': 'Institucional', '5': 'Lote'},
        'norma_uso_suelo': {
            '100': 'Protegido, Sin área de actividad, Sin Tratamiento', '101': 'Protegido, Sin área de actividad, Desarrollo',
            '102': 'Protegido, Sin área de actividad, Consolidación', '103': 'Protegido, Sin área de actividad, Mejoramiento', 
            '120': 'Protegido, Residencial, Sin Tratamiento', '121': 'Protegido, Residencial, Desarrollo',
            '122': 'Protegido, Residencial, Consolidación', '123': 'Protegido, Residencial, Mejoramiento',
            '130': 'Protegido, Comercio y servicios, Sin Tratamiento', '131': 'Protegido, Comercio y servicios, Desarrollo',
            '132': 'Protegido, Comercio y servicios, Consolidación', '133': 'Protegido, Comercio y servicios, Mejoramiento',
            '150': 'Protegido, Dotacional, Sin Tratamiento', '151': 'Protegido, Dotacional, Desarrollo',
            '152': 'Protegido, Dotacional, Consolidación', '153': 'Protegido, Dotacional, Mejoramiento',
            '200': 'No protegido, Sin área de actividad, Sin Tratamiento', '201': 'No protegido, Sin área de actividad, Desarrollo',
            '202': 'No protegido, Sin área de actividad, Consolidación', '203': 'No protegido, Sin área de actividad, Mejoramiento',
            '220': 'No protegido, Residencial, Sin Tratamiento', '221': 'No protegido, Residencial, Desarrollo',
            '222': 'No protegido, Residencial, Consolidación', '223': 'No protegido, Residencial, Mejoramiento',
            '230': 'No protegido, Comercio y servicios, Sin Tratamiento', '231': 'No protegido, Comercio y servicios, Desarrollo',
            '232': 'No protegido, Comercio y servicios, Consolidación', '233': 'No protegido, Comercio y servicios, Mejoramiento',
            '250': 'No protegido, Dotacional, Sin Tratamiento', '251': 'No protegido, Dotacional, Desarrollo',
            '252': 'No protegido, Dotacional, Consolidación', '253': 'No protegido, Dotacional, Mejoramiento'
        },
        'tipificacion_construccion': {
            'Residencial': {
                '1': 'Residencial_1_Bajo_Bajo',
                '2': 'Residencial_2_Bajo',
                '3': 'Residencial_3_Medio_Bajo',
                '4': 'Residencial_4_Medio',
                '5': 'Residencial_5_Medio_Alto',
                '6': 'Residencial_6_Alto',
                '0': ''
            },
            'Comercial': {
                '1': 'Comercial_Barrial',
                '2': 'Comercial_Sectorial',
                '0': ''
            }
    }},
# ESPECIAL PORQUE TIPIFICACION CONSTRUCCION ES CONDICIONAL. OK PARA EJECUTAR
    '25769': {
        'orden_variables': ['topografia', 'influencia_vial', 'servicio_publico', 'uso_actual_suelo', 'norma_uso_suelo', 'tipificacion_construccion'],
        'longitudes': [1, 1, 1, 1, 3, 1],
        'topografia': {'1': 'Plano', '2': 'Inclinado', '3': 'Empinado'},
        'influencia_vial': {'1': 'Pavimentadas', '2': 'Sin_Pavimentar', '3': 'Peatonales', '4': 'Sin_Vias'},
        'servicio_publico': {'1': 'Sin_Servicios', '2': 'Básicos_Incompletos', '3': 'Básicos_Completos ', '4': 'Básicos_Y_Complementarios'},
        'uso_actual_suelo': {'1': 'Residencial', '2': 'Comercial', '3': 'Industrial', '4': 'Institucional', '5': 'Lote', '6': 'Vias', '7': 'Espacio Publico'},
        'norma_uso_suelo': {
            '111': 'Protegido, Residencial, Desarrollo', '112': 'Protegido, Residencial, Consolidación', '113': 'Protegido, Residencial, Mejoramiento Integral', '114': 'Protegido, Residencial, Conjunto Urbano',
            '115': 'Protegido, Residencial, Histórico Paisajística', '116': 'Protegido, Residencial, Recursos Naturales', '117': 'Protegido, Residencial, Institucional', '118': 'Protegido, Residencial, Cementerio',
            '121': 'Protegido, Múltiple Central, Desarrollo', '122': 'Protegido, Múltiple Central, Consolidación', '123': 'Protegido, Múltiple Central, Mejoramiento Integral', '124': 'Protegido, Múltiple Central, Conjunto Urbano',
            '125': 'Protegido, Múltiple Central, Histórico Paisajística', '126': 'Protegido, Múltiple Central, Recursos Naturales', '127': 'Protegido, Múltiple Central, Institucional', '128': 'Protegido, Múltiple Central, Cementerio',
            '131': 'Protegido, Múltiple Periférica, Desarrollo', '132': 'Protegido, Múltiple Periférica, Consolidación', '133': 'Protegido, Múltiple Periférica, Mejoramiento Integral', '134': 'Protegido, Múltiple Periférica, Conjunto Urbano',
            '135': 'Protegido, Múltiple Periférica, Histórico Paisajística', '136': 'Protegido, Múltiple Periférica, Recursos Naturales', '137': 'Protegido, Múltiple Periférica, Institucional', '138': 'Protegido, Múltiple Periférica, Cementerio',
            '141': 'Protegido, Especializada, Desarrollo', '142': 'Protegido, Especializada, Consolidación', '143': 'Protegido, Especializada, Mejoramiento Integral', '144': 'Protegido, Especializada, Conjunto Urbano',
            '145': 'Protegido, Especializada, Histórico Paisajística', '146': 'Protegido, Especializada, Recursos Naturales', '147': 'Protegido, Especializada, Institucional', '148': 'Protegido, Especializada, Cementerio',
            '151': 'Protegido, Conservación y Protección, Desarrollo', '152': 'Protegido, Conservación y Protección, Consolidación', '153': 'Protegido, Conservación y Protección, Mejoramiento Integral', '154': 'Protegido, Conservación y Protección, Conjunto Urbano',
            '155': 'Protegido, Conservación y Protección, Histórico Paisajística', '156': 'Protegido, Conservación y Protección, Recursos Naturales', '157': 'Protegido, Conservación y Protección, Institucional', '158': 'Protegido, Conservación y Protección, Cementerio',
            '211': 'No protegido, Residencial, Desarrollo', '212': 'No protegido, Residencial, Consolidación', '213': 'No protegido, Residencial, Mejoramiento Integral', '214': 'No protegido, Residencial, Conjunto Urbano',
            '215': 'No protegido, Residencial, Histórico Paisajística', '216': 'No protegido, Residencial, Recursos Naturales', '217': 'No protegido, Residencial, Institucional', '218': 'No protegido, Residencial, Cementerio',
            '221': 'No protegido, Múltiple Central, Desarrollo', '222': 'No protegido, Múltiple Central, Consolidación', '223': 'No protegido, Múltiple Central, Mejoramiento Integral', '224': 'No protegido, Múltiple Central, Conjunto Urbano',
            '225': 'No protegido, Múltiple Central, Histórico Paisajística', '226': 'No protegido, Múltiple Central, Recursos Naturales', '227': 'No protegido, Múltiple Central, Institucional', '228': 'No protegido, Múltiple Central, Cementerio',
            '231': 'No protegido, Múltiple Periférica, Desarrollo', '232': 'No protegido, Múltiple Periférica, Consolidación', '233': 'No protegido, Múltiple Periférica, Mejoramiento Integral', '234': 'No protegido, Múltiple Periférica, Conjunto Urbano',
            '235': 'No protegido, Múltiple Periférica, Histórico Paisajística', '236': 'No protegido, Múltiple Periférica, Recursos Naturales', '237': 'No protegido, Múltiple Periférica, Institucional', '238': 'No protegido, Múltiple Periférica, Cementerio',
            '241': 'No protegido, Especializada, Desarrollo', '242': 'No protegido, Especializada, Consolidación', '243': 'No protegido, Especializada, Mejoramiento Integral', '244': 'No protegido, Especializada, Conjunto Urbano',
            '245': 'No protegido, Especializada, Histórico Paisajística', '246': 'No protegido, Especializada, Recursos Naturales', '247': 'No protegido, Especializada, Institucional', '248': 'No protegido, Especializada, Cementerio',
            '251': 'No protegido, Conservación y Protección, Desarrollo', '252': 'No protegido, Conservación y Protección, Consolidación', '253': 'No protegido, Conservación y Protección, Mejoramiento Integral', '254': 'No protegido, Conservación y Protección, Conjunto Urbano',
            '255': 'No protegido, Conservación y Protección, Histórico Paisajística', '256': 'No protegido, Conservación y Protección, Recursos Naturales', '257': 'No protegido, Conservación y Protección, Institucional', '258': 'No protegido, Conservación y Protección, Cementerio'
        },
        'tipificacion_construccion': {
            'Residencial': {
                '1': 'Residencial_1_Bajo_Bajo',
                '2': 'Residencial_2_Bajo',
                '3': 'Residencial_3_Medio_Bajo',
                '4': 'Residencial_4_Medio',
                '5': 'Residencial_5_Medio_Alto',
                '6': 'Residencial_6_Alto',
                '0': ''
            },
            'Comercial': {
                '1': 'Comercial_Barrial',
                '2': 'Comercial_Sectorial',
                '0': ''
            }
    }},
# ESPECIAL PORQUE TIPIFICACION CONSTRUCCION ES CONDICIONAL. OK PARA EJECUTAR
    '25805': {
        'orden_variables': ['topografia', 'influencia_vial', 'servicio_publico', 'uso_actual_suelo', 'norma_uso_suelo', 'tipificacion_construccion'],
        'longitudes': [1, 1, 1, 1, 3, 1],
        'topografia': {'1': 'Plano', '2': 'Inclinado', '3': 'Empinado'},
        'influencia_vial': {'1': 'Pavimentadas', '2': 'Sin_Pavimentar', '3': 'Peatonales', '4': 'Sin_Vias'},
        'servicio_publico': {'1': 'Sin_Servicios', '2': 'Básicos_Incompletos', '3': 'Básicos_Completos ', '4': 'Básicos_Y_Complementarios'},
        'uso_actual_suelo': {'1': 'Residencial', '2': 'Comercial', '3': 'Industrial', '4': 'Institucional', '5': 'Lote'},
        'norma_uso_suelo': {
            '110': 'Protegido, Múltiple, Sin Tratamiento', '111': 'Protegido, Múltiple, Desarrollo', '112': 'Protegido, Múltiple, Consolidación', '113': 'Protegido, Múltiple, Conservación',
            '114': 'Protegido, Múltiple, Mejoramiento integral', '115': 'Protegido, Múltiple, Renovación urbana', '120': 'Protegido, Residencial Neto, Sin Tratamiento', '121': 'Protegido, Residencial Neto, Desarrollo',
            '122': 'Protegido, Residencial Neto, Consolidación', '123': 'Protegido, Residencial Neto, Conservación', '124': 'Protegido, Residencial Neto, Mejoramiento integral', '125': 'Protegido, Residencial Neto, Renovación urbana',
            '130': 'Protegido, Residencial Mixto, Sin Tratamiento', '131': 'Protegido, Residencial Mixto, Desarrollo', '132': 'Protegido, Residencial Mixto, Consolidación', '133': 'Protegido, Residencial Mixto, Conservación',
            '134': 'Protegido, Residencial Mixto, Mejoramiento integral', '135': 'Protegido, Residencial Mixto, Renovación urbana', '140': 'Protegido, Industrial, Sin Tratamiento', '141': 'Protegido, Industrial, Desarrollo',
            '142': 'Protegido, Industrial, Consolidación', '143': 'Protegido, Industrial, Conservación', '144': 'Protegido, Industrial, Mejoramiento integral', '145': 'Protegido, Industrial, Renovación urbana',
            '150': 'Protegido, Dotacional, Sin Tratamiento', '151': 'Protegido, Dotacional, Desarrollo', '152': 'Protegido, Dotacional, Consolidación', '153': 'Protegido, Dotacional, Conservación',
            '154': 'Protegido, Dotacional, Mejoramiento integral', '155': 'Protegido, Dotacional, Renovación urbana', '160': 'Protegido, Parques, cultura, recreación, Sin Tratamiento', '161': 'Protegido, Parques, cultura, recreación, Desarrollo',
            '162': 'Protegido, Parques, cultura, recreación, Consolidación', '163': 'Protegido, Parques, cultura, recreación, Conservación', '164': 'Protegido, Parques, cultura, recreación, Mejoramiento integral', '165': 'Protegido, Parques, cultura, recreación, Renovación urbana',
            '170': 'Protegido, Suelos de protección, Sin Tratamiento', '171': 'Protegido, Suelos de protección, Desarrollo', '172': 'Protegido, Suelos de protección, Consolidación', '173': 'Protegido, Suelos de protección, Conservación',
            '174': 'Protegido, Suelos de protección, Mejoramiento integral', '175': 'Protegido, Suelos de protección, Renovación urbana', '180': 'Protegido, Residencial Rural, Sin Tratamiento', '181': 'Protegido, Residencial Rural, Desarrollo',
            '182': 'Protegido, Residencial Rural, Consolidación', '183': 'Protegido, Residencial Rural, Conservación', '184': 'Protegido, Residencial Rural, Mejoramiento integral', '185': 'Protegido, Residencial Rural, Renovación urbana',
            '190': 'Protegido, Industrial Rural, Sin Tratamiento', '191': 'Protegido, Industrial Rural, Desarrollo', '192': 'Protegido, Industrial Rural, Consolidación', '193': 'Protegido, Industrial Rural, Conservación',
            '194': 'Protegido, Industrial Rural, Mejoramiento integral', '195': 'Protegido, Industrial Rural, Renovación urbana', '210': 'No protegido, Múltiple, Sin Tratamiento', '211': 'No protegido, Múltiple, Desarrollo',
            '212': 'No protegido, Múltiple, Consolidación', '213': 'No protegido, Múltiple, Conservación', '214': 'No protegido, Múltiple, Mejoramiento integral', '215': 'No protegido, Múltiple, Renovación urbana',
            '220': 'No protegido, Residencial Neto, Sin Tratamiento', '221': 'No protegido, Residencial Neto, Desarrollo', '222': 'No protegido, Residencial Neto, Consolidación', '223': 'No protegido, Residencial Neto, Conservación',
            '224': 'No protegido, Residencial Neto, Mejoramiento integral', '225': 'No protegido, Residencial Neto, Renovación urbana', '230': 'No protegido, Residencial Mixto, Sin Tratamiento', '231': 'No protegido, Residencial Mixto, Desarrollo',
            '232': 'No protegido, Residencial Mixto, Consolidación', '233': 'No protegido, Residencial Mixto, Conservación', '234': 'No protegido, Residencial Mixto, Mejoramiento integral', '235': 'No protegido, Residencial Mixto, Renovación urbana',
            '240': 'No protegido, Industrial, Sin Tratamiento', '241': 'No protegido, Industrial, Desarrollo', '242': 'No protegido, Industrial, Consolidación', '243': 'No protegido, Industrial, Conservación',
            '244': 'No protegido, Industrial, Mejoramiento integral', '245': 'No protegido, Industrial, Renovación urbana', '250': 'No protegido, Dotacional, Sin Tratamiento', '251': 'No protegido, Dotacional, Desarrollo',
            '252': 'No protegido, Dotacional, Consolidación', '253': 'No protegido, Dotacional, Conservación', '254': 'No protegido, Dotacional, Mejoramiento integral', '255': 'No protegido, Dotacional, Renovación urbana',
            '260': 'No protegido, Parques, cultura, recreación, Sin Tratamiento', '261': 'No protegido, Parques, cultura, recreación, Desarrollo', '262': 'No protegido, Parques, cultura, recreación, Consolidación', '263': 'No protegido, Parques, cultura, recreación, Conservación',
            '264': 'No protegido, Parques, cultura, recreación, Mejoramiento integral', '265': 'No protegido, Parques, cultura, recreación, Renovación urbana', '270': 'No protegido, Suelos de protección, Sin Tratamiento', '271': 'No protegido, Suelos de protección, Desarrollo',
            '272': 'No protegido, Suelos de protección, Consolidación', '273': 'No protegido, Suelos de protección, Conservación', '274': 'No protegido, Suelos de protección, Mejoramiento integral', '275': 'No protegido, Suelos de protección, Renovación urbana',
            '280': 'No protegido, Residencial Rural, Sin Tratamiento', '281': 'No protegido, Residencial Rural, Desarrollo', '282': 'No protegido, Residencial Rural, Consolidación', '283': 'No protegido, Residencial Rural, Conservación',
            '284': 'No protegido, Residencial Rural, Mejoramiento integral', '285': 'No protegido, Residencial Rural, Renovación urbana', '290': 'No protegido, Industrial Rural, Sin Tratamiento', '291': 'No protegido, Industrial Rural, Desarrollo',
            '292': 'No protegido, Industrial Rural, Consolidación', '293': 'No protegido, Industrial Rural, Conservación', '294': 'No protegido, Industrial Rural, Mejoramiento integral', '295': 'No protegido, Industrial Rural, Renovación urbana'
        },
        'tipificacion_construccion': {
            'Residencial': {
                '1': 'Residencial_1_Bajo_Bajo',
                '2': 'Residencial_2_Bajo',
                '3': 'Residencial_3_Medio_Bajo',
                '4': 'Residencial_4_Medio',
                '5': 'Residencial_5_Medio_Alto',
                '6': 'Residencial_6_Alto',
                '0': ''
            },
            'Comercial': {
                '1': 'Comercial_Barrial',
                '2': 'Comercial_Sectorial',
                '0': ''
            }
    }},
# ESPECIAL PORQUE TIPIFICACION CONSTRUCCION ES CONDICIONAL. OK PARA EJECUTAR
    '25807': {
        'orden_variables': ['topografia', 'influencia_vial', 'servicio_publico', 'uso_actual_suelo', 'norma_uso_suelo', 'tipificacion_construccion'],
        'longitudes': [1, 1, 1, 1, 3, 1],
        'topografia': {'1': 'Plano', '2': 'Inclinado', '3': 'Empinado'},
        'influencia_vial': {'1': 'Pavimentadas', '2': 'Sin_Pavimentar', '3': 'Peatonales', '4': 'Sin_Vias'},
        'servicio_publico': {'1': 'Sin_Servicios', '2': 'Básicos_Incompletos', '3': 'Básicos_Completos ', '4': 'Básicos_Y_Complementarios'},
        'uso_actual_suelo': {'1': 'Residencial', '2': 'Comercial', '3': 'Industrial', '4': 'Institucional', '5': 'Lote'},
        'norma_uso_suelo': {
            '110': 'Protegido, Comercial Mixta, Sin Tratamiento', '111': 'Protegido, Comercial Mixta, Tratamiento Básico', '112': 'Protegido, Comercial Mixta, Consolidación Urbanística', '113': 'Protegido, Comercial Mixta, Conservación Arquitectónica',
            '114': 'Protegido, Comercial Mixta, Vivienda de Interés Social', '115': 'Protegido, Comercial Mixta, Zonas Verdes y Espacio Público', '120': 'Protegido, Conservación Arquitectónica, Sin Tratamiento', '121': 'Protegido, Conservación Arquitectónica, Tratamiento Básico',
            '122': 'Protegido, Conservación Arquitectónica, Consolidación Urbanística', '123': 'Protegido, Conservación Arquitectónica, Conservación Arquitectónica', '124': 'Protegido, Conservación Arquitectónica, Vivienda de Interés Social', '125': 'Protegido, Conservación Arquitectónica, Zonas Verdes y Espacio Público',
            '130': 'Protegido, Institucional Administrativo, Sin Tratamiento', '131': 'Protegido, Institucional Administrativo, Tratamiento Básico', '132': 'Protegido, Institucional Administrativo, Consolidación Urbanística', '133': 'Protegido, Institucional Administrativo, Conservación Arquitectónica',
            '134': 'Protegido, Institucional Administrativo, Vivienda de Interés Social', '135': 'Protegido, Institucional Administrativo, Zonas Verdes y Espacio Público', '140': 'Protegido, Institucional Educativo, Sin Tratamiento', '141': 'Protegido, Institucional Educativo, Tratamiento Básico',
            '142': 'Protegido, Institucional Educativo, Consolidación Urbanística', '143': 'Protegido, Institucional Educativo, Conservación Arquitectónica', '144': 'Protegido, Institucional Educativo, Vivienda de Interés Social', '145': 'Protegido, Institucional Educativo, Zonas Verdes y Espacio Público',
            '150': 'Protegido, Institucional Especial, Sin Tratamiento', '151': 'Protegido, Institucional Especial, Tratamiento Básico', '152': 'Protegido, Institucional Especial, Consolidación Urbanística', '153': 'Protegido, Institucional Especial, Conservación Arquitectónica',
            '154': 'Protegido, Institucional Especial, Vivienda de Interés Social', '155': 'Protegido, Institucional Especial, Zonas Verdes y Espacio Público', '160': 'Protegido, Residencial Con Servicios, Sin Tratamiento', '161': 'Protegido, Residencial Con Servicios, Tratamiento Básico',
            '162': 'Protegido, Residencial Con Servicios, Consolidación Urbanística', '163': 'Protegido, Residencial Con Servicios, Conservación Arquitectónica', '164': 'Protegido, Residencial Con Servicios, Vivienda de Interés Social', '165': 'Protegido, Residencial Con Servicios, Zonas Verdes y Espacio Público',
            '170': 'Protegido, Residencial Neto, Sin Tratamiento', '171': 'Protegido, Residencial Neto, Tratamiento Básico', '172': 'Protegido, Residencial Neto, Consolidación Urbanística', '173': 'Protegido, Residencial Neto, Conservación Arquitectónica',
            '174': 'Protegido, Residencial Neto, Vivienda de Interés Social', '175': 'Protegido, Residencial Neto, Zonas Verdes y Espacio Público', '180': 'Protegido, Vivienda Interés Social, Sin Tratamiento', '181': 'Protegido, Vivienda Interés Social, Tratamiento Básico',
            '182': 'Protegido, Vivienda Interés Social, Consolidación Urbanística', '183': 'Protegido, Vivienda Interés Social, Conservación Arquitectónica', '184': 'Protegido, Vivienda Interés Social, Vivienda de Interés Social', '185': 'Protegido, Vivienda Interés Social, Zonas Verdes y Espacio Público',
            '190': 'Protegido, Zonas Verdes y Espacio Publico, Sin Tratamiento', '191': 'Protegido, Zonas Verdes y Espacio Publico, Tratamiento Básico', '192': 'Protegido, Zonas Verdes y Espacio Publico, Consolidación Urbanística', '193': 'Protegido, Zonas Verdes y Espacio Publico, Conservación Arquitectónica',
            '194': 'Protegido, Zonas Verdes y Espacio Publico, Vivienda de Interés Social', '195': 'Protegido, Zonas Verdes y Espacio Publico, Zonas Verdes y Espacio Público', '210': 'No protegido, Comercial Mixta, Sin Tratamiento', '211': 'No protegido, Comercial Mixta, Tratamiento Básico',
            '212': 'No protegido, Comercial Mixta, Consolidación Urbanística', '213': 'No protegido, Comercial Mixta, Conservación Arquitectónica', '214': 'No protegido, Comercial Mixta, Vivienda de Interés Social', '215': 'No protegido, Comercial Mixta, Zonas Verdes y Espacio Público',
            '220': 'No protegido, Conservación Arquitectónica, Sin Tratamiento', '221': 'No protegido, Conservación Arquitectónica, Tratamiento Básico', '222': 'No protegido, Conservación Arquitectónica, Consolidación Urbanística', '223': 'No protegido, Conservación Arquitectónica, Conservación Arquitectónica',
            '224': 'No protegido, Conservación Arquitectónica, Vivienda de Interés Social', '225': 'No protegido, Conservación Arquitectónica, Zonas Verdes y Espacio Público', '230': 'No protegido, Institucional Administrativo, Sin Tratamiento', '231': 'No protegido, Institucional Administrativo, Tratamiento Básico',
            '232': 'No protegido, Institucional Administrativo, Consolidación Urbanística', '233': 'No protegido, Institucional Administrativo, Conservación Arquitectónica', '234': 'No protegido, Institucional Administrativo, Vivienda de Interés Social', '235': 'No protegido, Institucional Administrativo, Zonas Verdes y Espacio Público',
            '240': 'No protegido, Institucional Educativo, Sin Tratamiento', '241': 'No protegido, Institucional Educativo, Tratamiento Básico', '242': 'No protegido, Institucional Educativo, Consolidación Urbanística', '243': 'No protegido, Institucional Educativo, Conservación Arquitectónica',
            '244': 'No protegido, Institucional Educativo, Vivienda de Interés Social', '245': 'No protegido, Institucional Educativo, Zonas Verdes y Espacio Público', '250': 'No protegido, Institucional Especial, Sin Tratamiento', '251': 'No protegido, Institucional Especial, Tratamiento Básico',
            '252': 'No protegido, Institucional Especial, Consolidación Urbanística', '253': 'No protegido, Institucional Especial, Conservación Arquitectónica', '254': 'No protegido, Institucional Especial, Vivienda de Interés Social', '255': 'No protegido, Institucional Especial, Zonas Verdes y Espacio Público',
            '260': 'No protegido, Residencial Con Servicios, Sin Tratamiento', '261': 'No protegido, Residencial Con Servicios, Tratamiento Básico', '262': 'No protegido, Residencial Con Servicios, Consolidación Urbanística', '263': 'No protegido, Residencial Con Servicios, Conservación Arquitectónica',
            '264': 'No protegido, Residencial Con Servicios, Vivienda de Interés Social', '265': 'No protegido, Residencial Con Servicios, Zonas Verdes y Espacio Público', '270': 'No protegido, Residencial Neto, Sin Tratamiento', '271': 'No protegido, Residencial Neto, Tratamiento Básico',
            '272': 'No protegido, Residencial Neto, Consolidación Urbanística', '273': 'No protegido, Residencial Neto, Conservación Arquitectónica', '274': 'No protegido, Residencial Neto, Vivienda de Interés Social', '275': 'No protegido, Residencial Neto, Zonas Verdes y Espacio Público',
            '280': 'No protegido, Vivienda Interés Social, Sin Tratamiento', '281': 'No protegido, Vivienda Interés Social, Tratamiento Básico', '282': 'No protegido, Vivienda Interés Social, Consolidación Urbanística', '283': 'No protegido, Vivienda Interés Social, Conservación Arquitectónica',
            '284': 'No protegido, Vivienda Interés Social, Vivienda de Interés Social', '285': 'No protegido, Vivienda Interés Social, Zonas Verdes y Espacio Público', '290': 'No protegido, Zonas Verdes y Espacio Publico, Sin Tratamiento', '291': 'No protegido, Zonas Verdes y Espacio Publico, Tratamiento Básico',
            '292': 'No protegido, Zonas Verdes y Espacio Publico, Consolidación Urbanística', '293': 'No protegido, Zonas Verdes y Espacio Publico, Conservación Arquitectónica', '294': 'No protegido, Zonas Verdes y Espacio Publico, Vivienda de Interés Social', '295': 'No protegido, Zonas Verdes y Espacio Publico, Zonas Verdes y Espacio Público'
        },
        'tipificacion_construccion': {
            'Residencial': {
                '1': 'Residencial_1_Bajo_Bajo',
                '2': 'Residencial_2_Bajo',
                '3': 'Residencial_3_Medio_Bajo',
                '4': 'Residencial_4_Medio',
                '5': 'Residencial_5_Medio_Alto',
                '6': 'Residencial_6_Alto',
                '0': ''
            },
            'Comercial': {
                '1': 'Comercial_Barrial',
                '2': 'Comercial_Sectorial',
                '0': ''
            }
    }},
# ESPECIAL PORQUE TIPIFICACION CONSTRUCCION ES CONDICIONAL. OK PARA EJECUTAR
    '25815': {
        'orden_variables': ['topografia', 'influencia_vial', 'servicio_publico', 'uso_actual_suelo', 'norma_uso_suelo', 'tipificacion_construccion'],
        'longitudes': [1, 1, 1, 1, 3, 1],
        'topografia': {'1': 'Plano', '2': 'Inclinado', '3': 'Empinado'},
        'influencia_vial': {'1': 'Pavimentadas', '2': 'Sin_Pavimentar', '3': 'Peatonales', '4': 'Sin_Vias'},
        'servicio_publico': {'1': 'Sin_Servicios', '2': 'Básicos_Incompletos', '3': 'Básicos_Completos ', '4': 'Básicos_Y_Complementarios'},
        'uso_actual_suelo': {'1': 'Residencial', '2': 'Comercial', '3': 'Industrial', '4': 'Institucional', '5': 'Lote'},
        'norma_uso_suelo': {
            '110': 'Protegido, Residencial, Desarrollo', '111': 'Protegido, Residencial, Consolidación', '112': 'Protegido, Residencial, Conservación', '113': 'Protegido, Residencial, Mejoramiento integral',
            '120': 'Protegido, Comercial, Desarrollo', '121': 'Protegido, Comercial, Consolidación', '122': 'Protegido, Comercial, Conservación', '123': 'Protegido, Comercial, Mejoramiento integral',
            '130': 'Protegido, Protección, Desarrollo', '131': 'Protegido, Protección, Consolidación', '132': 'Protegido, Protección, Conservación', '133': 'Protegido, Protección, Mejoramiento integral',
            '140': 'Protegido, Servicios, Desarrollo', '141': 'Protegido, Servicios, Consolidación', '142': 'Protegido, Servicios, Conservación', '143': 'Protegido, Servicios, Mejoramiento integral',
            '150': 'Protegido, Institucional, Desarrollo', '151': 'Protegido, Institucional, Consolidación', '152': 'Protegido, Institucional, Conservación', '153': 'Protegido, Institucional, Mejoramiento integral',
            '210': 'No protegido, Residencial, Desarrollo', '211': 'No protegido, Residencial, Consolidación', '212': 'No protegido, Residencial, Conservación', '213': 'No protegido, Residencial, Mejoramiento integral',
            '220': 'No protegido, Comercial, Desarrollo', '221': 'No protegido, Comercial, Consolidación', '222': 'No protegido, Comercial, Conservación', '223': 'No protegido, Comercial, Mejoramiento integral',
            '230': 'No protegido, Protección, Desarrollo', '231': 'No protegido, Protección, Consolidación', '232': 'No protegido, Protección, Conservación', '233': 'No protegido, Protección, Mejoramiento integral',
            '240': 'No protegido, Servicios, Desarrollo', '241': 'No protegido, Servicios, Consolidación', '242': 'No protegido, Servicios, Conservación', '243': 'No protegido, Servicios, Mejoramiento integral',
            '250': 'No protegido, Institucional, Desarrollo', '251': 'No protegido, Institucional, Consolidación', '252': 'No protegido, Institucional, Conservación', '253': 'No protegido, Institucional, Mejoramiento integral'
        },
        'tipificacion_construccion': {
            'Residencial': {
                '1': 'Residencial_1_Bajo_Bajo',
                '2': 'Residencial_2_Bajo',
                '3': 'Residencial_3_Medio_Bajo',
                '4': 'Residencial_4_Medio',
                '5': 'Residencial_5_Medio_Alto',
                '6': 'Residencial_6_Alto',
                '0': ''
            },
            'Comercial': {
                '1': 'Comercial_Barrial',
                '2': 'Comercial_Sectorial',
                '0': ''
            }
    }},
#       ----- MPIOS INMOBILIARIA -------    
# LINEAL ESTE AUN NO ESTA LISTO PARA EJECUTAR
    '25053': {
        'orden_variables': ['topografia', 'influencia_vial', 'servicio_publico', 'uso_actual_suelo', 'norma_uso_suelo', 'tipificacion_construccion'],
        'longitudes': [1, 1, 1, 1, 3, 1],
        'topografia': {'1': 'Plano', '2': 'Inclinado', '3': 'Empinado'},
        'influencia_vial': {'1': 'Pavimentadas', '2': 'Sin_Pavimentar', '3': 'Peatonales', '4': 'Sin_Vias'},
        'servicio_publico': {'1': 'Servicios_Basicos_Y_Complementarios'},
        'uso_actual_suelo': {'1': 'Comercial', '3': 'Residencial', '5': 'Institucional'},
        'norma_uso_suelo': {
            '100': 'Protegido, Sin área de actividad, Sin Tratamiento', '101': 'Protegido, Sin área de actividad, Desarrollo',
            '102': 'Protegido, Sin área de actividad, Consolidación', '103': 'Protegido, Sin área de actividad, Mejoramiento', 
            '120': 'Protegido, Residencial, Sin Tratamiento', '121': 'Protegido, Residencial, Desarrollo',
            '122': 'Protegido, Residencial, Consolidación', '123': 'Protegido, Residencial, Mejoramiento',
            '130': 'Protegido, Comercio y servicios, Sin Tratamiento', '131': 'Protegido, Comercio y servicios, Desarrollo',
            '132': 'Protegido, Comercio y servicios, Consolidación', '133': 'Protegido, Comercio y servicios, Mejoramiento',
            '150': 'Protegido, Dotacional, Sin Tratamiento', '151': 'Protegido, Dotacional, Desarrollo',
            '152': 'Protegido, Dotacional, Consolidación', '153': 'Protegido, Dotacional, Mejoramiento',
            '200': 'No protegido, Sin área de actividad, Sin Tratamiento', '201': 'No protegido, Sin área de actividad, Desarrollo',
            '202': 'No protegido, Sin área de actividad, Consolidación', '203': 'No protegido, Sin área de actividad, Mejoramiento',
            '220': 'No protegido, Residencial, Sin Tratamiento', '221': 'No protegido, Residencial, Desarrollo',
            '222': 'No protegido, Residencial, Consolidación', '223': 'No protegido, Residencial, Mejoramiento',
            '230': 'No protegido, Comercio y servicios, Sin Tratamiento', '231': 'No protegido, Comercio y servicios, Desarrollo',
            '232': 'No protegido, Comercio y servicios, Consolidación', '233': 'No protegido, Comercio y servicios, Mejoramiento',
            '250': 'No protegido, Dotacional, Sin Tratamiento', '251': 'No protegido, Dotacional, Desarrollo',
            '252': 'No protegido, Dotacional, Consolidación', '253': 'No protegido, Dotacional, Mejoramiento'
        },
        'tipificacion_construccion': {
            'Residencial': {
                '1': 'Residencial_1_Bajo_Bajo',
                '2': 'Residencial_2_Bajo',
                '3': 'Residencial_3_Medio_Bajo',
                '4': 'Residencial_4_Medio',
                '5': 'Residencial_5_Medio_Alto',
                '6': 'Residencial_6_Alto',
                '0': ''
            },
            'Comercial': {
                '1': 'Comercial_Barrial',
                '2': 'Comercial_Sectorial',
                '0': ''
            }
    }},
}


# Parámetros de conexión a la base de datos
conexion_db = {
    'dbname': 'Novedades_V1_Municipios',
    'user': 'postgres',
    'password': '1234jcgg',
    'host': 'localhost',
    'port': '5432'
}

# Ruta para guardar los reportes
ruta_reportes = r'C:\ACC\Zonas_Homogeneas_Decodificacion\Reportes_Migraciones'
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

            # Verificamos si el municipio está en la lista de condicionales
            if mpio in municipios_condicionales and var == 'tipificacion_construccion':
                uso_actual = resultado.get('uso_actual_suelo', '0')  # por defecto '0'
                significado = dicc[var].get(uso_actual, {}).get(parte, '')
            else:
                significado = dicc.get(var, {}).get(parte, '')

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
    with open(nombre_archivo, 'w', encoding="utf-8") as f:
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

    # Unificamos las dos listas si vamos a procesarlos en conjunto
    municipios = municipios_condicionales + municipios_planos

    for mpio in municipios:
        print("📍 Procesando municipio:", mpio)
        # Lógica para conectarse, verificar campos y llamar a decodificar_codigo()

        campos_ok, campos_faltantes = verificar_campos_existentes(conn, mpio)

        resultados, procesados, errores = [], 0, 0
        if campos_ok:
            resultados, procesados, errores = procesar_municipio(conn, mpio)

        generar_reporte(mpio, resultados, procesados, errores, campos_faltantes)

    conn.close()
    print("\n✅ Proceso completado.")

if __name__ == '__main__':
    print("→ Iniciando procesamiento de municipios...")

    conn = conectar_db()
    if conn is None:
        print("❌ No se pudo establecer conexión con la base de datos.")
    else:
        # Unificamos las dos listas si vamos a procesarlos en conjunto
        municipios = municipios_condicionales + municipios_planos

        for mpio in municipios:
            print("📍 Procesando municipio:", mpio)
            # Lógica para conectarse, verificar campos y llamar a decodificar_codigo()

            campos_ok, faltantes = verificar_campos_existentes(conn, mpio)

            resultados, procesados, errores = procesar_municipio(conn, mpio) if campos_ok else ([], 0, 0)

            generar_reporte(mpio, resultados, procesados, errores, None if campos_ok else faltantes)

        conn.close()
        print("\n✅ Proceso finalizado.")

