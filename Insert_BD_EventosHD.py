import copy
import asyncio
import sys
import time
import re
import urllib.parse
import html
import unicodedata
import json
#import operator
import decimal
#import pdb

from datetime import datetime,timedelta

import urllib3
import requests
import telegram
import boto3

from bs4 import BeautifulSoup #
from unidecode import unidecode
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
#from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from fuzzywuzzy import fuzz
from boto3.dynamodb.conditions import Key

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


while True:
    horaIniciaEjecucion = datetime.now()
    print(f'Inicia ejecucion: {horaIniciaEjecucion}')
    vListDealers = []   # Lista para almacenar la lista de eventos
    v_list_eventos = []   # Lista para almacenar la lista de eventos
    v_list_eventos_copia = []   # Lista para almacenar la lista de eventos
    v_list_eventos_3 = []   # Lista para almacenar la lista de eventos
    v_list_eventos_news = []   # Lista para almacenar la lista de eventos
    v_list_eventos_LiveTV = []   # Lista para almacenar la lista de eventos de LiveTV
    v_list_eventos_Bases = []   # Lista para almacenar la lista de eventos de LiveTV

    v_list_eventos_olds = []   # Lista para almacenar la lista de viejos
    eventos_existentes = {}

    token_tg = "6559813109:AAEUKzEG6rRIFrt2pwkcHhZuA9Ynt3kqvlI"
    bot_tg_canal = telegram.Bot(token=token_tg)

    # Configura tus credenciales
    aws_access_key_id = 'AKIATCKAQMEJNSIO64FE'
    aws_secret_access_key = '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'
    region = 'us-east-1'

    # Inicializa DynamoDB
    dynamodb = boto3.resource(
        'dynamodb',
        region_name=region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )


    # Selecciona la tabla
    t_dia_evento = dynamodb.Table('dia_evento')
    t_dealers = dynamodb.Table('dealers')
    t_eventos = dynamodb.Table('eventos')

    def obtener_dia_actual():
        try:
            # Consulta el dia actual
            response = t_dia_evento.query(KeyConditionExpression=Key('id_dia_evento').eq(1))
            if 'Items' in response:
                dia_actual_bd = response['Items'][0]['f01_dia']
                return dia_actual_bd
            else:
                print('No se encontro la fecha en la BD')
                return '20000101'
        except Exception as e:
            print(f'Error al consultar la tabla dia_evento: {e}')
            return '20000101'

    #Eliminar registros de las tablas
    class MyDynamoDB_EliminarRegistrosTabla:
        def __init__(self, table_name):
            self.table = dynamodb.Table(table_name)

        def delete_all_items(self):
            try:
                # Escanea todos los elementos en la tabla
                response = self.table.scan()
                items = response.get('Items', [])
                for item in items:
                    # No elimina los items donde f02_proveedor contenga "Bases" en la tabla de eventos
                    if self.table.name == 'eventos' and "Bases" in item.get('f02_proveedor', ''):
                        continue  # Salta al siguiente item sin eliminar este
                    if self.table.name == 'eventos':
                        key = {'f01_id_document': item['f01_id_document'], 'f02_proveedor': item['f02_proveedor']}
                    elif self.table.name == 'dealers':
                        key = {'f01_id_dealer': item['f01_id_dealer'], 'f02_dealer_name': item['f02_dealer_name']}
                    else:
                        key = item  # Si la tabla no tiene clave compuesta, usa el item completo
                    self.table.delete_item(Key=key)
                print(f'Se eliminaron los datos de la tabla {self.table.name}')
            except Exception as e:
                print(f'Error al eliminar los datos de tabla: {self.table.name} {e}')

    # Crea una instancia para cada tabla que deseas limpiar
    eventos_table = MyDynamoDB_EliminarRegistrosTabla('eventos')
    dealers_table = MyDynamoDB_EliminarRegistrosTabla('dealers')
    dia_evento_table = MyDynamoDB_EliminarRegistrosTabla('dia_evento')

    dia_actual_bd = obtener_dia_actual()
    fecha_actual = datetime.now().strftime('%Y%m%d')
    actualizar_bases = 0

    if fecha_actual > dia_actual_bd:
        actualizar_bases = 1
        eventos_table.delete_all_items()
        dealers_table.delete_all_items()
        dia_evento_table.delete_all_items()

    if not vListDealers:  # Verifica si vListDealers está vacía
        response_DB_Dealers = t_dealers.scan()  # O el nombre correcto de tu variable de base de datos para dealers
        dealers_para_procesar = response_DB_Dealers.get('Items', [])
        # Actualiza vListDealers solo si se obtienen nuevos dealers
        if dealers_para_procesar:
            vListDealers = dealers_para_procesar
    else:
        # Usa vListDealers directamente si ya esta llena
        dealers_para_porcesar = vListDealers

    # Suponiendo que al inicio v_list_eventos_3 es None o una lista vacia
    if v_list_eventos_3 is None or not v_list_eventos_3:
        response_DB_Eventos = t_eventos.scan()
        eventos_para_procesar = response_DB_Eventos.get('Items', [])
        # Solo actualiza v_list_eventos_3 si response_DB_Eventos tiene items
        if eventos_para_procesar:
            v_list_eventos_3 = eventos_para_procesar
    else:
        # Usamos v_list_eventos_3 directamente si ya esta lleno
        eventos_para_procesar = v_list_eventos_3

    contador_registros = 0
    chat_id_channel = -1002035964627

    activaLiveTV = 0
    activaSportline = 0
    activaDirectatvHDme = 0
    activaLibreF = 0
    activaRojaOn = 0
    activaRojaTv = 0
    activaPlatin = 0
    activaDaddyLivehd = 0

    ind_miss_LibreF = 0
    #ind_miss_LiveTV = 0
    bool_estado_Sportline = False
    bool_estado_DirectatvHDme = False
    bool_estado_libref = False
    bool_estado_RojaOn = False
    bool_estado_RojaTv = False
    bool_estado_platin = False
    bool_estado_DaddyLivehd= False

    if response_DB_Dealers['Count'] == 0:
        activaLiveTV = 1
        activaSportline = 1
        activaDirectatvHDme = 1
        activaLibreF = 1
        activaRojaOn = 1
        activaRojaTv = 1
        activaPlatin = 1
        activaDaddyLivehd = 1
        #print('No habian datos de dealers se creara todo nuevo')
    else:
        #print('Si habian datos de dealers se repasara los errores')
        # Iterar sobre los documentos y procesarlos
        #for item in response_DB_Dealers['Items']:
            # proveedor = item['f02_dealer_name']
            # estado = item['f03_state']
            # vListDealers.append(item)
        for item in dealers_para_procesar:
            proveedor = item.get('f02_dealer_name', '')
            estado = item.get('f03_state', '')
            if proveedor == 'LiveTV':
                if not estado:
                    activaLiveTV = 1
                    #ind_miss_LiveTV = 1
                    #print(f'Se activa variable activaLiveTV por estado: {estado} en LiveTV')
            elif proveedor == 'Sportline':
                if not estado:
                    activaSportline = 1
                    #print(f'Se activa variable activaSportline por estado: {estado}')
            elif proveedor == 'DirectatvHDme':
                if not estado:
                    activaDirectatvHDme = 1
                    #print(f'Se activa variable activaDirectatvHDme por estado: {estado}')
            elif proveedor == 'LibreF':
                if not estado:
                    activaLibreF = 0
                    ind_miss_LibreF = 1
                    #print(f'Se activa variable ind_miss_LibreF por estado: {estado} en LibreF')
            elif proveedor == 'RojaOn':
                if not estado:
                    activaRojaOn = 1
                    #print(f'Se activa variable activaRojaOn por estado: {estado}')
            elif proveedor == 'RojaTv':
                if not estado:
                    activaRojaTv = 1
                    #print(f'Se activa variable activaRojaTv por estado: {estado}')
            elif proveedor == 'Platin':
                if not estado:
                    activaPlatin = 1
                    #print(f'Se activa variable activaPlatin por estado: {estado}')
            elif proveedor == 'DLHD':
                if not estado:
                    activaDaddyLivehd = 1
                    #print(f'Se activa variable activaDaddyLivehd por estado: {estado}')

    def convertToCorsProxyUrl(url, cors_proxy_url='https://corsproxy.io/?'):
        encoded_url = urllib.parse.quote(url, safe='')
        cors_proxy_url = cors_proxy_url + encoded_url
        return cors_proxy_url

    def convertToWhateverOriginUrl(url):
        # Usar la URL de Whatever Origin con la URL de destino codificada
        whatever_origin_url = f'https://www.whateverorigin.org/get?url={url}&callback=?'
        return whatever_origin_url

    def validate_and_get_url(url):
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
            fullallorigins_url = 'https://api.allorigins.win/raw?url=' + url
            #response_all = requests.get(fullallorigins_url, headers=headers)
            response_all = requests.get(fullallorigins_url, headers=headers, allow_redirects=True, verify=False)
            if response_all.status_code in (521, 502, 503, 403, 404, 204):
                #print(f"Error {response_all.status_code} para allorigins {fullallorigins_url}")
                fullallorigins_url_iso = 'https://api.allorigins.win/get?charset=ISO-8859-1&url='+ url
                #response_all_iso = requests.get(fullallorigins_url_iso, headers=headers)
                response_all_iso = requests.get(fullallorigins_url_iso, headers=headers, allow_redirects=True, verify=False)
                if response_all_iso.status_code in (521, 502, 503, 403, 404, 204):
                    #print(f"Error {response_all_iso.status_code} para allorigins {fullallorigins_url_iso}")
                    fullallorigins_url_myFunc = 'https://api.allorigins.win/get?callback=myFunc&url='+ url
                    #response_all_myFunc = requests.get(fullallorigins_url_myFunc, headers=headers)
                    response_all_myFunc = requests.get(fullallorigins_url_myFunc, headers=headers, allow_redirects=True, verify=False)
                    if response_all_myFunc.status_code in (521, 502, 503, 403, 404, 204):
                        #print(f"Error {response_all_myFunc.status_code} para allorigins {fullallorigins_url_myFunc}")
                        fullCors_url = convertToCorsProxyUrl(url)
                        #response_cors = requests.get(fullCors_url, headers=headers)
                        response_cors = requests.get(fullCors_url, headers=headers, allow_redirects=True, verify=False)
                        if response_cors.status_code in (521, 502, 503, 403, 404, 204):
                            #print(f"Error {response_cors.status_code} para corsproxy {fullCors_url}")
                            fullWhatever_url = convertToWhateverOriginUrl(url)
                            #response_whatever = requests.get(fullWhatever_url, headers=headers)
                            response_whatever = requests.get(fullWhatever_url, headers=headers, allow_redirects=True, verify=False)
                            if response_whatever.status_code in (521, 502, 503, 403, 404, 204):
                                #print(f"Error {response_whatever.status_code} para ullWhatever {fullWhatever_url}")
                                return fullWhatever_url
                        else:
                            #print(f"retorna bien para fullCors_url {fullCors_url}")
                            return fullCors_url
                    else:
                        #print(f"retorna bien para fullallorigins_url_myFunc {fullallorigins_url_myFunc}")
                        return fullallorigins_url_myFunc
                else:
                    #print(f"retorna bien para fullallorigins_url_iso {fullallorigins_url_iso}")
                    return fullallorigins_url_iso
            else:
                #print(f"retorna bien para full_url {fullallorigins_url}")
                return fullallorigins_url
        except requests.exceptions.RequestException as e:
            print(f"Error en la URL {fullallorigins_url}: {str(e)}")
            return None

    # URL de la pagina web
    urlPlatin = validate_and_get_url('https://www.platinsport.com')
    urlportsonline = validate_and_get_url('https://sportsonline.gl/prog.txt')
    urlRojaOn = validate_and_get_url('https://ww1.tarjetarojatvonline.sx')
    urlRojaTV =  validate_and_get_url('https://tarjetarojatv.run')
    urlLibreF =  'https://librefutboltv.net/agenda' #validate_and_get_url ('https://librefutboltv.net/agenda/') #('https://futbollibre.futbol/agenda/')
    #urlLibreF =  validate_and_get_url ('https://librefutboltv.net/agenda/') #('https://futbollibre.futbol/agenda/')
    urlDirectatvHDme = validate_and_get_url('https://directatvhd.me')
    urlDaddyLivehd =  validate_and_get_url ('https://dlhd.sx/schedule/schedule-generated.json')
    urlBases =  validate_and_get_url ('https://livetv.sx/enx/allupcomingsports/')

     # Variables para el evento maximo
    max_evento1 = None
    max_f01_id_document1 = 0  # Inicializa con un valor bajo o adecuado
    max_evento = None
    max_eventoLiveTV = None
     # Almacena los f01_id_document de los eventos que contienen 'LiveTV'
    f01_id_document_list = []

     # Ahora, eventos_para_procesar contiene la lista de eventos a procesar, ya sea de response_DB_Eventos o de v_list_eventos_3
    if eventos_para_procesar:
        # Iterar sobre los documentos y agregarlos a v_list_eventos_en_BD
        for evento_db in eventos_para_procesar:
            evento_data = evento_db
            #v_list_eventos_3.append(evento_data)
            # Nueva seccion de codigo para validar proveedores y activaciones
            f02_proveedor = evento_data.get('f02_proveedor', '')
            if (("LibreF" in f02_proveedor and ind_miss_LibreF == 1) or
                ("LiveTV" in f02_proveedor and activaLiveTV == 1)):

                detalles_evento = evento_data.get('f20_Detalles_Evento', [])
                # Verificar si hay al menos un detalle que contiene 'sin_data' en 'f22_opcion_Watch'
                if any('sin_data' in detalle.get('f22_opcion_Watch', '') for detalle in detalles_evento if detalle.get('f22_opcion_Watch') is not None):
                    v_list_eventos.append(evento_data)
                    ind_miss_LibreF = 1
                    #activaLibreF = 0

                # Si el evento contiene 'LiveTV', guarda su f01_id_document en la listas
                if 'LiveTV' in evento_data['f02_proveedor']:
                    activaLiveTV = 1
                    f01_id_document_list.append(evento_data['f01_id_document'])
                    v_list_eventos_LiveTV.append(evento_data)

            # Si el evento contiene 'LiveTV', guarda su f01_id_document en la lista
            if 'Bases' in evento_data['f02_proveedor']:
                v_list_eventos_Bases.append(evento_data)

            if (("Sportline" in f02_proveedor and activaSportline == 1) or
                ("DirectatvHDme" in f02_proveedor and activaDirectatvHDme == 1) or
                ("RojaOn" in f02_proveedor and activaRojaOn == 1) or
                ("RojaTv" in f02_proveedor and activaRojaTv == 1) or
                ("Platin" in f02_proveedor and activaPlatin == 1) or
                ("DLHD" in f02_proveedor and activaDaddyLivehd == 1)):
                v_list_eventos.append(evento_data)
            else:
                # Saltar al siguiente registro del bucle 'for'
                continue

        # Encuentra el maximo f01_id_document entre los eventos 'LiveTV'
        if f01_id_document_list:
            max_f01_id_document1 = max(f01_id_document_list)
            #max_eventoLiveTV = next((evento for evento in response_DB_Eventos['Items'] if evento['f01_id_document'] == max_f01_id_document1 and 'LiveTV' in evento['f02_proveedor']), None)
            max_eventoLiveTV = next((evento for evento in eventos_para_procesar if evento['f01_id_document'] == max_f01_id_document1 and 'LiveTV' in evento['f02_proveedor']), None)

        # Si encontramos el evento maximo, agregarlo a v_list_eventos
        if max_eventoLiveTV:
            v_list_eventos.append(max_eventoLiveTV)

        # Obtener todos los f01_id_document de eventos_db
        #f01_id_document_list = [evento_db['f01_id_document'] for evento_db in response_DB_Eventos['Items']]
        f01_id_document_list = [evento_db['f01_id_document'] for evento_db in eventos_para_procesar]

        # Encontrar el maximo f01_id_document
        if f01_id_document_list:
            contador_registros = max(f01_id_document_list)
            #print(f"El f01_id_document mas alto es: {contador_registros}")
        else:
            print("No hay registros en eventos_db")

    # Copiar el contenido de v_list_eventos a v_list_eventos_copia
    #v_list_eventos_copia.extend(v_list_eventos)
    v_list_eventos_copia = copy.deepcopy(v_list_eventos)

    def verificar_existencias():
        global activaLiveTV
        global activaSportline
        global activaDirectatvHDme
        global activaLibreF
        global activaRojaOn
        global activaRojaTv
        global activaPlatin
        global activaDaddyLivehd
        global ind_miss_LibreF

        for evento in v_list_eventos_3:
            proveedor = evento.get('f02_proveedor', '')
            if 'Sportline' in proveedor:
                break
        else:
            activaSportline = 1
            #print(f'Se activa variable activaSportline porque NO existe algun registro de Sportline')
        for evento in v_list_eventos_3:
            proveedor = evento.get('f02_proveedor', '')
            if 'DirectatvHDme' in proveedor:
                break
        else:
            activaDirectatvHDme = 1
            #print(f'Se activa variable activaDirectatvHDme porque NO existe algun registro de DirectatvHDme')
        for evento in v_list_eventos_3:
            proveedor = evento.get('f02_proveedor', '')
            if 'RojaOn' in proveedor:
                break
        else:
            activaRojaOn = 1
            #print(f'Se activa variable activaRojaOn porque NO existe algun registro de RojaOn')
        for evento in v_list_eventos_3:
            proveedor = evento.get('f02_proveedor', '')
            if 'RojaTv' in proveedor:
                break
        else:
            activaRojaTv = 1
            #print(f'Se activa variable activaRojaTv porque NO existe algun registro de RojaTv')
        for evento in v_list_eventos_3:
            proveedor = evento.get('f02_proveedor', '')
            if 'Platin' in proveedor:
                break
        else:
            activaPlatin = 1
            #print(f'Se activa variable activaPlatin porque NO existe algun registro de Platin')
        for evento in v_list_eventos_3:
            proveedor = evento.get('f02_proveedor', '')
            if 'DLHD' in proveedor:
                break
        else:
            activaDaddyLivehd = 1
            #print(f'Se activa variable activaDaddyLivehd porque NO existe algun registro de DLHD')
        for evento in v_list_eventos_3:
            proveedor = evento.get('f02_proveedor', '')
            if 'LibreF' in proveedor:
                break
        else:
            activaLibreF = 1
            ind_miss_LibreF = 0
            #print(f'Se activa variable activaLibreF = 1 porque NO existe algun registro de LibreF')
        for evento in v_list_eventos_3:
            proveedor = evento.get('f02_proveedor', '')
            if 'LiveTV' in proveedor:
                break
        else:
            activaLiveTV = 1
            #print(f'Se activa variable activaLiveTV = 1 porque NO existe algun registro de LiveTV')

    verificar_existencias()

    # Funcion para calcular la similitud entre textos normalizados
    def similar(a, b):
        return fuzz.ratio(a.lower(), b.lower())

    # Diccionario de caracteres especiales y sus reemplazos

    special_characters = {
        '\u0301': '',
        '̇': '',
        '&#105;': 'i',
        '&#775;': 'i',
        '&#225;': 'a',
        '&#233;': 'e',
        '&#204;': 'I',
        '&#205;': 'I',
        '&#206;': 'I',
        '&#207;': 'I',
        '&#236;': 'i',
        '&#237;': 'i',
        '&#238;': 'i',
        '&#239;': 'i',
        '&#8211;': '-',
        '&#304;': 'I',
        'Ä°': 'I',
        'Ã³': 'o',
        '&#243': 'o',
        '&#179': 'o',
        '&#225;': 'a',   # á
        '&#233;': 'e',   # é
        '&#237;': 'i',   # í
        '&#243;': 'o',   # ó
        '&#250;': 'u',   # ú
        '&#193;': 'A',   # Á
        '&#201;': 'E',   # É
        '&#205;': 'I',   # Í
        '&#211;': 'O',   # Ó
        '&#218;': 'U',   # Ú
        '&#228;': 'a',   # ä
        '&#235;': 'e',   # ë
        '&#239;': 'i',   # ï
        '&#246;': 'o',   # ö
        '&#252;': 'u',   # ü
        '&#196;': 'A',   # Ä
        '&#203;': 'E',   # Ë
        '&#207;': 'I',   # Ï
        '&#214;': 'O',   # Ö
        '&#220;': 'U',   # Ü
        '&#224;': 'a',   # à
        '&#232;': 'e',   # è
        '&#236;': 'i',   # ì
        '&#242;': 'o',   # ò
        '&#249;': 'u',   # ù
        '&#192;': 'A',   # À
        '&#200;': 'E',   # È
        '&#204;': 'I',   # Ì
        '&#210;': 'O',   # Ò
        '&#217;': 'U',   # Ù
        '&#229;': 'a',   # å
        '&#198;': 'AE',  # Æ
        '&#230;': 'ae',  # æ
        '&#339;': 'oe',  # œ
        '&#338;': 'OE',  # Œ
        '&#229;': 'aa',  # å
        '&#231;': 'c',   # ç
        '&#240;': 'eth', # ð
        '&#254;': 'thorn',# þ
        '&#222;': 'TH',   # Þ
        '&#241;': 'n',   # ñ
        '&#209;': 'N',   # Ñ
        '&#65533;': '',  # �
        '&#175;': '',    # ¯
        '&#161;': '',    # ¡
        '&#191;': '',    # ¿
        '&#8211;': '-', # en dash
        '&#8220;': '"', # left double quotation mark
        '&#8221;': '"', # right double quotation mark
        '&#8216;': "'", # left single quotation mark
        '&#8217;': "'", # right single quotation mark
        '&#8230;': '...',# horizontal ellipsis
        '&#8212;': '--',# em dash
        '&#8217;': "'", # right single quotation mark
        '&#8218;': ',', # single low-9 quotation mark
        '&#8222;': ',,',# double low-9 quotation mark
        '&#8224;': '†', # dagger
        '&#8225;': '‡', # double dagger
        '&#8226;': '•', # bullet
        '&#8722;': '-', # minus sign
        '&#8729;': '.', # bullet
        '&#160;': ' ',  # non-breaking space
        '&#173;': '',   # soft hyphen
        '&#8209;': '-', # non-breaking hyphen
        '&#x2013;': '-',# en dash
        '&#x2014;': '--',# em dash
        '&#x2018;': "'",# left single quotation mark
        '&#x2019;': "'",# right single quotation mark
        '&#x201c;': '"',# left double quotation mark
        '&#x201d;': '"',# right double quotation mark
        '&#x2022;': '•',# bullet
        '&#x2026;': '...',# horizontal ellipsis
        '&#x2030;': '‰',# per mille sign
        '&#x20AC;': '€',# euro sign
        '&#x2122;': '™',# trademark sign
        '&#x2C6;': '^', # circumflex accent
        '&#x2DC;': '~', # small tilde
        '&#x200B;': '', # zero width space
        '&#x200C;': '', # zero width non-joiner
        '&#x200D;': '', # zero width joiner
        '&#x200E;': '', # left-to-right mark
        '&#x200F;': '', # right-to-left mark
        'Ã©;': 'e',
    }

    def process_special_characters(text):
        # Normalize text to decomposed form
        normalized_text = unicodedata.normalize('NFKD', text)
        # Process character by character
        processed_text = ""
        for char in normalized_text:
            #print(f'char : {char}')
            if char in special_characters:
                processed_text += special_characters[char]
                #print(f'processed_text : {processed_text}')
            else:
                processed_text += char
        return processed_text

    def capitalize_words(text):
        words = text.lower().split()
        capitalized_words = [word.capitalize() for word in words]
        return ' '.join(capitalized_words)

    def obtenerUrlFinalPlatin(url_event):
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
        max_retries = 5
        retries = 0
        while retries < max_retries:
            #response = requests.get(url_event, headers=headers)
            response = requests.get(url_event, headers=headers, allow_redirects=True, verify=False)
            #print(f'response.status_code: {response.status_code} url_event: {url_event}')
            if response.status_code != 429:
                html_content = response.text
                soup = BeautifulSoup(html_content, 'html.parser')
                url_name_pairs = []
                # Encuentra todos los elementos <a> con href que comienzan con "acestream"
                a_elements = soup.find_all('a', href=True)

                for a_element in a_elements:
                    # Obten la URL final y elimina espacios en blanco al inicio y al final
                    url_fin = a_element['href'].strip()

                    # Verifica si comienza con "acestream://"
                    if url_fin.startswith("acestream://"):
                        # Obten el nombre del canal
                        name_channel = a_element.text.strip()
                        name_channel = capitalize_words(name_channel)

                        # Agrega la pareja URL-Nombre a la lista
                        url_name_pairs.append({"urlFin": url_fin, "nameChannel": name_channel})
                break   # La solicitud se realizo con exito, sal del bucle
            else:
                url_name_pairs = 'No pudo obtener ulr por 429 maximo de reintentos'
                time.sleep(10)   # Espera 5 segundos antes de reintentar
                retries += 1

        return url_name_pairs

    def obtenerUrlFinalRojaOn(url_inicial):
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
            max_retries = 5
            retries = 0
            while retries < max_retries:

                # Realiza una solicitud GET a la URL inicial para obtener el contenido HTML
                #response = requests.get(url_inicial)
                response = requests.get(url_inicial, headers=headers, allow_redirects=True, verify=False)
                if response.status_code != 429:
                    # Parsea el contenido HTML utilizando BeautifulSoup
                    soup = BeautifulSoup(response.text, 'html.parser')
                    # Encuentra el elemento 'iframe'
                    iframe_element = soup.find('iframe')
                    title_element = soup.find('title')

                    if iframe_element:
                        # Obtiene la URL final del atributo 'src' del elemento 'iframe'
                        url_final = iframe_element['src']
                        url_final = url_final.replace('https://tvhd.tutvlive.site/stream.php?ch=', '')
                        title_text = title_element.text
                        nameChannel_partes = title_text.split("En Vivo")
                        nameChannel = nameChannel_partes[0].strip()
                        #nameChannel = obtenerTitleRojaOn(url_final_con_api)
                        # Construye la cadena de texto con el formato deseado3
                        result = f"{nameChannel} | {url_final}"
                        return result
                    print(f"No se encontro el elemento 'iframe' en obtenerUrlFinalRojaOn para : {url_inicial}")
                    return None
                else:
                    #(f"No pudo obtener ulr por {response.status_code}. Reintentando...")
                    time.sleep(5)   # Espera 5 segundos antes de reintentar
                    retries += 1
        except Exception as e:
            print(f"Error en obtenerUrlFinalRojaOn: {str(e)}")
            return None

    def obtenerUrlFinalRojaTV(enlace):
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
            }
            max_retries = 5
            retries = 0

            while retries < max_retries:
                # Inicia roja
                if 'tarjetarojatv' in enlace or 'elitegoltv' in enlace:
                    response = requests.get(enlace, headers=headers, allow_redirects=True, verify=False)
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.text, 'html.parser')
                        divElement = soup.find('div', class_='iframe-container')
                        if divElement:
                            divContent = divElement.encode_contents().decode('utf-8')
                            scriptElement = divElement.find('script')
                            if scriptElement:
                                scriptContent = scriptElement.get_text()
                                fidRegex = re.compile(r'fid="([^"]+)";')
                                fidMatch = fidRegex.search(scriptContent)
                                if fidMatch:
                                    v_fid = fidMatch.group(1)
                                    srcRegex = re.compile(r'src="([^"]+?/[^/]+)')
                                    srcMatch = srcRegex.search(divContent)
                                    if srcMatch:
                                        urlevento = srcMatch.group(1)
                                        if 'radamel' in urlevento:
                                            maxReintentos = 5
                                            Reintentos = 0
                                            while Reintentos < maxReintentos:
                                                urlFinal_radamel = f'{urlevento}/reproductor/{v_fid}.php'
                                                urlFinal = validate_and_get_url(urlFinal_radamel)
                                                responseUrlFinRojaTv = requests.get(urlFinal, headers=headers, allow_redirects=True, verify=False)
                                                if responseUrlFinRojaTv.status_code == 200:
                                                    htmlBody = responseUrlFinRojaTv.text
                                                    iframeSrcRegex = re.compile(r'<iframe[^>]*?allowfullscreen="true"[^>]*?src="([^"]+)"', re.IGNORECASE)
                                                    iframeSrcMatch = iframeSrcRegex.search(htmlBody)
                                                    if iframeSrcMatch:
                                                        newUrlFin = iframeSrcMatch.group(1)
                                                        if 'livehdplay' in newUrlFin:
                                                            newUrlFin = urlFinal_radamel
                                                        return newUrlFin
                                                    else:
                                                        return urlFinal
                                                else:
                                                    time.sleep(3)  # Espera 3 segundos antes de reintentar
                                                    Reintentos += 1
                                        elif 'vikistream' in urlevento:
                                            newUrlFinal = f'https://vikistream.com/embed2.php?player=desktop&live={v_fid}'
                                            return newUrlFinal
                                        else:
                                            return enlace
                                    else:
                                        return enlace
                                else:
                                    return enlace
                            else:
                                return enlace
                        else:
                            return enlace
                    else:
                        time.sleep(5)
                        retries += 1
                else:
                    return enlace
            return enlace
        except Exception as e:
            print(f'Error en obtenerUrlFinalRojaTV: {e}')
            return enlace

    def obtenerUrlFinalLibreTV(url):
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
            max_retries = 5
            retries = 0

            while retries < max_retries:
                response = requests.get(url, headers=headers, allow_redirects=True, verify=False)
                if response.status_code == 200:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    iframe_element = soup.find('iframe')
                    src = iframe_element['src'] if iframe_element else None
                    opciones_enlaces_div = soup.find('div', {'align': 'left'})

                    # Si el src es "embed.html", extrae la URL final.
                    if src == 'embed.html':
                        src = url + src

                    if opciones_enlaces_div:
                        opciones_enlaces = opciones_enlaces_div.find_all('a', {'class': 'btn btn-fl'})
                        enlaces_y_textos = []

                        for opcion_enlace in opciones_enlaces:
                            enlace = opcion_enlace['href']
                            texto = opcion_enlace.get_text()

                            if enlace.startswith('//'):
                                enlace = 'https:' + enlace

                            enlace_decoded = html.unescape(enlace)
                            texto_decoded = process_special_characters(texto)

                            enlaces_y_textos.append(f'{enlace_decoded} | {texto_decoded}')

                        if enlaces_y_textos:
                            enlaces_textos_juntos = ' | '.join(enlaces_y_textos)
                            return enlaces_textos_juntos
                        else:
                            return src
                    else:
                        return src
                else:
                    time.sleep(5)  # Espera 5 segundos antes de reintentar
                    retries += 1

            return None  # Si se alcanzan los máximos reintentos y no se obtiene una respuesta satisfactoria
        except Exception as e:
            # Captura cualquier excepcion y muestra el mensaje de error
            print(f"Se produjo un error en obtenerUrlFinalLibreTV: {str(e)}")
            return None

    def obtenerUrlFinalLibreTVSelenium(initial_url):
        try:
            # Configurando las opciones de Chrome para navegacion en segundo plano y sin notificaciones
            chrome_options = Options()
            chrome_options.add_argument("--headless")  # Navegacion en segundo plano
            chrome_options.add_argument("--disable-notifications")  # Desactivar notificaciones
            chrome_options.add_argument("--log-level=3")  # Suprimir mensajes de log
            chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
            chrome_options.add_argument("--silent")

            # Crear una instancia de WebDriver con las opciones especificadas
            driver = webdriver.Chrome(options=chrome_options)

            try:
                driver.get(initial_url)
                wait = WebDriverWait(driver, 10)  # Esperar hasta 10 segundos para que aparezca el iframe
                iframe = wait.until(EC.presence_of_element_located((By.TAG_NAME, 'iframe')))
                final_url = iframe.get_attribute('src')
            finally:
                # Cerrar el WebDriver
                driver.quit()

            return final_url

        except Exception as e:
            # Captura cualquier excepción y muestra el mensaje de error
            print(f"Se produjo un error en obtenerUrlFinalLibreTVSelenium: {str(e)}")
            return None

    def obtenerUrlFinalRojaHDme(url):
        max_retries = 5
        retries = 0
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

        while retries < max_retries:
            #response = requests.get(url, headers=headers)
            response = requests.get(url, headers=headers, allow_redirects=True, verify=False)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                title = soup.head.title.text
                palabras_clave = ['rojadirecta', 'tarjetarojatvonline']   # Agrega aqui las palabras clave que deseas eliminar
                title_sin_palabras = title
                for palabra_clave in palabras_clave:
                    title_sin_palabras = title_sin_palabras.replace(palabra_clave, '')

                en_vivo_index = title_sin_palabras.lower().find('en vivo')
                src_value = ''

                if 'directatvhd' in url:
                    iframe_element = soup.select_one('iframe[allowfullscreen]')
                    if iframe_element:
                        src_value = iframe_element.get('src', '')

                if en_vivo_index != -1:
                    urlfin = f"{title_sin_palabras[:en_vivo_index].strip()} | {src_value}"
                else:
                    urlfin = f"{title_sin_palabras.strip()} | {src_value}"
                return urlfin
            else:
                #print(f"No pudo obtener ulr por {response.status_code}. Reintentando...")
                time.sleep(10)   # Espera 5 segundos antes de reintentar
                retries += 1
        return f'Web | No pudo obtener ulr despues de {max_retries} intentos'

    #def verificarExisteEvento(hora_event, name_event):
    # def verificarExisteEvento(fecha_hora, name_event):

    #     try:
    #         name_event_lower = name_event.lower()
    #         evento_existente = None
    #         debug
    #         pdb.set_trace()
    #         Verificar si ya existe un evento con el name_event y fecha_hora
    #         evento_existente = next((evento for evento in v_list_eventos
    #                                  if evento["f06_name_event"].lower() in name_event_lower and evento["f03_dia_event"] == fecha_hora), None)
    #         si no lo encuentra con IN se busca uno igual
    #         if evento_existente is None:
    #             evento_existente = next((evento for evento in v_list_eventos
    #                                      if evento["f06_name_event"].lower() == name_event_lower and evento["f03_dia_event"] == fecha_hora), None)
    #         si no encuentra con IN o igual se busca que el evento nuevo IN en la lista
    #         if evento_existente is None:
    #             evento_existente = next((evento for evento in v_list_eventos
    #                                      if name_event_lower in evento["f06_name_event"].lower() and evento["f03_dia_event"] == fecha_hora), None)

    #         if evento_existente is None:
    #             evento_existente = next((evento for evento in v_list_eventos
    #                                      if fuzz.partial_ratio(evento["f06_name_event"].lower(), name_event_lower) >= 80 and evento["f03_dia_event"] == fecha_hora), None)

    #         if evento_existente is None:
    #             evento_existente = next((evento for evento in v_list_eventos_3
    #                                      if evento["f06_name_event"].lower() in name_event_lower and evento["f03_dia_event"] == fecha_hora), None)
    #         si no lo encuentra con IN se busca uno igual
    #         if evento_existente is None:
    #             evento_existente = next((evento for evento in v_list_eventos_3
    #                                      if evento["f06_name_event"].lower() == name_event_lower and evento["f03_dia_event"] == fecha_hora), None)
    #         si no encuentra con IN o igual se busca que el evento nuevo IN en la lista
    #         if evento_existente is None:
    #             evento_existente = next((evento for evento in v_list_eventos_3
    #                                      if name_event_lower in evento["f06_name_event"].lower() and evento["f03_dia_event"] == fecha_hora), None)

    #         if evento_existente is None:
    #             evento_existente = next((evento for evento in v_list_eventos_3
    #                                      if fuzz.partial_ratio(evento["f06_name_event"].lower(), name_event_lower) >= 80 and evento["f03_dia_event"] == fecha_hora), None)

    #         if evento_existente:
    #             El evento existe, verificar si la URL ya existe para ese id_document
    #             id_document = evento_existente["f01_id_document"]
    #             return id_document
    #         else:
    #             return "No"   # El evento no existe
    #     except Exception as e:
    #         print(f'Error en verificarExisteEvento: {e}')

    def normalizar_nombre_evento(name_event):
        common_words = ["vs", "vs.", "-", "fc", "cf", "afc", "(w)", "w", "u17", "u-17", "u18", "u-18", "u19", "u-19", "/", ".", "sc", "lp", ",", "atl"]
        tokens = name_event.lower().split()
        filtered_tokens = [token for token in tokens if token not in common_words]
        return " ".join(filtered_tokens)

    def verificarExisteEvento(fecha_hora, name_event):
        try:
            name_event_normalizado = normalizar_nombre_evento(name_event)
            evento_existente = None

            # Funcion de comparacion de eventos
            def comparar_eventos(evento):
                evento_name_normalizado = normalizar_nombre_evento(evento["f06_name_event"])
                return (
                    fuzz.token_set_ratio(evento_name_normalizado, name_event_normalizado) >= 80
                    and evento["f03_dia_event"] == fecha_hora
                )

            # Verificar en la lista de eventos principal
            evento_existente = next((evento for evento in v_list_eventos if comparar_eventos(evento)), None)

            # Si no se encuentra en la lista principal, verificar en la lista secundaria
            if evento_existente is None:
                evento_existente = next((evento for evento in v_list_eventos_3 if comparar_eventos(evento)), None)

            if evento_existente:
                id_document = evento_existente["f01_id_document"]
                return id_document
            else:
                return "No"
        except Exception as e:
            print(f'Error en verificarExisteEvento: {e}')
            return "Error"

    def verificarExisteUrlEvento(id_document, urlFinal):
        try:
            # Buscar el evento con el id_document proporcionado
            evento = next((evento for evento in v_list_eventos + v_list_eventos_3 if evento["f01_id_document"] == id_document), None)
            if evento:
                detalles_evento = evento.get("f20_Detalles_Evento", [])   # Obtener la lista de detalles del evento si existe
                # Verificar si la URL final ya existe para ese id_document
                url_existe = any(detalle.get("f24_url_Final").lower() == urlFinal.lower() for detalle in detalles_evento)
            else:
                return "No_Existe_Url"   # La URL no existe para ese id_document

            if url_existe:
                return "Si_Existe_Url"   # La URL ya existe para ese id_document
            else:
                return "No_Existe_Url"   # La URL no existe para ese id_document
        except Exception as e:
            print(f'Error en verificarExisteUrlEvento: {e}')

    def contains_not_available_text(url):
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
            #response = requests.get(url, headers=headers)
            response = requests.get(url, headers=headers, allow_redirects=True, verify=False)
            if response.status_code == 200:
                document = BeautifulSoup(response.text, 'html.parser')
                # contains_text_1 = 'LiveStreams are currently not available for this broadcast.' in document.text
                # contains_text_2 = 'Live streams will be available approximately 30 minutes' in document.text
                # if contains_text_1 or contains_text_2:
                # Buscar la tabla especifica en el documento HTML
                tables = document.select('table.lnktbj')

                # Verificar si se encontro alguna tabla
                if tables:
                    return 'NO'
                else:
                    return 'SI'
            return 'NO'
        except Exception as e:
            print(f'Error en contains_not_available_text: {e}')

    def obtener_url_live_tv_final(enlace):
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
            #response = requests.get(enlace, headers=headers)
            response = requests.get(enlace, headers=headers, allow_redirects=True, verify=False)
            # Realizar solicitud HTTP para obtener el codigo HTML de la URL
            # response = requests.get(f"https:{enlace}")
            if response.status_code == 200:
                # Analizar el codigo HTML para buscar el iframe con allowfullscreen
                soup = BeautifulSoup(response.content, 'html.parser')
                iframe = soup.select_one('iframe[allowfullscreen]')
                if iframe:
                    urlFin = iframe.get('src')
                    urlFin = urlFin.replace('\n', '').replace('\r', '')   # Elimina saltos de linea y retorno de carro
                    if not urlFin.startswith('http'):
                        urlFin = f"https:{urlFin}"
                    if 'youtube' in urlFin and urlFin.startswith('//'):
                        urlFin = urlFin[2:]
                    return urlFin

        except Exception as e:
            print(f'Error en obtener_url_live_tv_final: {e}')

    def obtener_url_live_tv_final_tinyurl(url_acortada):
        try:
            response = requests.head(url_acortada, allow_redirects=True)
            url_final = response.url
            return url_final
        except Exception as e:
            print("Error al obtener la URL final:", e)
            return None

    def encontrar_indice_nombre_evento(evento, liveTds):
        for i, td in enumerate(liveTds):
            td_text = td.get_text().strip()
            similarity_ratio = fuzz.partial_ratio(evento.lower(), td_text.lower())
            if similarity_ratio >= 95:
                return i
        return None

    def month_str_to_num(month_str):
        months = {
            "January": "01", "February": "02", "March": "03", "April": "04",
            "May": "05", "June": "06", "July": "07", "August": "08",
            "September": "09", "October": "10", "November": "11", "December": "12"
        }
        return months.get(month_str, "00")

    def convert_to_24h(time_str):
        return datetime.strptime(time_str, "%I:%M %p").strftime("%H:%M")

    async def enviar_mensaje_telegram_channel(p_message):
        try:
            await bot_tg_canal.send_message(chat_id=chat_id_channel, text=p_message)
            print("Mensaje enviado al canal con exito.")
        except telegram.error.TelegramError as e:
            print(f"Error al enviar el mensaje al canal: {e}")

    eventNextDay = False
    fecha_event = None
    event_categoria = None
    url_flag = None
    jug_Local = None
    logo_Local = None
    jug_Visita = None
    logo_Visita = None
    channel_name = None
    imagenIdiom = None
    text_idiom = None
    existeEvent = "No"
    foundDayOfWeek = False
    diassemana = [
        'MONDAY',
        'TUESDAY',
        'WEDNESDAY',
        'THURSDAY',
        'FRIDAY',
        'SATURDAY',
        'SUNDAY'
    ]
    dias_traducidos = {
        'Lunes': 'MONDAY',
        'Martes': 'TUESDAY',
        'Miercoles': 'WEDNESDAY',
        'Jueves': 'THURSDAY',
        'Viernes': 'FRIDAY',
        'Sabado': 'SATURDAY',
        'Domingo': 'SUNDAY'
    }

    now = datetime.now()
    currentDayOfWeek = diassemana[now.weekday()]

    def procesar_Bases():
        try:
            print(f"Inicia procesar_Bases")
            global eventNextDay
            global event_categoria
            global url_flag
            global jug_Local
            global logo_Local
            global jug_Visita
            global logo_Visita
            global channel_name
            global imagenIdiom
            global text_idiom
            global existeEvent
            global contador_registros

            dia_event = None
            hora_event = None
            name_event = None

            name_Max_Event_Bases = None
            name_Penultimate_Event_Bases = None
            name_Antepenultimate_Event_Bases = None

            if v_list_eventos_Bases:
                # ultimo_evento = max(v_list_eventos_Bases, key=lambda x: x.get('f01_id_document'))
                # name_Max_Event_Bases = ultimo_evento.get('f06_name_event').replace('Vs', '–')
                # Asegurate de que la lista este ordenada por el criterio deseado
                v_list_eventos_Bases_sorted = sorted(v_list_eventos_Bases, key=lambda x: x.get('f01_id_document'))

                # Intenta obtener el ultimo, penultimo y antepenultimo elemento
                try:
                    ultimo_evento = v_list_eventos_Bases_sorted[-1]
                    name_Max_Event_Bases = ultimo_evento.get('f06_name_event').replace('Vs', '–')
                     #print(f"Ultimo evento: {name_Max_Event_Bases}")
                except IndexError:
                    print("No hay suficientes eventos para determinar el ultimo evento.")

                try:
                    penultimo_evento = v_list_eventos_Bases_sorted[-2]
                    name_Penultimate_Event_Bases = penultimo_evento.get('f06_name_event').replace('Vs', '–')
                    #print(f"Penultimo evento: {name_Penultimate_Event_Bases}")
                except IndexError:
                    print("No hay suficientes eventos para determinar el penultimo evento.")

                try:
                    antepenultimo_evento = v_list_eventos_Bases_sorted[-3]
                    name_Antepenultimate_Event_Bases = antepenultimo_evento.get('f06_name_event').replace('Vs', '–')
                     #print(f"Antepenultimo evento: {name_Antepenultimate_Event_Bases}")
                except IndexError:
                    print("No hay suficientes eventos para determinar el antepeniltimo evento.")

            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
            responseBases = requests.get(urlBases, headers=headers, allow_redirects=True, verify=False)
            html_contentBases = responseBases.text
            soup = BeautifulSoup(html_contentBases, 'html.parser')
            #td_elements = soup.find_all('td', {'colspan': '2'})
            td_elements = soup.find_all('td', colspan='2', height='38', valign='top', width='33%')
            indice_nombre_evento = None

            eventos_bases = {
                name_Max_Event_Bases: name_Max_Event_Bases,
                name_Penultimate_Event_Bases: name_Penultimate_Event_Bases,
                name_Antepenultimate_Event_Bases: name_Antepenultimate_Event_Bases
            }

            for nombre_evento, condicion in eventos_bases.items():
                if condicion is not None and indice_nombre_evento is None:
                    #print(f'nombre_evento: {nombre_evento}')
                    indice_nombre_evento = encontrar_indice_nombre_evento(nombre_evento, td_elements)
                    if indice_nombre_evento is not None:
                        break

            #print(f'entra: indice_nombre_evento: {indice_nombre_evento}')
            #sys.exit()
            # Si se encuentra el indice del nombre del evento
            if indice_nombre_evento is not None:
                v_list_events_web_base = []
                for td in td_elements:
                    texto_completo = td.get_text()
                    lines = [line.strip() for line in texto_completo.splitlines() if line.strip()]
                    if len(lines) >= 2:
                        date_str, time_str = lines[1].split(' at ')
                        day = date_str.split()[0]
                        month_str = date_str.split()[1]
                        month = month_str_to_num(month_str)
                        day = day.zfill(2)

                        hour, minute = time_str.split(':')
                        time_str = f"{hour.zfill(2)}:{minute}"
                        fecha_event_web = f"2024-{month}-{day} {time_str}"
                        fecha_event_web = datetime.strptime(f"{fecha_event_web}", '%Y-%m-%d %H:%M')
                        fecha_event_web -= timedelta(hours=6)
                        fecha_event_web = fecha_event_web.strftime('%Y-%m-%d %H:%M')
                    #     print(f'day: {day} | fecha_event_web {fecha_event_web} | lines {lines}')
                    #     continue
                    name_event = next((line.strip() for line in texto_completo.splitlines() if line.strip()), '')
                     #print(f'En PAGE - fecha_event_web: {fecha_event_web} | name_event: {name_event}')
                    v_list_events_web_base.append({
                        'name_event': name_event,
                        'fecha_event_web': fecha_event_web
                    })
                #sys.exit()
                 # # Iterar sobre los eventos en v_list_eventos_Bases
                for evento in v_list_eventos_Bases:
                    try:
                        name_event = evento.get('f06_name_event')
                        nombre_evento_base = name_event.replace('Vs', '–')
                        fecha_event_base = evento.get('f03_dia_event')
                         #print(f'En BASE - fecha_event_web: {fecha_event_web} | nombre_evento_base: {nombre_evento_base}')
                        exists = any(
                            fuzz.partial_ratio(nombre_evento_base.lower(), v_event['name_event'].lower()) > 80
                            and fecha_event_base == v_event['fecha_event_web']
                            for v_event in v_list_events_web_base
                        )
                        if not exists:
                            document_id = evento.get('f01_id_document')
                            proveedor = evento.get('f02_proveedor')
                            if document_id and "Bases" in proveedor:
                                try:
                                    t_eventos.delete_item(Key={"f01_id_document": document_id, "f02_proveedor": proveedor})
                                    print(f"Se elimino de Bases el ID: {document_id} | {fecha_event_base} | {name_event}")
                                except Exception as e:
                                    print(f"Ocurrio un error al eliminar el evento: {name_event} con el ID {document_id} | {e}")
                    except Exception as e:
                        print(f"Ocurrio un error en la eliminacion de eventos desde Bases: {name_event} con el nombre_evento_live {nombre_evento_base} | {e}")
                #sys.exit()
                for td in td_elements[indice_nombre_evento + 1:]:
                    tables = td.find_all('table', {'cellpadding': '1', 'cellspacing': '2', 'width': '100%'})
                    for table in tables:
                        span_evdesc = table.find('span', {'class': 'evdesc'})
                        span_evdesc = span_evdesc.get_text()
                        lines = [line.strip() for line in span_evdesc.splitlines() if line.strip()]
                        date_str, hora_event = lines[0].split(' at ')
                        day = date_str.split()[0]
                        month_str = date_str.split()[1]
                        month = month_str_to_num(month_str)
                        day = day.zfill(2)

                        hour, minute = hora_event.split(':')
                        hora_event = f"{hour.zfill(2)}:{minute}"
                        # hora_event_inicio = int(hora_event.split(':')[0].zfill(2))   # Asegura que siempre tenga dos caracteres
                        # # hora_event_inicio -= 6
                        # hora_event_inicio %= 24
                        dia_event = f"2024{month}{day}"
                        # print(f'lines: {lines[0]} | day: {day} | dia_event: {dia_event}')
                        # sys.exit()

                        # fecha_event_web = f"2024-{month}-{day} {hora_event}"
                        # fecha_event_web = datetime.strptime(f"{fecha_event_web}", '%Y-%m-%d %H:%M')
                        # fecha_event_web -= timedelta(hours=6)
                        # fecha_event_web = fecha_event_web.strftime('%Y-%m-%d %H:%M')

                        # print(f'fecha_event_web: {fecha_event_web}')
                        # sys.exit()
                        # if span_evdesc:
                        #     evdesc_text = span_evdesc.get_text(strip=True)
                        #     hora_match = re.search(r'\b\d{1,2}:\d{2}\b', evdesc_text)
                        #     if hora_match:
                        #         hora_event = hora_match.group()
                        #         # horas, minutos = hora_event.split(':')
                        #         # horas = horas.zfill(2)
                        #         # hora_event = f"{horas}:{minutos}"
                        #         hora_event_inicio = int(hora_event.split(':')[0].zfill(2))   # Asegura que siempre tenga dos caracteres
                        #         # hora_event_inicio -= 6
                        #         hora_event_inicio %= 24
                        #         hora_event = str(hora_event_inicio).zfill(2) + ':' + hora_event[2:]
                        #         hora_event = hora_event.replace('::',':')

                        img_alt = table.find('img')['alt']
                        event_categoria = img_alt
                        aElement = table.select_one('a.live')
                        if aElement is None:
                            continue
                        nameEventOld = aElement.text if aElement else ''
                        name_event = re.sub(r'\s+', ' ', re.sub(r'(?<=\s)[–](?=\s)', 'Vs', nameEventOld)).strip()
                        # if 'Auxerre' not in name_event:
                        #     continue
                        img_src = table.find('img')['src']
                        url_flag = img_src.lstrip('/')
                        url_flag = f"https://{url_flag}"

                        url_event = aElement['href']
                        url_event = f'https://livetv.sx{url_event}'
                        url_event =  validate_and_get_url(url_event)
                        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
                        responseL = requests.get(url_event, headers=headers, allow_redirects=True, verify=False)

                        document = BeautifulSoup(responseL.content, 'html.parser')
                        imagesWithItemprop = document.select('img[itemprop]')

                        if len(imagesWithItemprop) >= 2:
                            logoLocalOld = imagesWithItemprop[0]['src'] if 'src' in imagesWithItemprop[0].attrs else ''
                            logoVisitaOld = imagesWithItemprop[1]['src'] if 'src' in imagesWithItemprop[1].attrs else ''
                            jug_Local = imagesWithItemprop[0]['alt'] if 'alt' in imagesWithItemprop[0].attrs else ''
                            logo_Local = f"https:{logoLocalOld}"
                            jug_Visita = imagesWithItemprop[1]['alt'] if 'alt' in imagesWithItemprop[1].attrs else ''
                            logo_Visita = f"https:{logoVisitaOld}"

                        if logo_Local is not None and "/." in logo_Local:
                            logo_Local = None
                        if logo_Visita is not None and "/." in logo_Visita:
                            logo_Visita = None

                        # if contador_registros > 2 and hora_event_inicio > 18:
                        #     eventNextDay = True
                        # # Verificar si eventNextDay es True y hora_event < 9
                        # if eventNextDay and hora_event_inicio < 9:
                        #     # Incrementar dia_event en 1 dia
                        #     dia_event = datetime.strptime(dia_event, '%Y%m%d')
                        #     dia_event += timedelta(days=1)
                        #     dia_event = dia_event.strftime('%Y%m%d')

                        fecha_hora = datetime.strptime(f"{dia_event} {hora_event}", '%Y%m%d %H:%M')
                        fecha_hora -= timedelta(hours=6)
                        hora_event = fecha_hora.strftime("%H:%M")
                        fecha_hora = fecha_hora.strftime('%Y-%m-%d %H:%M')

                        if contador_registros > 0:
                            #existeEvent = verificarExisteEvento(hora_event,name_event)
                            existeEvent = verificarExisteEvento(fecha_hora,name_event)
                        if existeEvent == "No" or contador_registros == 0:
                            contador_registros += 1
                            evento = {
                                    "f01_id_document": contador_registros,
                                    "f02_proveedor" : "Bases",
                                    "f03_dia_event" : fecha_hora,
                                    'f04_hora_event': hora_event,
                                    'f05_event_categoria': event_categoria,
                                    'f06_name_event': name_event,
                                    'f07_URL_Flag': url_flag,
                                    'f08_jug_Local': jug_Local,
                                    'f09_logo_Local': logo_Local,
                                    'f10_jug_Visita': jug_Visita,
                                    'f11_logo_Visita': logo_Visita
                                    }
                            v_list_eventos.append(evento)
                            print(f'Se adiciona evento nuevo desde Bases: '
                                  f'ID: {contador_registros} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event}')
                        else:
                            evento_existente = next((evento for evento in v_list_eventos + v_list_eventos_3 if evento.get("f01_id_document") == existeEvent), None)
                            if evento_existente:
                                actualiza_algo = False
                                proveedor = evento_existente['f02_proveedor']
                                if 'Bases' not in evento_existente['f02_proveedor']:
                                    evento_existente['f02_proveedor'] += ' | Bases'
                                    actualiza_algo = True
                                if evento_existente['f07_URL_Flag'] is None and url_flag is not None:
                                    evento_existente['f07_URL_Flag'] = url_flag
                                    actualiza_algo = True
                                if evento_existente['f05_event_categoria'] is None and event_categoria is not None:
                                    evento_existente['f05_event_categoria'] = event_categoria
                                    actualiza_algo = True
                                if evento_existente['f08_jug_Local'] is None and jug_Local is not None:
                                    evento_existente['f08_jug_Local'] = jug_Local
                                    actualiza_algo = True
                                if evento_existente['f09_logo_Local'] is None and logo_Local is not None:
                                    evento_existente['f09_logo_Local'] = logo_Local
                                    actualiza_algo = True
                                if evento_existente['f10_jug_Visita'] is None and jug_Visita is not None:
                                    evento_existente['f10_jug_Visita'] = jug_Visita
                                    actualiza_algo = True
                                if evento_existente['f11_logo_Visita'] is None and logo_Visita is not None:
                                    evento_existente['f11_logo_Visita'] = logo_Visita
                                    actualiza_algo = True

                                if actualiza_algo:
                                    if evento_existente in v_list_eventos:
                                        v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                                        print(f'Se actualiza evento existente desde Bases: '
                                                f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event}')
                                    else:
                                        t_eventos.delete_item(Key={"f01_id_document": existeEvent, "f02_proveedor": proveedor})
                                        t_eventos.put_item(Item=evento_existente)
                                        print(f'Se actualiza evento en BD desde Bases: '
                                                f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event}')

                        name_event = None
                        hora_event = None
                        event_categoria = None
                        url_flag = None
                        jug_Local = None
                        logo_Local = None
                        jug_Visita = None
                        logo_Visita = None
            else:
                for td in td_elements:
                    tables = td.find_all('table', {'cellpadding': '1', 'cellspacing': '2', 'width': '100%'})
                    for table in tables:
                        span_evdesc = table.find('span', {'class': 'evdesc'})
                        span_evdesc = span_evdesc.get_text()
                        lines = [line.strip() for line in span_evdesc.splitlines() if line.strip()]
                        date_str, hora_event = lines[0].split(' at ')
                        day = date_str.split()[0]
                        month_str = date_str.split()[1]
                        month = month_str_to_num(month_str)
                        day = day.zfill(2)

                        hour, minute = hora_event.split(':')
                        hora_event = f"{hour.zfill(2)}:{minute}"
                        # hora_event_inicio = int(hora_event.split(':')[0].zfill(2))   # Asegura que siempre tenga dos caracteres
                        # # hora_event_inicio -= 6
                        # hora_event_inicio %= 24
                        dia_event = f"2024{month}{day}"

                        img_alt = table.find('img')['alt']
                        event_categoria = img_alt
                        aElement = table.select_one('a.live')
                        if aElement is None:
                            continue
                        nameEventOld = aElement.text if aElement else ''
                        name_event = re.sub(r'\s+', ' ', re.sub(r'(?<=\s)[–](?=\s)', 'Vs', nameEventOld)).strip()
                        # if 'Auxerre' not in name_event:
                        #     continue

                        img_src = table.find('img')['src']
                        url_flag = img_src.lstrip('/')
                        url_flag = f"https://{url_flag}"

                        url_event = aElement['href']
                        url_event = f'https://livetv.sx{url_event}'
                        url_event =  validate_and_get_url(url_event)
                        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
                        responseL = requests.get(url_event, headers=headers, allow_redirects=True, verify=False)

                        document = BeautifulSoup(responseL.content, 'html.parser')
                        imagesWithItemprop = document.select('img[itemprop]')

                        if len(imagesWithItemprop) >= 2:
                            logoLocalOld = imagesWithItemprop[0]['src'] if 'src' in imagesWithItemprop[0].attrs else ''
                            logoVisitaOld = imagesWithItemprop[1]['src'] if 'src' in imagesWithItemprop[1].attrs else ''
                            jug_Local = imagesWithItemprop[0]['alt'] if 'alt' in imagesWithItemprop[0].attrs else ''
                            logo_Local = f"https:{logoLocalOld}"
                            jug_Visita = imagesWithItemprop[1]['alt'] if 'alt' in imagesWithItemprop[1].attrs else ''
                            logo_Visita = f"https:{logoVisitaOld}"
                        if logo_Local is not None and "/." in logo_Local:
                            logo_Local = None
                        if logo_Visita is not None and "/." in logo_Visita:
                            logo_Visita = None

                        # if contador_registros > 2 and hora_event_inicio > 18:
                        #     eventNextDay = True
                        # # Verificar si eventNextDay es True y hora_event < 9
                        # if eventNextDay and hora_event_inicio < 9:
                        #     # Incrementar dia_event en 1 dia
                        #     dia_event = datetime.strptime(dia_event, '%Y%m%d')
                        #     dia_event += timedelta(days=1)
                        #     dia_event = dia_event.strftime('%Y%m%d')

                        fecha_hora = datetime.strptime(f"{dia_event} {hora_event}", '%Y%m%d %H:%M')
                        fecha_hora -= timedelta(hours=6)
                        hora_event = fecha_hora.strftime("%H:%M")
                        fecha_hora = fecha_hora.strftime('%Y-%m-%d %H:%M')

                        if contador_registros > 0:
                            #existeEvent = verificarExisteEvento(hora_event,name_event)
                            existeEvent = verificarExisteEvento(fecha_hora,name_event)
                        if existeEvent == "No" or contador_registros == 0:
                            contador_registros += 1
                            evento = {
                                    "f01_id_document": contador_registros,
                                    "f02_proveedor" : "Bases",
                                    "f03_dia_event" : fecha_hora,
                                    'f04_hora_event': hora_event,
                                    'f05_event_categoria': event_categoria,
                                    'f06_name_event': name_event,
                                    'f07_URL_Flag': url_flag,
                                    'f08_jug_Local': jug_Local,
                                    'f09_logo_Local': logo_Local,
                                    'f10_jug_Visita': jug_Visita,
                                    'f11_logo_Visita': logo_Visita
                                    }
                            v_list_eventos.append(evento)
                            print(f'Se adiciona evento desde Bases: '
                                  f'ID: {contador_registros} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event}')
                        else:
                            evento_existente = next((evento for evento in v_list_eventos + v_list_eventos_3 if evento.get("f01_id_document") == existeEvent), None)
                            if evento_existente:
                                actualiza_algo = False
                                proveedor = evento_existente['f02_proveedor']
                                if 'Bases' not in evento_existente['f02_proveedor']:
                                    evento_existente['f02_proveedor'] += ' | Bases'
                                    actualiza_algo = True
                                if evento_existente['f07_URL_Flag'] is None and url_flag is not None:
                                    evento_existente['f07_URL_Flag'] = url_flag
                                    actualiza_algo = True
                                if evento_existente['f05_event_categoria'] is None and event_categoria is not None:
                                    evento_existente['f05_event_categoria'] = event_categoria
                                    actualiza_algo = True
                                if evento_existente['f08_jug_Local'] is None and jug_Local is not None:
                                    evento_existente['f08_jug_Local'] = jug_Local
                                    actualiza_algo = True
                                if evento_existente['f09_logo_Local'] is None and logo_Local is not None:
                                    evento_existente['f09_logo_Local'] = logo_Local
                                    actualiza_algo = True
                                if evento_existente['f10_jug_Visita'] is None and jug_Visita is not None:
                                    evento_existente['f10_jug_Visita'] = jug_Visita
                                    actualiza_algo = True
                                if evento_existente['f11_logo_Visita'] is None and logo_Visita is not None:
                                    evento_existente['f11_logo_Visita'] = logo_Visita
                                    actualiza_algo = True

                                if actualiza_algo:
                                    if evento_existente in v_list_eventos:
                                        v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                                        print(f'Se actualiza evento desde Bases: '
                                                f'ID: {existeEvent} | {fecha_hora} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event}')
                                    else:
                                        t_eventos.delete_item(Key={"f01_id_document": existeEvent, "f02_proveedor": proveedor})
                                        t_eventos.put_item(Item=evento_existente)
                                        print(f'Se actualiza evento en BD desde Bases: '
                                                f'ID: {existeEvent} | {fecha_hora} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event}')
                        name_event = None
                        hora_event = None
                        event_categoria = None
                        url_flag = None
                        jug_Local = None
                        logo_Local = None
                        jug_Visita = None
                        logo_Visita = None
            print(f"Termina procesar_Bases")
        except Exception as e:
            print(f'Error en procesar_Bases: {e}')

    def procesar_LiveTV():
        try:
            print(f"Inicia procesar_LiveTV")
            global eventNextDay
            global event_categoria
            global url_flag
            global jug_Local
            global logo_Local
            global jug_Visita
            global logo_Visita
            global channel_name
            global imagenIdiom
            global text_idiom
            global existeEvent
            global contador_registros

            dia_event = None
            hora_event = None
            name_event = None
            urlFinal = None

            name_Max_Event_LiveTV = None
            name_Penultimate_Event_LiveTV = None
            name_Antepenultimate_Event_LiveTV = None

            contar_events_sin_links = 0
            cantidad_reg_pasar = 20
            if v_list_eventos_LiveTV:
                # Asegurate de que la lista este ordenada por el criterio deseado
                v_list_eventos_LiveTV_sorted = sorted(v_list_eventos_LiveTV, key=lambda x: x.get('f01_id_document'))

                # Intenta obtener el ultimo, penultimo y antepenutimo elemento
                try:
                    ultimo_evento = v_list_eventos_LiveTV_sorted[-1]
                    name_Max_Event_LiveTV = ultimo_evento.get('f06_name_event').replace('Vs', '–')
                    #print(f"Ultimo evento: {name_Max_Event_LiveTV}")
                except IndexError:
                    print("No hay suficientes eventos para determinar el ultimo evento.")

                try:
                    penultimo_evento = v_list_eventos_LiveTV_sorted[-2]
                    name_Penultimate_Event_LiveTV = penultimo_evento.get('f06_name_event').replace('Vs', '–')
                    #print(f"Penultimo evento: {name_Penultimate_Event_LiveTV}")
                except IndexError:
                    print("No hay suficientes eventos para determinar el penultimo evento.")

                try:
                    antepenultimo_evento = v_list_eventos_LiveTV_sorted[-3]
                    name_Antepenultimate_Event_LiveTV = antepenultimo_evento.get('f06_name_event').replace('Vs', '–')
                    #print(f"Antepenultimo evento: {name_Antepenultimate_Event_LiveTV}")
                except IndexError:
                    print("No hay suficientes eventos para determinar el antepeniltimo evento.")

            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
            responseLiveTV = requests.get(urlBases, headers=headers, allow_redirects=True, verify=False)
            html_contentLiveTV = responseLiveTV.text
            soup = BeautifulSoup(html_contentLiveTV, 'html.parser')
            #td_elements = soup.find_all('td', {'colspan': '2'})
            td_elements = soup.find_all('td', colspan='2', height='38', valign='top', width='33%')

            indice_nombre_evento = None

            eventos_LiveTV = {
                name_Max_Event_LiveTV: name_Max_Event_LiveTV,
                name_Penultimate_Event_LiveTV: name_Penultimate_Event_LiveTV,
                name_Antepenultimate_Event_LiveTV: name_Antepenultimate_Event_LiveTV
            }

            for nombre_evento, condicion in eventos_LiveTV.items():
                if condicion is not None and indice_nombre_evento is None:
                    indice_nombre_evento = encontrar_indice_nombre_evento(nombre_evento, td_elements)
                    if indice_nombre_evento is not None:
                        #print(f"indice_nombre_evento {indice_nombre_evento} | nombre_evento: {nombre_evento}")
                        break

            # Si se encuentra el indice del nombre del evento
            if indice_nombre_evento is not None:
                try:
                    v_list_events_web_base = []
                    for td in td_elements:
                        texto_completo = td.get_text()
                        lines = [line.strip() for line in texto_completo.splitlines() if line.strip()]
                        if len(lines) >= 2:
                            date_str, time_str = lines[1].split(' at ')
                            day = date_str.split()[0]
                            month_str = date_str.split()[1]
                            month = month_str_to_num(month_str)
                            day = day.zfill(2)
                            hour, minute = time_str.split(':')
                            time_str = f"{hour.zfill(2)}:{minute}"
                            fecha_event_web = f"2024-{month}-{day} {time_str}"

                            fecha_event_web = datetime.strptime(f"{fecha_event_web}", '%Y-%m-%d %H:%M')
                            fecha_event_web -= timedelta(hours=6)
                            fecha_event_web = fecha_event_web.strftime('%Y-%m-%d %H:%M')
                        name_event = next((line.strip() for line in texto_completo.splitlines() if line.strip()), '')
                        v_list_events_web_base.append({
                            'name_event': name_event,
                            'fecha_event_web': fecha_event_web
                        })
                    for evento in v_list_eventos_LiveTV:
                        try:
                            name_event = evento.get('f06_name_event')
                            nombre_evento_base = name_event.replace('Vs', '–')
                            fecha_event_base = evento.get('f03_dia_event')
                            exists = any(
                                fuzz.partial_ratio(nombre_evento_base.lower(), v_event['name_event'].lower()) > 80
                                and fecha_event_base == v_event['fecha_event_web']
                                for v_event in v_list_events_web_base
                            )
                            if not exists:
                                document_id = evento.get('f01_id_document')
                                proveedor = evento.get('f02_proveedor')
                                if document_id and "LiveTV" in proveedor:
                                    try:
                                        t_eventos.delete_item(Key={"f01_id_document": document_id, "f02_proveedor": proveedor})
                                        print(f"Se elimino de LiveTV el ID: {document_id} | {fecha_event_base} | {name_event}")
                                    except Exception as e:
                                        print(f"Ocurrio un error al eliminar el evento: {name_event} con el ID {document_id} | {e}")
                        except Exception as e:
                            print(f"Ocurrio un error en la eliminacion de eventos desde LiveTV: {name_event} con el nombre_evento_live {nombre_evento_base} | {e}")

                    for td in td_elements[indice_nombre_evento + 1:]:
                        if contar_events_sin_links >= cantidad_reg_pasar:
                            #print(f'Se sale break: contar_events_sin_links:{contar_events_sin_links}')
                            break
                        tables_a = td.find_all('table', {'cellpadding': '1', 'cellspacing': '2', 'width': '100%'})

                        for table_a in tables_a:
                            span_evdesc = table_a.find('span', {'class': 'evdesc'})
                            span_evdesc = span_evdesc.get_text()
                            lines = [line.strip() for line in span_evdesc.splitlines() if line.strip()]
                            date_str, hora_event = lines[0].split(' at ')
                            day = date_str.split()[0]
                            month_str = date_str.split()[1]
                            month = month_str_to_num(month_str)
                            day = day.zfill(2)

                            hour, minute = hora_event.split(':')
                            hora_event = f"{hour.zfill(2)}:{minute}"
                            # hora_event_inicio = int(hora_event.split(':')[0].zfill(2))   # Asegura que siempre tenga dos caracteres
                            # # hora_event_inicio -= 6
                            # hora_event_inicio %= 24
                            dia_event = f"2024{month}{day}"

                            img_alt = table_a.find('img')['alt']
                            event_categoria = img_alt
                            aElement = table_a.select_one('a.live')
                            if aElement is None:
                                continue
                            nameEventOld = aElement.text if aElement else ''
                            name_event = re.sub(r'\s+', ' ', re.sub(r'(?<=\s)[–](?=\s)', 'Vs', nameEventOld)).strip()
                            # if 'Auxerre' not in name_event:
                            #     continue
                            img_src = table_a.find('img')['src']
                            url_flag = img_src.lstrip('/')
                            url_flag = f"https://{url_flag}"

                            url_event = aElement['href']
                            url_event = f'https://livetv.sx{url_event}'
                            url_event =  validate_and_get_url(url_event)

                            containsText = contains_not_available_text(url_event)
                            if containsText == 'SI':
                                contar_events_sin_links = contar_events_sin_links + 1
                                if contar_events_sin_links >= cantidad_reg_pasar:
                                    #print(f'Se sale break: contar_events_sin_links:{contar_events_sin_links}')
                                    break
                                else:
                                    #print(f'Continua encontro sin sin_links name_event:{name_event}')
                                    continue

                            # if contador_registros > 2 and hora_event_inicio > 18:
                            #     eventNextDay = True
                            # # Verificar si eventNextDay es True y hora_event < 9
                            # if eventNextDay and hora_event_inicio < 9:
                            #     # Incrementar dia_event en 1 dia
                            #     dia_event = datetime.strptime(dia_event, '%Y%m%d')
                            #     dia_event += timedelta(days=1)
                            #     dia_event = dia_event.strftime('%Y%m%d')

                            fecha_hora = datetime.strptime(f"{dia_event} {hora_event}", '%Y%m%d %H:%M')
                            fecha_hora -= timedelta(hours=6)
                            hora_event = fecha_hora.strftime("%H:%M")
                            fecha_hora = fecha_hora.strftime('%Y-%m-%d %H:%M')

                            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
                            responseL = requests.get(url_event, headers=headers, allow_redirects=True, verify=False)

                            document = BeautifulSoup(responseL.content, 'html.parser')
                            imagesWithItemprop = document.select('img[itemprop]')
                            if len(imagesWithItemprop) >= 2:
                                logoLocalOld = imagesWithItemprop[0]['src'] if 'src' in imagesWithItemprop[0].attrs else ''
                                logoVisitaOld = imagesWithItemprop[1]['src'] if 'src' in imagesWithItemprop[1].attrs else ''
                                jug_Local = imagesWithItemprop[0]['alt'] if 'alt' in imagesWithItemprop[0].attrs else ''
                                logo_Local = f"https:{logoLocalOld}"
                                jug_Visita = imagesWithItemprop[1]['alt'] if 'alt' in imagesWithItemprop[1].attrs else ''
                                logo_Visita = f"https:{logoVisitaOld}"
                            if logo_Local is not None and "/." in logo_Local:
                                logo_Local = None
                            if logo_Visita is not None and "/." in logo_Visita:
                                logo_Visita = None

                            if contador_registros > 0:
                                #existeEvent = verificarExisteEvento(hora_event,name_event)
                                existeEvent = verificarExisteEvento(fecha_hora,name_event)
                            if existeEvent == "No" or contador_registros == 0:
                                contador_registros += 1
                                evento = {
                                        "f01_id_document": contador_registros,
                                        "f02_proveedor" : "LiveTV",
                                        "f03_dia_event" : fecha_hora,
                                        'f04_hora_event': hora_event,
                                        'f05_event_categoria': event_categoria,
                                        'f06_name_event': name_event,
                                        'f07_URL_Flag': url_flag,
                                        'f08_jug_Local': jug_Local,
                                        'f09_logo_Local': logo_Local,
                                        'f10_jug_Visita': jug_Visita,
                                        'f11_logo_Visita': logo_Visita
                                        }
                                #v_list_eventos.append(evento)
                                #print(f'Se adiciona evento nuevo desde LiveTV : {hora_event} |s {event_categoria} | {name_event}')

                            list_eventos_detalles = []   # Lista para almacenar la lista de eventos
                            list_eventos_detalles_existente = []   # Lista para almacenar la lista de eventos
                            # Obtener todas las tablas con clase "lnktbj"
                            tables_b = document.select('table.lnktbj')
                            list_eventos_detalles = []   # Lista para almacenar la lista de eventos
                            for table_b in tables_b:
                                # Verificar si la tabla contiene informacion relevante
                                img = table_b.select_one('td > img[title]')
                                if img and 'title' in img.attrs:
                                    imagenIdiom = img['src']
                                    imagenIdiom = f"https:{imagenIdiom}"
                                    text_idiom = img['title']
                                    enlaces = table_b.select('td > a')
                                    enlace = enlaces[1]['href'] if len(enlaces) > 1 else enlaces[0]['href']
                                    enlace = enlace.replace(" ","")
                                    channel_name = table_b.select_one('td.lnktyt > span')
                                    if channel_name and channel_name.text.strip():
                                        channel_name = channel_name.text.strip()
                                    else:
                                        channel_name = img['title']
                                    if 'tinyurl.com' in enlace:
                                        urlFinal = obtener_url_live_tv_final_tinyurl(enlace)
                                    if 'acestream://' in enlace:
                                        urlFinal = enlace
                                    elif ((not enlace.startswith('http')) and (not 'acestream://' in enlace)):
                                        enlace = f"https:{enlace}"
                                        urlFinal = obtener_url_live_tv_final(enlace)
                                    existeUrlEvent = None
                                    if urlFinal is None:
                                        continue
                                    if 'acestream://' in urlFinal:
                                        channel_name = "Acestream"

                                    if existeEvent == "No":
                                        detalle = {
                                                    'f21_imagen_Idiom': imagenIdiom,
                                                    'f22_opcion_Watch': channel_name,
                                                    'f23_text_Idiom': text_idiom,
                                                    'f24_url_Final': urlFinal
                                                    }
                                        list_eventos_detalles.append(detalle)
                                    else:
                                        existeUrlEvent = verificarExisteUrlEvento(existeEvent, urlFinal)
                                        if existeUrlEvent == "Si_Existe_Url":
                                            #print(f'Ya existe Url para evento desde LiveTV : {hora_event} | {name_event} | {urlFinal}')
                                            continue
                                        else:
                                            evento_existente = next((evento for evento in v_list_eventos + v_list_eventos_3 if evento.get("f01_id_document") == existeEvent), None)
                                            if evento_existente:
                                                proveedor = evento_existente['f02_proveedor']
                                                if 'LiveTV' not in evento_existente['f02_proveedor']:
                                                    evento_existente['f02_proveedor'] += ' | LiveTV'
                                                if evento_existente['f03_dia_event'] is None and fecha_hora is not None:
                                                    evento_existente['f03_dia_event'] = fecha_hora
                                                if url_flag is not None:
                                                    evento_existente['f07_URL_Flag'] = url_flag
                                                if event_categoria is not None:
                                                    evento_existente['f05_event_categoria'] = event_categoria
                                                if jug_Local is not None:
                                                    evento_existente['f08_jug_Local'] = jug_Local
                                                if logo_Local is not None:
                                                    evento_existente['f09_logo_Local'] = logo_Local
                                                if jug_Visita is not None:
                                                    evento_existente['f10_jug_Visita'] = jug_Visita
                                                if logo_Visita is not None:
                                                    evento_existente['f11_logo_Visita'] = logo_Visita

                                                list_eventos_detalles_existente = evento_existente.get("f20_Detalles_Evento", [])
                                                detalle = {
                                                            'f21_imagen_Idiom': imagenIdiom,
                                                            'f22_opcion_Watch': channel_name,
                                                            'f23_text_Idiom': text_idiom,
                                                            'f24_url_Final': urlFinal
                                                            }
                                                # Agregar detalle al evento existente
                                                list_eventos_detalles_existente.append(detalle)
                                                evento_existente['f20_Detalles_Evento'] = list_eventos_detalles_existente
                                                if evento_existente in v_list_eventos:
                                                    v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                                                    print(f'Se actualiza list_eventos desde LiveTV: '
                                                          f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}')
                                                else:
                                                    t_eventos.delete_item(Key={"f01_id_document": existeEvent, "f02_proveedor": proveedor})
                                                    t_eventos.put_item(Item=evento_existente)
                                                    print(f'Se actualiza evento en BD desde LiveTV: '
                                                          f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name} | proveedor: {proveedor}')

                                    if existeEvent == "No":
                                        # Agregar la lista de detallesEvento al evento
                                        evento['f20_Detalles_Evento'] = list_eventos_detalles
                                        v_list_eventos.append(evento)
                                        print(f'Se adiciona evento y detalles desde LiveTV : '
                                              f'ID: {contador_registros} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}')

                                    text_idiom = None
                                    channel_name = None
                                    imagenIdiom = None
                                    urlFinal = None
                            event_categoria = None
                            url_flag = None
                            jug_Local = None
                            logo_Local = None
                            jug_Visita = None
                            logo_Visita = None
                            name_event = None
                            hora_event = None
                except Exception as e:
                    print(f'Error en procesar_LiveTV 1: {e}')
                    # Datos del nuevo dealer
                    dealerLiveTV = {
                        "f01_id_dealer": 1,
                        "f02_dealer_name": f"LiveTV",
                        "f03_state": False,
                    }

                    # Verificar si el f01_id_dealer ya existe en vListDealers
                    dealer_exists = False
                    for dealer in vListDealers:
                        if dealer['f01_id_dealer'] == dealerLiveTV['f01_id_dealer']:
                            dealer['f03_state'] = dealerLiveTV['f03_state']  # Actualizar el campo f03_state
                            dealer_exists = True
                            break  # Salir del bucle una vez actualizado el dealer

                    # Si el dealer no existe, se agrega a la lista
                    if not dealer_exists:
                        vListDealers.append(dealerLiveTV)
            else:
                #sys.exit()
                try:
                    for td in td_elements:
                        if contar_events_sin_links >= cantidad_reg_pasar:
                            break
                        tables_cellpadding = td.find_all('table', {'cellpadding': '1', 'cellspacing': '2', 'width': '100%'})
                        for table in tables_cellpadding:
                            span_evdesc = table.find('span', {'class': 'evdesc'})
                            span_evdesc = span_evdesc.get_text()
                            lines = [line.strip() for line in span_evdesc.splitlines() if line.strip()]
                            date_str, hora_event = lines[0].split(' at ')
                            day = date_str.split()[0]
                            month_str = date_str.split()[1]
                            month = month_str_to_num(month_str)
                            day = day.zfill(2)

                            hour, minute = hora_event.split(':')
                            hora_event = f"{hour.zfill(2)}:{minute}"
                            # hora_event_inicio = int(hora_event.split(':')[0].zfill(2))   # Asegura que siempre tenga dos caracteres
                            # # hora_event_inicio -= 6
                            # hora_event_inicio %= 24
                            dia_event = f"2024{month}{day}"

                            img_alt = table.find('img')['alt']
                            event_categoria = img_alt
                            aElement = table.select_one('a.live')
                            if aElement is None:
                                continue
                            nameEventOld = aElement.text if aElement else ''
                            name_event = re.sub(r'\s+', ' ', re.sub(r'(?<=\s)[–](?=\s)', 'Vs', nameEventOld)).strip()
                            # if 'Auxerre' not in name_event:
                            #     continue

                            img_src = table.find('img')['src']
                            url_flag = img_src.lstrip('/')
                            url_flag = f"https://{url_flag}"

                            url_event = aElement['href']
                            url_event = f'https://livetv.sx{url_event}'
                            url_event =  validate_and_get_url(url_event)

                            containsText = contains_not_available_text(url_event)
                            if containsText == 'SI':
                                contar_events_sin_links = contar_events_sin_links + 1
                                if contar_events_sin_links >= cantidad_reg_pasar:
                                    break
                                else:
                                    continue

                            # if contador_registros > 2 and hora_event_inicio > 18:
                            #     eventNextDay = True
                            # # Verificar si eventNextDay es True y hora_event < 9
                            # if eventNextDay and hora_event_inicio < 9:
                            #     # Incrementar dia_event en 1 dia
                            #     dia_event = datetime.strptime(dia_event, '%Y%m%d')
                            #     dia_event += timedelta(days=1)
                            #     dia_event = dia_event.strftime('%Y%m%d')

                            fecha_hora = datetime.strptime(f"{dia_event} {hora_event}", '%Y%m%d %H:%M')
                            fecha_hora -= timedelta(hours=6)
                            hora_event = fecha_hora.strftime("%H:%M")
                            fecha_hora = fecha_hora.strftime('%Y-%m-%d %H:%M')

                            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
                            responseL = requests.get(url_event, headers=headers, allow_redirects=True, verify=False)
                            document = BeautifulSoup(responseL.content, 'html.parser')
                            imagesWithItemprop = document.select('img[itemprop]')

                            if len(imagesWithItemprop) >= 2:
                                logoLocalOld = imagesWithItemprop[0]['src'] if 'src' in imagesWithItemprop[0].attrs else ''
                                logoVisitaOld = imagesWithItemprop[1]['src'] if 'src' in imagesWithItemprop[1].attrs else ''
                                jug_Local = imagesWithItemprop[0]['alt'] if 'alt' in imagesWithItemprop[0].attrs else ''
                                logo_Local = f"https:{logoLocalOld}"
                                jug_Visita = imagesWithItemprop[1]['alt'] if 'alt' in imagesWithItemprop[1].attrs else ''
                                logo_Visita = f"https:{logoVisitaOld}"
                            if logo_Local is not None and "/." in logo_Local:
                                logo_Local = None
                            if logo_Visita is not None and "/." in logo_Visita:
                                logo_Visita = None

                            if contador_registros > 0:
                                #existeEvent = verificarExisteEvento(hora_event,name_event)
                                existeEvent = verificarExisteEvento(fecha_hora,name_event)
                            if existeEvent == "No" or contador_registros == 0:
                                contador_registros += 1
                                evento = {
                                        "f01_id_document": contador_registros,
                                        "f02_proveedor" : "LiveTV",
                                        "f03_dia_event" : fecha_hora,
                                        'f04_hora_event': hora_event,
                                        'f05_event_categoria': event_categoria,
                                        'f06_name_event': name_event,
                                        'f07_URL_Flag': url_flag,
                                        'f08_jug_Local': jug_Local,
                                        'f09_logo_Local': logo_Local,
                                        'f10_jug_Visita': jug_Visita,
                                        'f11_logo_Visita': logo_Visita
                                        }
                            list_eventos_detalles = []   # Lista para almacenar la lista de eventos
                            list_eventos_detalles_existente = []   # Lista para almacenar la lista de eventos
                            # Obtener todas las tablas con clase "lnktbj"
                            tables_lnktbj = document.select('table.lnktbj')
                            list_eventos_detalles = []   # Lista para almacenar la lista de eventos
                            for table in tables_lnktbj:
                                # Verificar si la tabla contiene informacion relevante
                                img = table.select_one('td > img[title]')
                                if img and 'title' in img.attrs:
                                    imagenIdiom = img['src']
                                    imagenIdiom = f"https:{imagenIdiom}"
                                    text_idiom = img['title']
                                    enlaces = table.select('td > a')
                                    enlace = enlaces[1]['href'] if len(enlaces) > 1 else enlaces[0]['href']
                                    enlace = enlace.replace(" ","")
                                    channel_name = table.select_one('td.lnktyt > span')
                                    if channel_name and channel_name.text.strip():
                                        channel_name = channel_name.text.strip()
                                    else:
                                        channel_name = img['title']
                                    if 'tinyurl.com' in enlace:
                                        urlFinal = obtener_url_live_tv_final_tinyurl(enlace)
                                    if 'acestream://' in enlace:
                                        urlFinal = enlace
                                    elif ((not enlace.startswith('http')) and (not 'acestream://' in enlace)):
                                        enlace = f"https:{enlace}"
                                        urlFinal = obtener_url_live_tv_final(enlace)
                                    existeUrlEvent = None
                                    if urlFinal is None:
                                        continue
                                    if 'acestream://' in urlFinal:
                                        channel_name = "Acestream"
                                    if existeEvent == "No":
                                        detalle = {
                                                    'f21_imagen_Idiom': imagenIdiom,
                                                    'f22_opcion_Watch': channel_name,
                                                    'f23_text_Idiom': text_idiom,
                                                    'f24_url_Final': urlFinal
                                                    }
                                        list_eventos_detalles.append(detalle)
                                    else:
                                        existeUrlEvent = verificarExisteUrlEvento(existeEvent, urlFinal)
                                        if existeUrlEvent == "Si_Existe_Url":
                                            #print(f'Ya existe Url para evento desde LiveTV : {hora_event} | {name_event} | {urlFinal}')
                                            continue
                                        else:
                                            evento_existente = next((evento for evento in v_list_eventos + v_list_eventos_3 if evento.get("f01_id_document") == existeEvent), None)
                                            if evento_existente:
                                                proveedor = evento_existente['f02_proveedor']
                                                if 'LiveTV' not in evento_existente['f02_proveedor']:
                                                    evento_existente['f02_proveedor'] += ' | LiveTV'
                                                if evento_existente['f03_dia_event'] is None and fecha_hora is not None:
                                                    evento_existente['f03_dia_event'] = fecha_hora
                                                if url_flag is not None:
                                                    evento_existente['f07_URL_Flag'] = url_flag
                                                if event_categoria is not None:
                                                    evento_existente['f05_event_categoria'] = event_categoria
                                                if jug_Local is not None:
                                                    evento_existente['f08_jug_Local'] = jug_Local
                                                if logo_Local is not None:
                                                    evento_existente['f09_logo_Local'] = logo_Local
                                                if jug_Visita is not None:
                                                    evento_existente['f10_jug_Visita'] = jug_Visita
                                                if logo_Visita is not None:
                                                    evento_existente['f11_logo_Visita'] = logo_Visita

                                                list_eventos_detalles_existente = evento_existente.get("f20_Detalles_Evento", [])
                                                detalle = {
                                                            'f21_imagen_Idiom': imagenIdiom,
                                                            'f22_opcion_Watch': channel_name,
                                                            'f23_text_Idiom': text_idiom,
                                                            'f24_url_Final': urlFinal
                                                            }
                                                # Agregar detalle al evento existente
                                                list_eventos_detalles_existente.append(detalle)
                                                evento_existente['f20_Detalles_Evento'] = list_eventos_detalles_existente
                                                if evento_existente in v_list_eventos:
                                                    v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                                                    print(f'Se actualiza list_eventos desde LiveTV: '
                                                          f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}')
                                                else:
                                                    t_eventos.delete_item(Key={"f01_id_document": existeEvent, "f02_proveedor": proveedor})
                                                    t_eventos.put_item(Item=evento_existente)
                                                    print(f'Se actualiza evento en BD desde LiveTV: '
                                                          f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name} | proveedor: {proveedor}')
                                    if existeEvent == "No":
                                        # Agregar la lista de detallesEvento al evento
                                        evento['f20_Detalles_Evento'] = list_eventos_detalles
                                        v_list_eventos.append(evento)
                                        print(f'Se adiciona evento y detalles desde LiveTV: '
                                              f'ID: {contador_registros} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}')

                                    text_idiom = None
                                    channel_name = None
                                    imagenIdiom = None
                                    urlFinal = None
                            name_event = None
                            hora_event = None
                            event_categoria = None
                            url_flag = None
                            jug_Local = None
                            logo_Local = None
                            jug_Visita = None
                            logo_Visita = None
                except Exception as e:
                    print(f'Error en procesar_LiveTV 1: {e}')
                    # Datos del nuevo dealer
                    dealerLiveTV = {
                        "f01_id_dealer": 1,
                        "f02_dealer_name": f"LiveTV",
                        "f03_state": False,
                    }

                    # Verificar si el f01_id_dealer ya existe en vListDealers
                    dealer_exists = False
                    for dealer in vListDealers:
                        if dealer['f01_id_dealer'] == dealerLiveTV['f01_id_dealer']:
                            dealer['f03_state'] = dealerLiveTV['f03_state']  # Actualizar el campo f03_state
                            dealer_exists = True
                            break  # Salir del bucle una vez actualizado el dealer

                    # Si el dealer no existe, se agrega a la lista
                    if not dealer_exists:
                        vListDealers.append(dealerLiveTV)

                        #continue

            # Datos del nuevo dealer
            dealerLiveTV = {
                "f01_id_dealer": 1,
                "f02_dealer_name": "LiveTV",
                "f03_state": False,
            }

            # Verificar si el f01_id_dealer ya existe en vListDealers
            dealer_exists = False
            for dealer in vListDealers:
                if dealer['f01_id_dealer'] == dealerLiveTV['f01_id_dealer']:
                    dealer['f03_state'] = dealerLiveTV['f03_state']  # Actualizar el campo f03_state
                    dealer_exists = True
                    break  # Salir del bucle una vez actualizado el dealer

            # Si el dealer no existe, se agrega a la lista
            if not dealer_exists:
                vListDealers.append(dealerLiveTV)

            print(f"Termina procesar_LiveTV")
        except Exception as e:
            print(f'Error en procesar_LiveTV 2: {e}')
            # Datos del nuevo dealer
            dealerLiveTV = {
                "f01_id_dealer": 1,
                "f02_dealer_name": f"LiveTV",
                "f03_state": False,
            }

            # Verificar si el f01_id_dealer ya existe en vListDealers
            dealer_exists = False
            for dealer in vListDealers:
                if dealer['f01_id_dealer'] == dealerLiveTV['f01_id_dealer']:
                    dealer['f03_state'] = dealerLiveTV['f03_state']  # Actualizar el campo f03_state
                    dealer_exists = True
                    break  # Salir del bucle una vez actualizado el dealer

            # Si el dealer no existe, se agrega a la lista
            if not dealer_exists:
                vListDealers.append(dealerLiveTV)

    def procesar_SportsLine():
        print(f"Inicia procesar_SportsLine")
        global eventNextDay
        global fecha_event
        global event_categoria
        global url_flag
        global jug_Local
        global logo_Local
        global jug_Visita
        global logo_Visita
        global channel_name
        global imagenIdiom
        global text_idiom
        global existeEvent
        global contador_registros
        global foundDayOfWeek
        global currentDayOfWeek
        try:
            global bool_estado_Sportline
            dia_event = None
            hora_event = None
            event_categoria = None
            name_event = None
            url_flag = None
            jug_Local = None
            logo_Local = None
            jug_Visita = None
            logo_Visita = None
            imagenIdiom = None
            channel_name = None
            text_idiom = None
            urlFinal = None
            idiomas = []

            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
            # Realizar la solicitud GET a la URL y obtener el contenido HTML
            #responseSportline = requests.get(urlportsonline)
            responseSportline = requests.get(urlportsonline, headers=headers, allow_redirects=True, verify=False)
            text_contentSportline = responseSportline.text
            text_contentSportline = responseSportline.content.decode('utf-8')
            lines_contentSportline= text_contentSportline.splitlines()
                # Lista para almacenar las extensiones de idioma (ej. "HD", "BR")
            extensiones_idioma = ["HD", "BR"]

            for line in lines_contentSportline:
                try:
                    line = line.strip()
                    if not foundDayOfWeek and line == currentDayOfWeek:
                        foundDayOfWeek = True
                    elif foundDayOfWeek:
                        if line in diassemana:
                            break
                        if any(extension in line for extension in extensiones_idioma):
                            extension = line[:4].strip()
                            idioma = line[4:].strip().capitalize()
                            idiomas.append({'extension': extension, 'idioma': idioma})
                            continue
                        if "/channels/" in line:
                            parts = line.split("|")
                            if len(parts) > 1:
                                try:
                                    name_event = parts[0].strip().encode('latin1').decode('latin1')
                                except Exception as e:
                                    name_event = parts[0].strip()
                                fecha_hora = None
                                dia_event = fecha_actual
                                 # Obtener la hora en formato de 24 horas
                                hora_event = name_event[:5]
                                hora_event_inicio = int(hora_event[:2])
                                #hora_event_inicio -= 6
                                hora_event_inicio %= 24
                                hora_event = str(hora_event_inicio).zfill(2) + hora_event[2:]

                                name_event = name_event[5:].lstrip()
                                name_event = name_event.replace(" x ", " Vs ")
                                name_event = name_event.replace(" @ ", " Vs ")
                                name_event = process_special_characters(name_event)
                                # if 'Auxerre' not in name_event:
                                #     continue

                                urlFinal = parts[1].strip()
                                    # Verifica si alguna extension de idioma esta en la urlFinal
                                for idioma_info in idiomas:
                                    extension_idioma = idioma_info['extension']
                                    if extension_idioma.lower() in urlFinal.lower():
                                        text_idiom = idioma_info['idioma']
                                        break   # Una vez que encuentras una coincidencia, sales del bucle
                                if "/pt/" in urlFinal:
                                    text_idiom = "Portuguese"

                                if contador_registros > 2 and hora_event_inicio > 18:
                                    eventNextDay = True
                                if eventNextDay and hora_event_inicio < 9:
                                    dia_event = datetime.strptime(dia_event, '%Y%m%d')
                                    dia_event += timedelta(days=1)
                                    dia_event = dia_event.strftime('%Y%m%d')

                                fecha_hora = datetime.strptime(f"{dia_event} {hora_event}", '%Y%m%d %H:%M')
                                fecha_hora -= timedelta(hours=6)
                                hora_event = fecha_hora.strftime("%H:%M")
                                fecha_hora = fecha_hora.strftime('%Y-%m-%d %H:%M')

                                list_eventos_detalles = []   # Lista para almacenar la lista de eventos
                                list_eventos_detalles_existente = []   # Lista para almacenar la lista de eventos
                                if contador_registros > 0:
                                    #existeEvent = verificarExisteEvento(hora_event,name_event)
                                    existeEvent = verificarExisteEvento(fecha_hora,name_event)
                                if existeEvent == "No" or contador_registros == 0:
                                    contador_registros += 1
                                    eventoSportLine = {
                                                        "f01_id_document": contador_registros,
                                                        "f02_proveedor" : "Sportline",
                                                        "f03_dia_event" : fecha_hora,
                                                        'f04_hora_event': hora_event,
                                                        'f05_event_categoria': event_categoria,
                                                        'f06_name_event': name_event,
                                                        'f07_URL_Flag': url_flag,
                                                        'f08_jug_Local': jug_Local,
                                                        'f09_logo_Local': logo_Local,
                                                        'f10_jug_Visita': jug_Visita,
                                                        'f11_logo_Visita': logo_Visita
                                                        }
                                    detalleSportLine = {
                                                        'f21_imagen_Idiom': imagenIdiom,
                                                        'f22_opcion_Watch': channel_name,
                                                        'f23_text_Idiom': text_idiom,
                                                        'f24_url_Final': urlFinal
                                                        }
                                    list_eventos_detalles.append(detalleSportLine)
                                        # Agregar la lista de detallesEvento al evento
                                    eventoSportLine['f20_Detalles_Evento'] = list_eventos_detalles
                                    v_list_eventos.append(eventoSportLine)
                                    print(f'Se adiciona evento y detalles desde SportLine: '
                                          f'ID: {contador_registros} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}')
                                    #v_message = (f'{hora_event} | {name_event} | {urlFinal} | {channel_name}')
                                    #asyncio.run(enviar_mensaje_telegram_channel(v_message))
                                    bool_estado_Sportline = True
                                    event_categoria = None
                                    url_flag = None
                                    jug_Local = None
                                    logo_Local = None
                                    jug_Visita = None
                                    logo_Visita = None
                                    channel_name = None
                                    imagenIdiom = None
                                    text_idiom = None
                                else:
                                    existeUrlEvent = None
                                    existeUrlEvent = verificarExisteUrlEvento(existeEvent, urlFinal)
                                    if existeUrlEvent == "Si_Existe_Url":
                                        # print(f'Ya existe Url para evento desde Sportline : {hora_event} | {name_event} | {urlFinal}')
                                        continue
                                    else:
                                        evento_existente = next((evento for evento in v_list_eventos + v_list_eventos_3 if evento.get("f01_id_document") == existeEvent), None)
                                        if evento_existente:
                                            proveedor = evento_existente['f02_proveedor']
                                            if 'Sportline' not in evento_existente['f02_proveedor']:
                                                evento_existente['f02_proveedor'] += ' | Sportline'
                                            if evento_existente['f03_dia_event'] is None and fecha_hora is not None:
                                                evento_existente['f03_dia_event'] = fecha_hora
                                            list_eventos_detalles_existente = evento_existente.get("f20_Detalles_Evento", [])
                                            detalle = {
                                                        'f21_imagen_Idiom': imagenIdiom,
                                                        'f22_opcion_Watch': channel_name,
                                                        'f23_text_Idiom': text_idiom,
                                                        'f24_url_Final': urlFinal
                                                        }
                                            # Agregar detalle al evento existente
                                            list_eventos_detalles_existente.append(detalle)
                                            evento_existente['f20_Detalles_Evento'] = list_eventos_detalles_existente
                                            if evento_existente in v_list_eventos:
                                                v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                                                print(f'Se actualiza list_eventos desde Sportline: '
                                                      f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}')
                                                # v_message = (f'{hora_event} | {name_event} | {urlFinal} | {channel_name}')
                                                # asyncio.run(enviar_mensaje_telegram_channel(v_message))
                                            else:
                                                t_eventos.delete_item(Key={"f01_id_document": existeEvent, "f02_proveedor": proveedor})
                                                t_eventos.put_item(Item=evento_existente)
                                                print(f'Se actualiza evento en BD desde Sportline: '
                                                      f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name} | proveedor: {proveedor}')

                                                # v_message = (f'{hora_event} | {name_event} | {urlFinal} | {channel_name}')
                                                # asyncio.run(enviar_mensaje_telegram_channel(v_message))
                                        channel_name = None
                                        imagenIdiom = None
                                        text_idiom = None
                except Exception as e:
                    print(f'Error en procesar_Sportline: {e}')
                    bool_estado_Sportline = False

                    continue
            # Datos del nuevo dealer
            dealerSportLine = {
                "f01_id_dealer": 2,
                "f02_dealer_name": "Sportline",
                "f03_state": bool_estado_Sportline,
            }

            # Verificar si el f01_id_dealer ya existe en vListDealers
            dealer_exists = False
            for dealer in vListDealers:
                if dealer['f01_id_dealer'] == dealerSportLine['f01_id_dealer']:
                    dealer['f03_state'] = dealerSportLine['f03_state']  # Actualizar el campo f03_state
                    dealer_exists = True
                    break  # Salir del bucle una vez actualizado el dealer

            # Si el dealer no existe, se agrega a la lista
            if not dealer_exists:
                vListDealers.append(dealerSportLine)

            print(f"Termina procesar_SportsLine")
        except Exception as e:
            print(f'Error en procesar_SportsLine: {e}')
            # Datos del nuevo dealer
            dealerSportLine = {
                "f01_id_dealer": 2,
                "f02_dealer_name": f"Sportline",
                "f03_state": False,
            }

            # Verificar si el f01_id_dealer ya existe en vListDealers
            dealer_exists = False
            for dealer in vListDealers:
                if dealer['f01_id_dealer'] == dealerSportLine['f01_id_dealer']:
                    dealer['f03_state'] = dealerSportLine['f03_state']  # Actualizar el campo f03_state
                    dealer_exists = True
                    break  # Salir del bucle una vez actualizado el dealer

            # Si el dealer no existe, se agrega a la lista
            if not dealer_exists:
                vListDealers.append(dealerSportLine)

    def procesar_DirectatvHDme():
        print(f"Inicia procesar_DirectatvHDme")
        global eventNextDay
        global event_categoria
        global url_flag
        global jug_Local
        global logo_Local
        global jug_Visita
        global logo_Visita
        global channel_name
        global imagenIdiom
        global text_idiom
        global existeEvent
        global contador_registros
        try:
            global bool_estado_DirectatvHDme
            dia_event = None
            hora_event = None
            event_categoria = None
            name_event = None
            url_flag = None
            jug_Local = None
            logo_Local = None
            jug_Visita = None
            logo_Visita = None

            imagenIdiom = None
            channel_name = None
            text_idiom = None
            urlFinal = None

            evento = {}
             # Realizar la solicitud GET a la URL y obtener el contenido HTML
             #responseDirecTVHDme = requests.get(urlDirectatvHDme)
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
            responseDirecTVHDme = requests.get(urlDirectatvHDme, headers=headers, allow_redirects=True, verify=False)

            soup = BeautifulSoup(responseDirecTVHDme.text, 'html.parser')

                # Obtener todos los elementos <tr>
            tr_elements = soup.find_all('tr')

                # Usado para evitar enlaces duplicados
            unique_enlaces = set()

            for tr_element in tr_elements:
                try:
                    # Obtener los elementos <td> dentro del <tr>
                    td_elements = tr_element.find_all('td')

                    if len(td_elements) >= 3:
                        fecha_hora = None
                        hora = td_elements[0].find(class_='t').text if td_elements[0].find(class_='t') else ''
                        url_flag = td_elements[1].find('img')['src'] if td_elements[1].find('img') else ''
                        a_element = td_elements[2].find('a')
                        hora_event = hora[:2] if len(hora) > 2 else ''

                        enlace = a_element['href'] if a_element else ''
                        text_event = a_element.find('b').text.strip().replace(' en Vivo', '') if a_element and a_element.find('b') else ''
                        event_categoria = td_elements[2].text.split(':')[0].strip()
                        try:
                            name_event = text_event.encode('latin-1', errors='ignore').decode('utf-8')
                        except UnicodeDecodeError:
                            name_event = text_event
                        name_event = process_special_characters(name_event)
                        # if 'Auxerre' not in name_event:
                        #     continue

                        enlace = enlace.strip().replace(' ', '%20')
                        enlace = re.sub(r'(.*)\.php\.php$', r'\1.php', enlace)
                            # Genera una clave unica para cada registro basada en event_categoria, name_event y enlace
                        record_key = f"{event_categoria}_{name_event}_{enlace}"

                            # Verificar que el enlace no se repita antes de imprimirlo
                        if record_key in unique_enlaces:
                            continue   # Saltar al siguiente ciclo si ya hemos visto este enlace

                        if url_flag and enlace:
                            url_flag = f'https://api.allorigins.win/raw?url=https://directatvhd.me{url_flag}'
                            enlaceLimpio = f'https://directatvhd.me{enlace}'
                            enlaceallorigins = f'https://api.allorigins.win/raw?url=https://directatvhd.me{enlace}'

                        unique_enlaces.add(record_key)

                        dia_event = fecha_actual
                        hora_event_inicio = int(hora_event[:2])
                        #hora_event_inicio -= 1
                        hora_event_inicio %= 24
                        hora_event = str(hora_event_inicio).zfill(2) + hora[2:]
                        hora_event = hora_event.replace("AM", "").replace("PM", "")

                        urlFinChannel = obtenerUrlFinalRojaHDme(enlaceallorigins)

                        if '404' in urlFinChannel:
                            EnlaceCors = convertToCorsProxyUrl(enlaceLimpio)
                            urlFinChannel = obtenerUrlFinalRojaHDme(EnlaceCors)

                        parts = urlFinChannel.split('|')

                        channel_name = parts[0].strip()
                        urlFinal = parts[1].strip()

                        if contador_registros > 2 and hora_event_inicio > 18:
                            eventNextDay = True
                            # Verificar si eventNextDay es True y hora_event < 9
                        if eventNextDay and hora_event_inicio < 9:
                                # Incrementar dia_event en 1 dia
                            dia_event = datetime.strptime(dia_event, '%Y%m%d')
                            dia_event += timedelta(days=1)
                            dia_event = dia_event.strftime('%Y%m%d')

                        fecha_hora = datetime.strptime(f"{dia_event} {hora_event}", '%Y%m%d %H:%M')
                        #fecha_hora -= timedelta(hours=1)
                        hora_event = fecha_hora.strftime("%H:%M")
                        fecha_hora = fecha_hora.strftime('%Y-%m-%d %H:%M')

                        list_eventos_detalles = []   # Lista para almacenar la lista de eventos
                        list_eventos_detalles_existente = []   # Lista para almacenar la lista de eventos
                        if contador_registros > 0:
                            #existeEvent = verificarExisteEvento(hora_event,name_event)
                            existeEvent = verificarExisteEvento(fecha_hora,name_event)

                        if existeEvent == "No" or contador_registros == 0:
                            contador_registros += 1
                            evento = {
                                    "f01_id_document": contador_registros,
                                    "f02_proveedor" : "DirectatvHDme",
                                    "f03_dia_event" : fecha_hora,
                                    'f04_hora_event': hora_event,
                                    'f05_event_categoria': event_categoria,
                                    'f06_name_event': name_event,
                                    'f07_URL_Flag': url_flag,
                                    'f08_jug_Local': jug_Local,
                                    'f09_logo_Local': logo_Local,
                                    'f10_jug_Visita': jug_Visita,
                                    'f11_logo_Visita': logo_Visita
                                    }
                            detalle = {
                                    'f21_imagen_Idiom': imagenIdiom,
                                    'f22_opcion_Watch': channel_name,
                                    'f23_text_Idiom': text_idiom,
                                    'f24_url_Final': urlFinal
                                    }
                            list_eventos_detalles.append(detalle)
                                    # Agregar la lista de detallesEvento al evento
                            evento['f20_Detalles_Evento'] = list_eventos_detalles
                            v_list_eventos.append(evento)
                            print(f'Se adiciona evento y detalles desde DirectatvHDme: '
                                  f'ID: {contador_registros} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}')
                            bool_estado_DirectatvHDme = True
                            event_categoria = None
                            url_flag = None
                            jug_Local = None
                            logo_Local = None
                            jug_Visita = None
                            logo_Visita = None
                            channel_name = None
                            imagenIdiom = None
                            text_idiom = None
                        else:
                            existeUrlEvent = None
                            existeUrlEvent = verificarExisteUrlEvento(existeEvent, urlFinal)

                            if existeUrlEvent == "Si_Existe_Url":
                                #print(f'Ya existe Url para evento desde DirectatvHDme : {hora_event} | {name_event} | {urlFinal}')
                                continue
                            else:
                                evento_existente = next((evento for evento in v_list_eventos + v_list_eventos_3 if evento.get("f01_id_document") == existeEvent), None)
                                if evento_existente:
                                    proveedor = evento_existente['f02_proveedor']
                                    if 'DirectatvHDme' not in evento_existente['f02_proveedor']:
                                        evento_existente['f02_proveedor'] += ' | DirectatvHDme'
                                    if evento_existente['f03_dia_event'] is None and fecha_hora is not None:
                                        evento_existente['f03_dia_event'] = fecha_hora
                                    if evento_existente['f07_URL_Flag'] is None and url_flag is not None:
                                        evento_existente['f07_URL_Flag'] = url_flag
                                    if evento_existente['f05_event_categoria'] is None and event_categoria is not None:
                                        evento_existente['f05_event_categoria'] = event_categoria

                                    list_eventos_detalles_existente = evento_existente.get("f20_Detalles_Evento", [])
                                    detalle = {
                                                'f21_imagen_Idiom': imagenIdiom,
                                                'f22_opcion_Watch': channel_name,
                                                'f23_text_Idiom': text_idiom,
                                                'f24_url_Final': urlFinal
                                                }
                                    # Agregar detalle al evento existente
                                    list_eventos_detalles_existente.append(detalle)
                                    evento_existente['f20_Detalles_Evento'] = list_eventos_detalles_existente
                                    if evento_existente in v_list_eventos:
                                        v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                                        print(f'Se actualiza list_eventos desde DirectatvHDme: '
                                              f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}')
                                    else:
                                        t_eventos.delete_item(Key={"f01_id_document": existeEvent, "f02_proveedor": proveedor})
                                        t_eventos.put_item(Item=evento_existente)
                                        print(f'Se actualiza evento en BD desde DirectatvHDme: '
                                              f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name} | proveedor: {proveedor}')

                                channel_name = None
                                imagenIdiom = None
                                text_idiom = None
                except Exception as e:
                    print(f'Error en procesar_DirectatvHDme: {e}')
                    bool_estado_DirectatvHDme = False

                    continue
            # Datos del nuevo dealer
            dealerDirectatvHDme = {
                "f01_id_dealer": 3,
                "f02_dealer_name": "DirectatvHDme",
                "f03_state": bool_estado_DirectatvHDme,
            }

            # Verificar si el f01_id_dealer ya existe en vListDealers
            dealer_exists = False
            for dealer in vListDealers:
                if dealer['f01_id_dealer'] == dealerDirectatvHDme['f01_id_dealer']:
                    dealer['f03_state'] = dealerDirectatvHDme['f03_state']  # Actualizar el campo f03_state
                    dealer_exists = True
                    break  # Salir del bucle una vez actualizado el dealer

            # Si el dealer no existe, se agrega a la lista
            if not dealer_exists:
                vListDealers.append(dealerDirectatvHDme)

            print(f"Termina procesar_DirectatvHDme")
        except Exception as e:
            print(f'Error en procesar_DirectatvHDme: {e}')
            # Datos del nuevo dealer
            dealerDirectatvHDme = {
                "f01_id_dealer": 3,
                "f02_dealer_name":  f"DirectatvHDme",
                "f03_state": False,
            }

            # Verificar si el f01_id_dealer ya existe en vListDealers
            dealer_exists = False
            for dealer in vListDealers:
                if dealer['f01_id_dealer'] == dealerDirectatvHDme['f01_id_dealer']:
                    dealer['f03_state'] = dealerDirectatvHDme['f03_state']  # Actualizar el campo f03_state
                    dealer_exists = True
                    break  # Salir del bucle una vez actualizado el dealer

            # Si el dealer no existe, se agrega a la lista
            if not dealer_exists:
                vListDealers.append(dealerDirectatvHDme)

    def procesar_LibreF():
        print(f"Inicia procesar_LibreF")
        global eventNextDay
        global event_categoria
        global url_flag
        global jug_Local
        global logo_Local
        global jug_Visita
        global logo_Visita
        global channel_name
        global imagenIdiom
        global text_idiom
        global existeEvent
        global contador_registros
        global bool_estado_libref
        global ind_miss_LibreF
        global currentDayOfWeek

        try:
            dia_event = None
            hora_event = None
            event_categoria = None
            name_event = None
            url_flag = None
            jug_Local = None
            logo_Local = None
            jug_Visita = None
            logo_Visita = None

            imagenIdiom = None
            channel_name = None
            text_idiom = None
            urlFinal = None
            urlInicial = None

            evento = {}

            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
            response = requests.get(urlLibreF, headers=headers, allow_redirects=True, verify=False)
            response.encoding = 'utf-8'
            soup = BeautifulSoup(response.text, 'html.parser')

            b_tag = soup.find('b')

            dia_ingles = None
            if b_tag:
                text = b_tag.text
                date_text = text.split(' - ')[1]
                first_word_spanish = date_text.split()[0]
                dia_sin_acentos = unidecode(first_word_spanish)
                dia_ingles = dias_traducidos.get(dia_sin_acentos, "Unknown day")

            if dia_ingles == currentDayOfWeek:
                eventos = soup.find_all('li')
                if ind_miss_LibreF == 0:
                    for evento in eventos:
                        try:
                            linkElement = evento.find('a')
                            linkText = linkElement.text if linkElement else ''
                            firstColonIndex = linkText.find(':')
                            if firstColonIndex != -1:
                                event_categoria = linkText[:firstColonIndex + 1].strip()
                                textEvent = linkText[firstColonIndex + 1:].split('\n')[0].strip()   # Obtener la primera linea
                            else:
                                event_categoria = linkText
                                textEvent = ''
                            event_categoria = event_categoria.replace(":", "")
                            event_categoria = process_special_characters(event_categoria)
                                # Decodificar el texto del evento (puede ser necesario si hay caracteres especiales)
                            try:
                                name_event = textEvent.encode('latin1').decode('utf8')
                            except:
                                name_event = textEvent
                            name_event = name_event.replace("vs.", "Vs").strip()
                            name_event = process_special_characters(name_event)
                            # if 'Cali' not in name_event:
                            #     continue
                            hora_evento = evento.find(class_='t').text if evento.find(class_='t') else ''
                                # Obtener todos los elementos de tipo <li> que son hijos de evento
                            canalesYEnlaces = evento.select('ul > li.subitem1')

                                # Recorrer cada <li> hijo para obtener los canales y enlaces
                            for ce in canalesYEnlaces:
                                try:
                                    fecha_hora = None
                                    hora_event_inicio = None
                                    chanel = ce.find('a').contents[0] if ce.find('a') else ''
                                    #enlace = '/es' + ce.find('a')['href'] if ce.find('a') else ''
                                    enlace = ce.find('a')['href'] if ce.find('a') else ''
                                    try:
                                        channel = chanel.encode('latin1').decode('utf8')
                                    except:
                                        channel = chanel
                                    if enlace.startswith("https://librefutboltv"):
                                        enlace = enlace.replace("https://librefutboltv","https://futbollibrehd")
                                    if "https://futbollibrehd.net" not in enlace:
                                        enlace = 'https://futbollibrehd.net' + enlace
                                    #urlcors = validate_and_get_url(enlace)
                                    urlcors = enlace
                                    if "/embed/" not in urlcors:
                                        urlIni = obtenerUrlFinalLibreTV(urlcors)
                                    else:
                                        urlIni = obtenerUrlFinalLibreTVSelenium(urlcors)
                                    dia_event = fecha_actual
                                    hora_event_inicio = int(hora_evento[:2])
                                    # hora_event_inicio -= 6
                                    hora_event_inicio %= 24
                                    hora_event = str(hora_event_inicio).zfill(2) + hora_evento[2:]

                                    if contador_registros > 2 and hora_event_inicio > 18:
                                        eventNextDay = True
                                        # Verificar si eventNextDay es True y hora_event < 9
                                    if eventNextDay and hora_event_inicio < 9:
                                            # Incrementar dia_event en 1 dia
                                        dia_event = datetime.strptime(dia_event, '%Y%m%d')
                                        dia_event += timedelta(days=1)
                                        dia_event = dia_event.strftime('%Y%m%d')

                                    fecha_hora = datetime.strptime(f"{dia_event} {hora_event}", '%Y%m%d %H:%M')
                                    fecha_hora -= timedelta(hours=6)
                                    hora_event = fecha_hora.strftime("%H:%M")
                                    fecha_hora = fecha_hora.strftime('%Y-%m-%d %H:%M')
                                    # : sin_data
                                    if urlIni is None:
                                        bool_estado_libref = False
                                        urlIni = urlcors + ' | sin_data'
                                        channel = channel + ' | sin_data'
                                    else:
                                        if "/star-plus/" in urlIni or "/vix-plus/" in urlIni or 'sin_data' not in urlIni:
                                            bool_estado_libref = False
                                            urlIni = urlcors + ' | sin_data'
                                            channel = channel + ' | sin_data'

                                    if contador_registros > 0:
                                        #existeEvent = verificarExisteEvento(hora_event,name_event)
                                        existeEvent = verificarExisteEvento(fecha_hora,name_event)
                                    if existeEvent == "No" or contador_registros == 0:
                                        contador_registros += 1
                                        eventoLibreF = {
                                                "f01_id_document": contador_registros,
                                                "f02_proveedor" : "LibreF",
                                                "f03_dia_event" : fecha_hora,
                                                'f04_hora_event': hora_event,
                                                'f05_event_categoria': event_categoria,
                                                'f06_name_event': name_event,
                                                'f07_URL_Flag': url_flag,
                                                'f08_jug_Local': jug_Local,
                                                'f09_logo_Local': logo_Local,
                                                'f10_jug_Visita': jug_Visita,
                                                'f11_logo_Visita': logo_Visita
                                                }
                                    list_eventos_detalles = []   # Lista para almacenar la lista de eventos
                                    list_eventos_detalles_existente = []   # Lista para almacenar la lista de eventos
                                    if ' | ' in urlIni:
                                            # Resto del codigo aqui
                                        url_final_parts = urlIni.split(' | ')
                                        for i in range(0, len(url_final_parts), 2):
                                            try:
                                                # Obtener la URL y el channelName
                                                urlFinal = url_final_parts[i]
                                                urlFinal = urlFinal.replace(" ","")
                                                channel_name = channel + ': ' + url_final_parts[i + 1] if i + 1 < len(url_final_parts) else None
                                                if urlFinal.startswith('/embed/'):
                                                    urlFinal = 'https://futbollibrehd.net' + urlFinal
                                                existeUrlEvent = None
                                                if existeEvent == "No":
                                                    detalleLibreF = {
                                                                'f21_imagen_Idiom': imagenIdiom,
                                                                'f22_opcion_Watch': channel_name,
                                                                'f23_text_Idiom': text_idiom,
                                                                'f24_url_Final': urlFinal
                                                                }
                                                    list_eventos_detalles.append(detalleLibreF)
                                                    print(f'Se adiciona evento y detalles desde LibreF: '
                                                          f'ID: {contador_registros} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}')
                                                    event_categoria = None
                                                    url_flag = None
                                                    jug_Local = None
                                                    logo_Local = None
                                                    jug_Visita = None
                                                    logo_Visita = None
                                                    channel_name = None
                                                    imagenIdiom = None
                                                    text_idiom = None
                                                else:
                                                    existeUrlEvent = verificarExisteUrlEvento(existeEvent, urlFinal)
                                                    if existeUrlEvent == "Si_Existe_Url":
                                                        #(f'Ya existe Url para evento desde LibreF : {hora_event} | {name_event} | {urlFinal} | {channel_name}')
                                                        continue
                                                    else:
                                                        evento_existente = next((evento for evento in v_list_eventos + v_list_eventos_3 if evento.get("f01_id_document") == existeEvent), None)
                                                        if evento_existente:
                                                            proveedor = evento_existente['f02_proveedor']
                                                            if 'LibreF' not in evento_existente['f02_proveedor']:
                                                                evento_existente['f02_proveedor'] += ' | LibreF'
                                                            if evento_existente['f03_dia_event'] is None and fecha_hora is not None:
                                                                evento_existente['f03_dia_event'] = fecha_hora
                                                            if evento_existente['f07_URL_Flag'] is None and url_flag is not None:
                                                                evento_existente['f07_URL_Flag'] = url_flag
                                                            if evento_existente['f05_event_categoria'] is None and event_categoria is not None:
                                                                evento_existente['f05_event_categoria'] = event_categoria

                                                            list_eventos_detalles_existente = evento_existente.get("f20_Detalles_Evento", [])
                                                            detalle = {
                                                                        'f21_imagen_Idiom': imagenIdiom,
                                                                        'f22_opcion_Watch': channel,
                                                                        'f23_text_Idiom': text_idiom,
                                                                        'f24_url_Final': urlFinal
                                                                        }
                                                            # Agregar detalle al evento existente
                                                            list_eventos_detalles_existente.append(detalle)
                                                            evento_existente['f20_Detalles_Evento'] = list_eventos_detalles_existente
                                                            if evento_existente in v_list_eventos:
                                                                v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                                                                print(f'Se actualiza list_eventos desde LibreF: '
                                                                      f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}')
                                                            #debug
                                                            #pdb.set_trace()
                                                            else:
                                                                t_eventos.delete_item(Key={"f01_id_document": existeEvent, "f02_proveedor": proveedor})
                                                                t_eventos.put_item(Item=evento_existente)
                                                                print(f'Se actualiza evento en BD desde LibreF: '
                                                                      f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name} | proveedor: {proveedor}')

                                                channel_name = None
                                                imagenIdiom = None
                                                text_idiom = None
                                            except Exception as e:
                                                print(f"Error en procesar_libreF 1: {e}")
                                                bool_estado_libref = False
                                                continue
                                        if existeEvent == "No":
                                                # Agregar la lista de detallesEvento al evento
                                            eventoLibreF['f20_Detalles_Evento'] = list_eventos_detalles
                                            v_list_eventos.append(eventoLibreF)
                                    else:  # if not ' | ' in urlIni:
                                        urlFinal = urlIni.replace(" ","")
                                        if urlFinal.startswith('/embed/'):
                                            urlFinal = 'https://futbollibrehd.net' + urlFinal
                                        existeUrlEvent = None
                                        if existeEvent == "No":
                                            detalleLibreF = {
                                                    'f21_imagen_Idiom': imagenIdiom,
                                                    'f22_opcion_Watch': channel,
                                                    'f23_text_Idiom': text_idiom,
                                                    'f24_url_Final': urlFinal
                                                    }
                                            list_eventos_detalles.append(detalleLibreF)
                                            eventoLibreF['f20_Detalles_Evento'] = list_eventos_detalles
                                            v_list_eventos.append(eventoLibreF)
                                            print(f'Se adiciona evento y detalles desde LibreF: '
                                                  f'ID: {contador_registros} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel}')
                                            event_categoria = None
                                            url_flag = None
                                            jug_Local = None
                                            logo_Local = None
                                            jug_Visita = None
                                            logo_Visita = None
                                            channel = None
                                            imagenIdiom = None
                                            text_idiom = None
                                        else:
                                            existeUrlEvent = verificarExisteUrlEvento(existeEvent, urlFinal)
                                            if existeUrlEvent == "Si_Existe_Url":
                                                #print(f'Ya existe Url para evento desde LibreF : {hora_event} | {name_event} | {urlFinal} | {channel}')
                                                continue
                                            else:
                                                evento_existente = next((evento for evento in v_list_eventos + v_list_eventos_3 if evento.get("f01_id_document") == existeEvent), None)
                                                if evento_existente:
                                                    proveedor = evento_existente['f02_proveedor']
                                                    if 'LibreF' not in evento_existente['f02_proveedor']:
                                                        evento_existente['f02_proveedor'] += ' | LibreF'
                                                    if evento_existente['f03_dia_event'] is None and fecha_hora is not None:
                                                        evento_existente['f03_dia_event'] = fecha_hora
                                                    if evento_existente['f07_URL_Flag'] is None and url_flag is not None:
                                                        evento_existente['f07_URL_Flag'] = url_flag
                                                    if evento_existente['f05_event_categoria'] is None and event_categoria is not None:
                                                        evento_existente['f05_event_categoria'] = event_categoria

                                                    list_eventos_detalles_existente = evento_existente.get("f20_Detalles_Evento", [])
                                                    detalle = {
                                                                'f21_imagen_Idiom': imagenIdiom,
                                                                'f22_opcion_Watch': channel,
                                                                'f23_text_Idiom': text_idiom,
                                                                'f24_url_Final': urlFinal
                                                                }
                                                    # Agregar detalle al evento existente
                                                    list_eventos_detalles_existente.append(detalle)
                                                    evento_existente['f20_Detalles_Evento'] = list_eventos_detalles_existente
                                                    if evento_existente in v_list_eventos:
                                                        v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                                                        print(f'Se actualiza list_eventos desde LibreF: '
                                                              f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel}')
                                                    #debug
                                                    #pdb.set_trace()
                                                    else:
                                                        t_eventos.delete_item(Key={"f01_id_document": existeEvent, "f02_proveedor": proveedor})
                                                        t_eventos.put_item(Item=evento_existente)
                                                        print(f'Se actualiza evento en BD desde LibreF: '
                                                              f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel}  | proveedor: {proveedor}')

                                                channel = None
                                                imagenIdiom = None
                                                text_idiom = None
                                except Exception as e:
                                    print(f"Error en procesar_libreF 2: {e}")
                                    bool_estado_libref = False
                                    continue
                        except Exception as e:
                            print(f"Error en procesar_libreF 3: {e}")
                            bool_estado_libref = False
                            continue
                else:
                    for event_index, events_miss in enumerate(v_list_eventos):
                        try:
                            #print(f"events_miss: {events_miss}")
                            detalles_miss_evento = events_miss.get('f20_Detalles_Evento', [])
                            #eventCategoria = events_miss.get('f05_event_categoria')
                            name_event = events_miss.get('f06_name_event')
                            # Crear el texto de busqueda
                            #eventoAbuscar = f"{eventCategoria}: {nameEvent.replace('Vs', 'vs.')}"
                            eventoAbuscar = f"{name_event.replace('Vs', 'vs.')}"
                            for detalle_index, detalle_miss in enumerate(detalles_miss_evento):
                                opcionWatch = detalle_miss.get('f22_opcion_Watch')
                                if opcionWatch is not None and 'sin_data' in opcionWatch:
                                    if "|" in opcionWatch:
                                        opcionWatch = opcionWatch.split("|")[0].strip()
                                    eventoAbuscar = eventoAbuscar.replace(" vs ", " vs. ").strip()
                                    # Encontrar eventos con una similitud alta
                                    #matching_eventos = [evento_li for evento_li in eventos if eventoAbuscar in evento_li.text.encode('latin1').decode('utf8')]

                                    eventoAbuscar = eventoAbuscar.lower()
                                    #print(f"eventoAbuscar: {eventoAbuscar}")
                                    for evento_li in eventos:

                                        linkElement = evento_li.find('a')
                                        linkText = linkElement.text if linkElement else ''
                                        firstColonIndex = linkText.find(':')
                                        if firstColonIndex != -1:
                                            textEvent = linkText[firstColonIndex + 1:].split('\n')[0].strip()   # Obtener la primera linea
                                            #print(f"textEvent: {textEvent.lower()}")
                                        #texto_completo = evento_li.get_text()
                                        #name_event = next((line.strip() for line in texto_completo.splitlines() if line.strip()), '')

                                        #if fuzz.partial_ratio(eventoAbuscar, name_event.lower()) >= 80:
                                            matching_eventos = [evento_li for evento_li in eventos if fuzz.partial_ratio(eventoAbuscar, textEvent.lower()) >= 75]
                                            if matching_eventos:
                                                #print(f"matching_eventos: {matching_eventos}")
                                                break

                                    if not matching_eventos:
                                        continue
                                    enlace_opcionWatch = None
                                    for evento_li in matching_eventos:
                                        links = evento_li.find_all('a', href=lambda value: value and value != '#')
                                        #print(f"opcionWatch: {opcionWatch}")
                                        #print(f"links: {links}")
                                        for link in links:
                                            if opcionWatch in link.text:
                                                enlace_opcionWatch = link.get('href')
                                                break
                                        if enlace_opcionWatch:
                                            break
                                    #if enlace_opcionWatch:
                                    urlInicial = "https://librefutboltv.net/" + enlace_opcionWatch
                                    #urlInicial =  validate_and_get_url(urlInicial)
                                    if "/embed/" not in urlInicial:
                                        urlFin = obtenerUrlFinalLibreTV(urlInicial)
                                    else:
                                        urlFin = obtenerUrlFinalLibreTVSelenium(urlInicial)

                                    if urlFin is None:
                                        bool_estado_libref = False
                                        urlFin = urlInicial

                                    if urlFin.startswith('//'):
                                        urlFin = 'https:' + urlFin
                                    if urlFin.startswith('/embed/'):
                                        urlFin = 'https://futbollibrehd.net' + urlFin
                                    if urlFin is not None and urlFin != urlInicial:
                                        document_id = events_miss.get('f01_id_document')
                                        imagenIdiom = detalle_miss.get('f21_imagen_Idiom')
                                        text_idiom = detalle_miss.get('f23_text_Idiom')
                                        #debug
                                        #pdb.set_trace()
                                        evento_existente = next((evento for evento in v_list_eventos if evento["f01_id_document"] == document_id), None)
                                        evento_existente['f20_Detalles_Evento'][detalle_index].update({
                                                                'f22_opcion_Watch': opcionWatch,
                                                                'f21_imagen_Idiom': imagenIdiom,
                                                                'f23_text_Idiom': text_idiom,
                                                                'f24_url_Final': urlFin
                                                            })
                                        t_eventos.put_item(Item=evento_existente)
                                        print(f'Se actualiza evento en BD desde LibreF Miss: '
                                              f'ID: {document_id} | {name_event} | {urlFin} | {opcionWatch}')

                        except Exception as e:
                            print(f"Error en obtener_eventos_miss desde LibreF MISS: {(e)}")

                # Datos del nuevo dealer
                dealerLibreF = {
                    "f01_id_dealer": 4,
                    "f02_dealer_name": "LibreF",
                    "f03_state": bool_estado_libref,
                }

                # Verificar si el f01_id_dealer ya existe en vListDealers
                dealer_exists = False
                for dealer in vListDealers:
                    if dealer['f01_id_dealer'] == dealerLibreF['f01_id_dealer']:
                        dealer['f03_state'] = dealerLibreF['f03_state']  # Actualizar el campo f03_state
                        dealer_exists = True
                        break  # Salir del bucle una vez actualizado el dealer

                # Si el dealer no existe, se agrega a la lista
                if not dealer_exists:
                    vListDealers.append(dealerLibreF)

                print(f"Termina procesar_LibreF")
        except Exception as e:
            print(f"Error en procesar_libreF 4: {e}")
            # Datos del nuevo dealer
            dealerLibreF = {
                "f01_id_dealer": 4,
                "f02_dealer_name": f"LibreF",
                "f03_state": False,
            }

            # Verificar si el f01_id_dealer ya existe en vListDealers
            dealer_exists = False
            for dealer in vListDealers:
                if dealer['f01_id_dealer'] == dealerLibreF['f01_id_dealer']:
                    dealer['f03_state'] = dealerLibreF['f03_state']  # Actualizar el campo f03_state
                    dealer_exists = True
                    break  # Salir del bucle una vez actualizado el dealer

            # Si el dealer no existe, se agrega a la lista
            if not dealer_exists:
                vListDealers.append(dealerLibreF)

    def procesar_RojaOnline():
        print(f"Inicia procesar_RojaOnline")
        global eventNextDay
        global event_categoria
        global url_flag
        global jug_Local
        global logo_Local
        global jug_Visita
        global logo_Visita
        global channel_name
        global imagenIdiom
        global text_idiom
        global existeEvent
        global contador_registros
        global bool_estado_RojaOn
        try:
            dia_event = None
            hora_event = None
            event_categoria = None
            name_event = None
            url_flag = None
            jug_Local = None
            logo_Local = None
            jug_Visita = None
            logo_Visita = None

            imagenIdiom = None
            channel_name = None
            text_idiom = None
            urlFinal = None

            evento = {}
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
            responseRojaOn = requests.get(urlRojaOn, headers=headers, allow_redirects=True, verify=False)
            #responseRojaOn = requests.get(urlRojaOn)
            soup = BeautifulSoup(responseRojaOn.text, 'html.parser')
                # Obtener las filas de la tabla
            tableRows = soup.find_all('tr')

            for row in tableRows:
                try:
                    linkElement = row.find_all('td')
                    if len(linkElement) > 0:
                            # Accede a la informacion que necesitas en funcion de la posicion de las celdas
                        hora_event = linkElement[0].text
                            # Selecciona el elemento 'a' dentro de la tercera columna
                        name_element = linkElement[2].find('a')
                        name_event_complet = linkElement[2].text.strip()
                        if "rojadirectahdenvivo" in name_element['href']:
                            #url = name_element['href']
                            continue
                        #url = "http://tarjetarojatvonline.sx" + name_element['href']
                        url = "https://ww1.tarjetarojatvonline.sx" + name_element['href']
                        url = validate_and_get_url(url)
                        partes_event_name = name_event_complet.split(':')
                        name_event = partes_event_name[1].strip()
                        event_categoria = partes_event_name[0].strip()
                        name_event = process_special_characters(name_event)
                        # if 'Auxerre' not in name_event:
                        #     continue
                        event_categoria = process_special_characters(event_categoria)
                        if 'resultado.rojadirectaonlinetv.net' in url:
                            continue   # Omitir el registro actual y continuar con el siguiente
                        channel_url = obtenerUrlFinalRojaOn(url)

                        if channel_url:
                            fecha_hora = None
                            channel_name, urlFinal = channel_url.split(" | ", 1)
                            urlFinal = urlFinal.replace(" ","")
                            dia_event = fecha_actual
                            hora_event_inicio = int(hora_event[:2])
                            #hora_event_inicio -= 1
                            hora_event_inicio %= 24
                            hora_event = str(hora_event_inicio).zfill(2) + hora_event[2:]

                            if contador_registros > 2 and hora_event_inicio > 18:
                                eventNextDay = True

                                # Verificar si eventNextDay es True y hora_event < 9
                            if eventNextDay and hora_event_inicio < 9:
                                    # Incrementar dia_event en 1 dia
                                dia_event = datetime.strptime(dia_event, '%Y%m%d')
                                dia_event += timedelta(days=1)
                                dia_event = dia_event.strftime('%Y%m%d')

                            fecha_hora = datetime.strptime(f"{dia_event} {hora_event}", '%Y%m%d %H:%M')
                            #fecha_hora -= timedelta(hours=6)
                            hora_event = fecha_hora.strftime("%H:%M")
                            fecha_hora = fecha_hora.strftime('%Y-%m-%d %H:%M')

                            list_eventos_detalles = []   # Lista para almacenar la lista de eventos
                            list_eventos_detalles_existente = []   # Lista para almacenar la lista de eventos
                            if contador_registros > 0:
                                #existeEvent = verificarExisteEvento(hora_event,name_event)
                                existeEvent = verificarExisteEvento(fecha_hora,name_event)

                            if existeEvent == "No" or contador_registros == 0:
                                contador_registros += 1
                                evento = {
                                        "f01_id_document": contador_registros,
                                        "f02_proveedor" : "RojaOn",
                                        "f03_dia_event" : fecha_hora,
                                        'f04_hora_event': hora_event,
                                        'f05_event_categoria': event_categoria,
                                        'f06_name_event': name_event,
                                        'f07_URL_Flag': url_flag,
                                        'f08_jug_Local': jug_Local,
                                        'f09_logo_Local': logo_Local,
                                        'f10_jug_Visita': jug_Visita,
                                        'f11_logo_Visita': logo_Visita
                                        }
                                detalle = {
                                        'f21_imagen_Idiom': imagenIdiom,
                                        'f22_opcion_Watch': channel_name,
                                        'f23_text_Idiom': text_idiom,
                                        'f24_url_Final': urlFinal
                                        }
                                list_eventos_detalles.append(detalle)
                                        # Agregar la lista de detallesEvento al evento
                                evento['f20_Detalles_Evento'] = list_eventos_detalles
                                v_list_eventos.append(evento)
                                print(f'Se adiciona evento y detalles desde RojaOn: '
                                      f'ID: {contador_registros} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}')
                                bool_estado_RojaOn = True
                                event_categoria = None
                                url_flag = None
                                jug_Local = None
                                logo_Local = None
                                jug_Visita = None
                                logo_Visita = None
                                channel_name = None
                                imagenIdiom = None
                                text_idiom = None
                            else:
                                existeUrlEvent = None
                                existeUrlEvent = verificarExisteUrlEvento(existeEvent, urlFinal)

                                if existeUrlEvent == "Si_Existe_Url":
                                    #print(f'Ya existe Url para evento desde RojaOn : {hora_event} | {name_event} | {urlFinal}')
                                    continue
                                else:
                                    evento_existente = next((evento for evento in v_list_eventos + v_list_eventos_3 if evento.get("f01_id_document") == existeEvent), None)
                                    if evento_existente:
                                        proveedor = evento_existente['f02_proveedor']
                                        if 'RojaOn' not in evento_existente['f02_proveedor']:
                                            evento_existente['f02_proveedor'] += ' | RojaOn'
                                        if evento_existente['f03_dia_event'] is None and fecha_hora is not None:
                                            evento_existente['f03_dia_event'] = fecha_hora
                                        if evento_existente['f07_URL_Flag'] is None and url_flag is not None:
                                            evento_existente['f07_URL_Flag'] = url_flag
                                        if evento_existente['f05_event_categoria'] is None and event_categoria is not None:
                                            evento_existente['f05_event_categoria'] = event_categoria

                                        list_eventos_detalles_existente = evento_existente.get("f20_Detalles_Evento", [])
                                        detalle = {
                                                    'f21_imagen_Idiom': imagenIdiom,
                                                    'f22_opcion_Watch': channel_name,
                                                    'f23_text_Idiom': text_idiom,
                                                    'f24_url_Final': urlFinal
                                                    }
                                        # Agregar detalle al evento existente
                                        list_eventos_detalles_existente.append(detalle)
                                        evento_existente['f20_Detalles_Evento'] = list_eventos_detalles_existente
                                        if evento_existente in v_list_eventos:
                                            v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                                            print(f'Se actualiza list_eventos desde RojaOn: '
                                                  f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}')
                                        else:
                                            t_eventos.delete_item(Key={"f01_id_document": existeEvent, "f02_proveedor": proveedor})
                                            t_eventos.put_item(Item=evento_existente)
                                            print(f'Se actualiza evento en BD desde RojaOn: '
                                                  f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name} | proveedor: {proveedor}')

                                    channel_name = None
                                    imagenIdiom = None
                                    text_idiom = None
                except Exception as e:
                    print(f"Error en procesar_RojaOn: {e}")
                    bool_estado_RojaOn = False
                    continue

            # Datos del nuevo dealer
            dealerRojaOn = {
                "f01_id_dealer": 5,
                "f02_dealer_name": "RojaOn",
                "f03_state": bool_estado_RojaOn,
            }

            # Verificar si el f01_id_dealer ya existe en vListDealers
            dealer_exists = False
            for dealer in vListDealers:
                if dealer['f01_id_dealer'] == dealerRojaOn['f01_id_dealer']:
                    dealer['f03_state'] = dealerRojaOn['f03_state']  # Actualizar el campo f03_state
                    dealer_exists = True
                    break  # Salir del bucle una vez actualizado el dealer

            # Si el dealer no existe, se agrega a la lista
            if not dealer_exists:
                vListDealers.append(dealerRojaOn)
            print(f"Termina procesar_RojaOnline")
        except Exception as e:
            print(f"Error en procesar_RojaOn: {e}")
            # Datos del nuevo dealer
            dealerRojaOn = {
                "f01_id_dealer": 5,
                "f02_dealer_name": f"RojaOn",
                "f03_state": False,
            }

            # Verificar si el f01_id_dealer ya existe en vListDealers
            dealer_exists = False
            for dealer in vListDealers:
                if dealer['f01_id_dealer'] == dealerRojaOn['f01_id_dealer']:
                    dealer['f03_state'] = dealerRojaOn['f03_state']  # Actualizar el campo f03_state
                    dealer_exists = True
                    break  # Salir del bucle una vez actualizado el dealer

            # Si el dealer no existe, se agrega a la lista
            if not dealer_exists:
                vListDealers.append(dealerRojaOn)

    def procesar_RojaTV():
        print(f"Inicia procesar_RojaTV")
        global eventNextDay
        global event_categoria
        global url_flag
        global jug_Local
        global logo_Local
        global jug_Visita
        global logo_Visita
        global channel_name
        global imagenIdiom
        global text_idiom
        global existeEvent
        global contador_registros
        try:
            global bool_estado_RojaTv
            dia_event = None
            hora_event = None
            event_categoria = None
            name_event = None
            url_flag = None
            jug_Local = None
            logo_Local = None
            jug_Visita = None
            logo_Visita = None

            imagenIdiom = None
            channel_name = None
            text_idiom = None
            urlFinal = None
            evento = {}
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
            response = requests.get(urlRojaTV, headers=headers, allow_redirects=True, verify=False)
            #response = requests.get(urlRojaTV)
            soup = BeautifulSoup(response.text, 'html.parser')

                # Encuentra todos los elementos <tr>
            RowsRoja = soup.find_all('tr')
            # print(f'urlRojaTV : {urlRojaTV}')
            # print(f'response : {response.status_code}')
            for row in RowsRoja:
                try:
                    tds = row.find_all('td')
                    if len(tds) >= 3:
                        fecha_hora = None
                        hora_event = tds[0].find('span').text
                        tdEvent = tds[2]

                            # Utiliza expresiones regulares para extraer el contenido entre <b></b>
                        match = re.search(r"<b>(.*?)<\/b>", tdEvent.decode_contents())
                        newEvent = match.group(1).replace('en Vivo', '').strip() if match else ''

                        textComplet = tdEvent.text.replace('en Vivo', '').strip()
                        indexOfNewEvent = textComplet.find(newEvent)

                        if indexOfNewEvent != -1:
                            event_categoria = textComplet[:indexOfNewEvent].strip()  # + ' '
                        else:
                            event_categoria = textComplet  # + ' '

                        try:
                            event_categoria = event_categoria.encode('latin1').decode('utf8')
                        except:
                            pass

                            # Construye la URL completa
                        aTag = tdEvent.find('a')
                        url = "https://tarjetarojatv.run" + (aTag['href'] if aTag else '')
                        title_channel = aTag['href'].split('/')[-1].split('.')[0]
                        title_channel = capitalize_words(title_channel)
                        #debug
                        #pdb.set_trace()

                        url =  validate_and_get_url(url)

                        if 'httpswww' in url:
                            continue

                        try:
                            name_event = newEvent.encode('latin1').decode('utf8')
                        except:
                            name_event = newEvent
                        name_event = process_special_characters(name_event)
                        # if 'Auxerre' not in name_event:
                        #     continue

                        urlFinal = obtenerUrlFinalRojaTV(url)
                        if '408' in urlFinal:
                            time.sleep(2)
                            urlFinal = obtenerUrlFinalRojaTV(url)

                        if "sin_data" in urlFinal:
                            bool_estado_RojaTv = False

                        #urlFinal, channel_name = urlFinal_title.split('|')
                        urlFinal = urlFinal.replace(" ","")
                        #channel_name = channel_name.strip()
                        channel_name = title_channel
                        dia_event = fecha_actual
                        hora_event_inicio = int(hora_event[:2])
                        #hora_event_inicio -= 1
                        hora_event_inicio %= 24
                        hora_event = str(hora_event_inicio).zfill(2) + hora_event[2:]

                        if contador_registros > 2 and hora_event_inicio > 18:
                            eventNextDay = True
                            # Verificar si eventNextDay es True y hora_event < 9
                        if eventNextDay and hora_event_inicio < 9:
                                # Incrementar dia_event en 1 dia
                            dia_event = datetime.strptime(dia_event, '%Y%m%d')
                            dia_event += timedelta(days=1)
                            dia_event = dia_event.strftime('%Y%m%d')

                        fecha_hora = datetime.strptime(f"{dia_event} {hora_event}", '%Y%m%d %H:%M')
                        #fecha_hora -= timedelta(hours=6)
                        hora_event = fecha_hora.strftime("%H:%M")
                        fecha_hora = fecha_hora.strftime('%Y-%m-%d %H:%M')

                        list_eventos_detalles = []   # Lista para almacenar la lista de eventos
                        list_eventos_detalles_existente = []   # Lista para almacenar la lista de eventos
                        if contador_registros > 0:
                            #existeEvent = verificarExisteEvento(hora_event,name_event)
                            existeEvent = verificarExisteEvento(fecha_hora,name_event)

                        if existeEvent == "No" or contador_registros == 0:
                            contador_registros += 1
                            evento = {
                                    "f01_id_document": contador_registros,
                                    "f02_proveedor" : "RojaTv",
                                    "f03_dia_event" : fecha_hora,
                                    'f04_hora_event': hora_event,
                                    'f05_event_categoria': event_categoria,
                                    'f06_name_event': name_event,
                                    'f07_URL_Flag': url_flag,
                                    'f08_jug_Local': jug_Local,
                                    'f09_logo_Local': logo_Local,
                                    'f10_jug_Visita': jug_Visita,
                                    'f11_logo_Visita': logo_Visita
                                    }
                            detalle = {
                                    'f21_imagen_Idiom': imagenIdiom,
                                    'f22_opcion_Watch': channel_name,
                                    'f23_text_Idiom': text_idiom,
                                    'f24_url_Final': urlFinal
                                    }
                            list_eventos_detalles.append(detalle)
                                    # Agregar la lista de detallesEvento al evento
                            evento['f20_Detalles_Evento'] = list_eventos_detalles
                            v_list_eventos.append(evento)
                            print(f'Se adiciona evento y detalles desde RojaTv: '
                                  f'ID: {contador_registros} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}')
                            bool_estado_RojaTv = True
                            event_categoria = None
                            url_flag = None
                            jug_Local = None
                            logo_Local = None
                            jug_Visita = None
                            logo_Visita = None
                            channel_name = None
                            imagenIdiom = None
                            text_idiom = None
                        else:
                            existeUrlEvent = None
                            existeUrlEvent = verificarExisteUrlEvento(existeEvent, urlFinal)

                            if existeUrlEvent == "Si_Existe_Url":
                                #print(f'Ya existe Url para evento desde RojaTv : {hora_event} | {name_event} | {urlFinal} | {channel_name}')
                                continue
                            else:
                                evento_existente = next((evento for evento in v_list_eventos + v_list_eventos_3 if evento.get("f01_id_document") == existeEvent), None)
                                if evento_existente:
                                    proveedor = evento_existente['f02_proveedor']
                                    if 'RojaTv' not in evento_existente['f02_proveedor']:
                                        evento_existente['f02_proveedor'] += ' | RojaTv'
                                    if evento_existente['f03_dia_event'] is None and fecha_hora is not None:
                                        evento_existente['f03_dia_event'] = fecha_hora
                                    if evento_existente['f07_URL_Flag'] is None and url_flag is not None:
                                        evento_existente['f07_URL_Flag'] = url_flag
                                    if evento_existente['f05_event_categoria'] is None and event_categoria is not None:
                                        evento_existente['f05_event_categoria'] = event_categoria

                                    list_eventos_detalles_existente = evento_existente.get("f20_Detalles_Evento", [])
                                    detalle = {
                                                'f21_imagen_Idiom': imagenIdiom,
                                                'f22_opcion_Watch': channel_name,
                                                'f23_text_Idiom': text_idiom,
                                                'f24_url_Final': urlFinal
                                                }
                                    # Agregar detalle al evento existente
                                    list_eventos_detalles_existente.append(detalle)
                                    evento_existente['f20_Detalles_Evento'] = list_eventos_detalles_existente
                                    if evento_existente in v_list_eventos:
                                        v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                                        print(f'Se actualiza list_eventos desde RojaTv: '
                                              f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}')
                                    else:
                                        t_eventos.delete_item(Key={"f01_id_document": existeEvent, "f02_proveedor": proveedor})
                                        t_eventos.put_item(Item=evento_existente)
                                        print(f'Se actualiza evento en BD desde RojaTv: '
                                              f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name} | proveedor: {proveedor}')
                                channel_name = None
                                imagenIdiom = None
                                text_idiom = None
                except Exception as e:
                    print(f"Error en procesar_RojaTv: {str(e)}")
                    bool_estado_RojaTv = False
                    continue

            # Datos del nuevo dealer
            dealerRojaTv = {
                "f01_id_dealer": 6,
                "f02_dealer_name": "RojaTv",
                "f03_state": bool_estado_RojaTv,
            }

            # Verificar si el f01_id_dealer ya existe en vListDealers
            dealer_exists = False
            for dealer in vListDealers:
                if dealer['f01_id_dealer'] == dealerRojaTv['f01_id_dealer']:
                    dealer['f03_state'] = dealerRojaTv['f03_state']  # Actualizar el campo f03_state
                    dealer_exists = True
                    break  # Salir del bucle una vez actualizado el dealer

            # Si el dealer no existe, se agrega a la lista
            if not dealer_exists:
                vListDealers.append(dealerRojaTv)
            print(f"Termina procesar_RojaTV")
        except Exception as e:
            print(f"Error en procesar_RojaTv: {str(e)}")
            # Datos del nuevo dealer
            dealerRojaTv = {
                "f01_id_dealer": 6,
                "f02_dealer_name": f"RojaTv",
                "f03_state": False,
            }

            # Verificar si el f01_id_dealer ya existe en vListDealers
            dealer_exists = False
            for dealer in vListDealers:
                if dealer['f01_id_dealer'] == dealerRojaTv['f01_id_dealer']:
                    dealer['f03_state'] = dealerRojaTv['f03_state']  # Actualizar el campo f03_state
                    dealer_exists = True
                    break  # Salir del bucle una vez actualizado el dealer

            # Si el dealer no existe, se agrega a la lista
            if not dealer_exists:
                vListDealers.append(dealerRojaTv)

    def procesar_Platin():
        try:
            print(f"Inicia procesar_Platin")
            global eventNextDay
            global event_categoria
            global url_flag
            global jug_Local
            global logo_Local
            global jug_Visita
            global logo_Visita
            global channel_name
            global imagenIdiom
            global text_idiom
            global existeEvent
            global contador_registros
            global bool_estado_platin
            global foundDayOfWeek
            global currentDayOfWeek

            dia_event = None
            hora_event = None
            event_categoria = None
            name_event = None
            url_flag = None
            jug_Local = None
            logo_Local = None
            jug_Visita = None
            logo_Visita = None

            imagenIdiom = None
            channel_name = None
            text_idiom = None
            urlFinal = None

            evento = {}
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
            responsePlatin = requests.get(urlPlatin, headers=headers, allow_redirects=True, verify=False)
            #responsePlatin = requests.get(urlPlatin)
            html_contentPlatin = responsePlatin.text
            # Crear el objeto BeautifulSoup
            soup = BeautifulSoup(html_contentPlatin, 'html.parser')
            # Encontrar todos los elementos 'tr' en el HTML
            tr_elements_Platin = soup.find_all('tr')
            # Recorrer los elementos 'tr' y obtener la informacion necesaria
            hrefs_list = [tr.find('a').get('href') for tr in tr_elements_Platin if tr.find('a') is not None]
            href_Repetidos = len(hrefs_list) != len(set(hrefs_list))

            lista_event_flag = []
            for tr in tr_elements_Platin:
                # Obtener el nombre del evento y la URL de la bandera
                td_elements = tr.find_all('td')

                if len(td_elements) >= 2:
                    nombre_evento = td_elements[1].get_text(strip=True)

                    # Obtener la URL de la bandera
                    img_element = td_elements[0].find('img')
                    if img_element:
                        bandera = img_element['src']
                    else:
                        bandera = None
                    nombre_evento = nombre_evento[6:]
                    # Agregar los datos a tu lista o hacer lo que necesites
                    lista_event_flag.append({'nombre_evento': nombre_evento, 'bandera': bandera})

            for tr in tr_elements_Platin:
                try:
                    th_element = tr.find('th')
                    if th_element:
                        title = th_element.text.strip()
                        first_word_title = title.split()[0]
                        first_word_title = process_special_characters(first_word_title)
                    else:
                        if first_word_title == currentDayOfWeek:
                            if not href_Repetidos:
                                fecha_hora = None
                                try:
                                    url_flag = tr.find('img')['src']
                                except (KeyError, TypeError):
                                    url_flag = None
                                url_flag = validate_and_get_url(url_flag)
                                td_text = tr.select('td')[1].text.strip()   # Use .strip() to remove leading/trailing spaces
                                    # Corregir caracteres especificos
                                #td_text = process_special_characters(td_text)
                                hora_event = td_text.split(' ', 1)[0]
                                name_event =  td_text.split(' ', 1)[1]
                                name_event = process_special_characters(name_event)
                                name_event = capitalize_words(name_event)
                                url_event = tr.find('a')['href'].split('https://www.platinsport.com', 1)[-1]
                                url_event = 'https://www.platinsport.com' + url_event
                                url_event = validate_and_get_url(url_event)
                                url_name_pairs = obtenerUrlFinalPlatin(url_event)
                                if url_name_pairs is None or not url_name_pairs:
                                    continue

                                dia_event = fecha_actual
                                hora_event_inicio = int(hora_event.split(':')[0].zfill(2))   # Asegura que siempre tenga dos caracteres
                                 # hora_event_inicio -= 7
                                hora_event_inicio %= 24
                                hora_event = str(hora_event_inicio).zfill(2) + hora_event[2:]

                                if contador_registros > 2 and hora_event_inicio > 18:
                                    eventNextDay = True
                                    # Verificar si eventNextDay es True y hora_event < 9
                                if eventNextDay and hora_event_inicio < 9:
                                        # Incrementar dia_event en 1 dia
                                    dia_event = datetime.strptime(dia_event, '%Y%m%d')
                                    dia_event += timedelta(days=1)
                                    dia_event = dia_event.strftime('%Y%m%d')

                                fecha_hora = datetime.strptime(f"{dia_event} {hora_event}", '%Y%m%d %H:%M')
                                fecha_hora -= timedelta(hours=7)
                                hora_event = fecha_hora.strftime("%H:%M")
                                fecha_hora = fecha_hora.strftime('%Y-%m-%d %H:%M')

                                if contador_registros > 0:
                                    #existeEvent = verificarExisteEvento(hora_event,name_event)
                                    existeEvent = verificarExisteEvento(fecha_hora,name_event)
                                if existeEvent == "No" or contador_registros == 0:
                                    contador_registros += 1
                                    eventoPlatin = {
                                            "f01_id_document": contador_registros,
                                            "f02_proveedor" : "Platin",
                                            "f03_dia_event" : fecha_hora,
                                            'f04_hora_event': hora_event,
                                            'f05_event_categoria': event_categoria,
                                            'f06_name_event': name_event,
                                            'f07_URL_Flag': url_flag,
                                            'f08_jug_Local': jug_Local,
                                            'f09_logo_Local': logo_Local,
                                            'f10_jug_Visita': jug_Visita,
                                            'f11_logo_Visita': logo_Visita
                                            }
                                list_eventos_detalles = []   # Lista para almacenar la lista de eventos
                                list_eventos_detalles_existente = []   # Lista para almacenar la lista de eventos
                                for event_details in url_name_pairs:
                                    try:
                                        urlFinal = event_details['urlFin']   # Obtiene la URL del evento
                                        urlFinal = urlFinal.replace(" ","")
                                        channel_name = event_details['nameChannel']   # Obtiene el nombre del canal
                                        match = re.search(r'\[([^\]]+)\]', channel_name)
                                        if match:
                                            text_idiom = match.group(1).capitalize()
                                                # Elimina el texto entre corchetes del channel_name
                                            channel_name = channel_name.replace(match.group(0), '').strip()
                                        else:
                                            text_idiom = ''   # Si no se encuentra nada entre corchetes, dejar textIdiom en blanco
                                        existeUrlEvent = None

                                        if 'acestream://' in urlFinal:
                                            channel_name = ' Acestream ' + channel_name

                                        if existeEvent == "No":
                                            detalle = {
                                                    'f21_imagen_Idiom': imagenIdiom,
                                                    'f22_opcion_Watch': channel_name,
                                                    'f23_text_Idiom': text_idiom,
                                                    'f24_url_Final': urlFinal
                                                    }
                                            list_eventos_detalles.append(detalle)
                                        else:
                                            existeUrlEvent = verificarExisteUrlEvento(existeEvent, urlFinal)
                                            if existeUrlEvent == "Si_Existe_Url":
                                                #print(f'Ya existe Url para evento desde Platin : {hora_event} | {name_event} | {urlFinal}')
                                                continue
                                            else:
                                                evento_existente = next((evento for evento in v_list_eventos + v_list_eventos_3 if evento.get("f01_id_document") == existeEvent), None)
                                                if evento_existente:
                                                    nombre_evento = evento_existente['f06_name_event']
                                                    event_categoria = name_event.split(nombre_evento)[0].strip() if nombre_evento in name_event else None
                                                    proveedor = evento_existente['f02_proveedor']
                                                    if 'Platin' not in evento_existente['f02_proveedor']:
                                                        evento_existente['f02_proveedor'] += ' | Platin'
                                                    if evento_existente['f03_dia_event'] is None and fecha_hora is not None:
                                                        evento_existente['f03_dia_event'] = fecha_hora
                                                    if evento_existente['f07_URL_Flag'] is None and url_flag is not None:
                                                        evento_existente['f07_URL_Flag'] = url_flag

                                                    evento_existente['f05_event_categoria'] = event_categoria
                                                    list_eventos_detalles_existente = evento_existente.get("f20_Detalles_Evento", [])
                                                    detalle = {
                                                                'f21_imagen_Idiom': imagenIdiom,
                                                                'f22_opcion_Watch': channel_name,
                                                                'f23_text_Idiom': text_idiom,
                                                                'f24_url_Final': urlFinal
                                                                }
                                                    # Agregar detalle al evento existente
                                                    list_eventos_detalles_existente.append(detalle)
                                                    evento_existente['f20_Detalles_Evento'] = list_eventos_detalles_existente
                                                    if evento_existente in v_list_eventos:
                                                        v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                                                        print(f'Se actualiza list_eventos desde Platin: '
                                                              f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}')
                                                    else:
                                                        t_eventos.delete_item(Key={"f01_id_document": existeEvent, "f02_proveedor": proveedor})
                                                        t_eventos.put_item(Item=evento_existente)
                                                        print(f'Se actualiza evento en BD desde Platin: '
                                                              f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}  | proveedor: {proveedor}')
                                    except Exception as e:
                                        print(f"Error desde procesar_Platin 1:  {str(e)}")
                                        bool_estado_platin = False
                                        continue
                                if existeEvent == "No":
                                        # Agregar la lista de detallesEvento al evento
                                    evento['f20_Detalles_Evento'] = list_eventos_detalles
                                    v_list_eventos.append(eventoPlatin)
                                    print(f'Se adiciona evento y detalles desde Platin: '
                                          f'ID: {contador_registros} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}')
                                    bool_estado_platin = True
                                    event_categoria = None
                                    url_flag = None
                                    jug_Local = None
                                    logo_Local = None
                                    jug_Visita = None
                                    logo_Visita = None
                                    text_idiom = None
                                    channel_name = None
                                    imagenIdiom = None
                            else:
                                url_event = hrefs_list[0].split('https://www.platinsport.com', 1)[-1]
                                url_event = 'https://www.platinsport.com' + url_event
                                url_event = validate_and_get_url(url_event)
                                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
                                responsePlainUnicoURL = requests.get(url_event, headers=headers, allow_redirects=True, verify=False)
                                #responsePlainUnicoURL = requests.get(url_event, headers=headers)
                                html_contentPlainUnicoURL = responsePlainUnicoURL.text
                                soupPlainUnicoURL = BeautifulSoup(html_contentPlainUnicoURL, 'html.parser')
                                divs = soupPlainUnicoURL.find_all('div', class_='myDiv')

                                if len(divs) >= 2:
                                    #second_myDiv = divs[1]  # El segundo div (indice 1)
                                    #first_word_title = second_myDiv.text.split()[0]
                                    if first_word_title == currentDayOfWeek:

                                        # Encontrar todo el contenido de la clase 'myDiv1'
                                        div_content = soupPlainUnicoURL.find('div', class_='myDiv1')
                                        # Obtener el contenido de la etiqueta como una cadena de texto
                                        content_text = str(div_content)
                                        # Dividir por saltos de linea
                                        lines = content_text.split('\n')
                                        # Lista para almacenar los eventos
                                        eventos = []
                                        urls_channels = []
                                        # Iterar a traves de las lineas del contenido

                                        for line in lines:
                                            try:
                                                line = line.strip()  # Eliminar espacios en blanco al principio y al final
                                                # Si la linea comienza con una hora (por ejemplo, "09:15")
                                                if line[:2].isdigit() and line[2] == ":" and line[3:5].isdigit():
                                                    # Si ya hay datos del evento anterior, guardarlos
                                                    if hora_event and name_event:
                                                        evento = {
                                                            "hora_event": hora_event,
                                                            "name_event": name_event,
                                                            "urlFinal_channel_name": urls_channels,
                                                        }
                                                        eventos.append(evento)

                                                    # Obtener hora y nombre del evento actual
                                                    hora_event = line[:5]
                                                    name_event = line[5:].strip()

                                                    # Reiniciar la lista de URLs y canales
                                                    urls_channels = []

                                                # Si la linea comienza con "<a"
                                                elif line.startswith("<a"):
                                                    # Extraer la URL y el canal
                                                    href_start = line.find('href="') + len('href="')
                                                    href_end = line.find('"', href_start)
                                                    href = line[href_start:href_end]
                                                    href = href.replace(" ", "")
                                                    channel = line.split(">")[1].split("<")[0]
                                                    channel = capitalize_words(channel)
                                                    # Agregar la URL y el canal a la lista
                                                    urls_channels.append({"urlFin": href, "nameChannel": channel})
                                            except Exception as e:
                                                print(f"Error desde procesar_Platin 2: {str(e)}")
                                                bool_estado_platin = False
                                                continue
                                        # Agregar el ultimo evento a la lista
                                        if hora_event and name_event:
                                            evento = {
                                                "hora_event": hora_event,
                                                "name_event": name_event,
                                                "urlFinal_channel_name": urls_channels,
                                            }
                                            eventos.append(evento)

                                        # Imprimir los eventos obtenidos
                                        for evento in eventos:
                                            try:
                                                fecha_hora = None
                                                hora_event=evento["hora_event"]
                                                name_event = evento["name_event"]
                                                #pdb.set_trace()
                                                name_event = process_special_characters(name_event)
                                                # if 'Auxerre' not in name_event:
                                                #     continue

                                                url_name_pairs = evento["urlFinal_channel_name"]
                                                if url_name_pairs is None or not url_name_pairs:
                                                    continue

                                                for banderas in lista_event_flag:
                                                    if banderas['nombre_evento'] == name_event:
                                                        url_flag = banderas['bandera']
                                                        url_flag = validate_and_get_url(url_flag)
                                                        break  # Salimos del bucle si encontramos el evento
                                                name_event = capitalize_words(name_event)

                                                dia_event = fecha_actual
                                                hora_event_inicio = int(hora_event.split(':')[0].zfill(2))   # Asegura que siempre tenga dos caracteres
                                                # hora_event_inicio -= 7
                                                hora_event_inicio %= 24
                                                hora_event = str(hora_event_inicio).zfill(2) + hora_event[2:]

                                                if contador_registros > 2 and hora_event_inicio > 18:
                                                    eventNextDay = True
                                                    # Verificar si eventNextDay es True y hora_event < 9
                                                if eventNextDay and hora_event_inicio < 9:
                                                        # Incrementar dia_event en 1 dia
                                                    dia_event = datetime.strptime(dia_event, '%Y%m%d')
                                                    dia_event += timedelta(days=1)
                                                    dia_event = dia_event.strftime('%Y%m%d')

                                                fecha_hora = datetime.strptime(f"{dia_event} {hora_event}", '%Y%m%d %H:%M')
                                                fecha_hora -= timedelta(hours=7)
                                                hora_event = fecha_hora.strftime("%H:%M")
                                                fecha_hora = fecha_hora.strftime('%Y-%m-%d %H:%M')

                                                if contador_registros > 0:
                                                    #existeEvent = verificarExisteEvento(hora_event,name_event)
                                                    existeEvent = verificarExisteEvento(fecha_hora,name_event)
                                                if existeEvent == "No" or contador_registros == 0:
                                                    contador_registros += 1
                                                    eventoPlatin = {
                                                                    "f01_id_document": contador_registros,
                                                                    "f02_proveedor" : "Platin",
                                                                    "f03_dia_event" : fecha_hora,
                                                                    'f04_hora_event': hora_event,
                                                                    'f05_event_categoria': event_categoria,
                                                                    'f06_name_event': name_event,
                                                                    'f07_URL_Flag': url_flag,
                                                                    'f08_jug_Local': jug_Local,
                                                                    'f09_logo_Local': logo_Local,
                                                                    'f10_jug_Visita': jug_Visita,
                                                                    'f11_logo_Visita': logo_Visita
                                                                    }
                                                list_eventos_detalles = []   # Lista para almacenar la lista de eventos
                                                list_eventos_detalles_existente = []   # Lista para almacenar la lista de eventos
                                                for event_details in url_name_pairs:
                                                    try:
                                                        urlFinal = event_details['urlFin']   # Obtiene la URL del evento
                                                        channel_name = event_details['nameChannel']   # Obtiene el nombre del canal
                                                        match = re.search(r'\[([^\]]+)\]', channel_name)
                                                        if match:
                                                            text_idiom = match.group(1).capitalize()
                                                                # Elimina el texto entre corchetes del channel_name
                                                            channel_name = channel_name.replace(match.group(0), '').strip()
                                                        else:
                                                            text_idiom = ''   # Si no se encuentra nada entre corchetes, dejar textIdiom en blanco
                                                        existeUrlEvent = None

                                                        if 'acestream://' in urlFinal:
                                                            channel_name = ' Acestream ' + channel_name

                                                        if existeEvent == "No":
                                                            detalle = {
                                                                    'f21_imagen_Idiom': imagenIdiom,
                                                                    'f22_opcion_Watch': channel_name,
                                                                    'f23_text_Idiom': text_idiom,
                                                                    'f24_url_Final': urlFinal
                                                                    }
                                                            list_eventos_detalles.append(detalle)
                                                        else:
                                                            existeUrlEvent = verificarExisteUrlEvento(existeEvent, urlFinal)
                                                            if existeUrlEvent == "Si_Existe_Url":
                                                                #print(f'Ya existe Url para evento desde Platin : {hora_event} | {name_event} | {urlFinal}')
                                                                continue
                                                            else:
                                                                evento_existente = next((evento for evento in v_list_eventos + v_list_eventos_3 if evento.get("f01_id_document") == existeEvent), None)
                                                                if evento_existente:
                                                                    nombre_evento = evento_existente['f06_name_event']
                                                                    event_categoria = name_event.split(nombre_evento)[0].strip() if nombre_evento in name_event else None
                                                                    proveedor = evento_existente['f02_proveedor']
                                                                    if 'Platin' not in evento_existente['f02_proveedor']:
                                                                        evento_existente['f02_proveedor'] += ' | Platin'
                                                                    if evento_existente['f03_dia_event'] is None and fecha_hora is not None:
                                                                        evento_existente['f03_dia_event'] = fecha_hora
                                                                    if evento_existente['f05_event_categoria'] is None and event_categoria is not None:
                                                                        evento_existente['f05_event_categoria'] = event_categoria
                                                                    if evento_existente['f07_URL_Flag'] is None and url_flag is not None:
                                                                        evento_existente['f07_URL_Flag'] = url_flag

                                                                    list_eventos_detalles_existente = evento_existente.get("f20_Detalles_Evento", [])
                                                                    detalle = {
                                                                                'f21_imagen_Idiom': imagenIdiom,
                                                                                'f22_opcion_Watch': channel_name,
                                                                                'f23_text_Idiom': text_idiom,
                                                                                'f24_url_Final': urlFinal
                                                                                }
                                                                    # Agregar detalle al evento existente
                                                                    list_eventos_detalles_existente.append(detalle)
                                                                    evento_existente['f20_Detalles_Evento'] = list_eventos_detalles_existente
                                                                    if evento_existente in v_list_eventos:
                                                                        v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                                                                        print(f'Se actualiza list_eventos desde Platin: '
                                                                              f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}')
                                                                    else:
                                                                        t_eventos.delete_item(Key={"f01_id_document": existeEvent, "f02_proveedor": proveedor})
                                                                        t_eventos.put_item(Item=evento_existente)
                                                                        print(f'Se actualiza evento en BD desde Platin: '
                                                                              f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name} | proveedor: {proveedor}')

                                                    except Exception as e:
                                                        print(f"Error desde procesar_Platin 3: {str(e)}")
                                                        bool_estado_platin = False
                                                        continue
                                                if existeEvent == "No":
                                                        # Agregar la lista de detallesEvento al evento
                                                    eventoPlatin['f20_Detalles_Evento'] = list_eventos_detalles
                                                    v_list_eventos.append(eventoPlatin)
                                                    print(f'Se adiciona evento y detalles desde Platin: '
                                                          f'ID: {contador_registros} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}')
                                                    bool_estado_platin = True
                                                    event_categoria = None
                                                    url_flag = None
                                                    jug_Local = None
                                                    logo_Local = None
                                                    jug_Visita = None
                                                    logo_Visita = None
                                                    text_idiom = None
                                                    channel_name = None
                                                    imagenIdiom = None
                                            except Exception as e:
                                                print(f"Error desde procesar_Platin 4: {str(e)}")
                                                bool_estado_platin = False
                                                continue
                                    else:
                                        bool_estado_platin = False
                            break
                        else:
                            bool_estado_platin = False
                except Exception as e:
                    print(f"Error desde procesar_Platin 5: {str(e)}")
                    bool_estado_platin = False
                    continue

            # Datos del nuevo dealer
            dealerPlatin = {
                "f01_id_dealer": 7,
                "f02_dealer_name": "Platin",
                "f03_state": bool_estado_platin,
            }

            # Verificar si el f01_id_dealer ya existe en vListDealers
            dealer_exists = False
            for dealer in vListDealers:
                if dealer['f01_id_dealer'] == dealerPlatin['f01_id_dealer']:
                    dealer['f03_state'] = dealerPlatin['f03_state']  # Actualizar el campo f03_state
                    dealer_exists = True
                    break  # Salir del bucle una vez actualizado el dealer

            # Si el dealer no existe, se agrega a la lista
            if not dealer_exists:
                vListDealers.append(dealerPlatin)
            print(f"Termina procesar_Platin")
        except Exception as e:
            print(f"Error desde procesar_Platin 6: {str(e)}")
            # Datos del nuevo dealer
            dealerPlatin = {
                "f01_id_dealer": 7,
                "f02_dealer_name": f"Platin",
                "f03_state": bool_estado_platin,
            }

            # Verificar si el f01_id_dealer ya existe en vListDealers
            dealer_exists = False
            for dealer in vListDealers:
                if dealer['f01_id_dealer'] == dealerPlatin['f01_id_dealer']:
                    dealer['f03_state'] = dealerPlatin['f03_state']  # Actualizar el campo f03_state
                    dealer_exists = True
                    break  # Salir del bucle una vez actualizado el dealer

            # Si el dealer no existe, se agrega a la lista
            if not dealer_exists:
                vListDealers.append(dealerPlatin)

    def procesar_DaddyLivehd():
        print(f"Inicia procesar_DaddyLivehd")
        global eventNextDay
        global event_categoria
        global url_flag
        global jug_Local
        global logo_Local
        global jug_Visita
        global logo_Visita
        global channel_name
        global imagenIdiom
        global text_idiom
        global existeEvent
        global contador_registros
        global bool_estado_platin
        global foundDayOfWeek
        global currentDayOfWeek
        try:
            global bool_estado_DaddyLivehd
            dia_event = None
            hora_event = None
            event_categoria = None
            name_event = None
            url_flag = None
            jug_Local = None
            logo_Local = None
            jug_Visita = None
            logo_Visita = None
            existeEvent = None
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
            responseDaddyLivehd = requests.get(urlDaddyLivehd, headers=headers, allow_redirects=True, verify=False)
            schedule_data = responseDaddyLivehd.json()
            fecha_actual_datetime = datetime.now()
            # fecha_actual_str = fecha_actual_datetime.strftime('%A %dth %B %Y - Schedule Time UK GMT')
            # Ajuste para el sufijo del dia (1st, 2nd, 3rd, etc.)
            dia_f = str(fecha_actual_datetime.day).zfill(2)
            dia =  int(dia_f)
            if 4 <= dia <= 20 or 24 <= dia <= 30:
                sufijo = "th"
            else:
                sufijo = ["st", "nd", "rd"][dia % 10 - 1]
            fecha_actual_formato_json = fecha_actual_datetime.strftime(f'%A {dia_f}{sufijo} %B %Y - Schedule Time UK GMT')
            # Acceder directamente a los datos de la fecha actual
            categories = schedule_data.get(fecha_actual_formato_json, {})

            for category, events in categories.items():
                # Verificar si la categoria es "Soccer"

                #if category != "Tv Show":
                if "Show" not in category:
                    # Iterar sobre los eventos en la categoria "Soccer"
                    for event in events:
                        try:

                            fecha_hora = None
                            hora_event = event["time"]
                            name_event = event["event"].split(":")[1].strip() if ":" in event["event"] else event["event"].strip()
                            event_categoria = event["event"].split(":")[0].strip() if ":" in event["event"] else event["event"].strip()
                            name_event = process_special_characters(name_event)
                            if event_categoria == name_event:
                                event_categoria = category
                            else:
                                event_categoria = category + ' ' + event_categoria
                            # Buscar "vs" o "vs." y dividir en consecuencia
                            if "vs" in name_event:
                                jug_Local, jug_Visita = map(str.strip, name_event.split("vs"))
                            elif "vs." in name_event:
                                jug_Local, jug_Visita = map(str.strip, name_event.split("vs."))
                            else:
                                jug_Local = None
                                jug_Visita = None
                            name_event = name_event.replace("vs.", "Vs").strip()
                            # if 'Auxerre' not in name_event:
                            #     continue

                            dia_event = fecha_actual
                            hora_event_inicio = int(hora_event.split(':')[0].zfill(2))   # Asegura que siempre tenga dos caracteres
                            # hora_event_inicio -= 5
                            hora_event_inicio %= 24
                            hora_event = str(hora_event_inicio).zfill(2) + hora_event[2:]

                            if contador_registros > 2 and hora_event_inicio > 18:
                                eventNextDay = True
                                # Verificar si eventNextDay es True y hora_event < 9
                            if eventNextDay and hora_event_inicio < 9:
                                    # Incrementar dia_event en 1 dia
                                dia_event = datetime.strptime(dia_event, '%Y%m%d')
                                dia_event += timedelta(days=1)
                                dia_event = dia_event.strftime('%Y%m%d')

                            fecha_hora = datetime.strptime(f"{dia_event} {hora_event}", '%Y%m%d %H:%M')
                            fecha_hora -= timedelta(hours=5)
                            hora_event = fecha_hora.strftime("%H:%M")
                            fecha_hora = fecha_hora.strftime('%Y-%m-%d %H:%M')

                            if contador_registros > 0:
                                #existeEvent = verificarExisteEvento(hora_event,name_event)
                                existeEvent = verificarExisteEvento(fecha_hora,name_event)
                            if existeEvent == "No" or contador_registros == 0:
                                contador_registros += 1
                                eventodlhd = {
                                            "f01_id_document": contador_registros,
                                            "f02_proveedor" : "DLHD",
                                            "f03_dia_event" : fecha_hora,
                                            'f04_hora_event': hora_event,
                                            'f05_event_categoria': event_categoria,
                                            'f06_name_event': name_event,
                                            'f07_URL_Flag': url_flag,
                                            'f08_jug_Local': jug_Local,
                                            'f09_logo_Local': logo_Local,
                                            'f10_jug_Visita': jug_Visita,
                                            'f11_logo_Visita': logo_Visita
                                            }
                            list_events_det_dlhd = []   # Lista para almacenar la lista de eventos
                            # Iterar sobre los canales
                            for channel in event["channels"]:
                                try:

                                    channel_name = channel["channel_name"]
                                    urlFinal = f"https://dlhd.sx/embed/stream-{channel['channel_id']}.php"
                                    urlFinal = urlFinal.replace(" ","")
                                    existeUrlEventldhd = None

                                    if existeEvent == "No":
                                        detalleldhd = {
                                                    'f21_imagen_Idiom': imagenIdiom,
                                                    'f22_opcion_Watch': channel_name,
                                                    'f23_text_Idiom': text_idiom,
                                                    'f24_url_Final': urlFinal
                                                    }
                                        list_events_det_dlhd.append(detalleldhd)
                                    else:
                                        existeUrlEventldhd = verificarExisteUrlEvento(existeEvent, urlFinal)
                                        if existeUrlEventldhd == "Si_Existe_Url":
                                            #print(f'Ya existe Url para evento desde DLHD : {hora_event} | {name_event} | {urlFinal}')
                                            continue
                                        else:
                                            evento_existente = next((evento for evento in v_list_eventos + v_list_eventos_3 if evento.get("f01_id_document") == existeEvent), None)
                                            if evento_existente:
                                                proveedor = evento_existente['f02_proveedor']
                                                if 'DLHD' not in evento_existente['f02_proveedor']:
                                                    evento_existente['f02_proveedor'] += ' | DLHD'
                                                if evento_existente['f03_dia_event'] is None and fecha_hora is not None:
                                                    evento_existente['f03_dia_event'] = fecha_hora
                                                if evento_existente['f05_event_categoria'] is None and event_categoria is not None:
                                                    evento_existente['f05_event_categoria'] = event_categoria

                                                list_eventos_detalles_existente = evento_existente.get("f20_Detalles_Evento", [])
                                                detalle = {
                                                            'f21_imagen_Idiom': imagenIdiom,
                                                            'f22_opcion_Watch': channel_name,
                                                            'f23_text_Idiom': text_idiom,
                                                            'f24_url_Final': urlFinal
                                                            }
                                                list_eventos_detalles_existente.append(detalle)
                                                evento_existente['f20_Detalles_Evento'] = list_eventos_detalles_existente
                                                if evento_existente in v_list_eventos:
                                                    v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                                                    print(f'Se actualiza list_eventos desde DLHD: '
                                                          f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}')
                                                else:
                                                    t_eventos.delete_item(Key={"f01_id_document": existeEvent, "f02_proveedor": proveedor})
                                                    t_eventos.put_item(Item=evento_existente)
                                                    print(f'Se actualiza evento en BD desde DLHD: '
                                                          f'ID: {existeEvent} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name} | proveedor: {proveedor}')
                                except Exception as e:
                                    print(f"Error en procesar_DLHD 1: {e}")
                                    bool_estado_DaddyLivehd = False
                                    continue
                            if existeEvent == "No":
                                 # Agregar la lista de detallesEvento al evento
                                eventodlhd['f20_Detalles_Evento'] = list_events_det_dlhd
                                v_list_eventos.append(eventodlhd)
                                print(f'Se adiciona evento y detalles desde DLHD: '
                                      f'ID: {contador_registros} | {fecha_hora} | {hora_event} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}')
                                bool_estado_DaddyLivehd = True
                        except Exception as e:
                            print(f"Error en procesar_DLHD 3: {e}")
                            bool_estado_DaddyLivehd = False
                            continue

            # Datos del nuevo dealer
            dealerDaddyLivehd = {
                "f01_id_dealer": 8,
                "f02_dealer_name": "DLHD",
                "f03_state": bool_estado_DaddyLivehd,
            }

            # Verificar si el f01_id_dealer ya existe en vListDealers
            dealer_exists = False
            for dealer in vListDealers:
                if dealer['f01_id_dealer'] == dealerDaddyLivehd['f01_id_dealer']:
                    dealer['f03_state'] = dealerDaddyLivehd['f03_state']  # Actualizar el campo f03_state
                    dealer_exists = True
                    break  # Salir del bucle una vez actualizado el dealer

            # Si el dealer no existe, se agrega a la lista
            if not dealer_exists:
                vListDealers.append(dealerDaddyLivehd)
            print(f"Termina procesar_DaddyLivehd")
        except Exception as e:
            print(f"Error en procesar_DLHD 4: {e}")
            # Datos del nuevo dealer
            dealerDaddyLivehd = {
                "f01_id_dealer": 8,
                "f02_dealer_name": f"DLHD",
                "f03_state": False,
            }

            # Verificar si el f01_id_dealer ya existe en vListDealers
            dealer_exists = False
            for dealer in vListDealers:
                if dealer['f01_id_dealer'] == dealerDaddyLivehd['f01_id_dealer']:
                    dealer['f03_state'] = dealerDaddyLivehd['f03_state']  # Actualizar el campo f03_state
                    dealer_exists = True
                    break  # Salir del bucle una vez actualizado el dealer

            # Si el dealer no existe, se agrega a la lista
            if not dealer_exists:
                vListDealers.append(dealerDaddyLivehd)

    def obtener_eventos():
        # activaLibreF = 0

        # global ind_miss_LibreF
        # actualizar_bases = 0
        # activaLiveTV = 0
        # activaSportline = 0
        # activaDirectatvHDme = 0
        # ind_miss_LibreF = 0
        # activaRojaOn = 0
        # activaRojaTv = 0
        # activaPlatin = 0
        # activaDaddyLivehd = 1

        # print(f"activaLiveTV: {activaLiveTV}")
        # print(f"activaSportline: {activaSportline}")
        # print(f"activaDirectatvHDme: {activaDirectatvHDme}")
        # print(f"activaLibreF: {activaLibreF}")
        # print(f"activaRojaOn: {activaRojaOn}")
        # print(f"activaRojaTv: {activaRojaTv}")
        # print(f"activaPlatin: {activaPlatin}")
        # print(f"activaDaddyLivehd: {activaDaddyLivehd}")
        # print(f"ind_miss_LibreF: {ind_miss_LibreF}")
        # sys.exit()
        try:
            if actualizar_bases > 0:
                procesar_Bases()

            if activaDaddyLivehd > 0:
                procesar_DaddyLivehd()

            if activaDirectatvHDme > 0:
                procesar_DirectatvHDme()

            if activaRojaOn > 0:
                procesar_RojaOnline()

            if activaRojaTv > 0:
                procesar_RojaTV()

            if activaLibreF > 0:
                procesar_LibreF()

            if activaSportline > 0:
                procesar_SportsLine()

            if activaPlatin > 0:
                procesar_Platin()

            if activaLiveTV > 0:
                procesar_LiveTV()

        except Exception as e:
            print(f'Error en obtener_eventos: {e}')

    # Invocar la funcion para obtener los eventos
    obtener_eventos()
    #sys.exit()

    def insertar_dato_en_bd_dealer(dealer_data):
        try:
            t_dealers.put_item(Item=dealer_data)
            #print("Se insertaron los datos en la coleccion de dealer", dealer_data)
        except Exception as e:
            print("Ocurrio un error al insertar los datos de dealer:", str(e))

    # Procesar los datos en vListDealers
    for dealer in vListDealers:
        #dealer_id = dealer.get('f01_id_dealer')   # Obten el ID del dealer
        dealer_id = decimal.Decimal(dealer.get('f01_id_dealer'))
        if dealer_id:
            #eliminar_dato_en_bd_dealer(dealer_id, dealer)
            insertar_dato_en_bd_dealer(dealer)
        else:
            print("El dato no tiene un ID, no se puede actualizar en la BD.")

    def procesar_cambios_eventos(v_list_eventos, v_list_eventos_copia):
        # Crear un diccionario de eventos previos para acceso rapido
        eventos_previos_dict = {str(e['f01_id_document']): e for e in v_list_eventos_copia}

        # Iterar sobre eventos actuales y comparar
        for evento_actual in v_list_eventos:
            id_documento = str(evento_actual['f01_id_document'])
            json_evento_actual = json.dumps(evento_actual, sort_keys=True, default=str)
            #print(f"Procesando evento con ID: {id_documento}")
            nuevo_evento = evento_actual.get('f06_name_event', '')
            if id_documento in eventos_previos_dict:
                evento_previo = eventos_previos_dict[id_documento]
                document_id = evento_previo.get('f01_id_document')
                proveedor = evento_previo.get('f02_proveedor')
                json_evento_previo = json.dumps(evento_previo, sort_keys=True, default=str)
                #print(f"Comparando evento actual con previo: {json_evento_actual} |vs| {json_evento_previo}")

                if json_evento_actual != json_evento_previo:
                    print(f"Evento modificado detectado, se actualizara: ID: {document_id} | {nuevo_evento}")
                    t_eventos.delete_item(Key={"f01_id_document": document_id, "f02_proveedor": proveedor})
                    t_eventos.put_item(Item=evento_actual)
            else:
                print(f"Nuevo evento detectado, se agregara: {nuevo_evento}")
                #v_message = (f'{hora_event} | {name_event} | {urlFinal}')
                #asyncio.run(enviar_mensaje_telegram_channel(v_message))
                t_eventos.put_item(Item=evento_actual)

    # Llamada a la funcion
    procesar_cambios_eventos(v_list_eventos, v_list_eventos_copia)

    ### Insertar el dia.
    if dia_actual_bd is None or fecha_actual > dia_actual_bd:
        # Insertar el nuevo dia en la coleccion
        #dia_evento_ref = db.collection('dia_evento').document('d_dia')
        #dia_evento_ref.set({'f01_dia': fecha_actual})
        item = {
            "id_dia_evento": 1,
            "f01_dia": fecha_actual
        }
        # Insertar el nuevo elemento en la tabla
        response = t_dia_evento.put_item(Item=item)
        #print(f"Se inserto el nuevo dia: {fecha_actual} - {response['ResponseMetadata']['HTTPStatusCode']}")

    ############################################# envio de mensaje y finalizacion #############################################
    horaFinEjecucion = datetime.now()
    diferencia_segundos = (horaFinEjecucion - horaIniciaEjecucion).total_seconds()
    minutos = int(diferencia_segundos // 60)  # Obtiene la parte entera de la división
    segundos = int(diferencia_segundos % 60)  # Obtiene el resto de la división (segundos)

    # Funcion para enviar un mensaje
    async def enviar_mensaje_telegram(chat_id, p_minutos,p_segundos):
        #message = f"El programa tardo  {round(variable_valor, 2):.2f}  minutos en ejecutarse."
        message = f"El programa tardo {p_minutos}:{p_segundos} minutos en ejecutarse."
        print(message)
        try:
            await bot.send_message(chat_id=chat_id, text=message)
            print("Mensaje enviado con exito.")
        except telegram.error.TelegramError as e:
            print(f"Error al enviar el mensaje: {e}")

    token = "6559813109:AAEUKzEG6rRIFrt2pwkcHhZuA9Ynt3kqvlI"
    bot = telegram.Bot(token=token)
    chat_id = '5954221232' #1002035964627:channel: - #'5954221232' #chat_id:bot
    asyncio.run(enviar_mensaje_telegram(chat_id, minutos,segundos))

    print(f'Finaliza ejecucion: {horaFinEjecucion}')
    if minutos < 10:
        print(f'Duerme 15 Min')
        time.sleep(900)
