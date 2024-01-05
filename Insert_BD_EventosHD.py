import requests
from bs4 import BeautifulSoup #
import html
from datetime import datetime,timedelta
import unicodedata
from unidecode import unidecode
import firebase_admin
from firebase_admin import credentials, firestore
import sys
import time
import re
import urllib.parse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

import telegram
import asyncio
 # import pycountry
import json
import operator
from google.cloud.firestore_v1 import Query
from fuzzywuzzy import fuzz


import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

 # Obtener la hora de inicio de la ejecución

horaIniciaEjecucion = datetime.now()
maxIntentos = 3   # Número máximo de intentos
vListDealers = []   # Lista para almacenar la lista de eventos
v_list_eventos = []   # Lista para almacenar la lista de eventos
v_list_eventos_2 = []   # Lista para almacenar la lista de eventos
v_list_eventos_news = []   # Lista para almacenar la lista de eventos
eventos_existentes = {}

# Rules Firebase
# rules_version = '2';
# ​
# service cloud.firestore {
#   match /databases/{database}/documents {
#     match /{document=**} {
#       allow read, write: if true;
#     }
#   }
# }

 # Datos del archivo JSON
firebase_credentials = {
  "type": "service_account",
  "project_id": "hdeportes-ed6bc",
  "private_key_id": "63085efdb9275077ec7a5d673615f0ecf8dbad3c",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCS2ki3buYAW/Za\n/Lmz3wNX4GXI6PqukdWfmQzRdLoMkQg+yIA3ZqN1zohU3aJPXSK/Bki8/zS4P89y\nPPB4xRvH1estpd2drRgb9RtB64Ilp+VtjJQr27Tih3RBfKTsKcsCkqxf+uKEwnIR\n/sI8uG/PEWFRR8ARaNUd/2RbUjSHEuHBKw5GccS9zRXD6lilQU4LvtAm7wm7dwSv\nxG+1MmKhJR3Z98SrePqi1viWHhjQdAQqMMPRUZgjSrGBI8uSkdFf2/+k9Y1V54BM\nLn0Vv2zw8EE6G3W/MSNWw+/BQ57bPuqdkvgNlJH3kXhyZaRPO0Z16I4E53Fjj9oR\nw+/f5vC3AgMBAAECggEABtshXqGoeUcnLD5KSTHV4nS71nJnabOt89fdt6r/GKEY\nxR2akXuS5r1wCKOZn/eVggeVaRrHvPu1qqavr2O8aqBRF34mkjryOjPqRDHuOeXU\nb0CLQQpD2qOU8428gWNox+5Nv+QNo+xpY5paOngojX6ekCs3tgMH7XDuoC4pUk5R\nka0Wl1xjtR2+dxBuH4LDdaZgVL8SijwTXhsmzKxYBCP2Yg/DH5IEIfFsfA5MklWF\nH//EO19y5kgHBBst9lNcEWzx1DnXXiPiyhwYyYINQVyYW1QNryREuVmt6vJdDDmQ\nau/zhyCfcTq2JppRDfjPR8K8nlA72ReUNMxYNZfXmQKBgQDMqwT4wAPYcoYB52iQ\nkPdPfqoXwml+4b97ivNJzB4j5eSVHVw7UuamBGgwsniQYCnMVbUbx96np+4j7uPg\nViN5/pR9hYpBO/QaaDaEZmIMrobAJq0MaUXpUnrmYsk8thgIEsoFtJlbPxbYhkro\npMgQgfBk9/kka2lRI1qA8hgnjQKBgQC3rycX+D/CgDPXPr5QhVXvAoKw75L2FrdR\nZ6c1tfH3deRe2CaGBvRcJwvDqmg06WEpLB8nl8wNXs781xfV6fZFWnH9sBxgGZEl\n97I6UJS6pLVGvKjsUUPmPWzvqHlS86b06EvZoXRDbD5SuZzeDsDARC3J280umRgs\nsjxHeFwWUwKBgHQzkRIOKuDEPPNtyD90HwXiW3474huo3ketX24B1wb4qmxDDC+e\nZNfTXrhvS+ZkwraB3t7T/sVfoeSC6JxhK6lC8lnF7PYWrQQlMPbBDmA24sjL7KpE\nRu4u5xpQ8gJnPIOw70SIAh39VuI+dN9bVz+QFsWsyKZ12Yi4stTNHwutAoGBAKsJ\nxKxUNALH3/3g5dQSpFcfjlR1cPIHawG5NAEWf84+j70lJpIMrcOVdCXpaolCe3+c\nHxJ+DbqxeqoDG6XL6s7fjl98ep+djwO8ptAYMrWtI0NP7zfF4CTnM6Xhyb9NNDy+\nhcqLxbf2df4P6hr3v9IIal6ZdnCai+EqwmiGkbETAoGAapzEUr1pa/o04w+rO8xt\nulvAtLVgFpW8BvG2lofuLI+ds58ELYQWHO1sjDZwsLPxiMOmd8yCs1Eae/ZBhtGK\nFO8aaa+WgQVENCZ3gSRuAOhr0xhOgkbhNogWVeb7cT48qTlYhsU2cpjDTZovjNKK\nfjLvbO+Z+rajKRgIgX2e7DI=\n-----END PRIVATE KEY-----\n",
  "client_email": "firebase-adminsdk-3pjex@hdeportes-ed6bc.iam.gserviceaccount.com",
  "client_id": "105680662039927735873",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-3pjex%40hdeportes-ed6bc.iam.gserviceaccount.com",
  "universe_domain": "googleapis.com"
}
# OLD_KEY
# {
#     "type": "service_account",
#     "project_id": "hdeportes-47611",
#     "private_key_id": "7acadd060d3aea01e23731df8ca494b4a9cc78dc",
#     "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC5A/DRb9bxcrsE\ntSlFII0xzv2z5Jz8uXmoxPArYScp7aJ6zfsVzTr4aq9uZyVctMXFxzuj9S7U08d7\nCTU9Ym9ue4A/P3HG+PxVTsaFVB4ceD2awYyQ6BeMnQWZDqyXgbV52t30pcrorJXo\nhnDHjCKr6+jkVvkqLNEK7y250qjjfxxR/rBN9NUtzSKlOFe1XN3Gxf+lozfM1VOF\nb6EYJgWjUmDsHSfonVW/C2llUpPlul1DoRKeG7TiPiON6cwP8mdeVmxUUiP+XKos\nT37m/77aOar+ZF+CokNmCQSIxo3z6bzmYcXR08ujnC/kSBFOtLHBqIWOZQi2J3bo\nwWoMd6TVAgMBAAECggEAA7M3b9NJkoMeTTavXKU0gpj/Lm8X997+LPyIhE2TNeUa\n7PD0x/VCzoloR5n+P49/o/gMZgatNW8aqsMNkqLkcTlQh9qqnmulecXkpwVDXuYA\njHlW1Pn5vG/SZcyjrMFig01kM6gPxjPXi372V+asIhFeTlMKzXaLwWQcJEOR1hiz\nAMmhnHAE9k/tdLw0xTcgX7tmZJKMhSovedw6X3+5Tzp3gD0yPWZnQ0xY4zT0r9Ib\n7GNUOBIClPgyibtrgH4f1+f15H293UvPCX7BgaVG2xnyjg/sCUnDJlt18jKVrIRm\nDabBmXHlfk4A6JXZsT2zJFTYrqN6gJ14yNMkFTVjFwKBgQDi6vJkU/VDgDhe3eh1\nA1aSo3Fiof1Stfged1yYTnGELVb/Xk0XfFPa+vGv/Mw9J0a2LZs6xFNZkpjFkwJn\nQKqn90R2QIvMCUVoSER76j9LeTT4UFCKwny2mD5/hmgKMH+7KO78YlQ2cf1d8s+m\n2R1TzQgtErZkV6WBi4CkxtRErwKBgQDQujL12TYVZVF/usHY95gyZ5/0DP9P2AUw\n1s+Ue1Eb6Pr5xmkDYLChal2s2DJ2DeAJ+Oh2vC02AEKuyioFRszmPkdQ6V0V5LuK\ngaIrF7vK7+9glxJNRLwaY6RP9p0iEyLywg+FiWjRN1aCWNfOf8bQznkg1Y8nEQ1m\nkh4HXY5XuwKBgDR1mooFOutbBi0wYiBDIE2QSZNC3dy0QuNQNvDjGa00CivJckse\nrAgxAZTs/Y6ZuOg/DOb+IkLP+E+c1+5k7c8dBA/OhkMqVYhxRJvdILrjD5Lkhmo9\n4jUYZ0J2ITFR2wW6xd1FIjDgE3IXw1fQU85e1SgObmouNQ4I4Tm22bXzAoGAQV6f\nSSnO62KA5LzrqvTYIKkdikRrzWi9L2+HMVxxqYkM2R8PknQ8vUDft5s6Kojr6pOe\n0lja2/e26qD8Cg0DbPSCR0/ezFWHerhgvpg/Qy5jzN9jZvmo46bjNfvpeFt/7YFj\ndRH01tk66bXDWLPh41FW1DauD7EifkYyy2G8i7MCgYAkr8xJil+EOr8X/EMYT/a7\ne6JpcWldYznA/44XX7U8ldXS9Wnb+QLPf6j18J2S5sT5pngwFYeeA7fGpxN2nAWp\ns0g2Vyrn6FBnmoOGaiuqnB0HzuhRzkcCzeGS3c59Jz8LRbOXUcrc18ebxiGogg0z\nK7Hd4fz96EirJxUmXGJ/KA==\n-----END PRIVATE KEY-----\n",
#     "client_email": "firebase-adminsdk-sim5t@hdeportes-47611.iam.gserviceaccount.com",
#     "client_id": "103843750171356262576",
#     "auth_uri": "https://accounts.google.com/o/oauth2/auth",
#     "token_uri": "https://oauth2.googleapis.com/token",
#     "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
#     "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-sim5t%40hdeportes-47611.iam.gserviceaccount.com",
#     "universe_domain": "googleapis.com"
# }

 # Inicializa Firebase
cred = credentials.Certificate(firebase_credentials)
firebase_admin.initialize_app(cred)

 # Inicializa Firestore
db = firestore.client()

def obtener_dia_actual():
    dia_evento_ref = db.collection('dia_evento').document('d_dia')
    doc = dia_evento_ref.get()

    if doc.exists:
        dia_evento_data = doc.to_dict()
        dia_actual_bd = dia_evento_data.get('f01_dia')
        return dia_actual_bd   # La fecha ya está en el formato 'YYYYMMDD'
    else:
        print(f'no encontro fecha en la bd {doc} | dia_evento_ref : {dia_evento_ref}')
         # Si no existe, retornamos la fecha actual
         # return None
        return '20000101'
         # return datetime.now().strftime('%Y%m%d')

def eliminar_datos_bd():
     # Eliminar colecciones y documentos según tu estructura
     # Supongamos que tienes una colección 'eventos' y deseas eliminar todos los documentos en ella
    col_eventos_ref = db.collection('eventos')
    col_dealers_ref = db.collection('dealers')
    col_dia_evento_ref = db.collection('dia_evento')
    docs_events = col_eventos_ref.get()
    docs_dealers = col_dealers_ref.get()
    docs_dia = col_dia_evento_ref.get()
    for doc in docs_events:
        doc.reference.delete()
    for doc in docs_dealers:
        doc.reference.delete()
    for doc in docs_dia:
        doc.reference.delete()
    print('Se eliminaron los datos de la bd')


dia_actual_bd = obtener_dia_actual()

 # Obtener el día actual en el formato correcto (debes adaptarlo según tu formato)
fecha_actual = datetime.now().strftime('%Y%m%d')

if fecha_actual > dia_actual_bd:
     # Eliminar datos y actualizar el día en la BD
    eliminar_datos_bd()
    
 # Nombre de la colección que deseas eliminar

col_eventos = "eventos"
col_dealers = "dealers"
col_events = firestore.client().collection(col_eventos)
col_provee = firestore.client().collection(col_dealers)
eventos_db = db.collection('eventos').get()
dealers_db = db.collection('dealers').get()

contador_registros = 0

activaLiveTv = 0
activaSportline = 0
activaDirectatvHDme = 0
activaLibreF = 0
activaRojaOn = 0
activaRojaTv = 0
activaPlatin = 0
activaDaddyLivehd = 0

ind_miss_LibreF = 0
bool_estado_Sportline = False
bool_estado_DirectatvHDme = False
bool_estado_libref = False
bool_estado_RojaOn = False
bool_estado_RojaTv = False
bool_estado_platin = False
bool_estado_DaddyLivehd= False

 # Verificar si dealers_db no contiene datos
if len(dealers_db) == 0:
    activaLiveTv = 1
    activaSportline = 1
    activaDirectatvHDme = 1
    activaLibreF = 1
    activaRojaOn = 1
    activaRojaTv = 1
    activaPlatin = 1
    activaDaddyLivehd = 1
    print('No habian datos de dealers se creara todo nuevo')
else:
    print('Si habian datos de dealers se repasara los errores')
     # Iterar sobre los documentos y procesarlos
    for doc in dealers_db:
        #vListDealers.append(doc)
        vListDealers.append(doc.to_dict())
        proveedor = doc.to_dict().get('f02_dealer_name')
        estado = doc.to_dict().get('f03_state')
        if proveedor == 'LiveTV':
            if not estado:
                activaLiveTv = 0
                ind_miss_LibreF = 1  
                print(f'Se activa variable ind_miss_LibreF por estado: {estado} en LiveTV')
        elif proveedor == 'Sportline':
            if not estado:
                activaSportline = 1
                print(f'Se activa variable activaSportline por estado: {estado}')
        elif proveedor == 'DirectatvHDme':
            if not estado:
                activaDirectatvHDme = 1
                print(f'Se activa variable activaDirectatvHDme por estado: {estado}')
        elif proveedor == 'LibreF':
            if not estado:
                activaLibreF = 0
                ind_miss_LibreF = 1
                print(f'Se activa variable ind_miss_LibreF por estado: {estado} en LibreF')
        elif proveedor == 'RojaOn':
            if not estado:
                activaRojaOn = 1
                print(f'Se activa variable activaRojaOn por estado: {estado}')
        elif proveedor == 'Rojatv':
            if not estado:
                activaRojaTv = 1
                print(f'Se activa variable activaRojaTv por estado: {estado}')
        elif proveedor == 'Platin':
            if not estado:
                activaPlatin = 1
                print(f'Se activa variable activaPlatin por estado: {estado}')
        elif proveedor == 'DLHD':
            if not estado:                
                activaDaddyLivehd = 1
                print(f'Se activa variable activaDaddyLivehd por estado: {estado}')

def convertToCorsProxyUrl(url, cors_proxy_url='https://corsproxy.io/?'):
    encoded_url = urllib.parse.quote(url, safe='')
    cors_proxy_url = cors_proxy_url + encoded_url
    return cors_proxy_url

def convertToWhateverOriginUrl(url):
    # Usar la URL de Whatever Origin con la URL de destino codificada
    whatever_origin_url = f'http://www.whateverorigin.org/get?url={url}&callback=?'
    return whatever_origin_url

def validate_and_get_url(url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
        fullallorigins_url = 'https://api.allorigins.win/raw?url=' + url        
        response_all = requests.get(fullallorigins_url, headers=headers)
        if response_all.status_code in (521, 502, 503, 403, 404, 204):
             #print(f"Error {response_all.status_code} para allorigins {fullallorigins_url}")            
            fullallorigins_url_iso = 'https://api.allorigins.win/get?charset=ISO-8859-1&url='+ url
            response_all_iso = requests.get(fullallorigins_url_iso, headers=headers)
            if response_all_iso.status_code in (521, 502, 503, 403, 404, 204):
                 #print(f"Error {response_all_iso.status_code} para allorigins {fullallorigins_url_iso}")            
                fullallorigins_url_myFunc = 'https://api.allorigins.win/get?callback=myFunc&url='+ url
                response_all_myFunc = requests.get(fullallorigins_url_myFunc, headers=headers)
                if response_all_myFunc.status_code in (521, 502, 503, 403, 404, 204):                
                     #print(f"Error {response_all_myFunc.status_code} para allorigins {fullallorigins_url_myFunc}")            
                    fullCors_url = convertToCorsProxyUrl(url)
                    response_cors = requests.get(fullCors_url, headers=headers)            
                    if response_cors.status_code in (521, 502, 503, 403, 404, 204):
                         #print(f"Error {response_cors.status_code} para corsproxy {fullCors_url}")
                        fullWhatever_url = convertToWhateverOriginUrl(url)
                        response_whatever = requests.get(fullWhatever_url, headers=headers)
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

 # URL de la página web

urlPlatin = validate_and_get_url('https://www.platinsport.com')
urlportsonline = validate_and_get_url('https://sportsonline.gl/prog.txt')
urlRojaOn = validate_and_get_url('https://ww1.tarjetarojatvonline.sx')
urlRojaTV =  validate_and_get_url('https://tarjetarojatv.run')
urlLibreF =  validate_and_get_url('https://futbollibre.futbol/agenda/')
urlDirectatvHDme = validate_and_get_url('https://directatvhd.me')
urlLiveTv =  validate_and_get_url('https://livetv744.me/enx/') #('https://livetv.sx/enx')
urlDaddyLivehd =  validate_and_get_url('https://dlhd.sx')

# Realizar una consulta para ver si existe algún registro con 'Sportline' en 'f02_proveedor'
querySportline = db.collection('eventos').where('f02_proveedor', '>=', 'Sportline').where('f02_proveedor', '<=', 'Sportline\uf8ff')
resultSportline = querySportline.stream()
# Verificar si NO hay ningún resultado en la consulta
if len(list(resultSportline)) == 0:
    activaSportline = 1
    print(f'Se activa variable activaSportline porque NO existe algún registro de Sportline')
# Realizar una consulta para ver si existe algún registro con 'DirectatvHDme' en 'f02_proveedor'
queryDirectatvHDme = db.collection('eventos').where('f02_proveedor', '>=', 'DirectatvHDme').where('f02_proveedor', '<=', 'DirectatvHDme\uf8ff')
resultDirectatvHDme = queryDirectatvHDme.stream()
# Verificar si NO hay ningún resultado en la consulta
if len(list(resultDirectatvHDme)) == 0:
    activaDirectatvHDme = 1
    print(f'Se activa variable activaDirectatvHDme porque NO existe algún registro de DirectatvHDme')
# Realizar una consulta para ver si existe algún registro con 'RojaOn' en 'f02_proveedor'
queryRojaOn = db.collection('eventos').where('f02_proveedor', '>=', 'RojaOn').where('f02_proveedor', '<=', 'RojaOn\uf8ff')
resultRojaOn = queryRojaOn.stream()
# Verificar si NO hay ningún resultado en la consulta
if len(list(resultRojaOn)) == 0:
    activaRojaOn = 1
    print(f'Se activa variable activaRojaOn porque NO existe algún registro de RojaOn')
# Realizar una consulta para ver si existe algún registro con 'RojaTv' en 'f02_proveedor'
queryRojaTv = db.collection('eventos').where('f02_proveedor', '>=', 'RojaTv').where('f02_proveedor', '<=', 'RojaTv\uf8ff')
resultRojaTv = queryRojaTv.stream()
# Verificar si NO hay ningún resultado en la consulta
if len(list(resultRojaTv)) == 0:
    activaRojaTv = 1
    print(f'Se activa variable activaRojaTv porque NO existe algún registro de RojaTv')
# Realizar una consulta para ver si existe algún registro con 'Platin' en 'f02_proveedor'
queryPlatin = db.collection('eventos').where('f02_proveedor', '>=', 'Platin').where('f02_proveedor', '<=', 'Platin\uf8ff')
resultPlatin = queryPlatin.stream()
# Verificar si NO hay ningún resultado en la consulta
if len(list(resultPlatin)) == 0:
    activaPlatin = 1
    print(f'Se activa variable activaPlatin porque NO existe algún registro de Platin')
# Realizar una consulta para ver si existe algún registro con 'DLHD' en 'f02_proveedor'
queryDLHD = db.collection('eventos').where('f02_proveedor', '>=', 'DLHD').where('f02_proveedor', '<=', 'DLHD\uf8ff')
resultDLHD = queryDLHD.stream()
# Verificar si NO hay ningún resultado en la consulta
if len(list(resultDLHD)) == 0:
    activaDaddyLivehd = 1
    print(f'Se activa variable activaDaddyLivehd porque NO existe algún registro de DLHD')
# Realizar una consulta para buscar registros que contengan 'LibreF' en 'f02_proveedor'
queryLiveTV = db.collection('eventos').where('f02_proveedor', '>=', 'LibreF').where('f02_proveedor', '<=', 'LibreF\uf8ff')
resultLibreF = queryLiveTV.stream()
# Verificar si NO hay ningún resultado en la consulta
if len(list(resultLibreF)) == 0:
    activaLibreF = 1
    ind_miss_LibreF = 0
    print(f'Se activa variable activaLibreF porque NO existe algún registro de LibreF')
# Realizar una consulta para buscar registros que contengan 'LiveTv' en 'f02_proveedor'
queryLiveTV = db.collection('eventos').where('f02_proveedor', '>=', 'LiveTv').where('f02_proveedor', '<=', 'LiveTv\uf8ff')
resultLiveTV = queryLiveTV.stream()
# Realizar una consulta para ver si existe algún registro con 'LiveTv' en 'f02_proveedor'
# Verificar si NO hay ningún resultado en la consulta
if len(list(resultLiveTV)) == 0:
    activaLiveTv = 1
    ind_miss_LibreF = 0
    print(f'Se activa variable activaLiveTv porque NO existe algún registro de LiveTv')
    
# Variables para el evento máximo
max_evento1 = None
max_f01_id_document1 = 0  # Inicializa con un valor bajo o adecuado
max_evento = None
max_eventoLiveTv = None
# Almacena los f01_id_document de los eventos que contienen 'LiveTv'
f01_id_document_list = []

if len(eventos_db) != 0:
    # Iterar sobre los documentos y agregarlos a v_list_eventos_en_BD
    for evento_db in eventos_db:
        evento_data = evento_db.to_dict()
        # Nueva sección de código para validar proveedores y activaciones
        f02_proveedor = evento_data.get('f02_proveedor', '')
        if ((f02_proveedor == "LibreF" and ind_miss_LibreF == 1) or
            (f02_proveedor == "LiveTv" and ind_miss_LibreF == 1)):
            
            detalles_evento = evento_data.get('f20_Detalles_Evento', [])
             # Verificar si hay al menos un detalle que contiene 'sin_data' en 'f22_opcion_Watch'
            if any('sin_data' in detalle.get('f22_opcion_Watch', '') for detalle in detalles_evento if detalle.get('f22_opcion_Watch') is not None):
                v_list_eventos.append(evento_data)
                ind_miss_LibreF = 1
                activaLibreF = 0
                            
             # Si el evento contiene 'LiveTv', guarda su f01_id_document en la lista
            if 'LiveTv' in evento_data['f02_proveedor']:
                f01_id_document_list.append(evento_data['f01_id_document'])

        if (
            (f02_proveedor == "Sportline" and activaSportline == 1) or
            (f02_proveedor == "DirectatvHDme" and activaDirectatvHDme == 1) or
            (f02_proveedor == "RojaOn" and activaRojaOn == 1) or
            (f02_proveedor == "RojaTv" and activaRojaTv == 1) or
            (f02_proveedor == "Platin" and activaPlatin == 1) or
            (f02_proveedor == "DLHD" and activaDaddyLivehd == 1)):
                v_list_eventos.append(evento_data)                 
               
        else:
             # Saltar al siguiente registro del bucle 'for'
            continue                

     # Encuentra el máximo f01_id_document entre los eventos 'LiveTv'
    if f01_id_document_list:
        max_f01_id_document1 = max(f01_id_document_list)
        max_eventoLiveTv = next((evento for evento in eventos_db if evento.to_dict().get('f01_id_document') == max_f01_id_document1 and 'LiveTv' in evento.to_dict().get('f02_proveedor')), None)

     # Si encontramos el evento máximo, lo agregamos a v_list_eventos
    if max_eventoLiveTv:
        max_eventoLiveTv_dict = max_eventoLiveTv.to_dict()
        v_list_eventos.append(max_eventoLiveTv_dict)
        
     # Obtener todos los f01_id_document de eventos_db
    f01_id_document_list = [evento_db.to_dict().get('f01_id_document', 0) for evento_db in eventos_db]
     # Encontrar el máximo f01_id_document
    if f01_id_document_list:
        contador_registros = max(f01_id_document_list)
        print(f"El f01_id_document más alto es: {contador_registros}")
    else:
        print("No hay registros en eventos_db")    

# Copiar el contenido de v_list_eventos a v_list_eventos_2
v_list_eventos_2.extend(v_list_eventos)    

# Función para calcular la similitud entre textos normalizados
def similar(a, b):
    return fuzz.ratio(a.lower(), b.lower())
                     
 # Diccionario de caracteres especiales y sus reemplazos

special_characters = {
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
    '&#179': 'o'
}

def process_special_characters(text):
    # Decodificar las entidades HTML en el texto
    decoded_text = html.unescape(text)

    # Procesar otros caracteres especiales, si es necesario
    for char, replacement in special_characters.items():
        decoded_text = decoded_text.replace(char, replacement)

    return decoded_text

def capitalize_words(text):
    words = text.lower().split()
    capitalized_words = [word.capitalize() for word in words]
    return ' '.join(capitalized_words)

def obtenerUrlFinalPlatin(url_event):
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
    max_retries = 5
    retries = 0
    while retries < max_retries:
        response = requests.get(url_event, headers=headers)
        #print(f'response.status_code: {response.status_code} url_event: {url_event}')
        if response.status_code != 429:
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')
            url_name_pairs = []
             # Encuentra todos los elementos <a> con href que comienzan con "acestream"
            a_elements = soup.find_all('a', href=True)

            for a_element in a_elements:
                 # Obtén la URL final y elimina espacios en blanco al inicio y al final
                url_fin = a_element['href'].strip()

                 # Verifica si comienza con "acestream://"
                if url_fin.startswith("acestream://"):
                     # Obtén el nombre del canal
                    name_channel = a_element.text.strip()
                    name_channel = capitalize_words(name_channel)

                     # Agrega la pareja URL-Nombre a la lista
                    url_name_pairs.append({"urlFin": url_fin, "nameChannel": name_channel})
            break   # La solicitud se realizó con éxito, sal del bucle
        else:
         url_name_pairs = 'No pudo obtener ulr por 429 máximo de reintentos'
        time.sleep(10)   # Espera 5 segundos antes de reintentar
        retries += 1

    return url_name_pairs

def obtenerUrlFinalRojaOn(url_inicial):
    try:
         # Antes de hacer la solicitud, espera un tiempo
        time.sleep(1)   # Espera 1 segundo
         # Realiza una solicitud GET a la URL inicial para obtener el contenido HTML
        response = requests.get(url_inicial)
         # Parsea el contenido HTML utilizando BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

         # Encuentra el elemento 'iframe' dentro de la clase 'embed-responsive'
        iframe_element = soup.find('div', class_='embed-responsive').find('iframe')
         # Obtiene la URL final del atributo 'src' del elemento 'iframe'
        url_final = iframe_element['src']
        partes_urlfinal = url_final.split("https://")
        if len(partes_urlfinal) >= 2:
             # La última parte será la URL deseada
            url_final = "https://" + partes_urlfinal[-1]
        else:
             # Si solo hay una parte, esa es la URL final
            url_final = url_final
        #url_final_con_api = "https://api.allorigins.win/raw?url=" + url_final
        url_final_con_api = validate_and_get_url(url_final)
        nameChannel = obtenerTitleRojaOn(url_final_con_api)
         # Construye la cadena de texto con el formato deseado
        result = f"{nameChannel} | {url_final}"
        return result
    except Exception as e:
        print(f"Error en obtenerUrlFinalRojaOn: {str(e)}")
        return None

def obtenerTitleRojaOn(url_final):
    try:
         # Antes de hacer la solicitud, espera un tiempo
         # Realiza una solicitud GET a la URL final para obtener el contenido HTML
        response = requests.get(url_final)

         # Verifica si la solicitud fue exitosa (código de respuesta 200)
         # if response.status_code == 200:
             # Parsea el contenido HTML utilizando BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')

         # Encuentra el elemento 'title' y obtén su contenido
        title_element = soup.find('title')

        if title_element:
            title = title_element.text
            return title
        else:
            return "No Channel Name"

         # else:
         #     return f"Error al obtener la página. Código de respuesta: {response.status_code}"

    except Exception as e:
        return f"Error en obtenerTitleRojaOn: {str(e)}"

def obtenerUrlFinalRojaTV(enlace):
    try:
         # Inicia roja
        if 'tarjetarojatv' in enlace or 'elitegoltv' in enlace:
            #newenlace = "https://api.allorigins.win/raw?url=" + enlace
            response = requests.get(enlace)
             #print(f'pasa 0 : {response.status_code} | {enlace}')
            if response.status_code == 200:
                 #print('pasa 1')
                soup = BeautifulSoup(response.text, 'html.parser')
                divElement = soup.find('div', class_='iframe-container')
                if divElement:
                     #print('pasa 2')
                    divContent = divElement.encode_contents().decode('utf-8')
                    scriptElement = divElement.find('script')
                    if scriptElement:
                         #print('pasa 3')
                        scriptContent = scriptElement.get_text()
                        fidRegex = re.compile(r'fid="([^"]+)";')
                        fidMatch = fidRegex.search(scriptContent)
                        if fidMatch:
                             #print('pasa 4')
                            v_fid = fidMatch.group(1)
                            srcRegex = re.compile(r'src="([^"]+?/[^/]+)')
                            srcMatch = srcRegex.search(divContent)
                            if srcMatch:
                                 #print('pasa 5')
                                urlevento = srcMatch.group(1)
                                if 'radamel' in urlevento:
                                     #print('pasa 6')
                                    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
                                    urlFinal = f'{urlevento}/reproductor/{v_fid}.php'
                                    urlFinal =  validate_and_get_url(urlFinal)
                                    responseUrlFinRojaTv = requests.get(urlFinal, headers=headers, allow_redirects=True)
                                    if responseUrlFinRojaTv.status_code == 200:
                                         #print('pasa 7')
                                        htmlBody = responseUrlFinRojaTv.text
                                        iframeSrcRegex = re.compile(r'<iframe[^>]*?allowfullscreen="true"[^>]*?src="([^"]+)"', re.IGNORECASE)
                                        iframeSrcMatch = iframeSrcRegex.search(htmlBody)
                                        if iframeSrcMatch:
                                             #print('pasa 8')
                                            newUrlFin = iframeSrcMatch.group(1)
                                            if 'sportsonline' in newUrlFin:
                                                result = f"{newUrlFin} | No_Channel_Name"
                                                return result                                            
                                             # Ahora, realiza una solicitud HTTP para obtener el contenido HTML de newUrlFin
                                            #responseNewUrlFin = requests.get(newUrlFin, headers=headers, allow_redirects=True)
                                            #print(f'newUrlFin : {newUrlFin}')
                                            newUrlFin =  validate_and_get_url(newUrlFin)
                                            responseNewUrlFin = requests.get(newUrlFin, headers=headers, allow_redirects=True)
                                            newHtmlBody = responseNewUrlFin.text

                                             # Parsea el contenido HTML para obtener el título de la página
                                            soup = BeautifulSoup(newHtmlBody, 'html.parser')
                                            title = soup.find("title").text if soup.find("title") else "No_Channel_Name"
                                             # Concatena el título con la URL final
                                            result = f"{newUrlFin} | {title}"
                                            return result
                                        else:
                                             #print(f'pasa 9')
                                            result = f"{urlFinal} | No_Channel_Name"
                                            return result                                                                                        
                                             #return f'No encuentra iframe en urlFinal: {urlFinal} | No encuentra iframe en newUrlFin: {urlFinal}'
                                    else:
                                        return f'sin_data_1 responseUrlFinRojaTv: {responseUrlFinRojaTv.status_code} | sin_data_1 responseUrlFinRojaTv: {responseUrlFinRojaTv.status_code}'
                                elif 'vikistream' in urlevento:
                                    newUrlFinal = f'https://vikistream.com/embed2.php?player=desktop&live={v_fid}'
                                    return newUrlFinal
                                else:
                                   return f'sin_data_2 urlevento: {urlevento} | sin_data_2 urlevento: {urlevento}'
                            else:
                                return "sin_data_3 | sin_data_3"
                        else:
                            return "sin_data_4 | sin_data_5"
                    else:
                        return "sin_data_5 | sin_data_5"
                else:
                    return "sin_data_6 | sin_data_6"
            else:
                return f'sin_data_7 Response: {response.status_code} enlace: {enlace} | sin_data_7 Response: {response.status_code} enlace: {enlace}'

        return "sin_data_8 | sin_data_8"
    except Exception as e:
        print(f'Error en obtenerUrlFinalRojaTV: {e}  newUrlFin: {newUrlFin}')
        return f"sin_data_9 enlace: {enlace} | {enlace}"   # O cualquier otro valor que desees utilizar para indicar un error

def obtenerUrlFinalLibreTV(url):
    time.sleep(2)   # Espera 1 segundo
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
        response = requests.get(url, headers=headers)

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
                     # texto_decoded = texto.encode('latin-1').decode('utf-8')
                     # Supongamos que 'enlace' es el enlace que deseas procesar
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
            print(f'Error en la solicitud HTTP: {response.status_code}')
            return None
    except Exception as e:
         # Captura cualquier excepción y muestra el mensaje de error
        print(f"Se produjo un error en obtenerUrlFinalLibreTV: {str(e)}")

def obtenerUrlFinalLibreTVSelenium(initial_url):
    try:
         # Configurando las opciones de Chrome para navegación en segundo plano y sin notificaciones
        chrome_options = Options()
        chrome_options.add_argument("--headless")   # Navegación en segundo plano
        chrome_options.add_argument("--disable-notifications")   # Desactivar notificaciones

         # Inicializar Chrome WebDriver con las opciones configuradas
        driver = webdriver.Chrome(options=chrome_options)

        try:
            # # Configurar el nivel de log del WebDriver
            # caps = DesiredCapabilities.CHROME
            # caps['goog:loggingPrefs'] = {'browser': 'SEVERE'}  # Establecer el nivel de log a 'SEVERE'
            
            # # Inicializar Chrome WebDriver con las opciones configuradas y el nivel de log establecido
            # driver = webdriver.Chrome(options=chrome_options, desired_capabilities=caps)            
            
             # Navigate to the initial URL
            driver.get(initial_url)

             # Esperar hasta 10 segundos para que aparezca el iframe
            wait = WebDriverWait(driver, 10)
            iframe = wait.until(EC.presence_of_element_located((By.TAG_NAME, 'iframe')))
             # Wait for the iframe to be present
             # iframe = driver.find_element(By.TAG_NAME, 'iframe')

             # Extract the 'src' attribute of the iframe
            final_url = iframe.get_attribute('src')

        finally:
             # Close the WebDriver
            driver.quit()

        return final_url

    except Exception as e:
         # Captura cualquier excepción y muestra el mensaje de error
        print(f"Se produjo un error en obtenerUrlFinalLibreTVSelenium: {str(e)}")

def obtenerUrlFinalRojaHDme(url):
    max_retries = 5
    retries = 0
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

    while retries < max_retries:
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            title = soup.head.title.text
            palabras_clave = ['rojadirecta', 'tarjetarojatvonline']   # Agrega aquí las palabras clave que deseas eliminar
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
            print(f"No pudo obtener ulr por {response.status_code}. Reintentando...")
            time.sleep(10)   # Espera 5 segundos antes de reintentar
            retries += 1
    return f'Web | No pudo obtener ulr después de {max_retries} intentos'

def verificarExisteEvento(hora_event, name_event):
    try:
        name_event_lower = name_event.lower()
        evento_existente = None
        # Verificar si ya existe un evento con el name_event y hora_event
        #if ind_platin == 1:
        evento_existente = next((evento for evento in v_list_eventos if evento["f06_name_event"].lower() in name_event_lower and evento["f04_hora_event"] == hora_event), None)
        #else:
        if evento_existente is None:
            evento_existente = next((evento for evento in v_list_eventos if evento["f06_name_event"].lower() == name_event_lower and evento["f04_hora_event"] == hora_event), None)
        
        if evento_existente is None:
            evento_existente = next((evento for evento in v_list_eventos if name_event_lower in evento["f06_name_event"].lower() and evento["f04_hora_event"] == hora_event), None)
        
        if evento_existente:
            # El evento existe, verificar si la URL ya existe para ese id_document
            id_document = evento_existente["f01_id_document"]
            return id_document
        else:
            return "No"   # El evento no existe
    except Exception as e:
        print(f'Error en verificarExisteUrlEvento: {e}')
        
def verificarExisteUrlEvento(id_document, urlFinal):
    try:
        # Buscar el evento con el id_document proporcionado
        evento = next((evento for evento in v_list_eventos if evento["f01_id_document"] == id_document), None)
        if evento:
            detalles_evento = evento.get("f20_Detalles_Evento", [])   # Obtener la lista de detalles del evento si existe
            # Verificar si la URL final ya existe para ese id_document
            url_existe = any(detalle.get("f24_url_Final") == urlFinal for detalle in detalles_evento)
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
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            document = BeautifulSoup(response.text, 'html.parser')
            contains_text = 'LiveStreams are currently not available for this broadcast.' in document.text
            return 'SI' if contains_text else 'NO'
        return 'NO'

    except Exception as e:
        print(f'Error en contains_not_available_text: {e}')
        
 # Función para obtener la URL final para enlaces de Live TV

def obtener_url_live_tv_final(enlace):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
        response = requests.get(enlace, headers=headers)
         # Realizar solicitud HTTP para obtener el código HTML de la URL
         # response = requests.get(f"https:{enlace}")
        if response.status_code == 200:
             # Analizar el código HTML para buscar el iframe con allowfullscreen
            soup = BeautifulSoup(response.content, 'html.parser')
            iframe = soup.select_one('iframe[allowfullscreen]')
            if iframe:
                urlFin = iframe.get('src')
                urlFin = urlFin.replace('\n', '').replace('\r', '')   # Elimina saltos de línea y retorno de carro
                if not urlFin.startswith('http'):
                    urlFin = f"https:{urlFin}"
                if 'youtube' in urlFin and urlFin.startswith('//'):
                    urlFin = urlFin[2:]
                return urlFin

    except Exception as e:
        print(f'Error en obtener_url_live_tv_final: {e}')

eventNextDay = False
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

now = datetime.now()
currentDayOfWeek = diassemana[now.weekday()]

def procesar_LiveTV():
    try:
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
        
        # Realizar la solicitud GET a la URL y obtener el contenido HTML
        fixes = requests.get(urlLiveTv)
        html_contentLiveTv = fixes.text
        # Parsear el contenido HTML
        document = BeautifulSoup(html_contentLiveTv, 'html.parser')
        liveTds = document.select('td[OnMouseOver^="\$(\'#cv"]')
        evento = {}
        for td in liveTds:
            try:
                url_flag = td.select_one('img')['src'] if td.select_one('img') else ''
                url_flag = "https:" + url_flag
                event_categoria = (td.select_one('img')['alt'] if td.select_one('img') else '').strip()  # + ' '
                aElement = td.select_one('a.live')
                claselive = aElement['class'] if aElement else ''
                if 'live' not in claselive:
                    continue   # Si la clase no es "live", saltar al siguiente registro
                urlLinksEvent = 'https://livetv744.me' + (aElement['href'] if aElement else '')
                enlaceEvent = urlLinksEvent
                    #enlaceEventCors = convertToCorsProxyUrl(enlaceEvent)
                enlaceEventCors =  validate_and_get_url(enlaceEvent)
                containsText = contains_not_available_text(enlaceEventCors)   # Assuming this is an asynchronous function
                if containsText == 'SI':
                    continue
                nameEventOld = aElement.text if aElement else ''
                name_event = re.sub(r'\s+', ' ', re.sub(r'(?<=\s)[–](?=\s)', 'Vs', nameEventOld)).strip()
                spanEvdesc = td.select_one('span.evdesc')
                horaEvent = spanEvdesc.text if spanEvdesc else ''
                horaRegex = re.compile(r'\b\d{1,2}:\d{2}\b')   # Expresión regular para buscar el formato de hora (por ejemplo, "8:30")
                horaMatch = horaRegex.search(horaEvent)
                hora = horaMatch.group(0) if horaMatch else ''   # Si se encuentra una coincidencia, se obtiene la hora
                # Ajustar la hora agregando o restando la cantidad de horas de la diferencia horaria
                dia_event = fecha_actual
                hora_event_inicio = int(hora.split(':')[0].zfill(2))   # Asegura que siempre tenga dos caracteres
                hora_event_inicio -= 7
                hora_event_inicio %= 24
                hora_event = str(hora_event_inicio).zfill(2) + ':' + hora[2:]
                hora_event = hora_event.replace('::',':')
                if contador_registros > 2 and hora_event_inicio > 18:
                    eventNextDay = True
                # Verificar si eventNextDay es True y hora_event < 9
                if eventNextDay and hora_event_inicio < 9:
                    # Incrementar dia_event en 1 día
                    dia_event = datetime.strptime(dia_event, '%Y%m%d')
                    dia_event += timedelta(days=1)
                    dia_event = dia_event.strftime('%Y%m%d')
                if 'livetv' in enlaceEventCors:
                    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
                    responseLiveTv = requests.get(enlaceEventCors, headers=headers, allow_redirects=True)
                    if responseLiveTv.status_code == 200:
                        document = BeautifulSoup(responseLiveTv.content, 'html.parser')
                        imagesWithItemprop = document.select('img[itemprop]')
                        if len(imagesWithItemprop) >= 2:
                            logoLocalOld = imagesWithItemprop[0]['src'] if 'src' in imagesWithItemprop[0].attrs else ''
                            logoVisitaOld = imagesWithItemprop[1]['src'] if 'src' in imagesWithItemprop[1].attrs else ''
                            jug_Local = imagesWithItemprop[0]['alt'] if 'alt' in imagesWithItemprop[0].attrs else ''
                            logo_Local = f"https:{logoLocalOld}"
                            jug_Visita = imagesWithItemprop[1]['alt'] if 'alt' in imagesWithItemprop[1].attrs else ''
                            logo_Visita = f"https:{logoVisitaOld}"
                        if contador_registros > 0:
                            existeEvent = verificarExisteEvento(hora_event,name_event)
                        if existeEvent == "No" or contador_registros == 0:
                            contador_registros += 1
                            evento = {
                                    "f01_id_document": contador_registros,
                                    "f02_proveedor" : "LiveTv",
                                    "f03_dia_event" : dia_event,
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
                        tables = document.select('table.lnktbj')
                        list_eventos_detalles = []   # Lista para almacenar la lista de eventos
                        for table in tables:
                            # Verificar si la tabla contiene información relevante
                            img = table.select_one('td > img[title]')
                            if img and 'title' in img.attrs:
                                imagenIdiom = img['src']
                                imagenIdiom = f"https:{imagenIdiom}"
                                text_idiom = img['title']
                                enlaces = table.select('td > a')
                                enlace = enlaces[1]['href'] if len(enlaces) > 1 else enlaces[0]['href']
                                channel_name = table.select_one('td.lnktyt > span')
                                if channel_name and channel_name.text.strip():
                                    channel_name = channel_name.text.strip()
                                else:
                                    channel_name = img['title']
                                if 'acestream://' in enlace or '/tinyurl.com' in enlace:
                                    urlFinal = enlace
                                elif ((not enlace.startswith('http')) and (not 'acestream://' in enlace or not '/tinyurl.com' in enlace)):
                                    enlace = f"https:{enlace}"
                                    urlFinal = obtener_url_live_tv_final(enlace)
                                existeUrlEvent = None
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
                                        print(f'Ya existe Url para evento desde LiveTv : {hora_event} | {name_event} | {urlFinal}')
                                        continue
                                    else:
                                        # Obtener el evento existente
                                        evento_existente = next((evento for evento in v_list_eventos if evento["f01_id_document"] == existeEvent), None)
                                        # Obtener la lista de detalles del evento existente
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
                                        # Actualizar el evento existente en la lista
                                        v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                                        print(f'Se adiciona detalles a evento desde LiveTv : {hora_event} | {name_event} | {urlFinal}')
                        if existeEvent == "No":
                            # Agregar la lista de detallesEvento al evento
                            evento['f20_Detalles_Evento'] = list_eventos_detalles
                            v_list_eventos.append(evento)
                            print(f'Se adiciona evento y detalles desde LiveTv : {hora_event} | {name_event} | {urlFinal}')
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
                print(f'Error en procesar_LiveTV: {e}')
                # Datos del nuevo dealer
                dealerLiveTV = {
                    "f01_id_dealer": 1,
                    "f02_dealer_name": f"LiveTV: {str(e)}",
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
                    
                continue
        
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


    except Exception as e:
        print(f'Error en procesar_LiveTV: {e}')
        # Datos del nuevo dealer
        dealerLiveTV = {
            "f01_id_dealer": 1,
            "f02_dealer_name": f"LiveTV: {str(e)}",
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
         # Realizar la solicitud GET a la URL y obtener el contenido HTML
        responseSportline = requests.get(urlportsonline)
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
                    if "sportsonline.so/channels/" in line:
                        parts = line.split("|")
                        if len(parts) > 1:
                                try:
                                    name_event = parts[0].strip().encode('latin1').decode('latin1')
                                except Exception as e:
                                    name_event = parts[0].strip()
                                dia_event = fecha_actual
                                    # Obtener la hora en formato de 24 horas
                                hora_event = name_event[:5]
                                hora_event_inicio = int(hora_event[:2])
                                hora_event_inicio -= 6
                                hora_event_inicio %= 24
                                hora_event = str(hora_event_inicio).zfill(2) + hora_event[2:]
                                name_event = name_event[5:].lstrip()
                                name_event = name_event.replace(" x ", " Vs ")
                                name_event = name_event.replace(" @ ", " Vs ")
                                urlFinal = parts[1].strip()
                                    # Verifica si alguna extensión de idioma está en la urlFinal
                                for idioma_info in idiomas:
                                    extension_idioma = idioma_info['extension']
                                    if extension_idioma.lower() in urlFinal.lower():
                                        text_idiom = idioma_info['idioma']
                                        break   # Una vez que encuentras una coincidencia, sales del bucle
                                if "/pt/" in urlFinal:
                                    text_idiom = "Portuguese"
                                if contador_registros > 2 and hora_event_inicio > 18:
                                    eventNextDay = True
                                    # Verificar si eventNextDay es True y hora_event < 9
                                if eventNextDay and hora_event_inicio < 9:
                                        # Incrementar dia_event en 1 día
                                    dia_event = datetime.strptime(dia_event, '%Y%m%d')
                                    dia_event += timedelta(days=1)
                                    dia_event = dia_event.strftime('%Y%m%d')
                                list_eventos_detalles = []   # Lista para almacenar la lista de eventos
                                list_eventos_detalles_existente = []   # Lista para almacenar la lista de eventos
                                if contador_registros > 0:
                                    existeEvent = verificarExisteEvento(hora_event,name_event)
                                if existeEvent == "No" or contador_registros == 0:
                                    contador_registros += 1
                                    eventoSportLine = {
                                                        "f01_id_document": contador_registros,
                                                        "f02_proveedor" : "Sportline",
                                                        "f03_dia_event" : dia_event,
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
                                    print(f'Se adiciona evento y detalles desde SportLine : {hora_event} | {name_event} | {urlFinal}')
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
                                        print(f'Ya existe Url para evento desde SportLine : {hora_event} | {name_event} | {urlFinal}')
                                        continue
                                    else:
                                            # Obtener el evento existente si existe
                                        evento_existente = next((evento for evento in v_list_eventos if evento.get("f01_id_document") == existeEvent), None)
                                            # Obtener la lista de detalles del evento existente
                                        list_eventos_detalles_existente = evento_existente.get("f20_Detalles_Evento", [])
                                        detalleSportLine = {
                                                            'f21_imagen_Idiom': imagenIdiom,
                                                            'f22_opcion_Watch': channel_name,
                                                            'f23_text_Idiom': text_idiom,
                                                            'f24_url_Final': urlFinal
                                                            }
                                            # Agregar detalle al evento existente
                                        list_eventos_detalles_existente.append(detalleSportLine)
                                            # Actualizar la lista de detalles del evento existente
                                        evento_existente['f20_Detalles_Evento'] = list_eventos_detalles_existente
                                            # Actualizar el evento existente en la lista
                                        v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                                        print(f'Se adiciona detalles a evento desde SportLine : {hora_event} | {name_event} | {urlFinal}')
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
        
    except Exception as e:
        print(f'Error en procesar_SportsLine: {e}')
        # Datos del nuevo dealer
        dealerSportLine = {
            "f01_id_dealer": 2,
            "f02_dealer_name": f"Sportline: {str(e)}",
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
        responseDirecTVHDme = requests.get(urlDirectatvHDme)
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

                    enlace = enlace.strip().replace(' ', '%20')
                    enlace = re.sub(r'(.*)\.php\.php$', r'\1.php', enlace)
                        # Genera una clave única para cada registro basada en event_categoria, name_event y enlace
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
                    hora_event_inicio -= 1
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
                            # Incrementar dia_event en 1 día
                        dia_event = datetime.strptime(dia_event, '%Y%m%d')
                        dia_event += timedelta(days=1)
                        dia_event = dia_event.strftime('%Y%m%d')


                    list_eventos_detalles = []   # Lista para almacenar la lista de eventos
                    list_eventos_detalles_existente = []   # Lista para almacenar la lista de eventos
                    if contador_registros > 0:
                        existeEvent = verificarExisteEvento(hora_event,name_event)

                    if existeEvent == "No" or contador_registros == 0:
                        contador_registros += 1
                        evento = {
                                "f01_id_document": contador_registros,
                                "f02_proveedor" : "DirectatvHDme",
                                "f03_dia_event" : dia_event,
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
                        print(f'Se adiciona evento y detalles desde DirectatvHDme : {hora_event} | {name_event} | {urlFinal}')
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
                            print(f'Ya existe Url para evento desde DirectatvHDme : {hora_event} | {name_event} | {urlFinal}')
                            continue
                        else:
                            # Obtener el evento existente si existe
                            evento_existente = next((evento for evento in v_list_eventos if evento.get("f01_id_document") == existeEvent), None)

                            # Actualizar los campos en el evento existente si están en None
                            evento_existente['f02_proveedor'] = evento_existente['f02_proveedor'] + ' | DirectatvHDme'

                            if evento_existente['f05_event_categoria'] is None and event_categoria is not None:
                                evento_existente['f05_event_categoria'] = event_categoria

                            if evento_existente['f07_URL_Flag'] is None and url_flag is not None:
                                evento_existente['f07_URL_Flag'] = url_flag

                            # Obtener la lista de detalles del evento existente
                            list_eventos_detalles_existente = evento_existente.get("f20_Detalles_Evento", [])

                            detalle = {
                                    'f21_imagen_Idiom': imagenIdiom,
                                    'f22_opcion_Watch': channel_name,
                                    'f23_text_Idiom': text_idiom,
                                    'f24_url_Final': urlFinal
                                    }
                                # Agregar detalle al evento existente
                            list_eventos_detalles_existente.append(detalle)
                                # Actualizar la lista de detalles del evento existente
                            evento_existente['f20_Detalles_Evento'] = list_eventos_detalles_existente
                                # Actualizar el evento existente en la lista
                            v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                            print(f'Se adiciona detalles a evento existente desde DirectatvHDme : {hora_event} | {name_event} | {urlFinal}')
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
        
    except Exception as e:
        print(f'Error en procesar_DirectatvHDme: {e}')
        # Datos del nuevo dealer
        dealerDirectatvHDme = {
            "f01_id_dealer": 3,
            "f02_dealer_name":  f"DirectatvHDme: {str(e)}",
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

        if ind_miss_LibreF == 0:
                # Realizar la solicitud GET y obtener el contenido HTML
            response = requests.get(urlLibreF)
            soup = BeautifulSoup(response.text, 'html.parser')
                # Obtener la lista de eventos (<li> elementos)
            eventos = soup.find_all('li')
                # Recorrer cada evento y obtener sus datos
            for evento in eventos:
                try:
                        # Obtener el elemento <a> dentro del evento
                    linkElement = evento.find('a')
                        # Obtener el texto completo del enlace
                    linkText = linkElement.text if linkElement else ''
                        # Encontrar la posición del primer ':' en el texto del enlace
                    firstColonIndex = linkText.find(':')
                        # Obtener textoCategoria y textEvent
                    if firstColonIndex != -1:
                        event_categoria = linkText[:firstColonIndex + 1].strip()
                        textEvent = linkText[firstColonIndex + 1:].split('\n')[0].strip()   # Obtener la primera línea
                    else:
                            # Si no se encuentra ':' en el texto, asumimos que textoCategoria es el texto completo y textEvent es una cadena vacía
                        event_categoria = linkText
                        textEvent = ''
                    event_categoria = event_categoria.replace(":", "")
                    event_categoria = process_special_characters(event_categoria)
                        # Decodificar el texto del evento (puede ser necesario si hay caracteres especiales)
                    try:
                        name_event = textEvent.encode('latin1').decode('utf8')
                    except:
                        name_event = textEvent
                    name_event = process_special_characters(name_event)                        
                    name_event = name_event.replace("vs.", "Vs").strip()
                    hora_evento = evento.find(class_='t').text if evento.find(class_='t') else ''
                        # Obtener todos los elementos de tipo <li> que son hijos de evento
                    canalesYEnlaces = evento.select('ul > li.subitem1')
                        # Recorrer cada <li> hijo para obtener los canales y enlaces
                    for ce in canalesYEnlaces:
                        try:
                            hora_event_inicio = None
                            chanel = ce.find('a').contents[0] if ce.find('a') else ''
                            #enlace = '/es' + ce.find('a')['href'] if ce.find('a') else ''
                            enlace = ce.find('a')['href'] if ce.find('a') else ''
                            try:
                                channel = chanel.encode('latin1').decode('utf8')
                            except:
                                channel = chanel
                            if enlace.startswith("https://futbollibrehd.com"):
                                enlace = enlace.replace("https://futbollibrehd.com","https://librefutboltv.com")
                            if "https://librefutboltv.com" not in enlace:
                                enlace = 'https://librefutboltv.com' + enlace
                            #urlcors = 'https://api.allorigins.win/raw?url=' + enlace
                            urlcors = validate_and_get_url(enlace)
                            if "/embed/" not in urlcors:
                                    # Si "/embed/" NO está en urlcors
                                urlIni = obtenerUrlFinalLibreTV(urlcors)
                            else:
                                urlIni = obtenerUrlFinalLibreTVSelenium(urlcors)
                            dia_event = fecha_actual
                            hora_event_inicio = int(hora_evento[:2])
                            hora_event_inicio -= 7
                            hora_event_inicio %= 24
                            hora_event = str(hora_event_inicio).zfill(2) + hora_evento[2:]
                            if contador_registros > 2 and hora_event_inicio > 18:
                                eventNextDay = True
                                # Verificar si eventNextDay es True y hora_event < 9
                            if eventNextDay and hora_event_inicio < 9:
                                    # Incrementar dia_event en 1 día
                                dia_event = datetime.strptime(dia_event, '%Y%m%d')
                                dia_event += timedelta(days=1)
                                dia_event = dia_event.strftime('%Y%m%d')
                            if urlIni is None:
                                bool_estado_libref  = False
                                urlIni = urlcors + ' | sin_data'
                            if contador_registros > 0:
                                existeEvent = verificarExisteEvento(hora_event,name_event)
                            if existeEvent == "No" or contador_registros == 0:
                                contador_registros += 1
                                eventoLibreF = {
                                        "f01_id_document": contador_registros,
                                        "f02_proveedor" : "LibreF",
                                        "f03_dia_event" : dia_event,
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
                                    # Resto del código aquí
                                url_final_parts = urlIni.split(' | ')
                                for i in range(0, len(url_final_parts), 2):
                                    try:
                                        # Obtener la URL y el channelName
                                        urlFinal = url_final_parts[i]
                                        channel_name = channel + ': ' + url_final_parts[i + 1] if i + 1 < len(url_final_parts) else None
                                        existeUrlEvent = None
                                        if existeEvent == "No":
                                            detalleLibreF = {
                                                        'f21_imagen_Idiom': imagenIdiom,
                                                        'f22_opcion_Watch': channel_name,
                                                        'f23_text_Idiom': text_idiom,
                                                        'f24_url_Final': urlFinal
                                                        }
                                            list_eventos_detalles.append(detalleLibreF)
                                            print(f'Se adiciona evento y detalles desde LibreF : {hora_event} | {name_event} | {urlFinal}')
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
                                                print(f'Ya existe Url para evento desde LibreF : {hora_event} | {name_event} | {urlFinal}')
                                                continue
                                            else:
                                                    # Obtener el evento existente
                                                evento_existente = next((evento for evento in v_list_eventos if evento["f01_id_document"] == existeEvent), None)
                                                    # Verifica si 'LibreF' no está ya en la lista de proveedores
                                                if 'LibreF' not in evento_existente['f02_proveedor']:
                                                        # Actualizar los campos en el evento existente si están en None
                                                    evento_existente['f02_proveedor'] = evento_existente['f02_proveedor'] + ' | LibreF'
                                                if evento_existente['f05_event_categoria'] is None and event_categoria is not None:
                                                    evento_existente['f05_event_categoria'] = event_categoria
                                                if evento_existente['f07_URL_Flag'] is None and url_flag is not None:
                                                    evento_existente['f07_URL_Flag'] = url_flag
                                                    # Obtener la lista de detalles del evento existente
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
                                                    # Actualizar el evento existente en la lista
                                                v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                                                print(f'Se adiciona detalles a evento desde LibreF : {hora_event} | {name_event} | {urlFinal}')
                                                channel_name = None
                                                imagenIdiom = None
                                                text_idiom = None
                                    except Exception as e:     
                                        print(f"Error en procesar_libreF: {e}") 
                                        bool_estado_libref = False
                                        continue                                                   
                                if existeEvent == "No":
                                        # Agregar la lista de detallesEvento al evento
                                    eventoLibreF['f20_Detalles_Evento'] = list_eventos_detalles
                                    v_list_eventos.append(eventoLibreF)
                            else:  # if not ' | ' in urlIni:
                                urlFinal = urlIni
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
                                    print(f'Se adiciona evento y detalles desde LibreF : {hora_event} | {name_event} | {urlFinal}')
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
                                        print(f'Ya existe Url para evento desde LibreF : {hora_event} | {name_event} | {urlFinal}')
                                        continue
                                    else:
                                            # Obtener el evento existente
                                        evento_existente = next((evento for evento in v_list_eventos if evento["f01_id_document"] == existeEvent), None)
                                                # Actualizar los campos en el evento existente si están en None
                                        evento_existente['f02_proveedor'] = evento_existente['f02_proveedor'] + ' | LibreF'
                                        if evento_existente['f05_event_categoria'] is None and event_categoria is not None:
                                            evento_existente['f05_event_categoria'] = event_categoria
                                        if evento_existente['f07_URL_Flag'] is None and url_flag is not None:
                                            evento_existente['f07_URL_Flag'] = url_flag
                                            # Obtener la lista de detalles del evento existente
                                        list_eventos_detalles_existente = evento_existente.get("f20_Detalles_Evento", [])
                                        detalleLibreF = {
                                                'f21_imagen_Idiom': imagenIdiom,
                                                'f22_opcion_Watch': channel,
                                                'f23_text_Idiom': text_idiom,
                                                'f24_url_Final': urlFinal
                                                }
                                            # Agregar detalle al evento existente
                                        list_eventos_detalles_existente.append(detalleLibreF)
                                        evento_existente['f20_Detalles_Evento'] = list_eventos_detalles_existente
                                            # Actualizar el evento existente en la lista
                                        v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                                        print(f'Se adiciona detalles a evento desde LibreF : {hora_event} | {name_event} | {urlFinal}')
                                        channel_name = None
                                        imagenIdiom = None
                                        text_idiom = None
                        except Exception as e:     
                            print(f"Error en procesar_libreF: {e}") 
                            bool_estado_libref = False
                            continue                                             
                except Exception as e:     
                    print(f"Error en procesar_libreF: {e}") 
                    bool_estado_libref = False
                    continue              
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
    
    except Exception as e:
        print(f"Error en procesar_libreF: {e}")
        # Datos del nuevo dealer
        dealerLibreF = {
            "f01_id_dealer": 4,
            "f02_dealer_name": f"LibreF: {str(e)}",
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
            # Realizar la solicitud GET a la URL y obtener el contenido HTML
        responseRojaOn = requests.get(urlRojaOn)
        soup = BeautifulSoup(responseRojaOn.text, 'html.parser')
            # Obtener las filas de la tabla
        tableRows = soup.find_all('tr')
        
        for row in tableRows:
            try:
                linkElement = row.find_all('td')
                if len(linkElement) > 0:
                        # Accede a la información que necesitas en función de la posición de las celdas
                    hora_event = linkElement[0].text
                        # Selecciona el elemento 'a' dentro de la tercera columna
                    name_element = linkElement[2].find('a')
                    name_event_complet = linkElement[2].text.strip()
                    url = "http://tarjetarojatvonline.sx" + name_element['href']
                    url = validate_and_get_url(url)
                    partes_event_name = name_event_complet.split(':')
                    name_event = partes_event_name[1].strip()
                    event_categoria = partes_event_name[0].strip()
                    if 'resultado.rojadirectaonlinetv.net' in url:
                        continue   # Omitir el registro actual y continuar con el siguiente

                    channel_url = obtenerUrlFinalRojaOn(url)
                    if channel_url:
                        channel_name, urlFinal = channel_url.split(" | ", 1)
                        dia_event = fecha_actual
                        hora_event_inicio = int(hora_event[:2])
                        hora_event_inicio -= 1
                        hora_event_inicio %= 24
                        hora_event = str(hora_event_inicio).zfill(2) + hora_event[2:]

                        if contador_registros > 2 and hora_event_inicio > 18:
                            eventNextDay = True

                            # Verificar si eventNextDay es True y hora_event < 9
                        if eventNextDay and hora_event_inicio < 9:
                                # Incrementar dia_event en 1 día
                            dia_event = datetime.strptime(dia_event, '%Y%m%d')
                            dia_event += timedelta(days=1)
                            dia_event = dia_event.strftime('%Y%m%d')

                        list_eventos_detalles = []   # Lista para almacenar la lista de eventos
                        list_eventos_detalles_existente = []   # Lista para almacenar la lista de eventos
                        if contador_registros > 0:
                            existeEvent = verificarExisteEvento(hora_event,name_event)

                        if existeEvent == "No" or contador_registros == 0:
                            contador_registros += 1
                            evento = {
                                    "f01_id_document": contador_registros,
                                    "f02_proveedor" : "RojaOn",
                                    "f03_dia_event" : dia_event,
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
                            print(f'Se adiciona evento y detalles desde RojaOn : {hora_event} | {name_event} | {urlFinal}')
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
                                print(f'Ya existe Url para evento desde RojaOn : {hora_event} | {name_event} | {urlFinal}')
                                continue
                            else:
                                    # Obtener el evento existente si existe
                                evento_existente = next((evento for evento in v_list_eventos if evento.get("f01_id_document") == existeEvent), None)

                                        # Verifica si 'RojaOn' no está ya en la lista de proveedores
                                if 'RojaOn' not in evento_existente['f02_proveedor']:
                                        # Actualizar los campos en el evento existente si están en None
                                    evento_existente['f02_proveedor'] = evento_existente['f02_proveedor'] + ' | RojaOn'

                                if evento_existente['f05_event_categoria'] is None and event_categoria is not None:
                                    evento_existente['f05_event_categoria'] = event_categoria

                                if evento_existente['f07_URL_Flag'] is None and url_flag is not None:
                                    evento_existente['f07_URL_Flag'] = url_flag

                                    # Obtener la lista de detalles del evento existente
                                list_eventos_detalles_existente = evento_existente.get("f20_Detalles_Evento", [])

                                detalle = {
                                        'f21_imagen_Idiom': imagenIdiom,
                                        'f22_opcion_Watch': channel_name,
                                        'f23_text_Idiom': text_idiom,
                                        'f24_url_Final': urlFinal
                                        }
                                    # Agregar detalle al evento existente
                                list_eventos_detalles_existente.append(detalle)
                                    # Actualizar la lista de detalles del evento existente
                                evento_existente['f20_Detalles_Evento'] = list_eventos_detalles_existente
                                    # Actualizar el evento existente en la lista
                                v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                                print(f'Se adiciona detalles a evento existente desde RojaOn : {hora_event} | {name_event} | {urlFinal}')
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
                          
    except Exception as e:
        print(f"Error en procesar_RojaOn: {e}")
        # Datos del nuevo dealer
        dealerRojaOn = {
            "f01_id_dealer": 5,
            "f02_dealer_name": f"RojaOn: {str(e)}",
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
            # Realiza la solicitud HTTP y obtiene el contenido HTML
        response = requests.get(urlRojaTV)
        soup = BeautifulSoup(response.text, 'html.parser')

            # Encuentra todos los elementos <tr>
        RowsRoja = soup.find_all('tr')
        # print(f'urlRojaTV : {urlRojaTV}')
        # print(f'response : {response.status_code}')
        for row in RowsRoja:    
            try:            
                tds = row.find_all('td')
                if len(tds) >= 3:
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
                    url =  validate_and_get_url(url)
                    
                    if 'httpswww' in url:
                        continue

                    try:
                        name_event = newEvent.encode('latin1').decode('utf8')
                    except:
                        name_event = newEvent
                    urlFinal_title = obtenerUrlFinalRojaTV(url)
                    if '408' in urlFinal_title:
                        time.sleep(2)
                        urlFinal_title = obtenerUrlFinalRojaTV(url)

                    if "sin_data" in urlFinal_title:
                        bool_estado_RojaTv = False

                    urlFinal, channel_name = urlFinal_title.split('|')
                    
                    urlFinal = urlFinal.strip()
                    channel_name = channel_name.strip()

                    dia_event = fecha_actual
                    hora_event_inicio = int(hora_event[:2])
                    hora_event_inicio -= 1
                    hora_event_inicio %= 24
                    hora_event = str(hora_event_inicio).zfill(2) + hora_event[2:]
                    
                    if contador_registros > 2 and hora_event_inicio > 18:
                        eventNextDay = True

                        # Verificar si eventNextDay es True y hora_event < 9
                    if eventNextDay and hora_event_inicio < 9:
                            # Incrementar dia_event en 1 día
                        dia_event = datetime.strptime(dia_event, '%Y%m%d')
                        dia_event += timedelta(days=1)
                        dia_event = dia_event.strftime('%Y%m%d')

                    list_eventos_detalles = []   # Lista para almacenar la lista de eventos
                    list_eventos_detalles_existente = []   # Lista para almacenar la lista de eventos
                    if contador_registros > 0:
                        existeEvent = verificarExisteEvento(hora_event,name_event)

                    if existeEvent == "No" or contador_registros == 0:
                        contador_registros += 1
                        evento = {
                                "f01_id_document": contador_registros,
                                "f02_proveedor" : "RojaTv",
                                "f03_dia_event" : dia_event,
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
                        print(f'Se adiciona evento y detalles desde Rojatv : {hora_event} | {name_event} | {urlFinal}')
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
                            print(f'Ya existe Url para evento desde Rojatv : {hora_event} | {name_event} | {urlFinal}')
                            continue
                        else:
                                # Obtener el evento existente si existe
                            evento_existente = next((evento for evento in v_list_eventos if evento.get("f01_id_document") == existeEvent), None)

                                    # Verifica si 'Rojatv' no está ya en la lista de proveedores
                            if 'Rojatv' not in evento_existente['f02_proveedor']:
                                    # Actualizar los campos en el evento existente si están en None
                                evento_existente['f02_proveedor'] = evento_existente['f02_proveedor'] + ' | Rojatv'

                            if evento_existente['f05_event_categoria'] is None and event_categoria is not None:
                                evento_existente['f05_event_categoria'] = event_categoria

                            if evento_existente['f07_URL_Flag'] is None and url_flag is not None:
                                evento_existente['f07_URL_Flag'] = url_flag

                                # Obtener la lista de detalles del evento existente
                            list_eventos_detalles_existente = evento_existente.get("f20_Detalles_Evento", [])

                            detalle = {
                                    'f21_imagen_Idiom': imagenIdiom,
                                    'f22_opcion_Watch': channel_name,
                                    'f23_text_Idiom': text_idiom,
                                    'f24_url_Final': urlFinal
                                    }
                                # Agregar detalle al evento existente
                            list_eventos_detalles_existente.append(detalle)
                                # Actualizar la lista de detalles del evento existente
                            evento_existente['f20_Detalles_Evento'] = list_eventos_detalles_existente
                                # Actualizar el evento existente en la lista
                            v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                            print(f'Se adiciona detalles a evento existente desde Rojatv : {hora_event} | {name_event} | {urlFinal}')
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
                 
    except Exception as e:
        print(f"Error en procesar_RojaTv: {str(e)}")
         # Datos del nuevo dealer
        dealerRojaTv = {
            "f01_id_dealer": 6,
            "f02_dealer_name": f"RojaTv: {str(e)}",
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
            # Realizar la solicitud GET a la URL y obtener el contenido HTML
        responsePlatin = requests.get(urlPlatin)
        html_contentPlatin = responsePlatin.text
            # Crear el objeto BeautifulSoup
        soup = BeautifulSoup(html_contentPlatin, 'html.parser')
            # Encontrar todos los elementos 'tr' en el HTML
        tr_elements_Platin = soup.find_all('tr')
            # Recorrer los elementos 'tr' y obtener la información necesaria
        hrefs_list = [tr.find('a').get('href') for tr in tr_elements_Platin if tr.find('a') is not None]
        href_Repetidos = len(hrefs_list) != len(set(hrefs_list))
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
                            try:
                                url_flag = tr.find('img')['src']
                            except (KeyError, TypeError):
                                url_flag = None
                            td_text = tr.select('td')[1].text.strip()   # Use .strip() to remove leading/trailing spaces
                                # Corregir caracteres específicos
                            td_text = process_special_characters(td_text)
                            hora_event = td_text.split(' ', 1)[0]
                            name_event =  td_text.split(' ', 1)[1]
                            name_event = capitalize_words(name_event)
                            url_event = tr.find('a')['href'].split('https://www.platinsport.com', 1)[-1]
                            url_event = 'https://www.platinsport.com' + url_event
                            url_event = validate_and_get_url(url_event)
                            url_name_pairs = obtenerUrlFinalPlatin(url_event)
                            if url_name_pairs is None or not url_name_pairs:
                                continue
                            
                            dia_event = fecha_actual
                            hora_event_inicio = int(hora_event.split(':')[0].zfill(2))   # Asegura que siempre tenga dos caracteres
                            hora_event_inicio -= 7
                            hora_event_inicio %= 24
                            hora_event = str(hora_event_inicio).zfill(2) + hora_event[2:]
                            
                            
                            if contador_registros > 2 and hora_event_inicio > 18:
                                eventNextDay = True
                                # Verificar si eventNextDay es True y hora_event < 9
                            if eventNextDay and hora_event_inicio < 9:
                                    # Incrementar dia_event en 1 día
                                dia_event = datetime.strptime(dia_event, '%Y%m%d')
                                dia_event += timedelta(days=1)
                                dia_event = dia_event.strftime('%Y%m%d')
                            if contador_registros > 0:
                                existeEvent = verificarExisteEvento(hora_event,name_event)
                            if existeEvent == "No" or contador_registros == 0:
                                contador_registros += 1
                                eventoPlatin = {
                                        "f01_id_document": contador_registros,
                                        "f02_proveedor" : "Platin",
                                        "f03_dia_event" : dia_event,
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
                                            print(f'Ya existe Url para evento desde Platin : {hora_event} | {name_event} | {urlFinal}')
                                            continue
                                        else:
                                            # Obtener el evento existente
                                            evento_existente = next((evento for evento in v_list_eventos if evento["f01_id_document"] == existeEvent), None)
                                                    # Verifica si 'Platin' no está ya en la lista de proveedores
                                            if 'Platin' not in evento_existente['f02_proveedor']:
                                                    # Actualizar los campos en el evento existente si están en None
                                                evento_existente['f02_proveedor'] = evento_existente['f02_proveedor'] + ' | Platin'
                                            if evento_existente['f05_event_categoria'] is None and event_categoria is not None:
                                                evento_existente['f05_event_categoria'] = event_categoria
                                            if evento_existente['f07_URL_Flag'] is None and url_flag is not None:
                                                evento_existente['f07_URL_Flag'] = url_flag
                                                # Obtener la lista de detalles del evento existente
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
                                                # Actualizar el evento existente en la lista
                                            v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                                            print(f'Se adiciona detalles a evento desde Platin : {hora_event} | {name_event} | {urlFinal}')
                                except Exception as e:
                                    print(f"Error desde procesar_Platin: {str(e)}")
                                    bool_estado_platin = False
                                    continue                                                 
                            if existeEvent == "No":
                                    # Agregar la lista de detallesEvento al evento
                                evento['f20_Detalles_Evento'] = list_eventos_detalles
                                v_list_eventos.append(eventoPlatin)
                                print(f'Se adiciona evento y detalles desde Platin : {hora_event} | {name_event} | {urlFinal}')
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
                            responsePlainUnicoURL = requests.get(url_event, headers=headers)   
                            html_contentPlainUnicoURL = responsePlainUnicoURL.text
                            soupPlainUnicoURL = BeautifulSoup(html_contentPlainUnicoURL, 'html.parser')                                                                   
                            divs = soupPlainUnicoURL.find_all('div', class_='myDiv')
                            if len(divs) >= 2:
                                second_myDiv = divs[1]  # El segundo div (índice 1)
                                first_word_title = second_myDiv.text.split()[0]
        
                                if first_word_title == currentDayOfWeek:                                
                                    # Encontrar todo el contenido de la clase 'myDiv1'
                                    div_content = soupPlainUnicoURL.find('div', class_='myDiv1')
                                    # Obtener el contenido de la etiqueta como una cadena de texto
                                    content_text = str(div_content)                                
                                    # Dividir por saltos de línea
                                    lines = content_text.split('\n')    
                                    # Lista para almacenar los eventos
                                    eventos = []
                                    urls_channels = []
                                    # Iterar a través de las líneas del contenido                                
                                    for line in lines:
                                        try:
                                            line = line.strip()  # Eliminar espacios en blanco al principio y al final
                                                                                    
                                            # Si la línea comienza con una hora (por ejemplo, "09:15")
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

                                            # Si la línea comienza con "<a"
                                            elif line.startswith("<a"):
                                                # Extraer la URL y el canal
                                                href_start = line.find('href="') + len('href="')
                                                href_end = line.find('"', href_start)
                                                href = line[href_start:href_end]

                                                channel_start = line.find('nofollow">') + len('nofollow">')
                                                channel_end = line.find("</a>")
                                                channel = line[channel_start:channel_end]
                                                channel = capitalize_words(channel)
                                                # Agregar la URL y el canal a la lista
                                                urls_channels.append({"urlFin": href, "nameChannel": channel})
                                        except Exception as e:
                                            print(f"Error desde procesar_Platin: {str(e)}")
                                            bool_estado_platin = False
                                            continue     
                                    # Agregar el último evento a la lista
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
                                            hora_event=evento["hora_event"]
                                            name_event = evento["name_event"]
                                            url_name_pairs = evento["urlFinal_channel_name"]
                                            name_event = capitalize_words(name_event)
                                            dia_event = fecha_actual
                                            hora_event_inicio = int(hora_event.split(':')[0].zfill(2))   # Asegura que siempre tenga dos caracteres
                                            hora_event_inicio -= 7
                                            hora_event_inicio %= 24
                                            hora_event = str(hora_event_inicio).zfill(2) + hora_event[2:]                                                                                               
                                            if contador_registros > 2 and hora_event_inicio > 18:
                                                eventNextDay = True
                                                # Verificar si eventNextDay es True y hora_event < 9
                                            if eventNextDay and hora_event_inicio < 9:
                                                    # Incrementar dia_event en 1 día
                                                dia_event = datetime.strptime(dia_event, '%Y%m%d')
                                                dia_event += timedelta(days=1)
                                                dia_event = dia_event.strftime('%Y%m%d')
                                            if contador_registros > 0:
                                                existeEvent = verificarExisteEvento(hora_event,name_event)
                                            if existeEvent == "No" or contador_registros == 0:
                                                contador_registros += 1
                                                eventoPlatin = {
                                                                "f01_id_document": contador_registros,
                                                                "f02_proveedor" : "Platin",
                                                                "f03_dia_event" : dia_event,
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
                                                            print(f'Ya existe Url para evento desde Platin : {hora_event} | {name_event} | {urlFinal}')
                                                            continue
                                                        else:
                                                            # Obtener el evento existente
                                                            evento_existente = next((evento for evento in v_list_eventos if evento["f01_id_document"] == existeEvent), None)
                                                                    # Verifica si 'Platin' no está ya en la lista de proveedores
                                                            if 'Platin' not in evento_existente['f02_proveedor']:
                                                                    # Actualizar los campos en el evento existente si están en None
                                                                evento_existente['f02_proveedor'] = evento_existente['f02_proveedor'] + ' | Platin'
                                                            if evento_existente['f05_event_categoria'] is None and event_categoria is not None:
                                                                evento_existente['f05_event_categoria'] = event_categoria
                                                            if evento_existente['f07_URL_Flag'] is None and url_flag is not None:
                                                                evento_existente['f07_URL_Flag'] = url_flag
                                                                # Obtener la lista de detalles del evento existente
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
                                                                # Actualizar el evento existente en la lista
                                                            v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                                                            print(f'Se adiciona detalles a evento desde Platin : {hora_event} | {name_event} | {urlFinal}')
                                                except Exception as e:
                                                    print(f"Error desde procesar_Platin: {str(e)}")
                                                    bool_estado_platin = False
                                                    continue                                                              
                                            if existeEvent == "No":
                                                    # Agregar la lista de detallesEvento al evento
                                                evento['f20_Detalles_Evento'] = list_eventos_detalles
                                                v_list_eventos.append(eventoPlatin)
                                                print(f'Se adiciona evento y detalles desde Platin : {hora_event} | {name_event} | {urlFinal}')
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
                                            print(f"Error desde procesar_Platin: {str(e)}")
                                            bool_estado_platin = False
                                            continue                                                                     
                                else:
                                    bool_estado_platin = False                                    
                        break
                    else:
                        bool_estado_platin = False
            except Exception as e:
                print(f"Error desde procesar_Platin: {str(e)}")
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
                       
    except Exception as e:
        print(f"Error desde procesar_Platin: {str(e)}")
         # Datos del nuevo dealer
        dealerPlatin = {
            "f01_id_dealer": 7,
            "f02_dealer_name": f"Platin: {str(e)}",
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
        urlFinal = None        
        existeEventdlhd = None
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
        responseDaddyLivehd = requests.get(urlDaddyLivehd, headers=headers)    
        
        # Obtener el contenido HTML de la página
        page_content = responseDaddyLivehd.text

        # Separar el contenido usando <hr> como delimitador
        events = page_content.split('<hr>')

        # Filtrar eventos que contienen información
        filtered_events = [event for event in events if event.strip()]

        # Definir patrones de expresiones regulares
        pattern1 = r'\b\d{2}:\d{2}'  # Coincide con '00:00' en el formato HH:MM
        pattern2 = r'<strong>\d{2}:\d{2}'  # Coincide con '<strong>00:00' en el formato HH:MM

        # Filtrar eventos que cumplan con los patrones
        filtered_events = [event for event in filtered_events if re.search(pattern1, event) or re.search(pattern2, event)]
        # Filtrar eventos para omitir aquellos con href que contienen "/extra/"
        filtered_events = [event for event in filtered_events if not re.search(r'href="/extra/', event)]

        # Variable booleana para indicar si encontramos el evento de interés
        found_soccer_event = False
        events_categoria = "Soccer"
              
        # Imprimir los eventos
        for event in filtered_events: 
            try:             
                event_categoria = ""           
                if not found_soccer_event:
                    # Buscar el evento que contiene "<h2 style="background-color:cyan">Soccer</h2>"
                    if '<h2 style="background-color:cyan">Soccer</h2>' in event:
                        found_soccer_event = True
                    continue            
                # Verificar si el evento cambia la categoría
                if '</h2>' in event:
                    # Extraer el texto entre '<h2>' y '</h2>' y actualizar events_categoria
                    categ_match = re.search(r'<h2 style="background-color:cyan">([^<]+)</h2>', event)
                    if categ_match:
                        events_categoria = categ_match.group(1).strip()
                # Obtener la hora_event (formato HH:MM)
                hora_match = re.search(r'(\b\d{2}:\d{2}|\b<strong>\d{2}:\d{2})', event)
                if hora_match:
                    hora_event = hora_match.group(1).strip()
                    
                    # Obtener el name_event (texto después del segundo ':' y antes de '<span')
                    name_match = re.search(r'\d{2}:\d{2}(.+?)<span', event)
                    
                    if name_match:
                        name_event_full = name_match.group(1).strip()                        
                        # Verificar si name_event debe llenarse con el evento completo menos los primeros 5 caracteres
                        
                        if name_event_full.startswith(':'):
                            name_event_full = name_event_full.lstrip(':')   
                            name_event_full = name_event_full.strip()
                            
                        if name_event_full.startswith(events_categoria):
                            name_event_match = re.search(r'[^:]+:[^:]+:\s*(.*?)\s*<span', event)
                            if name_event_match:
                                name_event = name_event_match.group(1).strip()
                                name_event = name_event.replace('Soccer :', '').strip()
                        else:     
                            # Dividir name_event en dos partes si contiene ':'
                            if ':' in name_event_full: 
                                name_parts = name_event_full.split(':')
                                event_categoria += name_parts[0].strip()
                                name_event = name_parts[1].strip()
                            else:
                                name_event = name_event_full
                        
                        # Corregir caracteres específicos
                        name_event = name_event.replace("</strong>", "")
                        name_event = process_special_characters(name_event)
                        event_categoria = process_special_characters(event_categoria)
                            #name_event = capitalize_words(name_event)
    
                #urlInicial_ChannerName = ""
                urlInicial_ChannerName = []  # Inicializa la lista de pares URL y nombre de canal
                url_matches = re.finditer(r'href="([^"]+)"', event)
                channel_matches = re.finditer(r'<a[^>]*>([^<]+)</a>', event)

                dia_event = fecha_actual
                hora_event_inicio = int(hora_event.split(':')[0].zfill(2))   # Asegura que siempre tenga dos caracteres
                hora_event_inicio -= 6
                hora_event_inicio %= 24
                hora_event = str(hora_event_inicio).zfill(2) + hora_event[2:]
                
                #freeviplive.com si se podria obtener
                #weblivehdplay NO se puede por ahora
                for url_match, channel_match in zip(url_matches, channel_matches):
                    url = url_match.group(1).strip()
                    url = url.replace('/stream/','/')
                    url = 'https://dlhd.sx/embed' + url
                    #url = url.replace('/embed/extra/','/extra-embed/')
                    
                    channel = channel_match.group(1).strip()
                    urlInicial_ChannerName.append({"urlFin": url, "nameChannel": channel})
                    #urlInicial_ChannerName += url + " , '" + channel + "' | "

                # Elimina el último '| ' que quedará al final de la cadena
                #urlInicial_ChannerName = urlInicial_ChannerName.rstrip(' | ')
                
                if contador_registros > 0:
                    existeEventdlhd = verificarExisteEvento(hora_event,name_event)

                if existeEventdlhd == "No" or contador_registros == 0:
                    contador_registros += 1
                    eventodlhd = {
                                "f01_id_document": contador_registros,
                                "f02_proveedor" : "DLHD",
                                "f03_dia_event" : dia_event,
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
                list_events_det_dlhd_exits = []   # Lista para almacenar la lista de eventos
                
                for event_details in urlInicial_ChannerName:
                    try:
                        urlFinal = event_details['urlFin']   # Obtiene la URL del evento
                        channel_name = event_details['nameChannel']   # Obtiene el nombre del canal
                        
                        existeUrlEventldhd = None
                        if existeEventdlhd == "No":
                            detalleldhd = {
                                        'f21_imagen_Idiom': imagenIdiom,
                                        'f22_opcion_Watch': channel_name,
                                        'f23_text_Idiom': text_idiom,
                                        'f24_url_Final': urlFinal
                                        }
                            list_events_det_dlhd.append(detalleldhd)

                        else:
                            existeUrlEventldhd = verificarExisteUrlEvento(existeEventdlhd, urlFinal)
                            
                            if existeUrlEventldhd == "Si_Existe_Url":
                                print(f'Ya existe Url para evento desde DLHD : {hora_event} | {name_event} | {urlFinal}')
                                continue
                            else:                  
                                
                                # Obtener el evento existente
                                evento_existente = next((evento for evento in v_list_eventos if evento["f01_id_document"] == existeEventdlhd), None)
                                                
                                # Verifica si 'DLHD' no está ya en la lista de proveedores
                                if 'DLHD' not in evento_existente['f02_proveedor']:
                                    # Actualizar los campos en el evento existente si están en None
                                    evento_existente['f02_proveedor'] = evento_existente['f02_proveedor'] + ' | DLHD'
                                
                                if evento_existente['f05_event_categoria'] is None and event_categoria is not None:
                                    evento_existente['f05_event_categoria'] = event_categoria

                                # if evento_existente['f07_URL_Flag'] is None and url_flag is not None:
                                #     evento_existente['f07_URL_Flag'] = url_flag
                                
                                    # Obtener la lista de detalles del evento existente
                                list_events_det_dlhd_exits = evento_existente.get("f20_Detalles_Evento", [])

                                detalleldhd = {
                                            'f21_imagen_Idiom': imagenIdiom,
                                            'f22_opcion_Watch': channel_name,
                                            'f23_text_Idiom': text_idiom,
                                            'f24_url_Final': urlFinal
                                            }
                                    # Agregar detalle al evento existente
                                list_events_det_dlhd_exits.append(detalleldhd)
                                evento_existente['f20_Detalles_Evento'] = list_events_det_dlhd_exits
                                    # Actualizar el evento existente en la lista
                                v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                                print(f'Se adiciona detalles a evento desde DLHD : {hora_event} | {name_event} | {urlFinal}')
                    except Exception as e:
                        print(f"Error en procesar_DLHD: {e}")    
                        bool_estado_DaddyLivehd = False
                        continue                                       
                if existeEventdlhd == "No":
                        # Agregar la lista de detallesEvento al evento
                    eventodlhd['f20_Detalles_Evento'] = list_events_det_dlhd
                    v_list_eventos.append(eventodlhd)
                    print(f'Se adiciona evento y detalles desde DLHD : {hora_event} | {name_event} | {urlFinal}')   
                    bool_estado_DaddyLivehd = True       
            except Exception as e:
                print(f"Error en procesar_DLHD: {e}")    
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
            
    except Exception as e:
        print(f"Error en procesar_DLHD: {e}")
         # Datos del nuevo dealer
        dealerDaddyLivehd = {
            "f01_id_dealer": 8,
            "f02_dealer_name": f"DLHD: {str(e)}",
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
    # activaLiveTv = 0
    # activaSportline = 0
    # activaDirectatvHDme = 0
    # activaLibreF = 0
    # activaRojaOn = 0
    # activaRojaTv = 0
    # activaPlatin = 1
    # activaDaddyLivehd = 0
    # print(f"activaLiveTv: {activaLiveTv}")
    # print(f"activaSportline: {activaSportline}")
    # print(f"activaDirectatvHDme: {activaDirectatvHDme}")
    # print(f"activaLibreF: {activaLibreF}")
    # print(f"activaRojaOn: {activaRojaOn}")
    # print(f"activaRojaTv: {activaRojaTv}")
    # print(f"activaPlatin: {activaPlatin}")
    # print(f"activaDaddyLivehd: {activaDaddyLivehd}")
    # sys.exit()    

    if activaSportline > 0:
        procesar_SportsLine()

    if activaDirectatvHDme > 0:
        procesar_DirectatvHDme()
    
    if activaRojaOn > 0:
        procesar_RojaOnline()

    if activaRojaTv > 0:
        procesar_RojaTV()
        
    if activaDaddyLivehd > 0:
        procesar_DaddyLivehd()

    if activaPlatin > 0:
        procesar_Platin()

    if activaLiveTv > 0:
        procesar_LiveTV()

    if activaLibreF > 0:
        procesar_LibreF()
 
 # Invocar la función para obtener los eventos
obtener_eventos()
sys.exit()
def obtener_eventos_miss():
    global bool_estado_libref
    global contador_registros
    eventNextDay = False
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
    
    urlInicial = None

    if ind_miss_LibreF > 0:
        try:
            
            evento_live_tv = [evento for evento in v_list_eventos if 'LiveTv' in evento['f02_proveedor']]
            nombre_evento_live_tv = [evento['f06_name_event'] for evento in evento_live_tv]
            #contador_registros = [evento['f01_id_document'] for evento in evento_live_tv]                 
            #contador_registros = contador_registros[0]
            nombre_evento_live_tv = nombre_evento_live_tv[0]
            nombre_evento_live_tv = nombre_evento_live_tv.replace('Vs', '–')
            
            # Realizar la solicitud GET a la URL y obtener el contenido HTML
            fixes = requests.get(urlLiveTv)
            html_contentLiveTv = fixes.text
            # Parsear el contenido HTML
            document = BeautifulSoup(html_contentLiveTv, 'html.parser')
            liveTds = document.select('td[OnMouseOver^="\$(\'#cv"]')
            #print(liveTds)    
            #sys.exit()
            # Encuentra el índice del <td> con el nombre_evento_live_tv
            indice_nombre_evento = None
            
            for i, td in enumerate(liveTds):
                if nombre_evento_live_tv in td.get_text():
                    indice_nombre_evento = i
                    break
            if indice_nombre_evento is not None:
                # Itera sobre los <td> a partir del índice encontrado + 1
                for td in liveTds[indice_nombre_evento + 1:]:
                   
                    url_flag = td.select_one('img')['src'] if td.select_one('img') else ''
                    url_flag = "https:" + url_flag                    
                    event_categoria = (td.select_one('img')['alt'] if td.select_one('img') else '').strip()  # + ' '
                    aElement = td.select_one('a.live')
                    claselive = aElement['class'] if aElement else ''
                    if 'live' not in claselive:
                        continue   # Si la clase no es "live", saltar al siguiente registro
                    urlLinksEvent = 'https://livetv744.me' + (aElement['href'] if aElement else '')
                    enlaceEvent = urlLinksEvent
                    enlaceEventCors =  validate_and_get_url(enlaceEvent)
                    containsText = contains_not_available_text(enlaceEventCors)   # Assuming this is an asynchronous function
                    if containsText == 'SI':
                        continue
                    nameEventOld = aElement.text if aElement else ''
                    name_event = re.sub(r'\s+', ' ', re.sub(r'(?<=\s)[–](?=\s)', 'Vs', nameEventOld)).strip()
                    spanEvdesc = td.select_one('span.evdesc')
                    horaEvent = spanEvdesc.text if spanEvdesc else ''
                    horaRegex = re.compile(r'\b\d{1,2}:\d{2}\b')   # Expresión regular para buscar el formato de hora (por ejemplo, "8:30")
                    horaMatch = horaRegex.search(horaEvent)
                    hora = horaMatch.group(0) if horaMatch else ''   # Si se encuentra una coincidencia, se obtiene la hora
                    # Ajustar la hora agregando o restando la cantidad de horas de la diferencia horaria
                    dia_event = fecha_actual
                    hora_event_inicio = int(hora.split(':')[0].zfill(2))   # Asegura que siempre tenga dos caracteres
                    hora_event_inicio -= 6
                    hora_event_inicio %= 24
                    hora_event = str(hora_event_inicio).zfill(2) + ':' + hora[2:]
                    hora_event = hora_event.replace('::',':')
                    
                    if contador_registros > 2 and hora_event_inicio > 18:
                        eventNextDay = True
                    # Verificar si eventNextDay es True y hora_event < 9
                    if eventNextDay and hora_event_inicio < 9:
                        # Incrementar dia_event en 1 día
                        dia_event = datetime.strptime(dia_event, '%Y%m%d')
                        dia_event += timedelta(days=1)
                        dia_event = dia_event.strftime('%Y%m%d')
                    if 'livetv' in enlaceEventCors:
                        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
                        responseLiveTv = requests.get(enlaceEventCors, headers=headers, allow_redirects=True)
                        if responseLiveTv.status_code == 200:
                            document = BeautifulSoup(responseLiveTv.content, 'html.parser')
                            imagesWithItemprop = document.select('img[itemprop]')
                            if len(imagesWithItemprop) >= 2:
                                logoLocalOld = imagesWithItemprop[0]['src'] if 'src' in imagesWithItemprop[0].attrs else ''
                                logoVisitaOld = imagesWithItemprop[1]['src'] if 'src' in imagesWithItemprop[1].attrs else ''
                                jug_Local = imagesWithItemprop[0]['alt'] if 'alt' in imagesWithItemprop[0].attrs else ''
                                logo_Local = f"https:{logoLocalOld}"
                                jug_Visita = imagesWithItemprop[1]['alt'] if 'alt' in imagesWithItemprop[1].attrs else ''
                                logo_Visita = f"https:{logoVisitaOld}"
                            if contador_registros > 0:
                                existeEvent = verificarExisteEvento(hora_event,name_event)
                            if existeEvent == "No" or contador_registros == 0:
                                contador_registros += 1
                                evento = {
                                        "f01_id_document": contador_registros,
                                        "f02_proveedor" : "LiveTv",
                                        "f03_dia_event" : dia_event,
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
                            tables = document.select('table.lnktbj')
                            list_eventos_detalles = []   # Lista para almacenar la lista de eventos
                            for table in tables:
                                # Verificar si la tabla contiene información relevante
                                img = table.select_one('td > img[title]')
                                if img and 'title' in img.attrs:
                                    imagenIdiom = img['src']
                                    imagenIdiom = f"https:{imagenIdiom}"
                                    text_idiom = img['title']
                                    enlaces = table.select('td > a')
                                    enlace = enlaces[1]['href'] if len(enlaces) > 1 else enlaces[0]['href']
                                    channel_name = table.select_one('td.lnktyt > span')
                                    if channel_name and channel_name.text.strip():
                                        channel_name = channel_name.text.strip()
                                    else:
                                        channel_name = img['title']
                                    if 'acestream://' in enlace or '/tinyurl.com' in enlace:
                                        urlFinal = enlace
                                    elif ((not enlace.startswith('http')) and (not 'acestream://' in enlace or not '/tinyurl.com' in enlace)):
                                        enlace = f"https:{enlace}"
                                        urlFinal = obtener_url_live_tv_final(enlace)
                                    existeUrlEvent = None
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
                                            print(f'Ya existe Url para evento desde LiveTv Miss : {hora_event} | {name_event} | {urlFinal}')
                                            continue
                                        else:
                                            # Obtener el evento existente
                                            evento_existente = next((evento for evento in v_list_eventos if evento["f01_id_document"] == existeEvent), None)
                                            # Obtener la lista de detalles del evento existente
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
                                            # Actualizar el evento existente en la lista
                                            v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                                            print(f'Se adiciona detalles a evento desde LiveTv Miss: {hora_event} | {name_event} | {urlFinal}')
                            if existeEvent == "No":
                                # Agregar la lista de detallesEvento al evento
                                evento['f20_Detalles_Evento'] = list_eventos_detalles
                                v_list_eventos.append(evento)
                                print(f'Se adiciona evento y detalles desde LiveTv Miss: {hora_event} | {name_event} | {urlFinal}')    
            else:
                print(f'No encuentra el ultimo evento LiveTv MISS: {nombre_evento_live_tv}')  
                # Iterar sobre los documentos y agregarlos a v_list_eventos_en_BD
                for evento_db in eventos_db:
                    evento_data = evento_db.to_dict()       
                    if 'LiveTv' in evento_data['f02_proveedor']:
                        v_list_eventos.append(evento_data)           
                procesar_LiveTV()
        except Exception as e:
            print(f"Error en obtener_eventos_miss desde LiveTV MISS: {(e)}")                                 
        try:
            #bool_estado_libref = True
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
            response = requests.get(urlLibreF, headers=headers)
            soup = BeautifulSoup(response.text, 'html.parser')
            # Obtener la lista de eventos (<li> elementos)
            eventos = soup.find_all('li')
            
            # print(f"eventos: {eventos}")
            for event_index, events_miss in enumerate(v_list_eventos):
                detalles_miss_evento = events_miss.get('f20_Detalles_Evento', [])
                #eventCategoria = events_miss.get('f05_event_categoria')
                nameEvent = events_miss.get('f06_name_event')
                # Crear el texto de búsqueda
                #eventoAbuscar = f"{eventCategoria}: {nameEvent.replace('Vs', 'vs.')}"
                eventoAbuscar = f"{nameEvent.replace('Vs', 'vs.')}"
              
                # print(f"detalles_miss_evento: {detalles_miss_evento}")
                for detalle_index, detalle_miss in enumerate(detalles_miss_evento):
                    if 'sin_data' in detalle_miss.get('f22_opcion_Watch', ''):
                        opcionWatch = detalle_miss.get('f22_opcion_Watch')
                        if ":" in opcionWatch:
                            opcionWatch = opcionWatch.split(":")[0].strip()                            
                        eventoAbuscar = eventoAbuscar.replace(" vs ", " vs. ").strip()
                        # Encontrar eventos con una similitud alta
                        matching_eventos = [evento_li for evento_li in eventos if eventoAbuscar in evento_li.text.encode('latin1').decode('utf8')]
                        # Recorrer los elementos li encontrados    
                        # print(f"matching_eventos: {matching_eventos} ") 
                        # sys.exit()
                        enlace_opcionWatch = None              
                        for evento_li in matching_eventos:
                            links = evento_li.find_all('a', href=lambda value: value and value != '#')
                            for link in links:
                                if opcionWatch in link.text:
                                    enlace_opcionWatch = link.get('href')
                                    break
                            if enlace_opcionWatch:
                                break
                            
                        urlInicial = "https://librefutboltv.com" + enlace_opcionWatch      
                        urlInicial =  validate_and_get_url(urlInicial)
                                             
                        if "/embed/" not in urlInicial:
                            urlFin = obtenerUrlFinalLibreTV(urlInicial)
                        else:
                            urlFin = obtenerUrlFinalLibreTVSelenium(urlInicial)

                        if urlFin is None:
                            bool_estado_libref = False
                            urlFin = urlInicial
                                                    
                        if urlFin.startswith('//'):
                            urlFin = 'https:' + urlFin
                        
                        if urlFin is not None and urlFin != urlInicial:
                            # Accede a los datos del evento
                            evento_data = events_miss  # Esto ya es un diccionario
                            # Accede a los detalles del evento
                            detalles_evento = evento_data.get('f20_Detalles_Evento', [])
                            # Actualiza la URL en los detalles del evento
                            detalles_evento[detalle_index]['f24_url_Final'] = urlFin
                            detalles_evento[detalle_index]['f22_opcion_Watch'] = opcionWatch
                            print(f"Actualiza la URL en los detalles del evento desde LibreF Miss: {eventoAbuscar} | urlInicial:{urlInicial} | urlFin:{urlFin}")
                            # Actualiza la lista de detalles en el evento
                            evento_data['f20_Detalles_Evento'] = detalles_evento
                            # Actualiza el evento en la lista principal
                            v_list_eventos[event_index] = evento_data 
        except Exception as e:
            print(f"Error en obtener_eventos_miss desde LibreF MISS: {(e)}")

obtener_eventos_miss()

def insertar_dato_en_bd_dealer(dealer_data):
    try:
        doc_ref = col_provee.document()
        doc_ref.set(dealer_data)
        print("Se insertaron los datos en la colección de dealer", col_provee)
    except Exception as e:
        print("Ocurrió un error al insertar los datos de dealer:", str(e))
        
def eliminar_y_insertar_dealer(dealer):
    dealer_id = dealer.get('f01_id_dealer')
    if dealer_id:
        eliminar_dato_en_bd_dealer(dealer_id, dealer)

def eliminar_dato_en_bd_dealer(dealer_id, dealer):
    try:
        #if ind_miss_LibreF > 0:
        # Realizar una consulta para encontrar el documento con el valor f01_id_dealer igual a 3
        query = col_provee.where("f01_id_dealer", "==", dealer_id)
        docs = query.stream()
        for doc in docs:
            doc.reference.delete()
            print(f"Se eliminó el documento: {doc.reference.id}")        
        print(f"Se eliminó el dato de la colección dealers: {col_provee}")
            
        insertar_dato_en_bd_dealer(dealer)
    except Exception as e:
        print(f"Ocurrió un error al eliminar el dato de dealer: {str(e)}")        

 # Procesar los datos en vListDealers

for dealer in vListDealers:
    dealer_id = dealer.get('f01_id_dealer')   # Obtén el ID del dealer
    if dealer_id:
        eliminar_y_insertar_dealer(dealer)
    else:
        print("El dato no tiene un ID, no se puede actualizar en la BD.")

### Procesar eventos
## Metodo Eliminar eventos 
def eliminar_dato_en_bd_evento(evento_id):
    try:
        # Realizar una consulta para encontrar el documento con el valor f01_id_dealer igual a 3
        query = col_events.where("f01_id_document", "==", evento_id)
        doc_ref = query.stream()
        for doc in doc_ref:
            doc.reference.delete()
            print(f"Se eliminó el documento:{evento_id} {doc.reference.id}")

        #print(f"Se eliminó el dato de la colección de eventos: {evento_id_str} {col_events}")
        #print("Se eliminó el dato de la colección de eventos", col_events)
    except Exception as e:
        print("Ocurrió un error al eliminar el dato de evento:", str(e))

## Metodo insertar eventos 
def insertar_dato_en_bd(evento):
    try:
        doc_ref = col_events.document()
        doc_ref.set(evento)
        print(f"Se insertaron los datos en la colección de eventos:{col_events.document()}")
    except Exception as e:
        print("Ocurrió un error al insertar los datos de evento:", str(e))

 # Convertimos los datos a cadenas JSON para una comparación completa

v_list_eventos_str = [json.dumps(item, sort_keys=True) for item in v_list_eventos]
v_list_eventos_2_str = [json.dumps(item, sort_keys=True) for item in v_list_eventos_2]

 # Obtenemos los datos nuevos comparando las cadenas JSON
for i, evento_str in enumerate(v_list_eventos_str):
    if evento_str not in v_list_eventos_2_str:
         # Si es un nuevo dato, lo agregamos a la lista de eventos nuevos
        v_list_eventos_news.append(v_list_eventos[i])

# Eliminamos y luego insertamos los registros
#print(f"v_list_eventos_news: {v_list_eventos_news}")
for evento in v_list_eventos_news:
    document_id = evento.get('f01_id_document')  # Obtén el ID documento del evento
    #print(f"document_id: {document_id}")
    if document_id:
        if ind_miss_LibreF > 0:
            eliminar_dato_en_bd_evento(document_id)

        # # Esperamos y verificamos si el documento se ha eliminado
        # max_retries = 3
        # retries = 0
        # while retries < max_retries:
        #     time.sleep(1)  # Esperamos 1 segundo
        #     doc_ref = col_events.document(document_id)
        #     if not doc_ref.get().exists:
        #         break
        #     retries += 1

        # if retries == max_retries:
        #     print("El dato no se eliminó correctamente después de varios intentos.")

        # Inserta el nuevo dato
        insertar_dato_en_bd(evento)
    else:
        print("El dato no tiene un ID, no se puede actualizar en la BD.")

### Insertar el dia.
if dia_actual_bd is None or fecha_actual > dia_actual_bd:
     # Insertar el nuevo día en la colección
    dia_evento_ref = db.collection('dia_evento').document('d_dia')
    dia_evento_ref.set({'f01_dia': fecha_actual})

############################################# envio de mensaje y finalizacion
# Obtener la hora de fin de la ejecución
horaFinEjecucion = datetime.now()

 # Calcular la diferencia de tiempo
diferencia_segundos = (horaFinEjecucion - horaIniciaEjecucion).total_seconds()
 # Convertir la diferencia de tiempo a minutos
diferencia_minutos = diferencia_segundos / 60

 # Imprimir la diferencia de tiempo en segundos
 # print(f"El programa tardó {diferencia_minutos} minutos en ejecutarse.")

 # Función para enviar un mensaje
async def enviar_mensaje_telegram(chat_id, variable_valor):
    message = f"El programa tardó  {round(variable_valor, 2):.2f}  minutos en ejecutarse."
    print(message)
    try:
        await bot.send_message(chat_id=chat_id, text=message)
        print("Mensaje enviado con éxito.")
    except telegram.error.TelegramError as e:
        print(f"Error al enviar el mensaje: {e}")

def obtener_chat_id(token):
    url = f'https://api.telegram.org/bot{token}/getUpdates'
    response = requests.get(url)
    data = response.json()

    if data['ok'] and data['result']:
        chat_id = data['result'][0]['message']['chat']['id']
        return chat_id
    else:
         # Manejar el caso en el que no se pudo obtener el chat_id
        print('No se pudo obtener el chat_id. Revisa tu conexión o el token.')
        return None

 # Reemplaza "TU_TOKEN_DEL_BOT" con el token de tu bot

token = "6559813109:AAEUKzEG6rRIFrt2pwkcHhZuA9Ynt3kqvlI"
 # Inicializar el bot
bot = telegram.Bot(token=token)
 # Obtén tu chat_id
chat_id = '5954221232'  # obtener_chat_id(token)
 # Llamar a la función para enviar un mensaje
 # Reemplaza 'YOUR_CHAT_ID' con el ID de chat al que deseas enviar el mensaje
asyncio.run(enviar_mensaje_telegram(chat_id, diferencia_minutos))
