import copy
import asyncio
# import pdb
import sys
import time
import re
import urllib.parse
import html
import unicodedata
import json
import decimal
import urllib3
import requests
import telegram
import boto3
import os
import logging
import pytz

from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from fuzzywuzzy import fuzz
from boto3.dynamodb.conditions import Key
from playwright.sync_api import sync_playwright
from deepdiff import DeepDiff
from collections import deque
from decimal import Decimal

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configura tus credenciales
aws_access_key_id = "AKIA5PXGRFNCH6O7E7U4"
aws_secret_access_key = "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"
region = "us-east-1"
dynamodb = boto3.resource("dynamodb",region_name=region,aws_access_key_id=aws_access_key_id,aws_secret_access_key=aws_secret_access_key,)

t_dia_evento = dynamodb.Table("dia_evento")
t_dealers = dynamodb.Table("dealers")
t_eventos = dynamodb.Table("eventos")

pintar_mensajes = False
logging_mensajes = True

urlBases = "https://livetv.sx/enx/allupcomingsports/" #https://livetv744.me/enx/'
urlPlatin = "https://www.platinsport.com"
urlportsonline = "https://sportsonline.gl/prog.txt"
urlRojaOn = "https://ww1.tarjetarojatvonline.sx"
urlRojaTV = "https://tarjetarojatv.run"
urlDirectatvHDme = "https://directatvhd.me"
baseurlDLHD = "https://daddylive.mp/"
urlDaddyLivehd = baseurlDLHD + "schedule/schedule-generated.php"
# urlDaddyLivehd = "https://daddylive.mp/" #"https://dlhd.sx/schedule/schedule-generated.json"
urlLibreFAgenda = "https://futbollibre.futbol/tv9/agenda/"
urlLFJson = "https://golazoplay.com/agenda.json"
base_url_LFJson = "https://futbollibreonline.org"  # 'https://futbollibrehd.pe'

# urlLibreSU = 'https://librefutbol.su/agenda/'
#https://pelota-libre.org/agenda/

json_anterior_LFJSON = None
json_anterior_DLHD = None
json_file_path_LFJSON = "json_anterior_LFJSON.json"
json_file_path_DLHD = "json_anterior_DLHD.json"
json_file_path_lista_eventos = "json_anterior_lista_eventos.json"
global_message_log = ""
global_errors_log = ""
hour_update_bases = datetime.now()

orden_proveedores = {
    'DLHD': 1,
    'Platin': 2,
    'LiveTV': 3,
    'Sportline': 4,
    'LFJson': 5,
    'LibreF': 6,
    'DirectatvHDme': 7,
    'RojaOn': 8,
    'RojaTv': 9,
    'Bases': 10 
}


def manejar_error_mensajes(mensaje, ind=0):
    if pintar_mensajes:
        print(mensaje)
    if logging_mensajes:
        if ind == 0:
            logging.info(mensaje)
        if ind == 1:
            logging.error(mensaje)
            agregar_mensaje_al_log(mensaje)


def convert_decimals(obj):
    """Convierte objetos Decimal a int o float según corresponda."""
    if isinstance(obj, Decimal):
        # Si el valor es un número entero, convertirlo a int
        if obj == int(obj):
            return int(obj)
        # Si tiene decimales, convertirlo a float
        return float(obj)
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimals(v) for v in obj]
    return obj


def guardar_json_local(json_file_path, json_data):
    try:
        if "lista_eventos" in json_file_path:
            # Convertir Decimal a float o int
            json_data = convert_decimals(json_data)

        with open(json_file_path, "w", encoding="utf-8") as file:
            json.dump(json_data, file, ensure_ascii=False, indent=4)
        manejar_error_mensajes(f"JSON guardado localmente. {json_file_path}", 0)
    except Exception as e:
        manejar_error_mensajes(f"Error guardando el JSON localmente: {json_file_path} | {e}", 1)


def cargar_json_local(json_file_path):
    try:
        if os.path.exists(json_file_path):
            with open(json_file_path, "r", encoding="utf-8") as file:
                manejar_error_mensajes(f"JSON Cargado correctamente. {json_file_path}", 0)
                return json.load(file)

        return None
    except Exception as e:
        manejar_error_mensajes(f"Error cargando el JSON localmente: {json_file_path} | {e}", 1)
        return None


def configurar_logger():
    # Nombre del archivo log
    nombre_log = os.path.splitext(os.path.basename(__file__))[0] + ".log"
    ruta_log = os.path.join(os.path.dirname(__file__), nombre_log)

    # Configurar el logger
    logging.basicConfig(
        level=logging.INFO,  # Nivel de los mensajes (INFO, DEBUG, etc.)
        format="%(asctime)s - %(levelname)s - %(message)s",  # Formato del log
        filename=ruta_log,  # Ruta del archivo de log
        filemode="a",  # 'a' = append, para concatenar mensajes
    )


# Funcion para calcular la similitud entre textos normalizados
def similar(a, b):
    return fuzz.ratio(a.lower(), b.lower())


# Diccionario de caracteres especiales y sus reemplazos
special_characters = {
    "\u0301": "",
    "\u0307": "",  # Punto superior
    "\u0130": "I",
    "̇": "",
    "&# 105;": "i",
    "&# 775;": "i",
    "&# 225;": "a",
    "&# 233;": "e",
    "&# 204;": "I",
    "&# 205;": "I",
    "&# 206;": "I",
    "&# 207;": "I",
    "&# 236;": "i",
    "&# 237;": "i",
    "&# 238;": "i",
    "&# 239;": "i",
    "&# 8211;": "-",
    "&# 304;": "I",
    "&# 305;": "i",
    "Ä°": "I",
    "Ã³": "o",
    "&# 243": "o",
    "&# 179": "o",
    "&# 225;": "a",  # a
    "&# 233;": "e",  # e
    "&# 237;": "i",  # i
    "&# 243;": "o",  # o
    "&# 250;": "u",  # u
    "&# 193;": "A",  # a
    "&# 201;": "E",  # e
    "&# 205;": "I",  # i
    "&# 211;": "O",  # o
    "&# 218;": "U",  # u
    "&# 228;": "a",  # ä
    "&# 235;": "e",  # ë
    "&# 239;": "i",  # ï
    "&# 246;": "o",  # ö
    "&# 252;": "u",  # ü
    "&# 196;": "A",  # Ä
    "&# 203;": "E",  # Ë
    "&# 207;": "I",  # Ï
    "&# 214;": "O",  # Ö
    "&# 220;": "U",  # Ü
    "&# 224;": "a",  # à
    "&# 232;": "e",  # è
    "&# 236;": "i",  # ì
    "&# 242;": "o",  # ò
    "&# 249;": "u",  # ù
    "&# 192;": "A",  # À
    "&# 200;": "E",  # È
    "&# 204;": "I",  # Ì
    "&# 210;": "O",  # Ò
    "&# 217;": "U",  # Ù
    "&# 198;": "AE",  # Æ
    "&# 230;": "ae",  # æ
    "&# 339;": "oe",  # œ
    "&# 338;": "OE",  # Œå
    "&# 229;": "a",  # å
    "&# 231;": "c",  # ç
    "&# 240;": "eth",  # ð
    "&# 254;": "thorn",  # þ
    "&# 222;": "TH",  # Þ
    "&# 241;": "n",  # ñ
    "&# 209;": "N",  # Ñ
    "&# 65533;": "",  # �
    "&# 175;": "",  # ¯
    "&# 161;": "",  # ¡
    "&# 191;": "",  # ¿
    "&# 8211;": "-",  # en dash
    "&# 8220;": '"',  # left double quotation mark
    "&# 8221;": '"',  # right double quotation mark
    "&# 8216;": "'",  # left single quotation mark
    "&# 8217;": "'",  # right single quotation mark
    "&# 8230;": "...",  # horizontal ellipsis
    "&# 8212;": "--",  # em dash
    "&# 8217;": "'",  # right single quotation mark
    "&# 8218;": ",",  # single low-9 quotation mark
    "&# 8222;": ",,",  # double low-9 quotation mark
    "&# 8224;": "†",  # dagger
    "&# 8225;": "‡",  # double dagger
    "&# 8226;": "•",  # bullet
    "&# 8722;": "-",  # minus sign
    "&# 8729;": ".",  # bullet
    "&# 160;": " ",  # non-breaking space
    "&# 173;": "",  # soft hyphen
    "&# 8209;": "-",  # non-breaking hyphen
    "&# x2013;": "-",  # en dash
    "&# x2014;": "--",  # em dash
    "&# x2018;": "'",  # left single quotation mark
    "&# x2019;": "'",  # right single quotation mark
    "&# x201c;": '"',  # left double quotation mark
    "&# x201d;": '"',  # right double quotation mark
    "&# x2022;": "•",  # bullet
    "&# x2026;": "...",  # horizontal ellipsis
    "&# x2030;": "‰",  # per mille sign
    "&# x20AC;": "€",  # euro sign
    "&# x2122;": "™",  # trademark sign
    "&# x2C6;": "^",  # circumflex accent
    "&# x2DC;": "~",  # small tilde
    "&# x200B;": "",  # zero width space
    "&# x200C;": "",  # zero width non-joiner
    "&# x200D;": "",  # zero width joiner
    "&# x200E;": "",  # left-to-right mark
    "&# x200F;": "",  # right-to-left mark
    "Ã©;": "e",
}


def process_special_characters(text):
    # Normalize text to decomposed form
    normalized_text = unicodedata.normalize("NFKD", text)
    # Process character by character
    processed_text = ""
    for char in normalized_text:
        # processed_text += special_characters.get(char, char)
        if char in special_characters:
            processed_text += special_characters[char]
        else:
            processed_text += char
    return processed_text


def capitalize_words(text):
    words = text.lower().split()
    capitalized_words = [word.capitalize() for word in words]
    return " ".join(capitalized_words)


def obtenerUrlFinalPlatin(url_event):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"}
    max_retries = 5
    retries = 0
    url_name_pairs = []
    while retries < max_retries:
        try:
            with requests.Session() as session:
                response = session.get(url_event, headers=headers, allow_redirects=True, verify=False, timeout=20)
                if response.status_code != 429:
                    html_content = response.text
                    soup = BeautifulSoup(html_content, "html.parser")
                    a_elements = soup.find_all("a", href=True)
                    for a_element in a_elements:
                        url_fin = a_element["href"].strip()
                        if url_fin.startswith("acestream://"):
                            name_channel = a_element.text.strip()
                            name_channel = capitalize_words(name_channel)
                            url_name_pairs.append({"urlFin": url_fin, "nameChannel": name_channel})
                    break
                else:
                    url_name_pairs = "No pudo obtener url por 429 maximo de reintentos"
                    time.sleep(10)
                    retries += 1
        except requests.exceptions.RequestException as e:
            manejar_error_mensajes(f"Error en obtenerUrlFinalPlatin: {e} | url_event: {url_event}", 1)
            retries += 1
            time.sleep(10)
    return url_name_pairs


def obtenerUrlFinalRojaOn(url_inicial):
    try:
        max_retries = 5
        retries = 0
        while retries < max_retries:
            # Obtener el HTML de la página usando Selenium
            # response = obtenerResponseSelenium(url_inicial)
            response = validate_and_get_url(url_inicial)

            if response:  # Verificar si se obtuvo una respuesta válida
                soup = BeautifulSoup(response, "html.parser")

                # Buscar elementos relevantes en la página
                iframe_element = soup.find("iframe")
                title_element = soup.find("title")

                if iframe_element:
                    url_final = iframe_element["src"]

                    # if "tutvlive" in url_final:
                    #     return None

                    # url_final = url_final.replace("https://tvhd.tutvlive.site/stream.php?ch=", "")
                    

                    title_text = title_element.text if title_element else "Desconocido"
                    nameChannel_partes = title_text.split("En Vivo")
                    nameChannel = nameChannel_partes[0].strip()

                    result = f"{nameChannel} | {url_final}"
                    return result

                manejar_error_mensajes(f"No se encontró el elemento 'iframe' en obtenerUrlFinalRojaOn para: {url_inicial}", 0)
                return None

            else:
                time.sleep(5)  # Espera antes de reintentar
                retries += 1

    except Exception as e:
        manejar_error_mensajes(f"Error en obtenerUrlFinalRojaOn: {e} | url_inicial: {url_inicial}", 1)
        return None


def obtenerUrlFinalRojaTV(enlace):
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"}
        max_retries = 5

        for _ in range(max_retries):
            if "tarjetarojatv" not in enlace and "elitegoltv" not in enlace:
                return enlace  # Si el enlace no coincide, devuelve el original

            response = requests.get(enlace, headers=headers, allow_redirects=True, verify=False)
            if response.status_code != 200:
                manejar_error_mensajes("Durmiendo 5 seg en obtenerUrlFinalRojaTV por response <> 200",0,)
                time.sleep(5)  # Espera antes de reintentar

                continue

            soup = BeautifulSoup(response.text, "html.parser")
            div_element = soup.find("div", class_="iframe-container")
            if not div_element:
                return enlace

            div_content = div_element.encode_contents().decode("utf-8")
            script_element = div_element.find("script")
            if not script_element:
                return enlace

            # Extraer fid y src
            script_content = script_element.get_text()
            fid_match = re.search(r'fid="([^"]+)";', script_content)
            src_match = re.search(r'src="([^"]+?/[^/]+)', div_content)
            v_fid = fid_match.group(1) if fid_match else None
            urlevento = src_match.group(1) if src_match else None

            if not urlevento:
                return enlace

            # Manejo de URLs especificas
            if "radamel" in urlevento:
                for _ in range(max_retries):
                    url_final = f"{urlevento}/reproductor/{v_fid}.php"
                    response_final = requests.get(url_final,headers=headers,allow_redirects=True,verify=False,)
                    if response_final.status_code == 200:
                        iframe_match = re.search(r'<iframe[^>]*?allowfullscreen="true"[^>]*?src="([^"]+)"',response_final.text,re.IGNORECASE,)
                        if iframe_match:
                            iframe_src = iframe_match.group(1)
                            return iframe_src if "livehdplay" not in iframe_src else url_final
                    manejar_error_mensajes("Durmiendo 3 seg en obtenerUrlFinalRojaTV por radamel",0,)
                    time.sleep(3)  # Espera antes de reintentar

                return url_final  # Devuelve la URL generada en caso de fallo

            if "vikistream" in urlevento:
                return f"https://vikistream.com/embed2.php?player=desktop&live={v_fid}"

            return enlace  # Si no coincide con ningun caso, devuelve el original

        return enlace  # Si todos los reintentos fallan, devuelve el enlace original

    except Exception as e:
        manejar_error_mensajes(f"Error en obtenerUrlFinalRojaTV: {e} | url_inicial: {enlace}", 1)
        return enlace


def obtenerUrlFinalLibreTV(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"}
        max_retries = 5
        retries = 0

        while retries < max_retries:
            response = requests.get(url, headers=headers, allow_redirects=True, verify=False)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                iframe_element = soup.find("iframe")
                src = iframe_element["src"] if iframe_element else None
                opciones_enlaces_div = soup.find("div", {"align": "left"})

                # Si el src es "embed.html", extrae la URL final.
                if src == "embed.html":
                    src = url + src

                if opciones_enlaces_div:
                    opciones_enlaces = opciones_enlaces_div.find_all("a", {"class": "btn btn-fl"})
                    enlaces_y_textos = []

                    for opcion_enlace in opciones_enlaces:
                        enlace = opcion_enlace["href"]
                        texto = opcion_enlace.get_text()

                        if enlace.startswith("//"):
                            enlace = "https:" + enlace

                        enlace_decoded = html.unescape(enlace)
                        texto_decoded = process_special_characters(texto)

                        enlaces_y_textos.append(f"{enlace_decoded} | {texto_decoded}")

                    if enlaces_y_textos:
                        enlaces_textos_juntos = " | ".join(enlaces_y_textos)
                        return enlaces_textos_juntos
                    else:
                        return src
                else:
                    return src
            else:
                time.sleep(5)  # Espera 5 segundos antes de reintentar
                retries += 1

        return None  # Si se alcanzan los maximos reintentos y no se obtiene una respuesta satisfactoria
    except Exception as e:
        manejar_error_mensajes(f"Error en obtenerUrlFinalLibreTV: {e} | url_inicial: {url}", 1)
        return None


def obtenerUrlFinalLibreTVSelenium(initial_url):
    try:
        # Configurando las opciones de Chrome para navegacion en segundo plano y sin notificaciones
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Ejecutar en modo headless
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)...")
        chrome_options.add_argument("--disable-notifications")  # Desactivar notificaciones
        chrome_options.add_argument("--log-level=3")  # Suprimir mensajes de log
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
        chrome_options.add_argument("--silent")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--ignore-certificate-errors")  # Ignorar errores de SSL
        chrome_options.add_argument("--incognito")  # Modo incognito para evitar interferencias
        # Usar el path sin el argumento 'executable_path'
        driver = webdriver.Chrome(options=chrome_options)  # No es necesario 'executable_path'

        try:
            driver.get(initial_url)
            # html = driver.page_source
            # with open("pagina_debug_selenium.html", "w", encoding="utf-8") as f:
            #     f.write(html)

            wait = WebDriverWait(driver, 10)  # Esperar hasta 10 segundos para que aparezca el iframe
            iframe = wait.until(EC.presence_of_element_located((By.TAG_NAME, "iframe")))
            final_url = iframe.get_attribute("src")

        finally:
            # Cerrar el WebDriver
            driver.quit()

        return final_url

    except Exception as e:
        manejar_error_mensajes(f"Se produjo un error en obtenerUrlFinalLibreTVSelenium: {str(e)}",1,)
        return None


def obtenerResponseSelenium(initial_url):
    try:
        # Configurando las opciones de Chrome para navegación en segundo plano y sin notificaciones
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Ejecutar en modo headless
        chrome_options.add_argument("--disable-notifications")  # Desactivar notificaciones
        chrome_options.add_argument("--log-level=3")  # Suprimir mensajes de log
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
        chrome_options.add_argument("--silent")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--ignore-certificate-errors")  # Ignorar errores de SSL
        chrome_options.add_argument("--incognito")  # Modo incognito para evitar interferencias

        # Usar el path sin el argumento 'executable_path'
        driver = webdriver.Chrome(options=chrome_options)  # No es necesario 'executable_path'

        try:
            driver.get(initial_url)

            # Esperar a que la página cargue completamente
            time.sleep(20)  # Esperar 5 segundos para asegurar que todo esté cargado

            # Obtener el HTML de la página después de que se haya cargado completamente
            page_html = driver.page_source

        finally:
            # Cerrar el WebDriver
            driver.quit()

        return page_html

    except Exception as e:
        manejar_error_mensajes(f"Se produjo un error en obtenerResponseSelenium: {str(e)}", 1)
        return None


def obtenerUrlFinalLibreTVPlaywright(initial_url):
    try:
        # Inicia Playwright y configura el navegador en modo headless
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            # context = browser.new_context(ignore_https_errors=True)  # Ignorar errores de certificado SSL
            context = browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64)...", ignore_https_errors=True)

            page = context.new_page()
            # Navegar a la URL inicial
            page.goto(initial_url, wait_until="load")  # Asegurarse de que la pagina cargue completamente
            # html = page.content()
            # with open("pagina_debug_Playwright.html", "w", encoding="utf-8") as f:
            #     f.write(html)
            # Esperar la presencia del iframe
            iframe = page.wait_for_selector("iframe", timeout=10000)  # Esperar hasta 10 segundos
            # Obtener el atributo 'src' del iframe
            final_url = iframe.get_attribute("src")
            # Cerrar el navegador
            browser.close()
            return final_url

    except Exception as e:
        manejar_error_mensajes(f"Se produjo un error en obtenerUrlFinalLibreTVPlaywright: {str(e)}",1,)
        return None


def obtenerUrlFinalRojaHDme(url):
    max_retries = 5
    retries = 0
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}

    while retries < max_retries:
        try:
            response = requests.get(url, headers=headers, allow_redirects=True, verify=False)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                title = soup.head.title.text
                palabras_clave = ["rojadirecta","tarjetarojatvonline", "Rojadirecta"]  # Agrega aqui las palabras clave que deseas eliminar
                title_sin_palabras = title
                for palabra_clave in palabras_clave:
                    title_sin_palabras = title_sin_palabras.replace(palabra_clave, "")

                en_vivo_index = title_sin_palabras.lower().find("en vivo")
                src_value = ""

                if "directatvhd" in url:
                    iframe_element = soup.select_one("iframe[allowfullscreen]")
                    if iframe_element:
                        src_value = iframe_element.get("src", "")

                if en_vivo_index != -1:
                    urlfin = f"{title_sin_palabras[:en_vivo_index].strip()} | {src_value}"
                else:
                    urlfin = f"{title_sin_palabras.strip()} | {src_value}"
                return urlfin
            else:
                retries += 1
                manejar_error_mensajes(f"No pudo obtener UrlFinalRojaHDme desde: {url} | por error: {response.status_code}. Reintentando...",0,)
                time.sleep(10)  # Espera 5 segundos antes de reintentar

        except Exception as e:
            retries += 1
            manejar_error_mensajes(f"No pudo obtener UrlFinalRojaHDme desde: {url} | por error: {response.status_code}. Reintentando...: {str(e)}",1,)
            return None                

    return f"Web | No pudo obtener url despues de {max_retries} intentos"


def normalizar_nombre_evento(name_event):
    common_words = ["vs","vs.","-","fc","cf","afc","(w)","w","u17","u-17","u18","u-18","u19","u-19","u21","u-21","/",".","sc","lp",",","atl","ud",]
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
            # return fuzz.token_set_ratio(evento_name_normalizado, name_event_normalizado) >= 80 and evento["f04_hora_event"] == fecha_hora
            return fuzz.token_set_ratio(evento_name_normalizado, name_event_normalizado) >= 80 and evento["f03_dia_event"] == fecha_hora

        # Verificar en la lista de eventos inicial
        evento_existente = next((evento for evento in v_list_eventos if comparar_eventos(evento)),None,)

        # Si no se encuentra en la lista inicial, verificar en la lista secundaria
        if evento_existente is None:
            evento_existente = next((evento for evento in v_list_eventos_3 if comparar_eventos(evento)),None,)

        if evento_existente:
            id_document = evento_existente["f01_id_document"]
            return id_document
        else:
            return "No"
    except Exception as e:
        manejar_error_mensajes(f"Error en verificarExisteEvento: {e}", 1)
        return "Error"


def verificarExisteUrlEvento(id_document, urlFinal):
    try:
        # Buscar el evento con el id_document proporcionado
        evento = next((evento for evento in v_list_eventos + v_list_eventos_3 if evento["f01_id_document"] == id_document),None,)
        if evento:
            detalles_evento = evento.get("f20_Detalles_Evento", [])  # Obtener la lista de detalles del evento si existe
            # Verificar si la URL final ya existe para ese id_document
            url_existe = any(detalle.get("f24_url_Final").lower() == urlFinal.lower() for detalle in detalles_evento)
        else:
            return "No_Existe_Url"  # La URL no existe para ese id_document

        if url_existe:
            return "Si_Existe_Url"  # La URL ya existe para ese id_document
        else:
            return "No_Existe_Url"  # La URL no existe para ese id_document
    except Exception as e:
        manejar_error_mensajes(f"Error en verificarExisteUrlEvento: {e} | id_document: {id_document} | urlFinal {urlFinal} ",1,)


def contains_not_available_text(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}
        # response = requests.get(url, headers=headers)
        response = requests.get(url, headers=headers, allow_redirects=True, verify=False)
        if response.status_code == 200:
            document = BeautifulSoup(response.text, "html.parser")
            # contains_text_1 = 'LiveStreams are currently not available for this broadcast.' in document.text
            # contains_text_2 = 'Live streams will be available approximately 30 minutes' in document.text
            # if contains_text_1 or contains_text_2:
            # Buscar la tabla especifica en el documento HTML
            tables = document.select("table.lnktbj")

            # Verificar si se encontro alguna tabla
            if tables:
                return "NO"
            else:
                return "SI"
        return "NO"
    except Exception as e:
        manejar_error_mensajes(f"Error en contains_not_available_text: {e} | url: {url}", 1)


def obtener_url_live_tv_final(enlace):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}
    proxies = [
        ("https://api.allorigins.win/raw?url=", None),
        ("https://api.allorigins.win/get?charset=ISO-8859-1&url=", None),
        ("https://api.allorigins.win/get?callback=myFunc&url=", None),
        (None, convertToCorsProxyUrl),
    ]

    # Intentar obtener el contenido HTML sin proxy primero
    try:
        with requests.Session() as session:
            session.headers.update(headers)
            response = session.get(enlace, allow_redirects=True, verify=False, timeout=20)

            if response.status_code == 200:
                # Analizar el codigo HTML para buscar el iframe
                soup = BeautifulSoup(response.content, "html.parser")
                iframe = soup.select_one("iframe[allowfullscreen]")
                if iframe:
                    urlFin = iframe.get("src")
                    urlFin = urlFin.replace("\n", "").replace("\r", "")  # Elimina saltos de linea y retorno de carro
                    if not urlFin.startswith("http"):
                        urlFin = f"https:{urlFin}"
                    if "youtube" in urlFin and urlFin.startswith("//"):
                        urlFin = urlFin[2:]
                    return urlFin
    except requests.exceptions.RequestException as e:
        manejar_error_mensajes(f"Error obtener_url_live_tv_final al intentar acceder sin proxy: {e} | enlace: {enlace}", 1)

    # Si el intento sin proxy falla, usar proxies
    with requests.Session() as session:
        session.headers.update(headers)

        for proxy_url, converter in proxies:
            full_url = converter(enlace) if converter else proxy_url + enlace

            try:
                response = session.get(full_url, allow_redirects=True, verify=False, timeout=20)
                if response.status_code == 200:
                    # Analizar el codigo HTML para buscar el iframe
                    soup = BeautifulSoup(response.content, "html.parser")
                    iframe = soup.select_one("iframe[allowfullscreen]")
                    if iframe:
                        urlFin = iframe.get("src")
                        urlFin = urlFin.replace("\n", "").replace("\r", "")  # Elimina saltos de linea y retorno de carro
                        if not urlFin.startswith("http"):
                            urlFin = f"https:{urlFin}"
                        if "youtube" in urlFin and urlFin.startswith("//"):
                            urlFin = urlFin[2:]
                        return urlFin

            except requests.exceptions.Timeout:
                manejar_error_mensajes(f"Tiempo de espera agotado en obtener_url_live_tv_final para {full_url}", 1)
            except requests.exceptions.RequestException as e:
                manejar_error_mensajes(f"Error en obtener_url_live_tv_final en la URL {full_url}: {str(e)}", 1)

    manejar_error_mensajes("Error en obtener_url_live_tv_final - No se pudo obtener el contenido HTML de ningun proxy.", 0)
    return None


def obtener_url_live_tv_final_tinyurl(url_acortada):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}
    proxies = [
        ("https://api.allorigins.win/raw?url=", None),
        ("https://api.allorigins.win/get?charset=ISO-8859-1&url=", None),
        ("https://api.allorigins.win/get?callback=myFunc&url=", None),
        (None, convertToCorsProxyUrl),
    ]

    # Intentar obtener la URL final sin proxy primero
    try:
        with requests.Session() as session:
            session.headers.update(headers)
            response = session.head(url_acortada, allow_redirects=True, timeout=20, verify=False)

            if response.status_code == 200:
                url_final = response.url

                return url_final  # Retorna la URL final si se resuelve correctamente

    except requests.exceptions.RequestException as e:
        manejar_error_mensajes(f"Error en obtener_url_live_tv_final_tinyurl al intentar resolver sin proxy: {str(e)} | URL: {url_acortada}", 0)
    # Si el intento sin proxy falla, usar proxies
    with requests.Session() as session:
        session.headers.update(headers)

        for proxy_url, converter in proxies:
            full_url = converter(url_acortada) if converter else proxy_url + url_acortada

            try:
                response = session.head(full_url, allow_redirects=True, timeout=20, verify=False)

                if response.status_code == 200:
                    url_final = response.url

                    return url_final  # Retorna la URL final resuelta

            except requests.exceptions.Timeout:
                manejar_error_mensajes(f"Error en obtener_url_live_tv_final_tinyurl - Tiempo de espera agotado para {full_url} | URL: {url_acortada}", 1)
            except requests.exceptions.RequestException as e:
                manejar_error_mensajes(f"Error en obtener_url_live_tv_final_tinyurl en la URL {full_url}: {str(e)} | URL: {url_acortada}", 1)

    manejar_error_mensajes(f"Error en obtener_url_live_tv_final_tinyurl - No se pudo resolver la URL con ningun proxy. | URL: {url_acortada}", 1)
    return None


def encontrar_indice_nombre_evento(evento, liveTds):
    for i, td in enumerate(liveTds):
        td_text = td.get_text().strip().splitlines()[0]  # Tomar solo la primera linea
        similarity_ratio = fuzz.partial_ratio(evento.lower(), td_text.lower())
        if similarity_ratio >= 95:
            return i
    return None


def month_str_to_num(month_str):
    months = {
        "January": "01",
        "February": "02",
        "March": "03",
        "April": "04",
        "May": "05",
        "June": "06",
        "July": "07",
        "August": "08",
        "September": "09",
        "October": "10",
        "November": "11",
        "December": "12",
    }
    return months.get(month_str, "00")


def convert_to_24h(time_str):
    return datetime.strptime(time_str, "%I:%M %p").strftime("%H:%M")


def agregar_mensaje_al_log(nuevo_mensaje):
    global global_message_log
    if global_message_log:
        global_message_log += "\n"
    global_message_log += nuevo_mensaje


async def enviar_mensaje_telegram_channel():
    global global_message_log
    if global_message_log:
        try:
            await bot_tg_canal.send_message(chat_id=chat_id, text=global_message_log)
            manejar_error_mensajes("Log enviado al canal con exito.", 0)
        except telegram.error.TelegramError as e:
            manejar_error_mensajes(f"Error al enviar el log al canal: {e}", 1)
        finally:
            global_message_log = ""  # Limpia el log despues de enviarlo


def obtener_dia_actual():
    try:
        # Consulta el dia actual
        response = t_dia_evento.query(KeyConditionExpression=Key("id_dia_evento").eq(1))
        if "Items" in response:
            dia_actual_bd = response["Items"][0]["f01_dia"]
            return dia_actual_bd
        else:
            return "20000101"
    except Exception as e:
        manejar_error_mensajes(f"Error al consultar la tabla dia_evento: {e}", 0)
        return "20000101"


class MyDynamoDB_EliminarRegistrosTabla:
    def __init__(self, table_name, v_list_eventos_3=None):
        self.table = dynamodb.Table(table_name)
        self.v_list_eventos_3 = v_list_eventos_3  # Lista de eventos en memoria

    def delete_all_items(self):
        try:
            # Escanea todos los elementos en la tabla
            response = self.table.scan()
            items = response.get("Items", [])
            for item in items:
                try:
                    # No elimina los items donde f02_proveedor contenga "Bases" en la tabla de eventos
                    if self.table.name == "eventos" and "Bases" in item.get("f02_proveedor", ""):
                        continue  # Salta al siguiente item sin eliminar este

                    # Eliminar el registro de la tabla
                    if self.table.name == "eventos":
                        key = {"f01_id_document": item["f01_id_document"], "f02_proveedor": item["f02_proveedor"]}
                    elif self.table.name == "dealers":
                        key = {"f01_id_dealer": item["f01_id_dealer"], "f02_dealer_name": item["f02_dealer_name"]}
                    else:
                        key = item  # Si la tabla no tiene clave compuesta, usa el item completo

                    self.table.delete_item(Key=key)
                    manejar_error_mensajes(f"Delete From {self.table.name} where key: {key}", 0)

                    # Si estamos eliminando de la tabla "eventos" y v_list_eventos_3 está definido
                    if self.table.name == "eventos" and self.v_list_eventos_3 is not None:
                        # Buscar y eliminar el registro correspondiente en v_list_eventos_3
                        evento_existente_index = next(
                            (index for (index, evento) in enumerate(self.v_list_eventos_3)
                             if evento["f01_id_document"] == item["f01_id_document"]
                             and evento["f02_proveedor"] == item["f02_proveedor"]), None)

                        if evento_existente_index is not None:
                            del self.v_list_eventos_3[evento_existente_index]
                            # manejar_error_mensajes(f"Delete From v_list_eventos_3 where key: {key}", 0)

                except Exception as e:
                    manejar_error_mensajes(f"Error al eliminar los datos de tabla: {self.table.name} {e}", 1)
        except Exception as e:
            manejar_error_mensajes(f"Error al eliminar los datos de tabla: {self.table.name} {e}", 1)


# Funcion para convertir URL a proxy CORS
def convertToCorsProxyUrl(url, cors_proxy_url="https://corsproxy.io/?"):
    if not url.endswith("/"):
        url += "/"
    encoded_url = urllib.parse.quote(url, safe="")
    return cors_proxy_url + encoded_url


def get_html_with_playwright(url):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)  # Ejecutar en modo headless para evitar ventanas visibles
        context = browser.new_context(
            ignore_https_errors=True,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36",
        )
        page = context.new_page()

        try:
            page.goto(url, wait_until="networkidle")  # Espera a que la pagina este completamente cargada
            page.wait_for_timeout(5000)  # Espera unos segundos para que se resuelvan los posibles checks de Cloudflare
            page_html = page.content()

            return page_html

        except Exception as e:
            manejar_error_mensajes(f"Error en get_html_with_playwright al cargar la pagina: {e}", 1)
            return None
        finally:
            browser.close()

# Funcion que intentara obtener el HTML de forma directa o con Selenium
def validate_and_get_url(url):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"}
    proxies = [
        ("https://api.allorigins.win/raw?url=", None),
        ("https://api.allorigins.win/get?charset=ISO-8859-1&url=", None),
        ("https://api.allorigins.win/get?callback=myFunc&url=", None),
        (None, convertToCorsProxyUrl),
    ]

    # Intentar obtener el HTML sin proxy primero
    try:
        with requests.Session() as session:
            session.headers.update(headers)
            response = session.get(url, timeout=20, verify=False)

            if response.status_code == 200:
                # response = requests.get(url, headers=headers, allow_redirects=True, verify=False)
                return response.text  # Retornar el HTML directamente

    except requests.exceptions.RequestException as e:
        manejar_error_mensajes(f"Error en validate_and_get_url al intentar acceder sin proxy: {str(e)} | URL: {url}", 1)

    # Si el intento sin proxy falla, se procede con los proxies
    with requests.Session() as session:
        session.headers.update(headers)

        for proxy_url, converter in proxies:
            full_url = converter(url) if converter else proxy_url + url

            try:
                response = session.get(full_url, timeout=20, verify=False)

                if response.status_code == 200:
                    return response.text  # Retornar el HTML del proxy
                else:
                    # Intentar obtener el HTML con playwright si el codigo de respuesta es 403
                    if response.status_code == 403:
                        return get_html_with_playwright(full_url)

            except requests.exceptions.Timeout:
                manejar_error_mensajes(f"Tiempo de espera agotado para {full_url} | URL: {url}", 1)
            except requests.exceptions.RequestException as e:
                manejar_error_mensajes(f"Error en la URL {full_url}: {str(e)} | URL: {url}", 1)
    manejar_error_mensajes(f"Error en validate_and_get_url - No se pudo obtener respuesta de ningun proxy | URL: {url}", 1)
    return None


def verificar_existencias():
    # global activaBases
    global activaLiveTV
    global activaSportline
    global activaDirectatvHDme
    global activaLibreF
    global activaRojaOn
    global activaRojaTv
    global activaPlatin
    global activaDaddyLivehd
    global activaLFJSON
    global ind_miss_LibreF

    # for evento in v_list_eventos_3:
    #     proveedor = evento.get("f02_proveedor", "")
    #     if "Bases" in proveedor:
    #         break
    # else:
    #     activaBases = 1

    for evento in v_list_eventos_3:
        proveedor = evento.get("f02_proveedor", "")
        if "Sportline" in proveedor:
            break
    else:
        activaSportline = 1

    for evento in v_list_eventos_3:
        proveedor = evento.get("f02_proveedor", "")
        if "DirectatvHDme" in proveedor:
            break
    else:
        activaDirectatvHDme = 1

    for evento in v_list_eventos_3:
        proveedor = evento.get("f02_proveedor", "")
        if "RojaOn" in proveedor:
            break
    else:
        activaRojaOn = 1
    for evento in v_list_eventos_3:
        proveedor = evento.get("f02_proveedor", "")
        if "RojaTv" in proveedor:
            break
    else:
        activaRojaTv = 1
    for evento in v_list_eventos_3:
        proveedor = evento.get("f02_proveedor", "")
        if "Platin" in proveedor:
            break
    else:
        activaPlatin = 1
    for evento in v_list_eventos_3:
        proveedor = evento.get("f02_proveedor", "")
        if "DLHD" in proveedor:
            break
    else:
        activaDaddyLivehd = 1
    for evento in v_list_eventos_3:
        proveedor = evento.get("f02_proveedor", "")
        if "LibreF" in proveedor:
            break
    else:
        activaLibreF = 1
    for evento in v_list_eventos_3:
        proveedor = evento.get("f02_proveedor", "")
        if "LiveTV" in proveedor:
            break
    else:
        activaLiveTV = 1
    for evento in v_list_eventos_3:
        proveedor = evento.get("f02_proveedor", "")
        if "LFJson" in proveedor:
            break
    else:
        activaLFJSON = 1


def procesar_hora_evento(fecha_actual, hora_event, horas_dif=0, eventNextDay=False):
    try:
        fecha_act = datetime.strptime(fecha_actual, "%Y%m%d")  # Convertir la fecha actual a un objeto datetime
        hora_evento = datetime.strptime(hora_event, "%H:%M").time()  # Convertir la hora del evento a un objeto datetime
        fecha_hora_evento = datetime.combine(fecha_act, hora_evento)  # Combinar fecha y hora en un solo objeto datetime
        fecha_hora_ajustada = fecha_hora_evento + timedelta(hours=horas_dif)  # Ajustar la hora según la diferencia de horas
        # hora_event_inicio = fecha_hora_ajustada.hour  # Obtener la hora de inicio del evento

        # Determinar si el evento es al día siguiente
        hora_original = hora_evento.hour
        hora_ajustada = fecha_hora_ajustada.hour

        # Caso 1: Hora original es tarde (después de las 18)
        if hora_original > 18:
            eventNextDay = True
        # Caso 2: Hora ajustada es tarde (después de las 18)
        elif hora_ajustada > 18:
            eventNextDay = True
        # Caso 3: Hora original es madrugada (antes de las 6 AM)
        elif hora_original < 8:
            eventNextDay = True

        # Ajustar fecha si el evento es al día siguiente y estamos en horas tempranas
        if eventNextDay and hora_ajustada < 12:  # Cambiado de 9 a 12 para mayor seguridad
            if horas_dif <= 0:  # Solo ajustar si no estamos sumando horas
                fecha_hora_ajustada += timedelta(days=1)

        # if hora_event_inicio > 18:
        #     eventNextDay = True
        # if eventNextDay and fecha_hora_ajustada.hour < 9 and horas_dif == 0:
        #     fecha_hora_ajustada += timedelta(days=1)

        # Convertir el objeto datetime a un string en formato ISO
        fecha_hora_ajustada_str = fecha_hora_ajustada.isoformat()

        return fecha_hora_ajustada_str, eventNextDay
    except Exception as e:
        manejar_error_mensajes(f"Error en procesar_hora_evento: {e}", 1)


def actualizar_estado_proveedor(proveedor, estado):
    global bool_estado_Sportline, bool_estado_DirectatvHDme, bool_estado_RojaOn, bool_estado_RojaTv, bool_estado_platin, bool_estado_LFJSON, bool_estado_DaddyLivehd

    # Actualizar el estado del proveedor
    if proveedor == "Sportline":
        bool_estado_Sportline = estado
    elif proveedor == "DirectatvHDme":
        bool_estado_DirectatvHDme = estado
    elif proveedor == "RojaOn":
        bool_estado_RojaOn = estado
    elif proveedor == "RojaTv":
        bool_estado_RojaTv = estado
    elif proveedor == "Platin":
        bool_estado_platin = estado
    elif proveedor == "LFJson":
        bool_estado_LFJSON = estado
    elif proveedor == "DLHD":
        bool_estado_DaddyLivehd = estado


def insertar_dato_en_bd_dealer(dealer_data):
    try:
        t_dealers.put_item(Item=dealer_data)
        # manejar_error_mensajes(f"Insert Dealer: {dealer_data}", 0)
    except Exception as e:
        manejar_error_mensajes(f"Ocurrio un error al insertar los datos de dealer: {e}", 1)


def procesar_cambios_eventos(v_list_eventos, v_list_eventos_copia):
    try:
        # Crear un diccionario de eventos previos para acceso rapido
        eventos_previos_dict = {str(e["f01_id_document"]): e for e in v_list_eventos_copia}

        # Iterar sobre eventos actuales y comparar
        for evento_actual in v_list_eventos:
            id_documento = str(evento_actual["f01_id_document"])
            json_evento_actual = json.dumps(evento_actual, sort_keys=True, default=str)
            nuevo_evento = evento_actual.get("f06_name_event", "")
            if id_documento in eventos_previos_dict:
                evento_previo = eventos_previos_dict[id_documento]
                document_id = evento_previo.get("f01_id_document")
                proveedor = evento_previo.get("f02_proveedor")
                json_evento_previo = json.dumps(evento_previo, sort_keys=True, default=str)

                if json_evento_actual != json_evento_previo:
                    t_eventos.delete_item(Key={"f01_id_document": document_id,"f02_proveedor": proveedor,})
                    t_eventos.put_item(Item=evento_actual)
                    manejar_error_mensajes(f"Upd DB ID: {id_documento} | {nuevo_evento}", 0)

                    # Buscar el evento existente en v_list_eventos_3
                    evento_existente_index = next((index for (index, evento) in enumerate(v_list_eventos_3)
                                                if evento["f01_id_document"] == evento_actual["f01_id_document"]
                                                and evento["f02_proveedor"] == evento_actual["f02_proveedor"]), None)

                    if evento_existente_index is not None:
                        # Si el evento existe, actualízalo
                        v_list_eventos_3[evento_existente_index] = evento_actual
                        # manejar_error_mensajes(f"Upd L3 ID: {id_documento} | {nuevo_evento}", 0)
                    else:
                        # Si no existe, agrégalo
                        v_list_eventos_3.append(evento_actual)
                        # manejar_error_mensajes(f"Add L3 ID: {id_documento} | {nuevo_evento}", 0)


            else:
                manejar_error_mensajes(f"Add DB ID: {id_documento} | {nuevo_evento}", 0)
                t_eventos.put_item(Item=evento_actual)
                v_list_eventos_3.append(evento_actual)
                # manejar_error_mensajes(f"Add L3 ID: {id_documento} | {nuevo_evento}", 0)
    except Exception as e:
        manejar_error_mensajes(f"Error en procesar_cambios_eventos: {e}", 1)


def manejar_detalles_evento(proveedor, fecha_hora, event_categoria, name_event, url_flag, imagenIdiom, channel_name, text_idiom, urlFinal, existeEvent, evento, contador_registros,jug_Local,logo_Local,jug_Visita,logo_Visita):
    try:
        list_eventos_detalles = []
        list_eventos_detalles_existente = []

        if proveedor != "Bases":
            orden = orden_proveedores.get(proveedor, 99)

            detalle = {
                "f21_imagen_Idiom": imagenIdiom,
                "f22_opcion_Watch": channel_name,
                "f23_text_Idiom": text_idiom,
                "f24_url_Final": urlFinal,
                "f25_proveedor": f"{orden} {proveedor}",
                "_orden_proveedor": orden
            }
            list_eventos_detalles.append(detalle)
        if existeEvent == "No":
            if proveedor != "Bases":
                evento["f20_Detalles_Evento"] = list_eventos_detalles
            v_list_eventos.append(evento)
            manejar_error_mensajes(f"Add L1 ID: {contador_registros} | IDBD: {existeEvent} | In: {proveedor} | {fecha_hora} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}", 0)
        else:
            if proveedor != "Bases":
                existeUrlEvent = verificarExisteUrlEvento(existeEvent, urlFinal)
                if existeUrlEvent == "Si_Existe_Url":
                    # manejar_error_mensajes(f"Already Exists ID: {contador_registros} | IDBD: {existeEvent} | In: {proveedor} | {fecha_hora} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}", 0)
                    return
                else:
                    evento_existente = next((evento for evento in v_list_eventos + v_list_eventos_3 if evento.get("f01_id_document") == existeEvent), None)
                    if evento_existente:
                        proveedor_existente = evento_existente["f02_proveedor"]
                        if proveedor not in proveedor_existente:
                            evento_existente["f02_proveedor"] += f" | {proveedor}"
                        if evento_existente["f03_dia_event"] is None and fecha_hora is not None:
                            evento_existente["f03_dia_event"] = fecha_hora
                        if evento_existente["f07_URL_Flag"] is None and url_flag is not None:
                            evento_existente["f07_URL_Flag"] = url_flag
                        if evento_existente["f05_event_categoria"] is None and event_categoria is not None:
                            evento_existente["f05_event_categoria"] = event_categoria
                        if evento_existente["f08_jug_Local"] is None and jug_Local is not None:
                            evento_existente["f08_jug_Local"] = jug_Local
                        if evento_existente["f09_logo_Local"] is None and logo_Local is not None:
                            evento_existente["f09_logo_Local"] = logo_Local
                        if evento_existente["f10_jug_Visita"] is None and jug_Visita is not None:
                            evento_existente["f10_jug_Visita"] = jug_Visita
                        if evento_existente["f11_logo_Visita"] is None and logo_Visita is not None:
                            evento_existente["f11_logo_Visita"] = logo_Visita
                        list_eventos_detalles_existente = evento_existente.get("f20_Detalles_Evento", [])
                        list_eventos_detalles_existente.extend(list_eventos_detalles)
                        evento_existente["f20_Detalles_Evento"] = list_eventos_detalles_existente

                        if not list_eventos_detalles and not list_eventos_detalles_existente:
                            return

                        if evento_existente in v_list_eventos:
                            v_list_eventos[v_list_eventos.index(evento_existente)] = evento_existente
                            # manejar_error_mensajes(f"Upd L1 ID: {contador_registros} | IDBD: {existeEvent} | In: {proveedor} | {fecha_hora} | {event_categoria} | {name_event} | {urlFinal} | {channel_name}", 0)
                        else:
                            t_eventos.delete_item(Key={"f01_id_document": existeEvent, "f02_proveedor": proveedor_existente})
                            t_eventos.put_item(Item=evento_existente)
                            manejar_error_mensajes(f"Upd BD ID: {contador_registros} | IDBD: {existeEvent} | In: {proveedor} | {fecha_hora} | {event_categoria} | {name_event} | {urlFinal} | {channel_name} | proveedor: {proveedor_existente}", 0)

                            # Buscar el evento existente en v_list_eventos_3
                            evento_existente_index = next((index for (index, evento) in enumerate(v_list_eventos_3)
                                                        if evento["f01_id_document"] == evento_existente["f01_id_document"]
                                                        and evento["f02_proveedor"] == evento_existente["f02_proveedor"]), None)

                            if evento_existente_index is not None:
                                # Si el evento existe, actualízalo
                                v_list_eventos_3[evento_existente_index] = evento_existente
                                #  manejar_error_mensajes(f"Upd L3 ID: {contador_registros} | IDBD: {existeEvent} | In: {proveedor} | {fecha_hora} | {event_categoria} | {name_event} | {urlFinal} | {channel_name} | proveedor: {proveedor_existente}", 0)
                            else:
                                # Si no existe, agrégalo
                                v_list_eventos_3.append(evento_existente)
                                # manejar_error_mensajes(f"Add L3 ID: {contador_registros} | IDBD: {existeEvent} | In: {proveedor} | {fecha_hora} | {event_categoria} | {name_event} | {urlFinal} | {channel_name} | proveedor: {proveedor_existente}", 0)

        imagenIdiom = None
        channel_name = None
        text_idiom = None
        urlFinal = None
    except requests.exceptions.RequestException as e:
        manejar_error_mensajes(f"Error dentro de manejar_detalles_evento: {str(e)}", 1)


def Insert_Update_Events_Unified(proveedor, fecha_hora, event_categoria, name_event, url_flag, jug_Local, logo_Local, jug_Visita, logo_Visita, imagenIdiom, channel_name, text_idiom, urlFinal, existeEvent, elementos=None):
    try:
        global contador_registros
        evento = {}

        actualizar_estado_proveedor(proveedor, True)

        if contador_registros > 0:
            existeEvent = verificarExisteEvento(fecha_hora, name_event)

        if proveedor == "Bases" and existeEvent != "No":
            return

        if existeEvent == "No" or contador_registros == 0:
            contador_registros += 1
            evento = {
                "f01_id_document": contador_registros,
                "f02_proveedor": proveedor,
                "f03_dia_event": fecha_hora,
                # "f04_hora_event": hora_event,
                "f05_event_categoria": event_categoria,
                "f06_name_event": name_event,
                "f07_URL_Flag": url_flag,
                "f08_jug_Local": jug_Local,
                "f09_logo_Local": logo_Local,
                "f10_jug_Visita": jug_Visita,
                "f11_logo_Visita": logo_Visita,
            }

        if proveedor == "DLHD":
            if (isinstance(elementos, list) and not elementos) or (isinstance(elementos, dict) and not elementos):
                return
            if isinstance(elementos, dict):
                elementos = list(elementos.values())

        if elementos is not None and isinstance(elementos, list) and elementos:
            # Si elementos no es None, es una lista y no está vacía, entonces procesar
            for elemento in elementos:
                try:
                    if proveedor == "Platin":
                        urlFinal = elemento["urlFin"]
                        urlFinal = urlFinal.replace(" ", "")
                        channel_name = elemento["nameChannel"]
                        imagenIdiom = elemento['channel_flag_url']
                        match = re.search(r"\[([^\]]+)\]", channel_name)
                        if match:
                            text_idiom = match.group(1).capitalize()
                            channel_name = channel_name.replace(match.group(0), "").strip()
                        else:
                            text_idiom = ""
                        if "acestream://" in urlFinal:
                            channel_name = channel_name + " | Acestream" 

                    elif proveedor == "LFJson":
                        embed_attributes = elemento.get("attributes", {})
                        channel_name = embed_attributes.get("embed_name", "")
                        urlInicial = embed_attributes.get("embed_iframe", "")
                        urlInicial = base_url_LFJson + urlInicial

                        if "/embed/" not in urlInicial:
                            urlFinal = obtenerUrlFinalLibreTV(urlInicial)
                        else:
                            urlFinal = obtenerUrlFinalLibreTVSelenium(urlInicial)
                        if urlFinal is None:
                            urlFinal = obtenerUrlFinalLibreTVPlaywright(urlInicial)

                    elif proveedor == "LiveTV":
                        img = elemento.select_one("td > img[title]")
                        if img and "title" in img.attrs:
                            imagenIdiom = img["src"]
                            imagenIdiom = f"https:{imagenIdiom}"
                            text_idiom = img["title"]
                            enlaces = elemento.select("td > a")
                            enlace = enlaces[1]["href"] if len(enlaces) > 1 else enlaces[0]["href"]
                            enlace = enlace.replace(" ", "")
                            channel_name = elemento.select_one("td.lnktyt > span")
                            if channel_name and channel_name.text.strip():
                                channel_name = channel_name.text.strip()
                            else:
                                channel_name = img["title"]
                            if channel_name == "Youtube":
                                imagenIdiom = "images/YouTube.png"                            
                            if "tinyurl.com" in enlace:
                                urlFinal = obtener_url_live_tv_final_tinyurl(enlace)
                            if "acestream://" in enlace:
                                urlFinal = enlace
                            elif (not enlace.startswith("http")) and ("acestream://" not in enlace):
                                enlace = f"https:{enlace}"
                                urlFinal = obtener_url_live_tv_final(enlace)
                            if "acestream://" in urlFinal:
                                channel_name = "Acestream"

                    elif proveedor == "DLHD":
                        channel_name = elemento["channel_name"]
                        urlFinal = f"https://dlhd.sx/embed/stream-{elemento['channel_id']}.php"
                        urlFinal = urlFinal.replace(" ", "")
                        # urlFinal = elemento["urlFinal"]

                    # Validación común para todos los proveedores
                    if urlFinal is None:
                        manejar_error_mensajes(f"urlFinal en {proveedor} es None: elemento: {elemento}", 1)
                        actualizar_estado_proveedor(proveedor, False)
                        continue

                    manejar_detalles_evento(proveedor, fecha_hora, event_categoria, name_event, url_flag, imagenIdiom, channel_name, text_idiom, urlFinal, existeEvent, evento, contador_registros,jug_Local,logo_Local,jug_Visita,logo_Visita)

                except Exception as e:
                    manejar_error_mensajes(f"Error en Insert_Update_Events_Unified en For elementos con {proveedor}: {e} | {elemento}", 1)
                    actualizar_estado_proveedor(proveedor, False)
                    continue

        else:
            manejar_detalles_evento(proveedor, fecha_hora, event_categoria, name_event, url_flag, imagenIdiom, channel_name, text_idiom, urlFinal, existeEvent, evento, contador_registros,jug_Local,logo_Local,jug_Visita,logo_Visita)

        # Resetear variables comunes
        event_categoria = None
        url_flag = None
        jug_Local = None
        logo_Local = None
        jug_Visita = None
        logo_Visita = None
        text_idiom = None
        channel_name = None
        imagenIdiom = None

    except requests.exceptions.RequestException as e:
        manejar_error_mensajes(f"Error dentro de Insert_Update_Events_Unified: {str(e)}", 1)
        actualizar_estado_proveedor(proveedor, False)


def actualizar_estado_dealer(dealer_id, dealer_name, estado):
    global vListDealers

    dealer = {
        "f01_id_dealer": dealer_id,
        "f02_dealer_name": dealer_name,
        "f03_state": estado,
    }

    dealer_exists = False
    for d in vListDealers:
        if d["f01_id_dealer"] == dealer_id:
            d["f03_state"] = estado
            dealer_exists = True
            break

    if not dealer_exists:
        vListDealers.append(dealer)


def procesar_Bases():
    try:
        manejar_error_mensajes(" ============================================= | Inicia procesar_Bases | ============================================= ", 0)
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
        contar_reg = 0

        dia_event = None
        hora_event = None
        name_event = None

        name_Max_Event_Bases = None
        name_Penultimate_Event_Bases = None
        name_Antepenultimate_Event_Bases = None

        if v_list_eventos_Bases:
            # Asegurate de que la lista este ordenada por el criterio deseado
            v_list_eventos_Bases_sorted = sorted(v_list_eventos_Bases, key=lambda x: x.get("f01_id_document"))

            # Intenta obtener el ultimo, penultimo y antepenultimo elemento
            try:
                ultimo_evento = v_list_eventos_Bases_sorted[-1]
                name_Max_Event_Bases = ultimo_evento.get("f06_name_event").replace("Vs", "–")
            except IndexError:
                manejar_error_mensajes("No hay suficientes eventos para determinar el ultimo evento en Bases.",1,)
            try:
                penultimo_evento = v_list_eventos_Bases_sorted[-2]
                name_Penultimate_Event_Bases = penultimo_evento.get("f06_name_event").replace("Vs", "–")
            except IndexError:
                manejar_error_mensajes("No hay suficientes eventos para determinar el penultimo evento en Bases.",1,)
            try:
                antepenultimo_evento = v_list_eventos_Bases_sorted[-3]
                name_Antepenultimate_Event_Bases = antepenultimo_evento.get("f06_name_event").replace("Vs", "–")
            except IndexError:
                manejar_error_mensajes("No hay suficientes eventos para determinar el antepeniltimo en Bases.",1,)

        responseBases = validate_and_get_url(urlBases)
        soup = BeautifulSoup(responseBases, "html.parser")
        td_elements = soup.find_all("td", colspan="2", height="38", valign="top", width="33%")
        
        indice_nombre_evento = None
        eventos_bases = {
            name_Max_Event_Bases: name_Max_Event_Bases,
            name_Penultimate_Event_Bases: name_Penultimate_Event_Bases,
            name_Antepenultimate_Event_Bases: name_Antepenultimate_Event_Bases,
        }

        for nombre_evento, condicion in eventos_bases.items():
            if condicion is not None and indice_nombre_evento is None:
                indice_nombre_evento = encontrar_indice_nombre_evento(nombre_evento, td_elements)
                if indice_nombre_evento is not None:
                    break

        # Si se encuentra el indice del nombre del evento
        if indice_nombre_evento is not None:
            v_list_events_web_base = []
            for td in td_elements:
                try:
                    texto_completo = td.get_text()
                    lines = [line.strip() for line in texto_completo.splitlines() if line.strip()]
                    if len(lines) >= 2:
                        date_str, time_str = lines[1].split(" at ")
                        day = date_str.split()[0]
                        month_str = date_str.split()[1]
                        month = month_str_to_num(month_str)
                        day = day.zfill(2)

                        hour, minute = time_str.split(":")
                        time_str = f"{hour.zfill(2)}:{minute}"
                        fecha_event_web = f"2025-{month}-{day} {time_str}"
                        fecha_event_web = datetime.strptime(f"{fecha_event_web}", "%Y-%m-%d %H:%M")
                        fecha_event_web -= timedelta(hours=1)
                        # fecha_event_web = fecha_event_web.strftime("%Y-%m-%d %H:%M")
                        fecha_event_web = fecha_event_web.isoformat()

                    name_event = next((line.strip() for line in texto_completo.splitlines() if line.strip()),"",)
                    v_list_events_web_base.append({"name_event": name_event,"fecha_event_web": fecha_event_web,}                    )
                except Exception as e:
                    manejar_error_mensajes(f"Error en Bases: leyendo registros web {texto_completo} con el nombre_evento_live {name_event} | {e}",1,)
                    continue

                # # Iterar sobre los eventos en v_list_eventos_Bases

            for evento in v_list_eventos_Bases:
                try:
                    name_event = evento.get("f06_name_event")
                    nombre_evento_base = name_event.replace("Vs", "–")
                    fecha_event_base = evento.get("f03_dia_event")
                    exists = any(fuzz.partial_ratio(nombre_evento_base.lower(),v_event["name_event"].lower(),) > 85 and fecha_event_base == v_event["fecha_event_web"] for v_event in v_list_events_web_base)
                    if not exists:
                        document_id = evento.get("f01_id_document")
                        proveedor = evento.get("f02_proveedor")
                        if document_id and "Bases" in proveedor:
                            try:
                                t_eventos.delete_item(Key={"f01_id_document": document_id,"f02_proveedor": proveedor,})
                                manejar_error_mensajes(f"Del DB ID: {document_id} | {fecha_event_base} | {name_event}",0,)
                                # Buscar el evento existente en v_list_eventos_3
                                evento_existente_index = next((index for (index, evento) in enumerate(v_list_eventos_3)
                                                            if evento["f01_id_document"] == document_id
                                                            and evento["f02_proveedor"] == proveedor), None)

                                if evento_existente_index is not None:
                                    # Si el evento existe, elimínalo
                                    del v_list_eventos_3[evento_existente_index]
                                    # manejar_error_mensajes(f"Del L3 ID: {document_id} | {fecha_event_base} | {name_event}",0,)
                            except Exception as e:
                                manejar_error_mensajes(f"Ocurrio un error al eliminar el evento: {name_event} con el ID {document_id} | {e}",10,)
                except Exception as e:
                    manejar_error_mensajes(f"Error en la eliminacion de eventos desde Bases: {name_event} con el name_event {nombre_evento_base} | {e}",1,)
                    continue

            for td in td_elements[indice_nombre_evento + 1 :]:
                tables = td.find_all("table",{"cellpadding": "1", "cellspacing": "2", "width": "100%"},)
                for table in tables:
                    try:
                        span_evdesc = table.find("span", {"class": "evdesc"})
                        span_evdesc = span_evdesc.get_text()
                        lines = [line.strip() for line in span_evdesc.splitlines() if line.strip()]
                        date_str, hora_event = lines[0].split(" at ")
                        day = date_str.split()[0]
                        month_str = date_str.split()[1]
                        month = month_str_to_num(month_str)
                        day = day.zfill(2)

                        hour, minute = hora_event.split(":")
                        hora_event = f"{hour.zfill(2)}:{minute}"
                        dia_event = f"2025{month}{day}"
                        img_alt = table.find("img")["alt"]
                        event_categoria = img_alt
                        aElement = table.select_one("a.live")
                        if aElement is None:
                            continue
                        nameEventOld = aElement.text if aElement else ""
                        name_event = re.sub(r"\s+", " ", re.sub(r"(?<=\s)[–](?=\s)", "Vs", nameEventOld),).strip()
                        img_src = table.find("img")["src"]
                        url_flag = img_src.lstrip("/")
                        url_flag = f"https://{url_flag}"
                        url_event = aElement["href"]
                        url_event = f"https://livetv.sx{url_event}"

                    except Exception as e:
                        manejar_error_mensajes(f"Error en Bases: {event_categoria} con el name_event {name_event} | {url_event} | {e}",1,)
                        continue


                     # Obtener logos
                    max_reintentos = 2
                    for intento in range(max_reintentos + 1):
                        try:
                            responseL = validate_and_get_url(url_event)
                            document = BeautifulSoup(responseL, "html.parser")
                            imagesWithItemprop = document.select("img[itemprop]")
                            # leyendos imagenes
                            if len(imagesWithItemprop) >= 2:
                                logoLocalOld = imagesWithItemprop[0]["src"] if "src" in imagesWithItemprop[0].attrs else ""
                                logoVisitaOld = imagesWithItemprop[1]["src"] if "src" in imagesWithItemprop[1].attrs else ""
                                jug_Local = imagesWithItemprop[0]["alt"] if "alt" in imagesWithItemprop[0].attrs else ""
                                logo_Local = f"https:{logoLocalOld}"
                                jug_Visita = imagesWithItemprop[1]["alt"] if "alt" in imagesWithItemprop[1].attrs else ""
                                logo_Visita = f"https:{logoVisitaOld}"

                                if all([jug_Local,logo_Local,jug_Visita,logo_Visita,]):
                                    break
                            else:
                                time.sleep(3)
                        except Exception as e:
                            manejar_error_mensajes(f"Error en Bases leyendos imagenes: | {e}", 1)
                            continue

                    if logo_Local is not None and "/." in logo_Local:
                        logo_Local = None
                    if logo_Visita is not None and "/." in logo_Visita:
                        logo_Visita = None

                    fecha_hora = datetime.strptime(f"{dia_event} {hora_event}", "%Y%m%d %H:%M")
                    fecha_hora -= timedelta(hours=1)
                    # hora_event = fecha_hora.strftime("%H:%M")
                    # fecha_hora = fecha_hora.strftime("%Y-%m-%d %H:%M")
                    fecha_hora = fecha_hora.isoformat()

                    Insert_Update_Events_Unified(
                        proveedor="Bases",
                        fecha_hora=fecha_hora,
                        # hora_event=hora_event,
                        event_categoria=event_categoria,
                        name_event=name_event,
                        url_flag=url_flag,
                        jug_Local=jug_Local,
                        logo_Local=logo_Local,
                        jug_Visita=jug_Visita,
                        logo_Visita=logo_Visita,
                        imagenIdiom=imagenIdiom,
                        channel_name=channel_name,
                        text_idiom=text_idiom,
                        urlFinal=None,
                        existeEvent=existeEvent,
                        elementos= []
                    )

                    contar_reg += 1

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
                tables = td.find_all("table",{"cellpadding": "1", "cellspacing": "2", "width": "100%"},)
                for table in tables:
                    try:
                        span_evdesc = table.find("span", {"class": "evdesc"})
                        span_evdesc = span_evdesc.get_text()
                        lines = [line.strip() for line in span_evdesc.splitlines() if line.strip()]
                        date_str, hora_event = lines[0].split(" at ")
                        day = date_str.split()[0]
                        month_str = date_str.split()[1]
                        month = month_str_to_num(month_str)
                        day = day.zfill(2)

                        hour, minute = hora_event.split(":")
                        hora_event = f"{hour.zfill(2)}:{minute}"
                        # hora_event_inicio = int(hora_event.split(':')[0].zfill(2))   # Asegura que siempre tenga dos caracteres
                        # # hora_event_inicio -= 6
                        # hora_event_inicio %= 24
                        dia_event = f"2025{month}{day}"

                        img_alt = table.find("img")["alt"]
                        event_categoria = img_alt
                        aElement = table.select_one("a.live")
                        if aElement is None:
                            continue
                        nameEventOld = aElement.text if aElement else ""
                        name_event = re.sub(r"\s+"," ",re.sub(r"(?<=\s)[–](?=\s)", "Vs", nameEventOld),).strip()
                        img_src = table.find("img")["src"]
                        url_flag = img_src.lstrip("/")
                        url_flag = f"https://{url_flag}"
                        url_event = aElement["href"]
                        url_event = f"https://livetv.sx{url_event}"

                        responseL = validate_and_get_url(url_event)
                        document = BeautifulSoup(responseL, "html.parser")
                        imagesWithItemprop = document.select("img[itemprop]")

                        if len(imagesWithItemprop) >= 2:
                            logoLocalOld = imagesWithItemprop[0]["src"] if "src" in imagesWithItemprop[0].attrs else ""
                            logoVisitaOld = imagesWithItemprop[1]["src"] if "src" in imagesWithItemprop[1].attrs else ""
                            jug_Local = imagesWithItemprop[0]["alt"] if "alt" in imagesWithItemprop[0].attrs else ""
                            logo_Local = f"https:{logoLocalOld}"
                            jug_Visita = imagesWithItemprop[1]["alt"] if "alt" in imagesWithItemprop[1].attrs else ""
                            logo_Visita = f"https:{logoVisitaOld}"
                        if logo_Local is not None and "/." in logo_Local:
                            logo_Local = None
                        if logo_Visita is not None and "/." in logo_Visita:
                            logo_Visita = None

                        fecha_hora = datetime.strptime(f"{dia_event} {hora_event}", "%Y%m%d %H:%M")
                        fecha_hora -= timedelta(hours=1)
                        # hora_event = fecha_hora.strftime("%H:%M")
                        # fecha_hora = fecha_hora.strftime("%Y-%m-%d %H:%M")
                        fecha_hora = fecha_hora.isoformat()

                        print(f"fecha_hora {fecha_hora} | event_categoria {event_categoria} | name_event {name_event} | url_flag {url_flag} | jug_Local {jug_Local} | logo_Local {logo_Local} | jug_Visita {jug_Visita} | logo_Visita {logo_Visita}")
                        # continue

                        Insert_Update_Events_Unified(
                            proveedor="Bases",
                            fecha_hora=fecha_hora,
                            # hora_event=hora_event,
                            event_categoria=event_categoria,
                            name_event=name_event,
                            url_flag=url_flag,
                            jug_Local=jug_Local,
                            logo_Local=logo_Local,
                            jug_Visita=jug_Visita,
                            logo_Visita=logo_Visita,
                            imagenIdiom=imagenIdiom,
                            channel_name=channel_name,
                            text_idiom=text_idiom,
                            urlFinal=None,
                            existeEvent=existeEvent,
                            elementos= []
                        )

                        contar_reg += 1

                    except Exception as e:
                        manejar_error_mensajes(f"Error en Bases FOR final de tables: | {e}", 1)
                        continue
                    name_event = None
                    hora_event = None
                    event_categoria = None
                    url_flag = None
                    jug_Local = None
                    logo_Local = None
                    jug_Visita = None
                    logo_Visita = None
        
        manejar_error_mensajes(f"Add {contar_reg} for Bases", 1)
        # manejar_error_mensajes("========| Termina procesar_Bases |========", 0)
        actualizar_estado_dealer(dealer_id=10, dealer_name="Bases", estado=False)
    except Exception as e:
        manejar_error_mensajes(f"Error en procesar_Bases: {e}", 1)
        actualizar_estado_dealer(dealer_id=10, dealer_name="Bases", estado=False)


def procesar_LiveTV():
    try:
        manejar_error_mensajes(" ============================================= | Inicia procesar_LiveTV | ============================================= ", 0)
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
        contar_reg = 0

        dia_event = None
        hora_event = None
        name_event = None
        urlFinal = None

        name_Max_Event_LiveTV = None
        name_Penultimate_Event_LiveTV = None
        name_Antepenultimate_Event_LiveTV = None

        contar_events_sin_links = 0
        cantidad_reg_pasar = 20

        contar_reg = 0
        if v_list_eventos_LiveTV:
            # Asegurate de que la lista este ordenada por el criterio deseado
            v_list_eventos_LiveTV_sorted = sorted(v_list_eventos_LiveTV, key=lambda x: x.get("f01_id_document"))
            # print (f"v_list_eventos_LiveTV_sorted: {v_list_eventos_LiveTV_sorted}")

            # Intenta obtener el ultimo, penultimo y antepenutimo elemento
            try:
                ultimo_evento = v_list_eventos_LiveTV_sorted[-1]
                name_Max_Event_LiveTV = ultimo_evento.get("f06_name_event").replace("Vs", "–")
                # print (f"name_Max_Event_LiveTV: {name_Max_Event_LiveTV}")
            except IndexError:
                manejar_error_mensajes("No hay suficientes eventos para determinar el ultimo evento en LiveTV.",1,)

            try:
                penultimo_evento = v_list_eventos_LiveTV_sorted[-2]
                name_Penultimate_Event_LiveTV = penultimo_evento.get("f06_name_event").replace("Vs", "–")
                # print (f"penultimo_evento: {penultimo_evento}")
            except IndexError:
                manejar_error_mensajes("No hay suficientes eventos para determinar el penultimo evento en LiveTV.",1,)

            try:
                antepenultimo_evento = v_list_eventos_LiveTV_sorted[-3]
                name_Antepenultimate_Event_LiveTV = antepenultimo_evento.get("f06_name_event").replace("Vs", "–")
                # print (f"antepenultimo_evento: {antepenultimo_evento}")
            except IndexError:
                manejar_error_mensajes("No hay suficientes eventos para determinar el antepeniltimo evento en LiveTV.",1,)

        responseLiveTV = validate_and_get_url(urlBases)
        soup = BeautifulSoup(responseLiveTV, "html.parser")
        td_elements = soup.find_all("td", colspan="2", height="38", valign="top", width="33%")

        indice_nombre_evento = None

        eventos_LiveTV = {
            name_Max_Event_LiveTV: name_Max_Event_LiveTV,
            name_Penultimate_Event_LiveTV: name_Penultimate_Event_LiveTV,
            name_Antepenultimate_Event_LiveTV: name_Antepenultimate_Event_LiveTV,
        }

        for nombre_evento, condicion in eventos_LiveTV.items():
            if condicion is not None and indice_nombre_evento is None:
                indice_nombre_evento = encontrar_indice_nombre_evento(nombre_evento, td_elements)
                if indice_nombre_evento is not None:
                    break

        # Si se encuentra el indice del nombre del evento
        if indice_nombre_evento is not None:
            try:
                v_list_events_web_base = []
                for td in td_elements:
                    try:
                        texto_completo = td.get_text()
                        lines = [line.strip() for line in texto_completo.splitlines() if line.strip()]
                        name_event = next((line.strip() for line in texto_completo.splitlines() if line.strip()),"",)
                        if len(lines) >= 2:
                            date_str, time_str = lines[1].split(" at ")
                            day = date_str.split()[0]
                            month_str = date_str.split()[1]
                            month = month_str_to_num(month_str)
                            day = day.zfill(2)
                            hour, minute = time_str.split(":")
                            time_str = f"{hour.zfill(2)}:{minute}"
                            fecha_event_web = f"2025-{month}-{day} {time_str}"

                            fecha_event_web = datetime.strptime(f"{fecha_event_web}", "%Y-%m-%d %H:%M")
                            fecha_event_web -= timedelta(hours=1)
                            # fecha_event_web = fecha_event_web.strftime("%Y-%m-%d %H:%M")
                            fecha_event_web = fecha_event_web.isoformat()

                        v_list_events_web_base.append({"name_event": name_event,"fecha_event_web": fecha_event_web,})
                    except Exception as e:
                        manejar_error_mensajes(f"Error en LiveTV inicial de td_elements: | {e}", 1)
                        continue

                for evento in v_list_eventos_LiveTV:
                    try:
                        name_event = evento.get("f06_name_event")
                        nombre_evento_base = name_event.replace("Vs", "–")
                        fecha_event_base = evento.get("f03_dia_event")
                        exists = any(fuzz.partial_ratio(nombre_evento_base.lower(),v_event["name_event"].lower(),) > 90 and fecha_event_base == v_event["fecha_event_web"] for v_event in v_list_events_web_base)
                        if not exists:
                            document_id = evento.get("f01_id_document")
                            proveedor = evento.get("f02_proveedor")
                            if document_id and "LiveTV" in proveedor:
                                try:
                                    t_eventos.delete_item(Key={"f01_id_document": document_id,"f02_proveedor": proveedor,})

                                    # Buscar el evento existente en v_list_eventos_3
                                    evento_existente_index = next((index for (index, evento) in enumerate(v_list_eventos_3)
                                                                if evento["f01_id_document"] == document_id
                                                                and evento["f02_proveedor"] == proveedor), None)

                                    if evento_existente_index is not None:
                                        # Si el evento existe, elimínalo
                                        del v_list_eventos_3[evento_existente_index]


                                    manejar_error_mensajes(f"Del LiveTV ID: {document_id} | {fecha_event_base} | {name_event}",0,)
                                except Exception as e:
                                    manejar_error_mensajes(f"Error in Del LiveTV: {name_event} ID {document_id} | {e}",1,)
                    except Exception as e:
                        manejar_error_mensajes(f"Error in Del LiveTV: {name_event} nombre_evento_live {nombre_evento_base} | {e}",1,)
                        continue

                for td in td_elements[indice_nombre_evento + 1 :]:
                    if contar_events_sin_links >= cantidad_reg_pasar:
                        break
                    tables_a = td.find_all("table",{"cellpadding": "1","cellspacing": "2","width": "100%",},)

                    for table_a in tables_a:
                        try:
                            span_evdesc = table_a.find("span", {"class": "evdesc"})
                            span_evdesc = span_evdesc.get_text()
                            lines = [line.strip() for line in span_evdesc.splitlines() if line.strip()]
                            date_str, hora_event = lines[0].split(" at ")
                            day = date_str.split()[0]
                            month_str = date_str.split()[1]
                            month = month_str_to_num(month_str)
                            day = day.zfill(2)

                            hour, minute = hora_event.split(":")
                            hora_event = f"{hour.zfill(2)}:{minute}"

                            # hora_event_inicio = int(hora_event.split(':')[0].zfill(2))   # Asegura que siempre tenga dos caracteres
                            # # hora_event_inicio -= 6
                            # hora_event_inicio %= 24
                            dia_event = f"2025{month}{day}"

                            img_alt = table_a.find("img")["alt"]
                            event_categoria = img_alt
                            aElement = table_a.select_one("a.live")
                            if aElement is None:
                                continue
                            nameEventOld = aElement.text if aElement else ""
                            name_event = re.sub(r"\s+"," ",re.sub(r"(?<=\s)[–](?=\s)", "Vs", nameEventOld),).strip()

                            img_src = table_a.find("img")["src"]
                            url_flag = img_src.lstrip("/")
                            url_flag = f"https://{url_flag}"

                            url_event = aElement["href"]
                            url_event = f"https://livetv.sx{url_event}"

                            if url_event is None:
                                continue
                            containsText = contains_not_available_text(url_event)
                            if containsText == "SI":
                                contar_events_sin_links = contar_events_sin_links + 1
                                if contar_events_sin_links >= cantidad_reg_pasar:
                                    break
                                else:
                                    continue


                            fecha_hora = datetime.strptime(f"{dia_event} {hora_event}", "%Y%m%d %H:%M")
                            fecha_hora -= timedelta(hours=1)
                            # hora_event = fecha_hora.strftime("%H:%M")
                            # fecha_hora = fecha_hora.strftime("%Y-%m-%d %H:%M")
                            fecha_hora = fecha_hora.isoformat()

                            responseL = validate_and_get_url(url_event)
                            document = BeautifulSoup(responseL, "html.parser")

                            # document = BeautifulSoup(responseL.content, 'html.parser')
                            imagesWithItemprop = document.select("img[itemprop]")
                             #  Obtener todas las tablas con clase "lnktbj"
                            tables_b = document.select("table.lnktbj")

                            title_element = document.find("title")
                            title_text = title_element.get_text()
                            first_slash_pos = title_text.find("/")
                            if first_slash_pos != -1:
                                # Encuentra la posicion del primer '.' despues del primer '/'
                                first_dot_after_slash_pos = title_text.find(".", first_slash_pos)

                                if first_dot_after_slash_pos != -1:
                                    # Extrae el texto entre el '/' y el '.'
                                    desired_text = title_text[first_slash_pos + 1 : first_dot_after_slash_pos].strip()  # Elimina los espacios en blanco
                                else:
                                    desired_text = None

                                event_categoria = f"{desired_text} - {event_categoria}"

                            if len(imagesWithItemprop) >= 2:
                                logoLocalOld = imagesWithItemprop[0]["src"] if "src" in imagesWithItemprop[0].attrs else ""
                                logoVisitaOld = imagesWithItemprop[1]["src"] if "src" in imagesWithItemprop[1].attrs else ""
                                jug_Local = imagesWithItemprop[0]["alt"] if "alt" in imagesWithItemprop[0].attrs else ""
                                logo_Local = f"https:{logoLocalOld}"
                                jug_Visita = imagesWithItemprop[1]["alt"] if "alt" in imagesWithItemprop[1].attrs else ""
                                logo_Visita = f"https:{logoVisitaOld}"
                            if logo_Local is not None and "/." in logo_Local:
                                logo_Local = None
                            if logo_Visita is not None and "/." in logo_Visita:
                                logo_Visita = None

                            Insert_Update_Events_Unified(
                                proveedor="LiveTV",
                                fecha_hora=fecha_hora,
                                # hora_event=hora_event,
                                event_categoria=event_categoria,
                                name_event=name_event,
                                url_flag=url_flag,
                                jug_Local=jug_Local,
                                logo_Local=logo_Local,
                                jug_Visita=jug_Visita,
                                logo_Visita=logo_Visita,
                                imagenIdiom=imagenIdiom,
                                channel_name=channel_name,
                                text_idiom=text_idiom,
                                urlFinal=None,
                                existeEvent=existeEvent,
                                elementos=tables_b
                            )

                            contar_reg += 1

                        except Exception as e:
                            manejar_error_mensajes(f"Error en LiveTV for antes de detalles: | {e} | {table_a}",1,)
                            continue

                        event_categoria = None
                        url_flag = None
                        jug_Local = None
                        logo_Local = None
                        jug_Visita = None
                        logo_Visita = None
                        name_event = None
                        hora_event = None
                 # cantidad de registros procesados
                # manejar_error_mensajes(f"Add: {contar_reg} for LiveTV", 0)

            except Exception as e:
                manejar_error_mensajes(f"Error en procesar_LiveTV 1: {e}", 1)
                actualizar_estado_dealer(dealer_id=1, dealer_name="LiveTV", estado=False)
        else:
            try:
                for td in td_elements:
                    if contar_events_sin_links >= cantidad_reg_pasar:
                        break
                    tables_cellpadding = td.find_all("table",{"cellpadding": "1","cellspacing": "2","width": "100%",},)
                    for table in tables_cellpadding:
                        try:
                            span_evdesc = table.find("span", {"class": "evdesc"})
                            span_evdesc = span_evdesc.get_text()
                            lines = [line.strip() for line in span_evdesc.splitlines() if line.strip()]
                            date_str, hora_event = lines[0].split(" at ")
                            day = date_str.split()[0]
                            month_str = date_str.split()[1]
                            month = month_str_to_num(month_str)
                            day = day.zfill(2)

                            hour, minute = hora_event.split(":")
                            hora_event = f"{hour.zfill(2)}:{minute}"
                            # hora_event_inicio = int(hora_event.split(':')[0].zfill(2))   # Asegura que siempre tenga dos caracteres
                            # # hora_event_inicio -= 6
                            # hora_event_inicio %= 24
                            dia_event = f"2025{month}{day}"

                            img_alt = table.find("img")["alt"]
                            event_categoria = img_alt
                            aElement = table.select_one("a.live")
                            if aElement is None:
                                continue
                            nameEventOld = aElement.text if aElement else ""
                            name_event = re.sub(r"\s+"," ",re.sub(r"(?<=\s)[–](?=\s)", "Vs", nameEventOld),).strip()

                            img_src = table.find("img")["src"]
                            url_flag = img_src.lstrip("/")
                            url_flag = f"https://{url_flag}"

                            url_event = aElement["href"]
                            url_event = f"https://livetv.sx{url_event}"

                            if url_event is None:
                                continue
                            containsText = contains_not_available_text(url_event)
                            if containsText == "SI":
                                contar_events_sin_links = contar_events_sin_links + 1
                                if contar_events_sin_links >= cantidad_reg_pasar:
                                    break
                                else:
                                    continue

                            fecha_hora = datetime.strptime(f"{dia_event} {hora_event}", "%Y%m%d %H:%M")
                            fecha_hora -= timedelta(hours=1)
                            # hora_event = fecha_hora.strftime("%H:%M")
                            # fecha_hora = fecha_hora.strftime("%Y-%m-%d %H:%M")
                            fecha_hora = fecha_hora.isoformat()

                            responseL = validate_and_get_url(url_event)
                            document = BeautifulSoup(responseL, "html.parser")
                            imagesWithItemprop = document.select("img[itemprop]")
                             # Obtener todas las tablas con clase "lnktbj"
                            tables_b = document.select("table.lnktbj")
                            title_element = document.find("title")
                            title_text = title_element.get_text()
                            first_slash_pos = title_text.find("/")
                            if first_slash_pos != -1:
                                # Encuentra la posicion del primer '.' despues del primer '/'
                                first_dot_after_slash_pos = title_text.find(".", first_slash_pos)

                                if first_dot_after_slash_pos != -1:
                                    # Extrae el texto entre el '/' y el '.'
                                    desired_text = title_text[first_slash_pos + 1 : first_dot_after_slash_pos].strip()  # Elimina los espacios en blanco
                                else:
                                    desired_text = None

                                event_categoria = f"{desired_text} - {event_categoria}"

                            if len(imagesWithItemprop) >= 2:
                                logoLocalOld = imagesWithItemprop[0]["src"] if "src" in imagesWithItemprop[0].attrs else ""
                                logoVisitaOld = imagesWithItemprop[1]["src"] if "src" in imagesWithItemprop[1].attrs else ""
                                jug_Local = imagesWithItemprop[0]["alt"] if "alt" in imagesWithItemprop[0].attrs else ""
                                logo_Local = f"https:{logoLocalOld}"
                                jug_Visita = imagesWithItemprop[1]["alt"] if "alt" in imagesWithItemprop[1].attrs else ""
                                logo_Visita = f"https:{logoVisitaOld}"
                            if logo_Local is not None and "/." in logo_Local:
                                logo_Local = None
                            if logo_Visita is not None and "/." in logo_Visita:
                                logo_Visita = None

                            Insert_Update_Events_Unified(
                                proveedor="LiveTV",
                                fecha_hora=fecha_hora,
                                # hora_event=hora_event,
                                event_categoria=event_categoria,
                                name_event=name_event,
                                url_flag=url_flag,
                                jug_Local=jug_Local,
                                logo_Local=logo_Local,
                                jug_Visita=jug_Visita,
                                logo_Visita=logo_Visita,
                                imagenIdiom=imagenIdiom,
                                channel_name=channel_name,
                                text_idiom=text_idiom,
                                urlFinal=None,
                                existeEvent=existeEvent,
                                elementos=tables_b
                            )

                            contar_reg += 1

                        except Exception as e:
                            manejar_error_mensajes(f"Error en LiveTV esle for inicial antes de detalles: | {e}",1,)
                            continue

                        name_event = None
                        hora_event = None
                        event_categoria = None
                        url_flag = None
                        jug_Local = None
                        logo_Local = None
                        jug_Visita = None
                        logo_Visita = None

                # manejar_error_mensajes(f"Add: {contar_reg} for LiveTV 2", 0)
            except Exception as e:
                manejar_error_mensajes(f"Error en procesar_LiveTV 2: {e}", 1)
                actualizar_estado_dealer(dealer_id=1, dealer_name="LiveTV", estado=False)
        
        manejar_error_mensajes(f"Add {contar_reg} for LiveTV", 1)
        actualizar_estado_dealer(dealer_id=1, dealer_name="LiveTV", estado=False)
        # manejar_error_mensajes("Termina procesar_LiveTV", 0)
    except Exception as e:
        manejar_error_mensajes(f"Error en procesar_LiveTV 3: {e}", 1)
        actualizar_estado_dealer(dealer_id=1, dealer_name="LiveTV", estado=False)


def procesar_SportsLine():
    manejar_error_mensajes(" ============================================= | Inicia procesar_SportsLine | ============================================= ", 0)
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
        eventNextDay = False  # Inicializar eventNextDay
        contar_reg = 0

        responseSportline = validate_and_get_url(urlportsonline)
        lines_contentSportline = responseSportline.splitlines()
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
                        idiomas.append({"extension": extension, "idioma": idioma})
                        continue
                    if "/channels/" in line:
                        parts = line.split("|")
                        if len(parts) > 1:
                            try:
                                name_event = parts[0].strip().encode("latin1").decode("latin1")
                            except:
                                name_event = parts[0].strip()
                            # Obtener la hora en formato de 24 horas
                            hora_event = name_event[:5]

                            name_event = name_event[5:].lstrip()
                            name_event = name_event.replace(" x ", " Vs ")
                            name_event = name_event.replace(" @ ", " Vs ")
                            name_event = process_special_characters(name_event)

                            urlFinal = parts[1].strip()
                            # Verifica si alguna extension de idioma esta en la urlFinal
                            for idioma_info in idiomas:
                                extension_idioma = idioma_info["extension"]
                                if extension_idioma.lower() in urlFinal.lower():
                                    text_idiom = idioma_info["idioma"]
                                    break  # Una vez que encuentras una coincidencia, sales del bucle
                            if "/pt/" in urlFinal:
                                text_idiom = "Portuguese"

                            if urlFinal is None:
                                manejar_error_mensajes(f"urlFinal en procesar_SportsOnline es None: line: {line}",1,)
                                continue

                            fecha_hora, eventNextDay = procesar_hora_evento(fecha_actual, hora_event, -1, eventNextDay)
                            # print(f"fecha_hora: {fecha_hora} | hora_event: {hora_event} | name_event: {name_event} | eventNextDay: {eventNextDay}")
                            # continue

                            Insert_Update_Events_Unified(
                                proveedor="Sportline",
                                fecha_hora=fecha_hora,
                                # hora_event=hora_event,
                                event_categoria=event_categoria,
                                name_event=name_event,
                                url_flag=url_flag,
                                jug_Local=jug_Local,
                                logo_Local=logo_Local,
                                jug_Visita=jug_Visita,
                                logo_Visita=logo_Visita,
                                imagenIdiom=imagenIdiom,
                                channel_name=channel_name,
                                text_idiom=text_idiom,
                                urlFinal=urlFinal,
                                existeEvent=existeEvent,
                                elementos= []
                            )

                            contar_reg += 1

            except Exception as e:
                manejar_error_mensajes(f"Error en procesar_Sportline: {e} | {line}", 1)
                bool_estado_Sportline = False
                continue

        manejar_error_mensajes(f"Add {contar_reg} for Sportline", 1)
        actualizar_estado_dealer(dealer_id=2, dealer_name="Sportline", estado=bool_estado_Sportline)
        # manejar_error_mensajes("Termina procesar_SportsLine", 0)

    except Exception as e:
        manejar_error_mensajes(f"Error en procesar_SportsLine: {e}", 1)
        actualizar_estado_dealer(dealer_id=2, dealer_name="Sportline", estado=False)


def procesar_DirectatvHDme():
    manejar_error_mensajes(" ============================================= | Inicia procesar_DirectatvHDme | ============================================= ", 0)
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

        contar_reg = 0

        eventNextDay = False  # Inicializar eventNextDay
        responseDirecTVHDme = validate_and_get_url(urlDirectatvHDme)
        soup = BeautifulSoup(responseDirecTVHDme, "html.parser")
        fecha_texto = soup.find('th', string=re.compile(r'Partidos de hoy')).get_text()
        patron_fecha = re.search(r'(\d{2})-(\d{2})-(\d{4})', fecha_texto)
        dia_pag, mes_pag, anio_pag = patron_fecha.groups()
        fecha_pagina = f"{anio_pag}{mes_pag}{dia_pag}"     

        # Obtener todos los elementos <tr>
        tr_elements = soup.find_all("tr")
        # Usado para evitar enlaces duplicados
        unique_enlaces = set()
        if fecha_pagina == fecha_actual:
            for tr_element in tr_elements:
                try:
                    # Obtener los elementos <td> dentro del <tr>
                    td_elements = tr_element.find_all("td")
                
                    if len(td_elements) >= 3:
                        hora = td_elements[0].find(class_="t").text if td_elements[0].find(class_="t") else ""
                        url_flag = td_elements[1].find("img")["src"] if td_elements[1].find("img") else ""

                        a_element = td_elements[2].find("a")
                        # hora_event = hora[:2] if len(hora) > 2 else ""
                        hora_event = hora[:5] if len(hora) > 5 else "00:00"  # Asegurar el formato "HH:MM"
                        
                        enlace = a_element["href"] if a_element else ""
                        text_event = a_element.find("b").text.strip().replace(" en Vivo", "") if a_element and a_element.find("b") else ""
                        event_categoria = td_elements[2].text.split(":")[0].strip()
                        try:
                            name_event = text_event.encode("latin-1", errors="ignore").decode("utf-8")
                        except UnicodeDecodeError:
                            name_event = text_event
                        name_event = process_special_characters(name_event)

                        enlace = enlace.strip().replace(" ", "%20")
                        enlace = re.sub(r"(.*)\.php\.php$", r"\1.php", enlace)
                        enlace = enlace.replace("/tv-","/")

                        # Genera una clave unica para cada registro basada en event_categoria, name_event y enlace
                        record_key = f"{event_categoria}_{name_event}_{enlace}"

                        # Verificar que el enlace no se repita antes de imprimirlo
                        if record_key in unique_enlaces:
                            continue  # Saltar al siguiente ciclo si ya hemos visto este enlace

                        if url_flag and enlace:
                            url_flag = f"https://directatvhd.me{url_flag}"
                            enlaceLimpio = f"https://directatvhd.me{enlace}"
                            enlaceallorigins = f"https://directatvhd.me{enlace}"

                        unique_enlaces.add(record_key)
                        urlFinChannel = obtenerUrlFinalRojaHDme(enlaceallorigins)

                        if "tutvlive.info" in urlFinChannel:
                            continue

                        if "404" in urlFinChannel:
                            EnlaceCors = convertToCorsProxyUrl(enlaceLimpio)
                            urlFinChannel = obtenerUrlFinalRojaHDme(EnlaceCors)

                        if "No pudo obtener url" in urlFinChannel:
                            continue
                        parts = urlFinChannel.split("|")

                        channel_name = parts[0].strip()
                        urlFinal = parts[1].strip()
                        if urlFinal is None:
                            manejar_error_mensajes(f"urlFinal en procesar_DirectatvHDme es None: urlFinChannel: {urlFinChannel}",1,)
                            continue

                        fecha_hora, eventNextDay = procesar_hora_evento(fecha_actual, hora_event, +5, eventNextDay)
                        # print(f"fecha_hora: {fecha_hora} | name_event : {name_event}")
                        # continue
                        Insert_Update_Events_Unified(
                            proveedor="DirectatvHDme",
                            fecha_hora=fecha_hora,
                            # hora_event=hora_event,
                            event_categoria=event_categoria,
                            name_event=name_event,
                            url_flag=url_flag,
                            jug_Local=jug_Local,
                            logo_Local=logo_Local,
                            jug_Visita=jug_Visita,
                            logo_Visita=logo_Visita,
                            imagenIdiom=imagenIdiom,
                            channel_name=channel_name,
                            text_idiom=text_idiom,
                            urlFinal=urlFinal,
                            existeEvent=existeEvent,
                            elementos= []
                        )

                        contar_reg += 1

                except Exception as e:
                    manejar_error_mensajes(f"Error en procesar_DirectatvHDme: {e} | {tr_element}", 1)
                    bool_estado_DirectatvHDme = False
                    continue
        else:
            # manejar_error_mensajes(f"Fecha de DirecTVHDme {fecha_pagina} no coincide con la fecha actual {fecha_actual}. No se procesarán eventos.", 1)
            bool_estado_DirectatvHDme = False                        
        

        manejar_error_mensajes(f"Add {contar_reg} for DirectatvHDme", 1)
        actualizar_estado_dealer(dealer_id=3, dealer_name="DirectatvHDme", estado=bool_estado_DirectatvHDme)
        # manejar_error_mensajes("Termina procesar_DirectatvHDme", 0)

    except Exception as e:
        manejar_error_mensajes(f"Error en procesar_DirectatvHDme: {e}", 1)
        actualizar_estado_dealer(dealer_id=3, dealer_name="DirectatvHDme", estado=False)


def procesar_LibreF():
    manejar_error_mensajes(" ============================================= | Inicia procesar_LibreF | ============================================= ", 0)
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

        contar_reg = 0

        eventNextDay = False  # Inicializar eventNextDay

        evento = {}
        response = validate_and_get_url(urlLibreFAgenda)
        soup = BeautifulSoup(response, "html.parser")

        b_tag = soup.find('b')

        dia_LibreF = None
        if b_tag:
            text = b_tag.text
            date_text = text.split(' - ')[1]
            match = re.search(r'(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})', date_text, re.IGNORECASE)
            if not match:
                return None      
            
            dia = match.group(1).zfill(2)  # Asegurar 2 dígitos
            mes_texto = match.group(2).lower()
            año = match.group(3)           
            
            mes = meses.get(mes_texto, '00')
            dia_LibreF = f"{año}{mes}{dia}"                 

            if not dia_LibreF:
                manejar_error_mensajes(f"No se pudo extraer la fecha de LibreF.", 0)
                actualizar_estado_dealer(dealer_id=4, dealer_name="LibreF", estado=False)
                return False              

        if dia_LibreF == fecha_actual:
            eventos = soup.find_all("li")
            if eventos:
                for evento in eventos:
                    # Iterar a traves de los registros
                    try:
                        linkElement = evento.find("a")
                        linkText = linkElement.text if linkElement else ""
                        firstColonIndex = linkText.find(":")
                        if firstColonIndex != -1:
                            event_categoria = linkText[: firstColonIndex + 1].strip()
                            textEvent = linkText[firstColonIndex + 1 :].split("\n")[0].strip()  # Obtener la primera linea
                        else:
                            event_categoria = linkText
                            textEvent = ""
                        event_categoria = event_categoria.replace(":", "")
                        event_categoria = process_special_characters(event_categoria)
                        # Decodificar el texto del evento (puede ser necesario si hay caracteres especiales)
                        try:
                            name_event = textEvent.encode("latin1").decode("utf8")
                        except:
                            name_event = textEvent
                        name_event = name_event.replace("vs.", "Vs").strip()
                        name_event = process_special_characters(name_event)
                        # if 'Cali' not in name_event:
                        #     continue
                        hora_event = evento.find(class_="t").text if evento.find(class_="t") else ""

                        # Obtener todos los elementos de tipo <li> que son hijos de evento
                        canalesYEnlaces = evento.select("ul > li.subitem1")

                        # Recorrer cada <li> hijo para obtener los canales y enlaces
                        for ce in canalesYEnlaces:
                            try:
                                chanel = ce.find("a").contents[0] if ce.find("a") else ""
                                # enlace = '/es' + ce.find('a')['href'] if ce.find('a') else ''
                                enlace = ce.find("a")["href"] if ce.find("a") else ""
                                try:
                                    channel_name = chanel.encode("latin1").decode("utf8")
                                except:
                                    channel_name = chanel
                                if "futbollibre.futbol" not in enlace:
                                    enlace = "https://futbollibre.futbol" + enlace
                                urlcors = enlace
                                if "/embed/" not in urlcors:
                                    urlIni = obtenerUrlFinalLibreTV(urlcors)
                                else:
                                    urlIni = obtenerUrlFinalLibreTVPlaywright(urlcors)
                                if urlIni is None:
                                    urlIni = obtenerUrlFinalLibreTVSelenium(urlcors)
                                
                                # si  urlIni inicia con // agregar https:
                                if urlIni.startswith("//"):
                                    urlIni = "https:" + urlIni

                                if "stgruber.world" in urlIni or urlIni is None:
                                    bool_estado_libref = False
                                    continue

                                fecha_hora, eventNextDay = procesar_hora_evento(fecha_actual, hora_event, -1, eventNextDay)
                                # print(f"fecha_hora: {fecha_hora} | name_event: {name_event}")
                                # continue

                                # print(f"Procesando evento: {name_event} | Fecha y hora: {fecha_hora} | Canal: {channel_name} | URL: {urlIni} | urlcors : {urlcors} | enlace: {enlace}")
                                # continue

                                Insert_Update_Events_Unified(
                                    proveedor="LibreF",
                                    fecha_hora=fecha_hora,
                                    # hora_event=hora_event,
                                    event_categoria=event_categoria,
                                    name_event=name_event,
                                    url_flag=url_flag,
                                    jug_Local=jug_Local,
                                    logo_Local=logo_Local,
                                    jug_Visita=jug_Visita,
                                    logo_Visita=logo_Visita,
                                    imagenIdiom=imagenIdiom,
                                    channel_name=channel_name,
                                    text_idiom=text_idiom,
                                    urlFinal=urlIni,
                                    existeEvent=existeEvent,
                                    elementos=None
                                )       

                                contar_reg += 1                 

                            except Exception as e:
                                manejar_error_mensajes(f"Error en procesar_libreF 2: {e} | {ce}", 1)
                                bool_estado_libref = False
                                continue
                    except Exception as e:
                        manejar_error_mensajes(f"Error en procesar_libreF 3: {e} | {evento}", 1)
                        bool_estado_libref = False
                        continue

            else:
                # manejar_error_mensajes(f"No se encontraron eventos para procesar en LibreF",1,)
                bool_estado_libref = False

        manejar_error_mensajes(f"Add {contar_reg} for LibreF", 1)
        actualizar_estado_dealer(dealer_id=4, dealer_name="LibreF", estado=bool_estado_libref)
        # manejar_error_mensajes("Termina procesar_LibreF", 0)

    except Exception as e:
        manejar_error_mensajes(f"Error en procesar_libreF 4: {e}", 1)
        actualizar_estado_dealer(dealer_id=4, dealer_name="LibreF", estado=False)


def procesar_RojaOnline():
    manejar_error_mensajes(" ============================================= | Inicia procesar_RojaOnline | ============================================= ", 0)
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
        contar_reg = 0

        eventNextDay = False  # Inicializar eventNextDay

        evento = {}
        # headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
        # responseRojaOn = requests.get(urlRojaOn, headers=headers, allow_redirects=True, verify=False)
        # responseRojaOn = requests.get(urlRojaOn)
        responseRojaOn = validate_and_get_url(urlRojaOn)
        # responseRojaOn = obtenerResponseSelenium(urlRojaOn)

        soup = BeautifulSoup(responseRojaOn, "html.parser")
        # Obtener las filas de la tabla
        tableRows = soup.find_all("tr")

        for row in tableRows:
            try:
                linkElement = row.find_all("td")
                if len(linkElement) > 0:
                    # Accede a la informacion que necesitas en funcion de la posicion de las celdas
                    hora_event = linkElement[0].text
                    # Selecciona el elemento 'a' dentro de la tercera columna
                    name_element = linkElement[2].find("a")
                    name_event_complet = linkElement[2].text.strip()
                    if "rojadirectahdenvivo" in name_element["href"]:
                        # url = name_element['href']
                        continue
                    # url = "http://tarjetarojatvonline.sx" + name_element['href']
                    # element = name_element["href"]
                    url = "https://ww1.tarjetarojatvonline.sx" + name_element["href"]

                    partes_event_name = name_event_complet.split(":")
                    name_event = partes_event_name[1].strip()
                    event_categoria = partes_event_name[0].strip()
                    name_event = process_special_characters(name_event)
                    event_categoria = process_special_characters(event_categoria)
                    if "resultado.rojadirectaonlinetv.net" in url:
                        continue  # Omitir el registro actual y continuar con el siguiente
                    channel_url = obtenerUrlFinalRojaOn(url)

                    if channel_url:
                        channel_name, urlFinal = channel_url.split(" | ", 1)
                        urlFinal = urlFinal.replace(" ", "")
                        if urlFinal is None:
                            manejar_error_mensajes(f"urlFinal en procesar_RojaOnline es None: channel_url: {channel_url}",0,)
                            continue

                        hora_limpia = hora_event.lower().replace(' ', '')
                        if 'pm' in hora_limpia or 'am' in hora_limpia:
                            hora, minutos = hora_limpia.split(':')[0], hora_limpia.split(':')[1][:2]
                            minutos = minutos.replace('am', '').replace('pm', '')
                            
                            hora_int = int(hora)
                            if 'pm' in hora_limpia and hora_int != 12:
                                hora_int += 12
                            elif 'am' in hora_limpia and hora_int == 12:
                                hora_int = 0  # Medianoche (12am → 0h)    

                            hora_event = f"{hora_int:02d}:{minutos}"

                        # print(f"hora_event antes am-pm: {hora_event} | name_event: {name_event}")
                        # hora_event = (lambda hora: f"{(int(hora.split(':')[0]) % 12) + (12 if 'pm' in hora.lower() else 0):02d}:{hora.split(':')[1][:2]}")(hora_event)
                        # print(f"hora_event despues: {hora_event} | name_event: {name_event}")
                        fecha_hora, eventNextDay = procesar_hora_evento(fecha_actual, hora_event, +5, eventNextDay)
                        # print(f"fecha_hora : {fecha_hora} | name_event: {name_event}")
                        # continue

                        Insert_Update_Events_Unified(
                            proveedor="RojaOn",
                            fecha_hora=fecha_hora,
                            # hora_event=hora_event,
                            event_categoria=event_categoria,
                            name_event=name_event,
                            url_flag=url_flag,
                            jug_Local=jug_Local,
                            logo_Local=logo_Local,
                            jug_Visita=jug_Visita,
                            logo_Visita=logo_Visita,
                            imagenIdiom=imagenIdiom,
                            channel_name=channel_name,
                            text_idiom=text_idiom,
                            urlFinal=urlFinal,
                            existeEvent=existeEvent,
                            elementos= []
                        )

                        contar_reg += 1

            except Exception as e:
                manejar_error_mensajes(f"Error en procesar_RojaOn: {e} | {row}", 1)
                bool_estado_RojaOn = False
                continue
        
        manejar_error_mensajes(f"Add {contar_reg} for RojaOn", 1)
        actualizar_estado_dealer(dealer_id=5, dealer_name="RojaOn", estado=bool_estado_RojaOn)
        # manejar_error_mensajes("Termina procesar_RojaOnline", 0)

    except Exception as e:
        manejar_error_mensajes(f"Error en procesar_RojaOn: {e}", 1)
        actualizar_estado_dealer(dealer_id=5, dealer_name="RojaOn", estado=False)


def procesar_RojaTV():
    manejar_error_mensajes(" ============================================= | Inicia procesar_RojaTV | ============================================= ", 0)
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
    global bool_estado_RojaTv
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
        contar_reg = 0

        eventNextDay = False  # Inicializar eventNextDay

        evento = {}
        response = validate_and_get_url(urlRojaTV)
        soup = BeautifulSoup(response, "html.parser")

        # Encuentra todos los elementos <tr>
        RowsRoja = soup.find_all("tr")
        for row in RowsRoja:
            try:
                tds = row.find_all("td")
                if len(tds) >= 3:
                    hora_event = tds[0].find("span").text
                    tdEvent = tds[2]

                    # Utiliza expresiones regulares para extraer el contenido entre <b></b>
                    match = re.search(r"<b>(.*?)<\/b>", tdEvent.decode_contents())
                    newEvent = match.group(1).replace("en Vivo", "").strip() if match else ""

                    textComplet = tdEvent.text.replace("en Vivo", "").strip()
                    indexOfNewEvent = textComplet.find(newEvent)

                    if indexOfNewEvent != -1:
                        event_categoria = textComplet[:indexOfNewEvent].strip()
                    else:
                        event_categoria = textComplet

                    try:
                        event_categoria = event_categoria.encode("latin1").decode("utf8")
                    except:
                        pass

                        # Construye la URL completa
                    aTag = tdEvent.find("a")
                    url = "https://tarjetarojatv.run" + (aTag["href"] if aTag else "")
                    title_channel = aTag["href"].split("/")[-1].split(".")[0]
                    title_channel = capitalize_words(title_channel)
                    # debug
                    # pdb.set_trace()

                    if "httpswww" in url:
                        continue

                    try:
                        name_event = newEvent.encode("latin1").decode("utf8")
                    except:
                        name_event = newEvent
                    name_event = process_special_characters(name_event)
                    # if 'Auxerre' not in name_event:
                    #     continue

                    urlFinal = obtenerUrlFinalRojaTV(url)
                    if "408" in urlFinal:
                        time.sleep(2)
                        urlFinal = obtenerUrlFinalRojaTV(url)

                    if "sin_data" in urlFinal:
                        bool_estado_RojaTv = False
                        manejar_error_mensajes(f"urlFinal sin data: {urlFinal}",1,)

                    urlFinal = urlFinal.replace(" ", "")
                    if urlFinal is None:
                        manejar_error_mensajes(f"urlFinal en procesar_RojaTV es None: url: {url}",1,)
                        continue

                    # channel_name = channel_name.strip()
                    channel_name = title_channel

                    fecha_hora, eventNextDay = procesar_hora_evento(fecha_actual, hora_event, +5, eventNextDay)

                    Insert_Update_Events_Unified(
                        proveedor="RojaTv",
                        fecha_hora=fecha_hora,
                        # hora_event=hora_event,
                        event_categoria=event_categoria,
                        name_event=name_event,
                        url_flag=url_flag,
                        jug_Local=jug_Local,
                        logo_Local=logo_Local,
                        jug_Visita=jug_Visita,
                        logo_Visita=logo_Visita,
                        imagenIdiom=imagenIdiom,
                        channel_name=channel_name,
                        text_idiom=text_idiom,
                        urlFinal=urlFinal,
                        existeEvent=existeEvent,
                        elementos= []
                    )

                    contar_reg += 1

            except Exception as e:
                manejar_error_mensajes(f"Error en procesar_RojaTv: {str(e)} | {row}", 1)
                bool_estado_RojaTv = False
                continue
        
        manejar_error_mensajes(f"Add {contar_reg} for RojaTv", 1)
        actualizar_estado_dealer(dealer_id=6, dealer_name="RojaTv", estado=bool_estado_RojaTv)
        # manejar_error_mensajes("Termina procesar_RojaTV", 0)

    except Exception as e:
        manejar_error_mensajes(f"Error en procesar_RojaTv: {str(e)}", 1)
        actualizar_estado_dealer(dealer_id=6, dealer_name="RojaTv", estado=bool_estado_RojaTv)


def procesar_Platin():
    try:
        manejar_error_mensajes(" ============================================= | Inicia procesar_Platin | ============================================= ", 0)
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
        contar_reg = 0

        eventNextDay = False  # Inicializar eventNextDay

        evento = {}
        responsePlatin = validate_and_get_url(urlPlatin)
        soup = BeautifulSoup(responsePlatin, "html.parser")
        tr_elements_Platin = soup.find_all("tr")
        hrefs_list = [tr.find("a").get("href") for tr in tr_elements_Platin if tr.find("a") is not None]
        href_Repetidos = len(hrefs_list) != len(set(hrefs_list))

        # lista_event_flag = []

        # for tr in tr_elements_Platin:
        #     # Obtener el nombre del evento y la URL de la bandera
        #     td_elements = tr.find_all("td")
        #     if len(td_elements) >= 2:
        #         nombre_evento = td_elements[1].get_text(strip=True)
        #         # Obtener la URL de la bandera
        #         img_element = td_elements[0].find("img")
        #         if img_element:
        #             bandera = img_element["src"]
        #         else:
        #             bandera = None
        #         nombre_evento = nombre_evento[6:]
        #         print(f"nombre_evento: {nombre_evento} bandera {bandera}")
        #         # Agregar los datos a tu lista o hacer lo que necesites
        #         lista_event_flag.append({"nombre_evento": nombre_evento, "bandera": bandera})


        # Buscar el <div> que contiene el día
        day_div = soup.find("div", class_="div-1")
        if day_div:
            title = day_div.text.strip()

            # Extraer la primera palabra (el día de la semana)
            first_word_title = title.split()[0]

            # Corregir caracteres especiales si es necesario
            first_word_title = first_word_title.upper()

            # Verificar si la primera palabra es un día de la semana
            # days_of_week = {"MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"}
            # if first_word_title in days_of_week:
            #     manejar_error_mensajes(f"Día encontrado: {first_word_title}", 0)
            # else:
            #     manejar_error_mensajes(f"No se encontró un día válido. Platin", 0)
        else:
            manejar_error_mensajes(f"No se encontró el div con el día. Platin", 0)

        for tr in tr_elements_Platin:
            try:
                
                if first_word_title == currentDayOfWeek:
                    
                    if not href_Repetidos:
                        try:
                            url_flag = tr.find("img")["src"]
                        except (KeyError, TypeError):
                            url_flag = None
                        td_text = tr.select("td")[1].text.strip()  # Use .strip() to remove leading/trailing spaces
                        hora_event = td_text.split(" ", 1)[0]
                        name_event = td_text.split(" ", 1)[1]
                        name_event = process_special_characters(name_event)
                        name_event = capitalize_words(name_event)
                        url_event = tr.find("a")["href"].split("https://www.platinsport.com", 1)[-1]
                        url_event = "https://www.platinsport.com" + url_event
                        url_name_pairs = obtenerUrlFinalPlatin(url_event)
                        if url_name_pairs is None or not url_name_pairs:
                            continue

                        fecha_hora, eventNextDay = procesar_hora_evento(fecha_actual, hora_event, 0, eventNextDay)
                        # print(f"fecha_hora1: {fecha_hora} | name_event: {name_event}")
                        # continue                         

                        Insert_Update_Events_Unified(
                            proveedor="Platin",
                            fecha_hora=fecha_hora,
                            event_categoria=event_categoria,
                            name_event=name_event,
                            url_flag=url_flag,
                            jug_Local=jug_Local,
                            logo_Local=logo_Local,
                            jug_Visita=jug_Visita,
                            logo_Visita=logo_Visita,
                            imagenIdiom=imagenIdiom,
                            channel_name=channel_name,
                            text_idiom=text_idiom,
                            urlFinal=None,
                            existeEvent=existeEvent,
                            elementos=url_name_pairs
                        )
                        contar_reg += 1

                    else:
                        a_elements = soup.find_all("a", href=True)
                        contador_urls = 0
                        for a in a_elements:
                            href = a["href"]
                            if href.endswith("/01.php"):
                                contador_urls += 1 
                                if contador_urls == 8:
                                    # Extraer la parte de la URL que comienza con https://www.platinsport.com
                                    url_event = href.split("https://www.platinsport.com")[-1]
                                    url_event = "https://www.platinsport.com" + url_event
                                    break  # Tomar la primera URL que cumple con el criterio

                        # url_event = hrefs_list[0].split("https://www.platinsport.com", 1)[-1]
                        # url_event = "https://www.platinsport.com" + url_event
                        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"}
                        responsePlainUnicoURL = requests.get(url_event,headers=headers,allow_redirects=True,verify=False,)
                        # responsePlainUnicoURL = requests.get(url_event, headers=headers)
                        html_contentPlainUnicoURL = responsePlainUnicoURL.text
                        soupPlainUnicoURL = BeautifulSoup(html_contentPlainUnicoURL, "html.parser")
                        divs = soupPlainUnicoURL.find_all("div", class_="myDiv")
                        if len(divs) >= 2:
                            # second_myDiv = divs[1]  # El segundo div (indice 1)
                            # first_word_title = second_myDiv.text.split()[0]
                            
                            if first_word_title == currentDayOfWeek:
                                base_flags_url = "https://www.platinsport.com/style/flag-icons-main/flags/4x3/"

                                # Encontrar todo el contenido de la clase 'myDiv1'
                                div_content = soupPlainUnicoURL.find("div", class_="myDiv1")
                                # Obtener el contenido de la etiqueta como una cadena de texto
                                content_text = str(div_content)
                                # Dividir por saltos de linea
                                lines = content_text.split("\n")
                                # Lista para almacenar los eventos
                                eventos = []
                                urls_channels = []
                                # Iterar a traves de las lineas del contenido
                                for index, line in enumerate(lines):  # Usamos 'enumerate' para obtener el índice
                                
                                    line = line.strip()  # Eliminar espacios en blanco al inicio y al final
                                    # Si la linea comienza con una hora (por ejemplo, "09:15")
                                    # print(f"line : {line}")
                                    # Si la linea contiene un <time> (hora del evento)
                                    if "<time" in line:
                                        try:
                                            # Extraer la hora del evento desde el atributo 'datetime' de la etiqueta <time>
                                            soup_line = BeautifulSoup(line, "html.parser")
                                            time_tag = soup_line.find("time")
                                            if time_tag and time_tag.get('datetime'):
                                                hora_event = time_tag['datetime']
                                                hora_event = datetime.strptime(hora_event, "%Y-%m-%dT%H:%M:%SZ").strftime("%H:%M")
                                                # print(f"Hora del evento: {hora_event}")

                                                # Buscar la siguiente línea con el nombre del evento
                                                next_line = None
                                                # Asegurarse de que la siguiente línea no esté vacía
                                                for next_line_candidate in lines[index + 1:]:
                                                    if next_line_candidate.strip():  # Ignorar líneas vacías
                                                        next_line = next_line_candidate.strip()
                                                        break

                                                if next_line:
                                                    name_event = next_line
                                                    # print(f"Nombre del evento: {name_event}")

                                                # Reiniciar la lista de URLs y canales
                                                urls_channels = []

                                        except Exception as e:
                                            manejar_error_mensajes(f"Error desde procesar_Platin <time: {str(e)}", 1)
                                            bool_estado_platin = False
                                            continue
                                            
                                    # Si la linea comienza con "<a"
                                    elif line.startswith("<a"):
                                        try:
                                            # Extraer la URL y el canal
                                            href_start = line.find('href="') + len('href="')
                                            href_end = line.find('"', href_start)
                                            href = line[href_start:href_end]
                                            href = line[href_start:href_end].strip()
                                            # href = href.replace(" ", "")
                                            # channel = line.split(">")[1].split("<")[0]
                                            span_end = line.find("</span>")
                                            channel_start = span_end + len("</span>")
                                            channel_end = line.find("</a>", channel_start)
                                            channel = line[channel_start:channel_end].strip()
                                            channel = capitalize_words(channel)
                                            if "acestream://" in channel:
                                                channel = "Channe 01"
                                            flag_class_start = line.find('class="fi ') + len('class="fi ')
                                            flag_class_end = line.find('"', flag_class_start)
                                            flag_class = line[flag_class_start:flag_class_end]
                                            if "-" in flag_class:
                                                country_code = flag_class.split("-")[1]
                                            else:
                                                country_code = "gb"

                                            channel_flag_url = f"{base_flags_url}{country_code}.svg"
                                            # Agregar la URL y el canal a la lista
                                            # print(f"href : {href} | channel {channel} | channel_flag_url {channel_flag_url}")
                                            urls_channels.append(
                                                {
                                                    "urlFin": href,
                                                    "nameChannel": channel,
                                                    "channel_flag_url": channel_flag_url,
                                                }
                                            )
                                        except Exception as e:
                                            manejar_error_mensajes(f"Error desde procesar_Platin <a: {str(e)}",1,)
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
                                        hora_event = evento["hora_event"]
                                        name_event = evento["name_event"]
                                        # pdb.set_trace()
                                        name_event = process_special_characters(name_event)

                                        url_name_pairs = evento["urlFinal_channel_name"]
                                        if url_name_pairs is None or not url_name_pairs:
                                            continue

                                        # for banderas in lista_event_flag:
                                        #     if banderas["nombre_evento"] == name_event:
                                        #         url_flag = banderas["bandera"]
                                        #         break  # Salimos del bucle si encontramos el evento

                                        name_event = capitalize_words(name_event)
                                        fecha_hora, eventNextDay = procesar_hora_evento(fecha_actual, hora_event, 0, eventNextDay)
                                        # print(f"fecha_hora: {fecha_hora} | hora_event: {hora_event} | name_event: {name_event} | eventNextDay: {eventNextDay}")
                                        # continue                                        

                                        Insert_Update_Events_Unified(
                                            proveedor="Platin",
                                            fecha_hora=fecha_hora,
                                            # hora_event=hora_event,
                                            event_categoria=event_categoria,
                                            name_event=name_event,
                                            url_flag=url_flag,
                                            jug_Local=jug_Local,
                                            logo_Local=logo_Local,
                                            jug_Visita=jug_Visita,
                                            logo_Visita=logo_Visita,
                                            imagenIdiom=imagenIdiom,
                                            channel_name=channel_name,
                                            text_idiom=text_idiom,
                                            urlFinal=None,
                                            existeEvent=existeEvent,
                                            elementos=url_name_pairs
                                        )

                                        contar_reg += 1

                                    except Exception as e:
                                        manejar_error_mensajes(f"Error desde procesar_Platin 4: {str(e)}",1,)
                                        bool_estado_platin = False
                                        continue
                            else:
                                bool_estado_platin = False
                        break
                else:
                    bool_estado_platin = False
            except Exception as e:
                manejar_error_mensajes(f"Error desde procesar_Platin 5: {str(e)}", 1)
                bool_estado_platin = False
                continue
        
        manejar_error_mensajes(f"Add {contar_reg} for Platin", 1)
        actualizar_estado_dealer(dealer_id=7, dealer_name="Platin", estado=bool_estado_platin)
        # manejar_error_mensajes("Termina procesar_Platin", 0)

    except Exception as e:
        manejar_error_mensajes(f"Error desde procesar_Platin 6: {str(e)}", 1)
        actualizar_estado_dealer(dealer_id=7, dealer_name="Platin", estado=False)


def procesar_DaddyLivehd():
    manejar_error_mensajes(" ============================================= | Inicia procesar_DLHD | ============================================= ", 0)
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
        contar_reg = 0

        json_anterior_DLHD = cargar_json_local(json_file_path_DLHD)

        # headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"}
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
            "Referer": baseurlDLHD,
            "Origin": baseurlDLHD,
            "Accept": "application/json",
            "Sec-Fetch-Site": "same-origin",
            "DNT": "1",
        }        
        
        responseDaddyLivehd = requests.get(urlDaddyLivehd, headers=headers, verify=False, timeout=10)

        if responseDaddyLivehd.status_code != 200:
            manejar_error_mensajes(f"Error al acceder a la API: Código {responseDaddyLivehd.status_code}", 1)
            bool_estado_DaddyLivehd = False
            return

        schedule_data = responseDaddyLivehd.json()

        if json_anterior_DLHD is not None:
            diferencias = DeepDiff(json_anterior_DLHD, schedule_data, ignore_order=True)
            if not diferencias:
                manejar_error_mensajes("Los JSON son iguales", 0)
                return

        guardar_json_local(json_file_path_DLHD ,schedule_data)

        dia_semana = hora_inicia_ejecucion.strftime("%A")
        # Filtrar las categorias en el JSON basandose en el dia de la semana
        categories = None

        for key in schedule_data.keys():
            if dia_semana in key:
                categories = schedule_data[key]
                break
        # Verifica si se encontro la categoria correspondiente
        if categories is None:
            manejar_error_mensajes(f"No se encontraron datos para el dia {dia_semana} en el JSON. procesar_DaddyLivehd",1,)
        else:
            for category, events in categories.items():
                # Verificar si la categoria es "Soccer"
                # if category != "Tv Show":
                if "Show" not in category:
                    # Iterar sobre los eventos en la categoria "Soccer"
                    for event in events:
                        try:
                            channels_data = event["channels"]
                            hora_event = event["time"]
                            name_event = event["event"].split(":")[1].strip() if ":" in event["event"] else event["event"].strip()
                            event_categoria = event["event"].split(":")[0].strip() if ":" in event["event"] else event["event"].strip()
                            name_event = process_special_characters(name_event)
                            if event_categoria == name_event:
                                event_categoria = category
                            else:
                                event_categoria = category + " - " + event_categoria
                            # Buscar "vs" o "vs." y dividir en consecuencia
                            jug_Local, jug_Visita = (None,None,)  # Inicializar por defecto
                            if " vs " in name_event or " vs. " in name_event:
                                try:
                                    # Usar regex para identificar el delimitador
                                    delimitador = " vs " if " vs " in name_event else " vs. "
                                    partes = re.split(rf"\s*{re.escape(delimitador)}\s*",name_event,)
                                    if len(partes) == 2:  # Solo procesar si hay exactamente dos partes
                                        jug_Local, jug_Visita = map(str.strip, partes)
                                    else:
                                        manejar_error_mensajes(f"Formato inesperado para name_event: {name_event}",1,)
                                except Exception as e:
                                    manejar_error_mensajes(f"Error al dividir jugadores: {e} | Evento: {name_event}",1,)

                            name_event = name_event.replace("vs.", "Vs").strip()


                            if channels_data:
                                fecha_hora, eventNextDay = procesar_hora_evento(fecha_actual, hora_event, 0, eventNextDay)

                            Insert_Update_Events_Unified(
                                proveedor="DLHD",
                                fecha_hora=fecha_hora,
                                # hora_event=hora_event,
                                event_categoria=event_categoria,
                                name_event=name_event,
                                url_flag=url_flag,
                                jug_Local=jug_Local,
                                logo_Local=logo_Local,
                                jug_Visita=jug_Visita,
                                logo_Visita=logo_Visita,
                                imagenIdiom=imagenIdiom,
                                channel_name=channel_name,
                                text_idiom=text_idiom,
                                urlFinal=None,
                                existeEvent=existeEvent,
                                elementos= channels_data
                            )
                            contar_reg += 1

                        except Exception as e:
                            manejar_error_mensajes(f"Error en procesar_DLHD 3: {e} | {event}", 1)
                            bool_estado_DaddyLivehd = False
                            continue

        manejar_error_mensajes(f"Add {contar_reg} for DLHD", 1)
        actualizar_estado_dealer(dealer_id=8, dealer_name="DLHD", estado=bool_estado_DaddyLivehd)
    except Exception as e:
        manejar_error_mensajes(f"Error en procesar_DLHD 4: {e}", 1)
        actualizar_estado_dealer(dealer_id=8, dealer_name="DLHD", estado=False)


# def procesar_DaddyLivehd():
#     manejar_error_mensajes(" ============================================= | Inicia procesar_DLHD | ============================================= ", 0)
#     global eventNextDay
#     global event_categoria
#     global url_flag
#     global jug_Local
#     global logo_Local
#     global jug_Visita
#     global logo_Visita
#     global channel_name
#     global imagenIdiom
#     global text_idiom
#     global existeEvent
#     global contador_registros
#     global bool_estado_platin
#     global foundDayOfWeek
#     global currentDayOfWeek
#     global bool_estado_DaddyLivehd
#     try:        
#         hora_event = None
#         contar_reg = 0

#         responseDaddyLivehd = obtenerResponseSelenium(urlDaddyLivehd)     
#         page_source = responseDaddyLivehd
#         # if debuguear == 1:
#         #     with open('DLHD.html', 'w', encoding='utf-8') as file:
#         #         file.write(page_source)
        
#         soup = BeautifulSoup(page_source, 'html.parser')

#         fecha_pag = soup.find('tr', class_='date-row')
#         fecha_pag = fecha_pag.find('strong').get_text(strip=True)
#         match = re.search(r'(\d{1,2})(?:st|nd|rd|th)?\s+(\w+)\s+(\d{4})', fecha_pag)
#         dia, mes_texto, anio = match.groups()
#         mes = month_str_to_num(mes_texto)
#         dia = dia.zfill(2)  # Asegurar 2 dígitos
#         fecha_pagina = f"{anio}{mes}{dia}"

#         if fecha_pagina == fecha_actual:
#             # Verificar si existe el botón que marca el fin de los eventos útiles
#             boton_fin = soup.find('button', {'id': 'toggleExtraSchedule'})
#             if boton_fin:
#                 # Limitar el procesamiento solo a los elementos antes del botón
#                 contenido_util = page_source.split(str(boton_fin))[0]
#                 soup = BeautifulSoup(contenido_util, 'html.parser')


#             categorias = soup.find_all('tr', class_='category-row')

#             for categoria in categorias:
#                 try:
#                     if "TV Shows" in categoria.get_text():
#                         continue  # Saltamos TV Shows
                        
#                     # Encontrar todos los eventos siguientes hasta la próxima categoría
#                     elementos = []
#                     siguiente_elemento = categoria.find_next_sibling('tr')
                    
#                     while siguiente_elemento and not ('category-row' in siguiente_elemento.get('class', [])):
#                         elementos.append(siguiente_elemento)
#                         siguiente_elemento = siguiente_elemento.find_next_sibling('tr')

#                     # Recolectar todos los elementos de esta categoría
#                     elementos = []
#                     siguiente_elemento = categoria.find_next_sibling('tr')
                    
#                     while siguiente_elemento and not ('category-row' in siguiente_elemento.get('class', [])):
#                         elementos.append(siguiente_elemento)
#                         siguiente_elemento = siguiente_elemento.find_next_sibling('tr')

#                     # Procesar elementos
#                     idx = 0
#                     while idx < len(elementos):
#                         elemento_actual = elementos[idx]
                        
#                         try:
#                             if 'event-row' in elemento_actual.get('class', []):
#                                 # Procesar evento
#                                 hora_tag = elemento_actual.find('div', class_='event-time')
#                                 hora_event = hora_tag.get_text(strip=True) if hora_tag else None
                                
#                                 nombre_tag = elemento_actual.find('div', class_='event-info')
#                                 if nombre_tag:
#                                     nombre_completo = nombre_tag.get_text(strip=True)
#                                     if " : " in nombre_completo:
#                                         event_categoria, name_event = nombre_completo.split(" : ", 1)
#                                     else:
#                                         event_categoria = " "
#                                         name_event = nombre_completo
                                    
#                                     # Procesar equipos
#                                     jug_Local, jug_Visita = (None, None)
#                                     if "vs" in name_event or "vs." in name_event:
#                                         try:
#                                             delimitador = "vs" if "vs" in name_event else "vs."
#                                             partes = re.split(rf"\s*{re.escape(delimitador)}\s*", name_event)
#                                             if len(partes) == 2:
#                                                 jug_Local, jug_Visita = map(str.strip, partes)
#                                         except Exception as e:
#                                             manejar_error_mensajes(f"Error al dividir jugadores: {e}", 1)
                                    
#                                     name_event = name_event.replace("vs.", "Vs").strip()
                                    
#                                     # Procesar canales (siguiente elemento)
#                                     channels_data = []
#                                     if idx+1 < len(elementos) and 'channel-row' in elementos[idx+1].get('class', []):
#                                         canales_tag = elementos[idx+1]
#                                         enlaces = canales_tag.find_all('a', class_='channel-button-small')
                                        
#                                         for enlace in enlaces:
#                                             url_inicial = enlace.get('href')
#                                             nombre_canal = enlace.get_text(strip=True)
#                                             nombre_canal_limpio = re.sub(r'\s*\([^)]*\)', '', nombre_canal)
#                                             if '/bet.' in url_inicial:
#                                                 continue
                                            
#                                             url_inicial = url_inicial.replace("/stream/", "/")

#                                             # https://daddylive.mp/extra/stream-1237.php
#                                             if '/extra/' in url_inicial:
#                                                 continue
                                                
#                                             url_completa = f"https://daddylive.mp/embed{url_inicial}"
                                            
#                                             canal = {
#                                                 'channel_name': nombre_canal_limpio,
#                                                 'urlFinal': url_completa
#                                             }
#                                             channels_data.append(canal)

#                                     if channels_data:
#                                         fecha_hora, eventNextDay = procesar_hora_evento(fecha_actual, hora_event, 0, eventNextDay)
#                                         # print(f"fecha_hora: {fecha_hora} | name_event: {name_event}")
#                                         # for canal in channels_data:
#                                         #     print(f"        channel_name: {canal['channel_name']} | urlFinal: {canal['urlFinal']}")
                                        
#                                         Insert_Update_Events_Unified(
#                                             proveedor="DLHD",
#                                             fecha_hora=fecha_hora,
#                                             # hora_event=hora_event,
#                                             event_categoria=event_categoria,
#                                             name_event=name_event,
#                                             url_flag=url_flag,
#                                             jug_Local=jug_Local,
#                                             logo_Local=logo_Local,
#                                             jug_Visita=jug_Visita,
#                                             logo_Visita=logo_Visita,
#                                             imagenIdiom=imagenIdiom,
#                                             channel_name=channel_name,
#                                             text_idiom=text_idiom,
#                                             urlFinal=None,
#                                             existeEvent=existeEvent,
#                                             elementos= channels_data
#                                         )

#                                         contar_reg += 1
#                                         idx += 2  # Saltamos al siguiente evento (pasamos los canales)
#                                     else:
#                                         idx += 1  # No había canales, avanzamos
#                                 else:
#                                     idx += 1  # No había nombre de evento, avanzamos
#                             else:
#                                 idx += 1  # No era un evento, avanzamos

#                         except Exception as e:
#                             manejar_error_mensajes(f"Error procesando elemento {idx}: {e}", 1)
#                             bool_estado_DaddyLivehd = False
#                             idx += 1  # Asegurarnos de avanzar incluso si hay error

#                 except Exception as e:
#                     manejar_error_mensajes(f"Error en procesar_DLHD 3: {e} | {categoria}", 1)
#                     bool_estado_DaddyLivehd = False
#                     continue
            
#         manejar_error_mensajes(f"Add {contar_reg} for DLHD", 1)
#         actualizar_estado_dealer(dealer_id=8, dealer_name="DLHD", estado=bool_estado_DaddyLivehd)
#             # manejar_error_mensajes("Termina procesar_DaddyLivehd", 0)        
#     except Exception as e:
#         manejar_error_mensajes(f"Error en procesar_DLHD 4: {e}", 1)
#         actualizar_estado_dealer(dealer_id=8, dealer_name="DLHD", estado=False)


def procesar_LFJson():
    manejar_error_mensajes(" ============================================= | Inicia procesar_LFJson | ============================================= ", 0)
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
    global bool_estado_LFJSON
    global json_anterior_LFJSON
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
        existeEvent = None
        eventNextDay = False  # Inicializar eventNextDay
        contar_reg = 0  

        json_anterior_LFJSON = cargar_json_local(json_file_path_LFJSON)

        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"}
        response = requests.get(urlLFJson, headers=headers, allow_redirects=True, verify=False)
        json_data = response.json()
        if json_anterior_LFJSON is not None:
            diferencias = DeepDiff(json_anterior_LFJSON, json_data, ignore_order=True)
            if not diferencias:
                manejar_error_mensajes("Los JSON son iguales", 0)
                return

        guardar_json_local(json_file_path_LFJSON ,json_data)

        eventos = json_data["data"]
        # Iterar a traves de los registros
        for evento in eventos:
            try:
                attributes = evento["attributes"]
                embeds = attributes.get("embeds", {}).get("data", [])
                hora_event = attributes.get("diary_hour", "")[:5]
                descripcion = attributes.get("diary_description", "").strip()
                if descripcion.strip().startswith("<") and descripcion.strip().endswith(">"):
                    descripcion_limpia = BeautifulSoup(descripcion, "html.parser").get_text()
                else:
                    descripcion_limpia = descripcion

                if ":" in descripcion_limpia:
                    event_categoria, name_event = descripcion_limpia.split(":", 1)
                    event_categoria = event_categoria.strip()
                    name_event = name_event.strip().replace("\\n", "")  # Limpiar saltos de linea
                else:
                    event_categoria, name_event = "", ""

                name_event = name_event.replace("vs.", "Vs").strip()
                name_event = process_special_characters(name_event)
                country_data = attributes.get("country", {}).get("data", {}).get("attributes", {})
                country_name = country_data.get("name", "")
                url_flag = country_data.get("image", {}).get("data", {}).get("attributes", {}).get("url", "").strip()
                base_url_flag = "https://img.golazoplay.com"  # 'https://panel.atvenvivo.com'
                event_categoria = f"{country_name} {event_categoria}"
                url_flag = f"{base_url_flag} {url_flag}"
                url_flag = url_flag.replace(" ", "")      
                fecha_hora, eventNextDay = procesar_hora_evento(fecha_actual, hora_event, +5, eventNextDay)
        
                Insert_Update_Events_Unified(
                    proveedor="LFJson",
                    fecha_hora=fecha_hora,
                    # hora_event=hora_event,
                    event_categoria=event_categoria,
                    name_event=name_event,
                    url_flag=url_flag,
                    jug_Local=jug_Local,
                    logo_Local=logo_Local,
                    jug_Visita=jug_Visita,
                    logo_Visita=logo_Visita,
                    imagenIdiom=imagenIdiom,
                    channel_name=channel_name,
                    text_idiom=text_idiom,
                    urlFinal=None,
                    existeEvent=existeEvent,
                    elementos=embeds
                )

                contar_reg += 1

                bool_estado_LFJSON = False

            except Exception as e:
                manejar_error_mensajes(f"Error en LFJson {e}", 1)
                bool_estado_LFJSON = False
                continue

        manejar_error_mensajes(f"Add {contar_reg} for LFJson", 1)
        actualizar_estado_dealer(dealer_id=9, dealer_name="LFJson", estado=bool_estado_LFJSON)
        # manejar_error_mensajes("Termina procesar_LFJson", 0)

    except Exception as e:
        manejar_error_mensajes(f"Error en procesar_LFJson 4: {e}", 1)
        actualizar_estado_dealer(dealer_id=9, dealer_name="LFJson", estado=False)


def obtener_eventos():

    # activaBases = 0
    # activaLiveTV = 0
    # activaSportline = 0
    # activaDirectatvHDme = 0
    # activaLibreF = 0
    # activaRojaTv = 0
    # activaPlatin = 0
    # activaLFJSON = 0
    # activaDaddyLivehd = 0
    # activaRojaOn = 0

    # print(f"activaBases: {activaBases} | activaLiveTV: {activaLiveTV} | activaSportline: {activaSportline} | activaDirectatvHDme: {activaDirectatvHDme} | activaLibreF: {activaLibreF} | activaRojaTv: {activaRojaTv} | activaPlatin: {activaPlatin} | activaLFJSON: {activaLFJSON} | activaDaddyLivehd: {activaDaddyLivehd} | activaRojaOn: {activaRojaOn}")
    try:
        if activaBases > 0:
            procesar_Bases()

        if activaDaddyLivehd > 0:
            procesar_DaddyLivehd()

        if activaLFJSON > 0:
            procesar_LFJson()

        if activaLibreF > 0:
            procesar_LibreF()

        if activaSportline > 0:
            procesar_SportsLine()

        if activaPlatin > 0:
            procesar_Platin()            

        if activaDirectatvHDme > 0:
            procesar_DirectatvHDme()

        if activaRojaOn > 0:
            procesar_RojaOnline()

        if activaRojaTv > 0:
            procesar_RojaTV()

        if activaLiveTV > 0:
            procesar_LiveTV()

    except Exception as e:
        manejar_error_mensajes(f"Error en obtener_eventos: {e}", 1)


configurar_logger()

while True:
    try:
        hora_inicia_ejecucion = datetime.now()      
        # hora_inicia_ejecucion = hora_inicia_ejecucion.replace(tzinfo=None)
        fecha_actual = hora_inicia_ejecucion.strftime("%Y%m%d")
        manejar_error_mensajes(f"Inicia ejecucion: {hora_inicia_ejecucion}", 0)

        # eventos_existentes = {}
        vListDealers = []
        # v_list_eventos = deque(maxlen=500)
        v_list_eventos = []
        v_list_eventos_copia = []
        v_list_eventos_3 = []
        v_list_eventos_news = []
        v_list_eventos_LiveTV = []
        v_list_eventos_Bases = []

        token = "6559813109:AAEUKzEG6rRIFrt2pwkcHhZuA9Ynt3kqvlI"
        bot_tg_canal = telegram.Bot(token=token)
        chat_id = "5954221232"  # 1002035964627:channel: - # '5954221232' # chat_id:bot
        chat_id_channel = -1002035964627

        v_list_eventos_3 = cargar_json_local(json_file_path_lista_eventos) or []

        # Crea una instancia para cada tabla que deseas limpiar
        # eventos_table = MyDynamoDB_EliminarRegistrosTabla("eventos")
        eventos_table = MyDynamoDB_EliminarRegistrosTabla("eventos", v_list_eventos_3)
        dealers_table = MyDynamoDB_EliminarRegistrosTabla("dealers")
        dia_evento_table = MyDynamoDB_EliminarRegistrosTabla("dia_evento")

        dia_actual_bd = obtener_dia_actual()

        if fecha_actual > dia_actual_bd:
            eventos_table.delete_all_items()
            dealers_table.delete_all_items()
            dia_evento_table.delete_all_items()

        if not vListDealers:  # Verifica si vListDealers esta vacia
            response_DB_Dealers = t_dealers.scan()  # O el nombre correcto de tu variable de base de datos para dealers
            dealers_para_procesar = response_DB_Dealers.get("Items", [])
            # Actualiza vListDealers solo si se obtienen nuevos dealers
            if dealers_para_procesar:
                vListDealers = dealers_para_procesar
        else:
            # Usa vListDealers directamente si ya esta llena
            dealers_para_procesar = vListDealers

        # Cargar la lista desde el archivo JSON al inicio de la iteración

        # Suponiendo que al inicio v_list_eventos_3 es None o una lista vacia
        if v_list_eventos_3 is None or not v_list_eventos_3:
            response_DB_Eventos = t_eventos.scan()            
            # print(f"Hace t_eventos.scan(), pendiente arreglar esto")            
            eventos_para_procesar = response_DB_Eventos.get("Items", [])
            # Solo actualiza v_list_eventos_3 si response_DB_Eventos tiene items
            if eventos_para_procesar:
                v_list_eventos_3 = eventos_para_procesar
        else:
            # Usamos v_list_eventos_3 directamente si ya esta lleno
            eventos_para_procesar = v_list_eventos_3

        # print(f"v_list_eventos_3 : {v_list_eventos_3}")
        # print(f"eventos_para_procesar : {eventos_para_procesar}")

        contador_registros = 0

        # activaBases cada 6 horas

        if (hora_inicia_ejecucion - hour_update_bases).total_seconds() >= 21600:  # 6 horas en segundos
            hour_update_bases = hora_inicia_ejecucion
            activaBases = 1
            # print(f"procesar_Bases() ejecutado, hora de actualizacion: {hour_update_bases}")    
        else:
            activaBases = 0
            # print(f"procesar_Bases() no ejecutado, hora de actualizacion: {hour_update_bases}")

        
        activaLiveTV = 0
        activaSportline = 0
        activaDirectatvHDme = 0
        activaLibreF = 0
        activaRojaOn = 0
        activaRojaTv = 0
        activaPlatin = 0
        activaDaddyLivehd = 0
        activaLFJSON = 0

        ind_miss_LibreF = 0
        # ind_miss_LiveTV = 0
        bool_estado_Sportline = False
        bool_estado_DirectatvHDme = False
        bool_estado_libref = False
        bool_estado_RojaOn = False
        bool_estado_RojaTv = False
        bool_estado_platin = False
        bool_estado_DaddyLivehd = False
        bool_estado_LFJSON = False

        if response_DB_Dealers["Count"] == 0:
            activaBases = 1
            activaLiveTV = 1
            activaSportline = 1
            activaDirectatvHDme = 1
            activaLibreF = 1
            activaRojaOn = 1
            activaRojaTv = 1
            activaPlatin = 1
            activaDaddyLivehd = 1
            activaLFJSON = 1
        else:
            for item in dealers_para_procesar:
                proveedor = item.get("f02_dealer_name", "")
                estado = item.get("f03_state", "")
                if proveedor == "LiveTV":
                    if not estado:
                        activaLiveTV = 1                
                # elif proveedor == "Bases":
                #     if not estado:
                #         activaBases = 1
                elif proveedor == "Sportline":
                    if not estado:
                        activaSportline = 1
                elif proveedor == "DirectatvHDme":
                    if not estado:
                        activaDirectatvHDme = 1
                elif proveedor == "LibreF":
                    if not estado:
                        activaLibreF = 1
                elif proveedor == "RojaOn":
                    if not estado:
                        activaRojaOn = 1
                elif proveedor == "RojaTv":
                    if not estado:
                        activaRojaTv = 1
                elif proveedor == "Platin":
                    if not estado:
                        activaPlatin = 1
                elif proveedor == "DLHD":
                    if not estado:
                        activaDaddyLivehd = 1
                elif proveedor == "LFJson":
                    if not estado:
                        activaLFJSON = 1

        max_evento1 = None
        max_f01_id_document1 = 0
        max_evento = None
        max_eventoLiveTV = None
        f01_id_document_list = []

        # Ahora, eventos_para_procesar contiene la lista de eventos a procesar, ya sea de response_DB_Eventos o de v_list_eventos_3
        if eventos_para_procesar:
            # Iterar sobre los documentos y agregarlos a v_list_eventos_en_BD
            for evento_db in eventos_para_procesar:
                evento_data = evento_db
                f02_proveedor = evento_data.get("f02_proveedor", "")
                if ("LibreF" in f02_proveedor and ind_miss_LibreF == 1) or ("LiveTV" in f02_proveedor and activaLiveTV == 1):
                    detalles_evento = evento_data.get("f20_Detalles_Evento", [])
                    # Verificar si hay al menos un detalle que contiene 'sin_data' en 'f22_opcion_Watch'
                    if any("sin_data" in detalle.get("f22_opcion_Watch", "") for detalle in detalles_evento if detalle.get("f22_opcion_Watch") is not None):
                        v_list_eventos.append(evento_data)
                        activaLibreF = 0

                    # Si el evento contiene 'LiveTV', guarda su f01_id_document en la listas
                    if "LiveTV" in evento_data["f02_proveedor"]:
                        activaLiveTV = 1
                        f01_id_document_list.append(evento_data["f01_id_document"])
                        v_list_eventos_LiveTV.append(evento_data)
                        # print(f"adiciona a v_list_eventos_LiveTV : {evento_data}")

                # Si el evento contiene 'LiveTV', guarda su f01_id_document en la lista
                if "Bases" in evento_data["f02_proveedor"]:
                    v_list_eventos_Bases.append(evento_data)

                if (   ("Sportline" in f02_proveedor and activaSportline == 1)
                    or ("DirectatvHDme" in f02_proveedor and activaDirectatvHDme == 1)
                    or ("RojaOn" in f02_proveedor and activaRojaOn == 1)
                    or ("RojaTv" in f02_proveedor and activaRojaTv == 1)
                    or ("Platin" in f02_proveedor and activaPlatin == 1)
                    or ("DLHD" in f02_proveedor and activaDaddyLivehd == 1)
                    or ("LFJson" in f02_proveedor and activaLFJSON == 1)):
                    v_list_eventos.append(evento_data)
                else:
                    continue

            # Encuentra el maximo f01_id_document entre los eventos 'LiveTV'
            if f01_id_document_list:
                max_f01_id_document1 = max(f01_id_document_list)
                max_eventoLiveTV = next((evento for evento in eventos_para_procesar if evento["f01_id_document"] == max_f01_id_document1 and "LiveTV" in evento["f02_proveedor"]),None,)

            # Si encontramos el evento maximo, agregarlo a v_list_eventos
            if max_eventoLiveTV:
                v_list_eventos.append(max_eventoLiveTV)

            # Obtener todos los f01_id_document de eventos_db
            # f01_id_document_list = [evento_db['f01_id_document'] for evento_db in response_DB_Eventos['Items']]
            f01_id_document_list = [evento_db["f01_id_document"] for evento_db in eventos_para_procesar]

            # Encontrar el maximo f01_id_document
            if f01_id_document_list:
                contador_registros = max(f01_id_document_list)
        # Copiar el contenido de v_list_eventos a v_list_eventos_copia
        v_list_eventos_copia = copy.deepcopy(v_list_eventos)

        verificar_existencias()

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
            "MONDAY",
            "TUESDAY",
            "WEDNESDAY",
            "THURSDAY",
            "FRIDAY",
            "SATURDAY",
            "SUNDAY",
        ]
        
        dias_traducidos = {
            "Lunes": "MONDAY",
            "Martes": "TUESDAY",
            "Miercoles": "WEDNESDAY",
            "Jueves": "THURSDAY",
            "Viernes": "FRIDAY",
            "Sabado": "SATURDAY",
            "Domingo": "SUNDAY",
        }

        # Diccionario de meses en español a número
        meses = {
            'enero': '01', 'febrero': '02', 'marzo': '03',
            'abril': '04', 'mayo': '05', 'junio': '06',
            'julio': '07', 'agosto': '08', 'septiembre': '09',
            'octubre': '10', 'noviembre': '11', 'diciembre': '12'
        }        

        now = datetime.now()
        currentDayOfWeek = diassemana[now.weekday()]

        # Invocar la funcion para obtener los eventos
        obtener_eventos()
        # sys.exit()

        # Procesar los datos en vListDealers
        for dealer in vListDealers:
            dealer_id = decimal.Decimal(dealer.get("f01_id_dealer"))
            if dealer_id:
                # eliminar_dato_en_bd_dealer(dealer_id, dealer)
                insertar_dato_en_bd_dealer(dealer)
                # manejar_error_mensajes(f"Insert Dealer: {dealer} | {dealer_id}", 0,)
            else:
                manejar_error_mensajes(f"El dato no tiene un ID, no se puede actualizar en la BD. dealer: {dealer} | {dealer_id}",0,)

        # Llamada a la funcion
        procesar_cambios_eventos(v_list_eventos, v_list_eventos_copia)
        guardar_json_local(json_file_path_lista_eventos ,v_list_eventos_3)

        # # # Insertar el dia.
        if dia_actual_bd is None or fecha_actual > dia_actual_bd:
            # Insertar el nuevo dia en la coleccion
            item = {"id_dia_evento": 1, "f01_dia": fecha_actual}
            # Insertar el nuevo elemento en la tabla
            response = t_dia_evento.put_item(Item=item)

        # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # envio de mensaje y finalizacion # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
        horaFinEjecucion = datetime.now()
        # horaFinEjecucion = datetime.now(tz_colombia)
        # horaFinEjecucion = horaFinEjecucion.replace(tzinfo=None)
        diferencia_segundos = (horaFinEjecucion - hora_inicia_ejecucion).total_seconds()
        minutos = int(diferencia_segundos // 60)  # Obtiene la parte entera de la division
        segundos = int(diferencia_segundos % 60)  # Obtiene el resto de la division (segundos)

        # manejar_error_mensajes(f"El programa tardo {minutos}:{segundos} minutos en ejecutarse.",0,)
        agregar_mensaje_al_log(f"Script took {minutos}:{segundos}.")
        # asyncio.run(enviar_mensaje_telegram(chat_id, minutos,segundos))
        asyncio.run(enviar_mensaje_telegram_channel())

        manejar_error_mensajes(f"Finaliza ejecucion: {horaFinEjecucion}", 0)

        if minutos < 10:
            manejar_error_mensajes("Sleeping 10 Min", 0)
            time.sleep(600)
    except Exception as e:
        manejar_error_mensajes(f"Error detectado: {e}", 1)

