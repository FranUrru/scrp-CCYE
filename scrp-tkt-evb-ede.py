import io
import base64
from email.message import EmailMessage
from googleapiclient.discovery import build
import os
import joblib

def log(mensaje):
    timestamp = datetime.now().strftime('%H:%M:%S')
    linea = f"[{timestamp}] {mensaje}"
    print(linea)
    log_buffer.write(linea + "\n")
# Buffer para acumular los prints
log_buffer = io.StringIO()

_modelo_clasificador = None
def cargar_modelo_clasificador():
    global _modelo_clasificador
    if _modelo_clasificador is not None:
        return _modelo_clasificador

    rutas_posibles = [
        "modelo_clasificador_eventos.pkl",
        "__modelo_clasificador_eventos.pkl",
        os.path.join(os.path.dirname(__file__), "modelo_clasificador_eventos.pkl"),
    ]

    for ruta in rutas_posibles:
        if os.path.exists(ruta):
            # Verificación extra: ¿El archivo tiene contenido?
            if os.path.getsize(ruta) < 100: 
                log(f"❌ El archivo en {ruta} es demasiado pequeño para ser un modelo válido.")
                continue
                
            try:
                with open(ruta, "rb") as f:
                    _modelo_clasificador = joblib.load(ruta)
                log(f"✅ Modelo clasificador cargado desde: {ruta}")
                return _modelo_clasificador
            except Exception as e:
                log(f"❌ Error crítico al leer el archivo pkl en {ruta}: {e}")
                # Si un archivo está mal, probamos la siguiente ruta en lugar de romper todo
                continue

    log("⚠️ ADVERTENCIA: No se pudo cargar ningún modelo válido. Se omitirá la clasificación.")
    return None

def aplicar_clasificador(df, col_nombre, col_lugar, col_tipo_evento, col_confianza="confianza_clasificacion"):
    """
    Aplica el modelo logístico a las filas donde 'col_tipo_evento' está vacía.

    Args:
        df: DataFrame a procesar.
        col_nombre: Nombre de la columna con el nombre del evento (columna A).
        col_lugar: Nombre de la columna con el lugar (columna B).
        col_tipo_evento: Nombre de la columna de tipo de evento (columna E).
        col_confianza: Nombre de la columna donde se registrará el nivel de confianza.

    Returns:
        DataFrame con predicciones y confianzas completadas, y un dict de métricas.
    """
    modelo = cargar_modelo_clasificador()
    metricas = {"predicciones": 0, "confianza_promedio": None, "fuente": ""}

    if modelo is None or df.empty:
        return df, metricas

    # Aseguramos que la columna de confianza exista
    if col_confianza not in df.columns:
        df[col_confianza] = None

    # Identificamos filas a predecir: tipo de evento vacío/nulo
    mask_vacio = df[col_tipo_evento].isna() | (df[col_tipo_evento].astype(str).str.strip() == "")

    if not mask_vacio.any():
        return df, metricas

    df_a_predecir = df[mask_vacio].copy()

    # Construimos el vector de features igual que en el entrenamiento
    X = (
        df_a_predecir[col_nombre].astype(str).fillna("")
        + " "
        + df_a_predecir[col_lugar].astype(str).fillna("")
    )

    try:
        predicciones = modelo.predict(X)
        # predict_proba devuelve la prob de cada clase; tomamos el máximo como confianza
        probs = modelo.predict_proba(X)
        confianzas = np.max(probs, axis=1).round(4)

        df.loc[mask_vacio, col_tipo_evento] = predicciones
        df.loc[mask_vacio, col_confianza] = confianzas

        metricas["predicciones"] = int(mask_vacio.sum())
        metricas["confianza_promedio"] = round(float(np.mean(confianzas)), 4)

    except Exception as e:
        log(f"⚠️ Error al ejecutar el clasificador: {e}")

    return df, metricas

def click_load_more_until_disappears(driver):
    """
    Hace clic en el botón 'Cargar más' repetidamente hasta que desaparece.

    Args:
        driver (webdriver): El objeto webdriver de Selenium.
    """
    try:
        while True:  # Bucle infinito, se rompe cuando el botón desaparece
            try:
                # Espera hasta que el botón esté presente y sea clickeable (máximo 10 segundos)
                load_more_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//div[@class='infinite-scroll refresh col-xs-10 text-center padding-vertical-hard margin-top']/span[@class='text-uppercase bg-light-blue padding-vertical padding-horizontal-hard']"))
                )

                # Hace clic en el botón
                load_more_button.click()
                time.sleep(5)  # Espera un poco para que se carguen más elementos
                print("Botón 'Cargar más' clickeado.")

            except NoSuchElementException:
                # Si el botón ya no existe, salimos del bucle
                print("El botón 'Cargar más' ya no está presente.")
                break  # Sale del bucle while

            except Exception as e:
                # Captura otras excepciones (por ejemplo, TimeoutException si el botón tarda en aparecer)
                print(f"Error al hacer clic en el botón 'Cargar más': {e}")
                break  # Sale del bucle while
    except Exception as e:
        log(f"Error general: {e}")


def extract_artist_data(soup):
    """
    Extrae el título y el href de los elementos 'tkt-artist-list-image-item' y los almacena en un DataFrame.

    Args:
        soup (BeautifulSoup): El objeto BeautifulSoup que contiene el HTML analizado.

    Returns:
        pandas.DataFrame: Un DataFrame con las columnas 'title' y 'href'.
    """
    artist_elements = soup.find_all('div', class_='tkt-artist-list-image-item relative col-xs-10 col-sm-25 margin-bottom')
    artist_data = []

    for artist_element in artist_elements:
        a_tag = artist_element.find('a', class_='info-container absolute')
        if a_tag:
            title = a_tag.get('title')
            href = 'ticketek.com.ar/' + a_tag.get('href', '')  # Agrega el prefijo y maneja hrefs faltantes
            artist_data.append({'title': title, 'href': href})

    return pd.DataFrame(artist_data)
import time
from bs4 import BeautifulSoup
from urllib.parse import quote
from selenium.common.exceptions import WebDriverException  # Importa la excepción
import numpy as np

def extract_details_from_page(driver, href):
    try:
        driver.get(href)
        time.sleep(2) 
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        details = {'price': None, 'lugar': None, 'description': None}

        # --- LUGAR (Estrategia Multicapa) ---
        # 1. Intentar por el widget de compra (lo que ya hacíamos)
        lugar_element = soup.find('div', class_='padding-vertical pull-left')
        if lugar_element:
            details['lugar'] = lugar_element.get_text(strip=True)
        
        # 2. Si falló, buscar en los atributos 'data-venue' del header (Caso Sin Bandera)
        if not details['lugar']:
            header = soup.find('div', attrs={'data-tkt-show-header': True})
            if header and header.has_attr('data-venue'):
                details['lugar'] = header['data-venue']

        # --- PRECIO (Sección lateral) ---
        left_sidebar = soup.find('section', id='left-sidebar')
        if left_sidebar:
            details['price'] = left_sidebar.get_text(separator=" ", strip=True)

        # --- DESCRIPCIÓN (Barrido Total) ---
        # Buscamos en 'top' y 'main-content'. 
        # En Sin Bandera, la info está en un div dentro de 'top'.
        textos = []
        for section_id in ['top', 'main-content', 'left-sidebar']:
            section = soup.find('section', id=section_id)
            if section:
                # Buscamos todos los bloques de texto
                bloques = section.find_all('div', class_='tkt-content-content')
                for b in bloques:
                    textos.append(b.get_text(separator=" ", strip=True))
        
        if textos:
            details['description'] = " . ".join(textos)

        return details
    except Exception as e:
        return {'error': str(e), 'price': None, 'lugar': None, 'description': None}
    
import re

def extract_details_from_location(driver, href):
    try:
        driver.get(href)
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        details = {'price': None, 'lugar': None, 'description': None}

        # --- 1. LUGAR (Enfoque por URL Robusto) ---
        print(f"Procesando URL: {href}")
        
        # Eliminamos posibles barras al final y dividimos
        # Esto funciona tanto para URLs completas como para paths
        parts = [p for p in href.strip('/').split('/') if p]
        
        # Si la URL tiene la estructura de Ticketek (/artista/lugar)
        # El lugar siempre será el último segmento
        if len(parts) >= 2:
            last_part = parts[-1]
            # Limpiamos guiones y capitalizamos (ej: quality-arena -> Quality Arena)
            lugar_limpio = last_part.replace('-', ' ').title()
            details['lugar'] = lugar_limpio
        else:
            # Fallback: Intentar extraer del HTML si la URL es muy corta
            header_widget = soup.find(attrs={'data-tkt-show-header': True})
            if header_widget and header_widget.get('data-venue'):
                details['lugar'] = header_widget['data-venue'].strip()

        # --- 2. PRECIO ---
        left_sidebar = soup.find('section', id='left-sidebar')
        if left_sidebar:
            details['price'] = left_sidebar.get_text(separator=" ", strip=True)

        # --- 3. DESCRIPCIÓN ---
        textos_acumulados = []
        # Añadimos 'footer' por si acaso, pero mantenemos tu estructura
        for sec_id in ['top', 'main-content', 'left-sidebar']:
            seccion = soup.find('section', id=sec_id)
            if seccion:
                bloques = seccion.find_all('div', class_='tkt-content-content')
                for bloque in bloques:
                    txt = bloque.get_text(separator=" ", strip=True)
                    if txt:
                        textos_acumulados.append(txt)

        if textos_acumulados:
            details['description'] = " . ".join(textos_acumulados)

        return details

    except Exception as e:
        log(f"Error en extracción: {e}")
        return {'error': str(e), 'price': None, 'lugar': None, 'description': None}
import time
from bs4 import BeautifulSoup
from urllib.parse import quote
from selenium.common.exceptions import WebDriverException  # Importa la excepción

import pandas as pd
import re
from datetime import datetime

def clean_data(df):
    """
    Limpia los datos en el DataFrame df, extrayendo precios y fechas.
    """

    def extract_and_sum_prices(text):
        """
        Extrae y suma precios del formato "$NNNN + $NNNN".
        """
        if text is None or not isinstance(text, str):
            return None
        
        # Eliminar todos los puntos de la cadena de texto
        text_sin_puntos = text.replace('.', '')
        # Captura formatos de precios
        prices = re.findall(r'(?:Desde )?\$(\d{1,6})(?: \+ \$(\d{1,6}))?', text_sin_puntos)
        
        if prices:
            total_prices = []
            for match in prices:
                price1 = match[0]
                price2 = match[1]
                price1_int = int(price1)
                if price2:
                    price2_int = int(price2)
                    total_price = price1_int + price2_int
                    #log(f"Precio 1: {price1_int}, Precio 2: {price2_int}, Suma: {total_price}")
                    total_prices.append(total_price)
                else:
                    print(f"Precio: {price1_int}")
                    total_prices.append(price1_int)
            return total_prices
        return None

    def calculate_average(price_list):
        """
        Calcula el promedio de una lista de precios.
        """
        if price_list:
            avg = sum(price_list) / len(price_list)
            #log(f"Lista de precios: {price_list}, Promedio: {avg}")
            return avg
        return None

    def extraer_fecha(texto):
        """
        Extrae la fecha priorizando el segmento de Córdoba/Quality.
        """
        if not texto or not isinstance(texto, str):
            return None

        # 1. Segmentación por ciudad
        segmento_interes = texto
        if "córdoba" in texto.lower() or "quality" in texto.lower():
            partes = texto.split('.')
            for parte in partes:
                if "córdoba" in parte.lower() or "quality" in parte.lower():
                    segmento_interes = parte
                    break

        # 2. Diccionario de meses
        meses = {
            'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4, 'mayo': 5, 'junio': 6,
            'julio': 7, 'agosto': 8, 'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12
        }
        
        # Regex mejorado para capturar día, mes y opcionalmente el año
        patron = re.compile(r'(\d{1,2})\s+de\s+([a-z]+)(?:\s+de\s+)?(\d{4})?', re.IGNORECASE)
        
        match = patron.search(segmento_interes)
        if match:
            dia, mes_str, anio_str = match.groups()
            mes = meses.get(mes_str.lower())
            
            if mes:
                if anio_str:
                    anio = int(anio_str)
                else:
                    ahora = datetime.now()
                    anio = ahora.year + (1 if mes < ahora.month else 0)
                
                return f"{anio:04}-{mes:02}-{int(dia):02}"
        return None

    # --- Ejecución de las transformaciones ---
    df['price_list'] = df['price'].apply(extract_and_sum_prices)
    df['price_avg'] = df['price_list'].apply(calculate_average)
    df.drop(columns=['price_list'], inplace=True)
    
    df['date'] = df['description'].apply(extraer_fecha)

    return df
from urllib.parse import quote

def process_hrefs(driver, df):
    prices = []
    lugares = []
    descriptions = []
    errors = []  # Lista para almacenar los errores

    for href in df['href']:
        if href:  # verifica si el href existe.
            full_href = "https://" + quote(href)  # agrega el esquema y codifica la url.
            try:
                if href.count('/') == 2:
                    details = extract_details_from_page(driver, full_href)
                elif href.count('/') == 1:
                    driver.get(full_href)
                    time.sleep(2)

                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    location_links = soup.find_all('a', class_='artist-shows-item')
                    details = {'price': None, 'lugar': None, 'description': None}  # Inicializa details

                    for link in location_links:
                        location_data = link.get('data-venue-locality')
                        if location_data and 'Córdoba' in location_data:
                            location_href = "https://ticketek.com.ar/" + quote(link.get('data-link'))
                            details = extract_details_from_location(driver, location_href)
                            break
                else:
                    details = {'price': None, 'lugar': None, 'description': None}

                prices.append(details.get('price'))
                lugares.append(details.get('lugar'))
                descriptions.append(details.get('description'))
                errors.append(details.get('error') if 'error' in details else None)  # Almacena el error, si existe

            except Exception as e:
                log(f"Error processing {full_href}: {e}")
                prices.append(None)
                lugares.append(None)
                descriptions.append(None)
                errors.append(str(e))  # Almacena el error

        else:
            prices.append(None)
            lugares.append(None)
            descriptions.append(None)
            errors.append(None)  # No hay error si no hay href

    df['price'] = prices
    df['lugar'] = lugares
    df['description'] = descriptions
    df['error'] = errors  # Agrega la columna de errores
    return df

def reordenar_y_agregar_columnas(df):
    """
    Reordena las columnas del DataFrame y agrega nuevas columnas según las especificaciones.

    Args:
        df (pandas.DataFrame): El DataFrame original con las columnas:
                              ['title', 'href', 'price', 'lugar', 'description', 'price_avg', 'date'].

    Returns:
        pandas.DataFrame: El DataFrame modificado con las columnas reordenadas y las nuevas columnas agregadas:
                          ['title', 'lugar', 'date', 'finaliza', 'tipo de evento', 'detalle', 'alcance',
                           'price_avg', 'fuente', 'href', 'price', 'description'].
                          Las columnas 'finaliza', 'tipo de evento', 'detalle' y 'alcance' estarán vacías (None).
                          La columna 'fuente' contendrá el valor 'Ticketek' en todas las filas.
    """

    # Crear un nuevo orden de columnas
    nuevo_orden_columnas = ['title', 'lugar', 'date', 'finaliza', 'tipo de evento', 'detalle', 'alcance',
                           'price_avg', 'fuente', 'href', 'price', 'description']

    # Crear las nuevas columnas vacías
    df['finaliza'] = None
    df['tipo de evento'] = 'Espectáculo'
    df['detalle'] = None
    df['alcance'] = None

    # Crear la columna 'fuente' con el valor 'Ticketek'
    df['fuente'] = 'Ticketek'

    # Reordenar las columnas
    df = df[nuevo_orden_columnas]

    return df
def limpiar_lugar(nombre):
    # Convertimos a string por seguridad y verificamos
    nombre_str = str(nombre)
    if 'Quality Espacio' in nombre_str:
        return 'Quality Espacio'
    elif 'Quality Arena' in nombre_str:
        return 'Quality Arena'
    elif 'Quality Teatro' in nombre_str:
        return 'Quality Teatro'
    elif 'Teatro Comedia' in nombre_str:
        return 'Teatro Comedia'
    else:
        return nombre



import pandas as pd
import time
import re
import gspread
from datetime import datetime
from urllib.parse import quote
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, WebDriverException

# --- CONFIGURACIÓN INICIAL ---
def iniciar_driver():
    chrome_options = Options()
    # Ocultamos los logs de errores de Google que mencionaste antes
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    chrome_options.add_argument('--log-level=3')
    
    # --- Cambio solicitado: Modo Headless ---
    chrome_options.add_argument("--headless=new") 
    
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    chrome_options.add_argument(f"user-agent={user_agent}")
    
    # He añadido el retorno del driver para que la función sea operativa
    driver = webdriver.Chrome(options=chrome_options)
    return driver
    
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)

def subir_a_google_sheets(df, nombre_tabla, nombre_hoja="sheet1", retries=3):
    import numpy as np
    import time
    import pandas as pd
    import os
    import json
    import gspread
    from google.oauth2 import service_account
    from datetime import datetime

    secreto_json = os.environ.get('GCP_SERVICE_ACCOUNT_JSON')
    if secreto_json is None:
        print("🔴 DIAGNÓSTICO: La variable os.environ no encuentra 'GCP_SERVICE_ACCOUNT_JSON'. Revisa el YAML.")
        return False
    
    intentos = 0
    while intentos < retries:
        try:
            # --- CONEXIÓN ---
            info_claves = json.loads(secreto_json)
            creds = service_account.Credentials.from_service_account_info(
                info_claves, 
                scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            )
            client = gspread.authorize(creds)
            sheet = client.open(nombre_tabla).worksheet(nombre_hoja)
            
            # Obtener datos existentes
            existing_data = sheet.get_all_values()
            
            # --- 1. PREPARACIÓN DE DATOS ENTRANTES ---
            df_entrada = df.copy()
            conteo_reales = 0
            
            # Identificadores de lógica
            tablas_acumulativas = ['Rechazados', 'Duplicados']
            es_acumulativa = nombre_tabla in tablas_acumulativas
            es_ferias_auto = nombre_tabla == 'Ferias y Congresos (Auto)'

            # --- 2. LÓGICA DE DETECCIÓN DE FILAS NUEVAS ---
            if len(existing_data) > 1:
                existing_df = pd.DataFrame(existing_data[1:], columns=existing_data[0])
                
                if es_acumulativa:
                    # Caso A: Pase libre total (No filtramos nada contra lo existente)
                    df_nuevas_reales = df_entrada.copy()
                    print(f"ℹ️ Modo Acumulativo: Agregando todas las filas a '{nombre_tabla}'.")
                
                elif es_ferias_auto:
                    # Caso B: Filtro especial por columna 'Eventos'
                    col_busqueda = 'Eventos'
                    if col_busqueda in existing_df.columns and col_busqueda in df_entrada.columns:
                        mask_nuevos = ~df_entrada[col_busqueda].astype(str).isin(existing_df[col_busqueda].astype(str))
                        df_nuevas_reales = df_entrada[mask_nuevos].copy()
                    else:
                        df_nuevas_reales = df_entrada.copy()
                
                else:
                    # Caso C: Lógica estándar por ID (Origen, href, etc.)
                    columnas_id = ['Origen', 'href', 'Link', 'URL']
                    id_col = next((c for c in columnas_id if c in df_entrada.columns), None)
                    
                    if id_col and id_col in existing_df.columns:
                        mask_nuevos = ~df_entrada[id_col].astype(str).isin(existing_df[id_col].astype(str))
                        df_nuevas_reales = df_entrada[mask_nuevos].copy()
                    else:
                        df_nuevas_reales = df_entrada.copy()
                
                combined_df = pd.concat([existing_df, df_nuevas_reales], ignore_index=True)
                conteo_reales = len(df_nuevas_reales)
            else:
                # Caso Hoja Vacía
                combined_df = df_entrada
                df_nuevas_reales = df_entrada.copy()
                conteo_reales = len(df_entrada)

            # --- 3. LIMPIEZA Y ELIMINACIÓN DE DUPLICADOS INTERNOS ---
            if not combined_df.empty:
                # Si no es acumulativa, nos aseguramos de que no haya duplicados en el set final
                if not es_acumulativa:
                    if es_ferias_auto:
                        combined_df = combined_df.drop_duplicates(subset=['Eventos'], keep='first')
                    else:
                        columnas_id = ['Origen', 'href', 'Link', 'URL']
                        id_col_final = next((c for c in columnas_id if c in combined_df.columns), None)
                        if id_col_final:
                            combined_df = combined_df.drop_duplicates(subset=[id_col_final], keep='first')

                # --- 4. ORDENAMIENTO (Lo más nuevo arriba) ---
                col_fecha_carga = next((c for c in ['fecha de carga', 'Fecha Scrp'] if c in combined_df.columns), None)
                if col_fecha_carga:
                    combined_df[col_fecha_carga] = pd.to_datetime(combined_df[col_fecha_carga], errors='coerce')
                    combined_df = combined_df.sort_values(by=col_fecha_carga, ascending=False)

                # --- 5. FORMATEO ANTI-ERROR (JSON Serializing) ---
                def serializar_datos(val):
                    if pd.isna(val) or val is pd.NaT: return ""
                    if isinstance(val, (datetime, pd.Timestamp)):
                        return val.strftime('%Y-%m-%d %H:%M:%S')
                    return str(val) if isinstance(val, (dict, list)) else val

                combined_df = combined_df.replace([np.inf, -np.inf], np.nan).fillna("")
                data_final = combined_df.map(serializar_datos)
                
                # --- 6. SUBIDA FINAL ---
                sheet.clear()
                valores_a_subir = [data_final.columns.values.tolist()] + data_final.values.tolist()
                sheet.update(valores_a_subir, value_input_option='USER_ENTERED')
                
                log(f"✅ Hoja '{nombre_tabla}' actualizada.")
                log(f"📊 Se agregaron {conteo_reales} filas nuevas.")
                return True 
            else:
                print(f"⚠️ DataFrame vacío para {nombre_tabla}")
                return False
        
        except Exception as e:
            intentos += 1
            print(f"⚠️ Error al subir a Sheets (Intento {intentos}/{retries}): {e}")
            if intentos < retries:
                time.sleep(5)
            
    return False
    
def ejecutar_scraper_ticketek():
    """
    Ejecuta el scraper y devuelve un reporte del estado.
    """
    driver = None
    reporte = {
        "nombre": "Ticketek",
        "estado": "Pendiente",
        "filas_procesadas": 0,
        "error": None,
        "inicio": datetime.now().strftime('%H:%M:%S')
    }
    
    # Este DataFrame vive en el ámbito de ejecutar_scraper_ticketek
    df_rechazados = pd.DataFrame(columns=['Nombre', 'Locación', 'Fecha', 'Motivo', 'Linea', 'Fuente'])

    # Esta función DEBE estar aquí adentro (un nivel de tabulación más)
    def registrar_rechazo(nombre, loc, fecha, motivo, linea, fuente, href, col_href="Link"):
        nonlocal df_rechazados # Ahora sí puede encontrar la variable de arriba
        nuevo = pd.DataFrame([{
            'Nombre': nombre, 
            'Locación': loc, 
            'Fecha': fecha,
            'Motivo': motivo, 
            'Linea': str(linea), 
            'Fuente': fuente,
            col_href: href
        }])
        df_rechazados = pd.concat([df_rechazados, nuevo], ignore_index=True)
    
    try:
        driver = iniciar_driver()

        # 1. Cargar página y expandir
        url = "https://www.ticketek.com.ar/buscar/?f%5B0%5D=field_artist_node_eb%253Afield_show_venue%253Afield_city:C%C3%B3rdoba"
        driver.get(url)
        time.sleep(10)
        
        # Usamos tus funciones
        click_load_more_until_disappears(driver)
        
        # 2. Extraer lista base
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        df_artists = extract_artist_data(soup)
        
        if df_artists.empty:
            log("No se encontraron artistas. Finalizando tarea Ticketek.")
            return

        # 3. Procesar detalles de cada link
        df_artists2 = process_hrefs(driver, df_artists)

        #3.1 Auditoría
        df_con_errores = df_artists2[df_artists2['error'].notna()]
        
        for _, row in df_con_errores.iterrows():
            motivo_error = f"Error de carga/navegación: {row['error']}"
            registrar_rechazo(
                nombre=row['title'], 
                loc="N/A", 
                fecha="N/A", 
                motivo=motivo_error, 
                linea="570",
                fuente='Ticketek',
                href=row['href']
            )
        
        # 4. Limpieza y Reordenamiento
        df_artists2_cleaned = clean_data(df_artists2.copy())
        #4.1 Auditoría
        mask_sin_fecha = (df_artists2_cleaned['date'].isna()) & (df_artists2_cleaned['error'].isna())
        df_fallos_fecha = df_artists2_cleaned[mask_sin_fecha]
        
        for _, row in df_fallos_fecha.iterrows():
            # Guardamos la descripción completa para auditoría técnica
            descripcion_completa = row['description'] if row['description'] else "SIN DESCRIPCIÓN DISPONIBLE"
            
            registrar_rechazo(
                nombre=row['title'], 
                loc=row['lugar'] if row['lugar'] else "No detectado", 
                fecha="No encontrada", 
                motivo=f"FALLO DE EXTRACCIÓN de fecha. Texto analizado: {descripcion_completa}", 
                linea="586",
                fuente='Ticketek',
                href=row['href']
            )
        df_artists2_cleaned['lugar'] = df_artists2_cleaned['lugar'].apply(limpiar_lugar)
        
        # --- PASO 2: Registro de Auditoría para Lugares Inválidos (Línea 244) ---
        # Solo registramos los que llegaron aquí con fecha pero el lugar resultó None/Vacio
        sin_lugar = df_artists2_cleaned[df_artists2_cleaned['lugar'].isna()]
        for _, row in sin_lugar.iterrows():
            registrar_rechazo(
                nombre=row['title'], 
                loc="No detectado", 
                fecha=row['date'], 
                motivo="Se descarta por falta de lugar (lugar es None después de limpiar_lugar)", 
                linea="620",
                fuente='Ticketek',
                href=row['href']
            )

        # --- PASO 3: El descarte (Dropna) ---
        # Eliminamos filas que no tengan Fecha (vienen de la 239) o Lugar (de la 244)
        df_artists2_cleaned = df_artists2_cleaned.dropna(subset=['date', 'lugar'])
        
        df_final = reordenar_y_agregar_columnas(df_artists2_cleaned.copy())
        df_final['finaliza'] = df_final['date']
        df_final['fecha de carga'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        df_final['price_avg'] = df_final['price_avg'].astype(str).str.replace("'", "", regex=False).astype(float)

        df_final, metricas_tkt = aplicar_clasificador(
            df=df_final,
            col_nombre='title',
            col_lugar='lugar',
            col_tipo_evento='tipo de evento',
            col_confianza='confianza_clasificacion'
            )
        log(f"🤖 Ticketek — Predicciones: {metricas_tkt['predicciones']} | Confianza promedio: {metricas_tkt['confianza_promedio']}")

        subir_a_google_sheets(df_final,'Ticketek historico (Auto)','Hoja 1')
        reporte["estado"] = "Exitoso"
        reporte["filas_procesadas"] = len(df_final)
        print(f"⚠️ Se registraron {len(df_con_errores)} fallos de carga en la auditoría.")
        if not df_rechazados.empty:
            subir_a_google_sheets(df_rechazados, 'Rechazados', 'Eventos')
            print("Rechazados Ticketek subidos exitosamente")
    except Exception as e:
        reporte["estado"] = "Fallido"
        reporte["error"] = str(e)
        log(f"❌ Error en Ticketek: {e}")
    finally:
        if driver:
            driver.quit()
        reporte["fin"] = datetime.now().strftime('%H:%M:%S')
        return reporte
log('TICKETEK')
ejecutar_scraper_ticketek()

###########################################################################
################### EDEN ##################################################
###########################################################################
import pandas as pd
import time
import re
import numpy as np
from datetime import datetime
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- FUNCIONES DE APOYO PARA EDEN ---
df_final=[]
def extraer_promedio_precios_formato2(soup):
    precios_totales = []
    sector_divs = soup.find_all('div', class_='item sectorOption animated fadeInUp')
    for sector_div in sector_divs:
        precio_span = sector_div.find('span')
        if precio_span:
            precio_texto = precio_span.text.replace('Desde $', '').replace('.', '').replace(',', '.').strip()
            match = re.match(r'(\d+\.?\d*)\s*\+\s*.*?\$?\s*(\d+\.?\d*)', precio_texto)
            if match:
                precio_total = float(match.group(1)) + float(match.group(2))
                precios_totales.append(precio_total)
    return sum(precios_totales) / len(precios_totales) if precios_totales else None

def extraer_promedio_precios(soup):
    precios_f1 = []
    for sector in soup.find_all('div', class_='festival-shows sectors'):
        for price_div in sector.find_all('div', class_='additional-price'):
            try:
                precios_f1.append(float(price_div.text.replace('$', '').replace('.', '').replace(',', '.').strip()))
            except: pass
    
    prom_f1 = sum(precios_f1) / len(precios_f1) if precios_f1 else None
    prom_f2 = extraer_promedio_precios_formato2(soup)
    return prom_f2 if prom_f2 is not None else prom_f1

def normalizar_fecha_complejo(fecha_str):
    """Normaliza una cadena de fecha y hora con múltiples formatos."""
    fechas_normalizadas = []
    año_actual = pd.Timestamp.now().year
    dias_semana_esp = {'Lunes': 'Monday', 'Martes': 'Tuesday', 'Miércoles': 'Wednesday',
                       'Miercoles': 'Wednesday', 'Jueves': 'Thursday', 'Viernes': 'Friday',
                       'Sábado': 'Saturday', 'Sabado': 'Saturday', 'Domingo': 'Sunday'}
    meses_esp = {'Enero': 'January', 'Febrero': 'February', 'Marzo': 'March',
                 'Abril': 'April', 'Mayo': 'May', 'Junio': 'June', 'Julio': 'July',
                 'Agosto': 'August', 'Septiembre': 'September', 'Setiembre': 'September',
                 'Octubre': 'October', 'Noviembre': 'November', 'Diciembre': 'December'}

    # Reemplazar nombres de días y meses al inglés
    for esp, eng in dias_semana_esp.items():
        fecha_str = re.sub(r'\b' + esp + r'\b', eng, fecha_str)
    for esp, eng in meses_esp.items():
        fecha_str = re.sub(r'\b' + esp + r'\b', eng, fecha_str)

    # Caso de rangos de fechas (ej: Viernes 23 y Sábado 24 de Mayo 16hs)
    match_rango = re.search(r'(\w+) (\d+) y (\w+) (\d+) de (\w+) (\d+)(?:hs|)', fecha_str)
    if match_rango:
        dia1_sem, dia1_num, dia2_sem, dia2_num, mes, hora = match_rango.groups()
        try:
            fecha1_dt = pd.to_datetime(f'{dia1_num} {mes} {año_actual} {hora[:-2]}:00', format='%d %B %Y %H:%M')
            fecha2_dt = pd.to_datetime(f'{dia2_num} {mes} {año_actual} {hora[:-2]}:00', format='%d %B %Y %H:%M')
            fechas_normalizadas.append(fecha1_dt)
            fechas_normalizadas.append(fecha2_dt)
            return fechas_normalizadas
        except ValueError:
            pass

    # Caso de rangos de fechas con tres días (ej: Viernes 11, Sábado 12 y Domingo 13 de Julio)
    match_rango_tres = re.search(r'(\w+) (\d+), (\w+) (\d+) y (\w+) (\d+) de (\w+)', fecha_str)
    if match_rango_tres:
        dia1_sem, dia1_num, dia2_sem, dia2_num, dia3_sem, dia3_num, mes = match_rango_tres.groups()
        try:
            fecha1_dt = pd.to_datetime(f'{dia1_num} {mes} {año_actual}', format='%d %B %Y')
            fecha2_dt = pd.to_datetime(f'{dia2_num} {mes} {año_actual}', format='%d %B %Y')
            fecha3_dt = pd.to_datetime(f'{dia3_num} {mes} {año_actual}', format='%d %B %Y')
            fechas_normalizadas.append(fecha1_dt)
            fechas_normalizadas.append(fecha2_dt)
            fechas_normalizadas.append(fecha3_dt)
            return fechas_normalizadas
        except ValueError:
            pass

    # Caso de fecha y hora simple (ej: Miércoles 24 de Septiembre 21hs)
    match_simple = re.search(r'(?:\w+ )?(\d+) de (\w+) (\d+(?:\.\d+)?)hs', fecha_str)
    if match_simple:
        dia, mes, hora_str = match_simple.groups()
        hora_parts = hora_str.split('.')
        hora = int(hora_parts[0])
        minuto = int(hora_parts[1]) * 60 // 100 if len(hora_parts) > 1 else 0
        try:
            fecha_dt = pd.to_datetime(f'{dia} {mes} {año_actual} {hora}:{minuto}:00', format='%d %B %Y %H:%M:%S')
            fechas_normalizadas.append(fecha_dt)
            return fechas_normalizadas
        except ValueError:
            pass
    elif match_simple := re.search(r'(?:\w+ )?(\d+) de (\w+) (\d+)hs', fecha_str):
        dia, mes, hora = match_simple.groups()
        try:
            fecha_dt = pd.to_datetime(f'{dia} {mes} {año_actual} {hora}:00:00', format='%d %B %Y %H:%M:%S')
            fechas_normalizadas.append(fecha_dt)
            return fechas_normalizadas
        except ValueError:
            pass
    elif match_simple := re.search(r'(?:\w+ )?(\d+) de (\w+) (\d+:\d+)(?:hs|)', fecha_str):
        dia, mes, hora = match_simple.groups()
        try:
            fecha_dt = pd.to_datetime(f'{dia} {mes} {año_actual} {hora}', format='%d %B %Y %H:%M')
            fechas_normalizadas.append(fecha_dt)
            return fechas_normalizadas
        except ValueError:
            pass
    elif match_simple := re.search(r'(?:\w+ )?(\d+) de (\w+)', fecha_str): # Para casos sin hora
        dia, mes = match_simple.groups()
        try:
            fecha_dt = pd.to_datetime(f'{dia} {mes} {año_actual}', format='%d %B %Y')
            fechas_normalizadas.append(fecha_dt)
            return fechas_normalizadas
        except ValueError:
            pass
    return []

def procesar_dataframe_complejo(df, columna_fecha='Fecha'):
    """Procesa el DataFrame para normalizar la columna de fecha con la función compleja."""
    filas_nuevas = []
    for index, row in df.iterrows():
        fecha_str = str(row[columna_fecha]).strip() # Asegurarse de que sea string y eliminar espacios alrededor
        fechas_normalizadas = normalizar_fecha_complejo(fecha_str)
        for fecha in fechas_normalizadas:
            nueva_fila = row.copy()
            nueva_fila[columna_fecha] = fecha
            filas_nuevas.append(nueva_fila)

    df_normalizado = pd.DataFrame(filas_nuevas)
    return df_normalizado

def ejecutar_scraper_eden():
    from google.oauth2 import service_account
    import json
    import os
    driver = None
    reporte = {
        "nombre": "Eden Entradas",
        "estado": "Pendiente",
        "filas_procesadas": 0,
        "error": None,
        "inicio": datetime.now().strftime('%H:%M:%S')
    }
    
    # --- ÁREA DE AUDITORÍA ---
    df_rechazados = pd.DataFrame(columns=['Nombre', 'Locación', 'Fecha', 'Motivo', 'Linea', 'Fuente', 'Link','Fecha Scrp'])

    def registrar_rechazo(nombre, loc, fecha, motivo, linea, fuente, href, col_href="Link"):
        nonlocal df_rechazados
        nuevo = pd.DataFrame([{
            'Nombre': nombre, 'Locación': loc, 'Fecha': fecha,
            'Motivo': motivo, 'Linea': str(linea), 'Fuente': fuente,
            col_href: href, 'Fecha Scrp': datetime.now().strftime('%Y-%m-%d')
        }])
        df_rechazados = pd.concat([df_rechazados, nuevo], ignore_index=True)

    try:
        driver = iniciar_driver()
        BASE_URL = "https://www.edenentradas.ar"
        driver.get(BASE_URL + "/")
        time.sleep(5)

        # 2. Scrapeo de lista principal
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        eventos_html = soup.find_all('div', class_='grid_element')
        
        if not eventos_html:
            registrar_rechazo("Página Principal", "N/A", "N/A", "No se detectaron elementos grid_element", "116", "Eden", BASE_URL)
            return reporte

        data = []
        for evento in eventos_html:
            data.append({
                'Nombre': evento.find('div', class_='item_title').text.strip() if evento.find('div', class_='item_title') else None,
                'Locación': evento.find('strong').text.strip() if evento.find('strong') else None,
                'Fecha': evento.find('span').text.strip() if evento.find('span') else None,
                'href': evento.find('a')['href'] if evento.find('a') else None
            })
        
        # --- AUDITORÍA POST-LISTA (Datos básicos incompletos) ---
        data_df = pd.DataFrame(data)
        sin_datos_basicos = data_df[data_df['Locación'].isna() | data_df['Nombre'].isna()]
        for _, row in sin_datos_basicos.iterrows():
            registrar_rechazo(row['Nombre'], "Incompleto", row['Fecha'], "Falta Locación o Nombre en el Grid", "851", "Eden", row['href'])

        data_df = data_df.dropna(subset=['Locación', 'href']).drop_duplicates(subset=['href']).reset_index(drop=True)
        log(f"📊 Eden: {len(data_df)} eventos únicos detectados tras eliminar duplicados por link")

        # 3. Recorrido de detalles
        for index, row in data_df.iterrows():
            full_href = f"{BASE_URL}{row['href'].replace('..', '')}"
            try:
                driver.get(full_href)
                time.sleep(3)
                soup_det = BeautifulSoup(driver.page_source, 'html.parser')
                
                cols = soup_det.find_all('div', class_='col-xs-7')
                ciudad_texto = ', '.join([e.text.strip() for e in cols]) if cols else ""
                data_df.loc[index, 'filtro_ciudad'] = ciudad_texto

                # --- AUDITORÍA: Filtro de Ciudad (Córdoba) ---
                if not any(x in ciudad_texto for x in ['Córdoba', 'Cordoba']):
                    registrar_rechazo(row['Nombre'], row['Locación'], row['Fecha'], f"Evento fuera de Córdoba: {ciudad_texto}", "862", "Eden", full_href)
                    continue

                # Precios...
                try:
                    btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "div.picker-full button.next, #buyButton")))
                    driver.execute_script("arguments[0].click();", btn)
                    time.sleep(4)
                    data_df.loc[index, 'precio_promedio'] = extraer_promedio_precios(BeautifulSoup(driver.page_source, 'html.parser'))
                except:
                    data_df.loc[index, 'precio_promedio'] = None

            except Exception as e:
                registrar_rechazo(row['Nombre'], row['Locación'], row['Fecha'], f"Error navegando detalle: {str(e)}", "871", "Eden", full_href)
                continue

        # 4. Filtrado y Normalización
        data_df = data_df[data_df['filtro_ciudad'].str.contains('Córdoba|Cordoba', case=False, na=False)]
        
        # --- AUDITORÍA POST-NORMALIZACIÓN DE FECHA ---
        df_norm = procesar_dataframe_complejo(data_df)
        
        # Verificamos si procesar_dataframe_complejo devolvió filas para este evento
        eventos_antes = set(data_df['Nombre'])
        eventos_despues = set(df_norm['Nombre'])
        fallos_fecha = eventos_antes - eventos_despues
        
        for nombre in fallos_fecha:
            # Buscamos el row original para el href
            orig = data_df[data_df['Nombre'] == nombre].iloc[0]
            registrar_rechazo(nombre, orig['Locación'], orig['Fecha'], f"Regex falló: No se pudo normalizar la fecha: {orig['Fecha']}", "894", "Eden", orig['href'])

        # 5. Formateo Final
        if not df_norm.empty:
            df_final = pd.DataFrame({
                'Eventos': df_norm['Nombre'],
                'Lugar': df_norm['Locación'],
                'Comienza': df_norm['Fecha'],
                'Finaliza': df_norm['Fecha'],
                'Tipo de evento': 'Espectáculo',
                'Detalle': None,
                'Alcance': None,
                'Costo de entrada': df_norm['precio_promedio'],
                'Fuente': 'Eden Entradas',
                'Origen': df_norm['href'].str.replace('..', 'https://www.edenentradas.ar', regex=False),
                # USAMOS SOLO FECHA (sin hora/min/seg) para que coincida con lo ya subido hoy
                'fecha de carga': datetime.today().strftime('%Y-%m-%d %H:%M:%S') 
            }).dropna(subset=['Comienza'])
        df_final = df_final.drop_duplicates(subset=['Origen'])
        
        df_final, metricas_eden = aplicar_clasificador(
            df=df_final,
            col_nombre='Eventos',
            col_lugar='Lugar',
            col_tipo_evento='Tipo de evento',
            col_confianza='confianza_clasificacion'
            )
        log(f"🤖 Eden — Predicciones: {metricas_eden['predicciones']} | Confianza promedio: {metricas_eden['confianza_promedio']}")
        
        subir_a_google_sheets(df_final, 'Eden historico (Auto)', 'Hoja 1')

        # 6. Subida de Rechazados
        if not df_rechazados.empty:
            subir_a_google_sheets(df_rechazados, 'Rechazados', 'Eventos')

        reporte["estado"] = "Exitoso"
        #reporte["filas_procesadas"] = len(df_final) if not df_norm.empty else 0

    except Exception as e:
        reporte["estado"] = "Fallido"
        reporte["error"] = str(e)
    finally:
        if driver: driver.quit()
        return reporte
log('')
log('EDÉN')
#ejecutar_scraper_eden()

##################################################################################################################
####################################### EVENTBRITE ###############################################################
##################################################################################################################
import pandas as pd
import time
import re
import requests
import numpy as np
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- FUNCIONES DE APOYO ---

def limpiar_fecha_texto(fecha):
    """Limpia el texto de Eventbrite antes de procesarlo."""
    if not fecha or fecha == 'N/A': return "Formato desconocido"
    fecha = re.sub(r"\+.*", "", fecha).strip()
    return fecha

def convertir_fechas(fecha):
    if not fecha or fecha == "N/A": return "Formato desconocido"
    fecha_low = fecha.lower()
    ahora = datetime.now()
    
    try:
        # 1. HOY
        if "hoy" in fecha_low:
            match = re.search(r'(\d{1,2}:\d{2})', fecha_low)
            if match:
                hora, minuto = map(int, match.group(1).split(":"))
                return ahora.replace(hour=hora, minute=minuto, second=0, microsecond=0)
        
        # 2. MAÑANA
        elif "mañana" in fecha_low:
            match = re.search(r'(\d{1,2}:\d{2})', fecha_low)
            if match:
                hora, minuto = map(int, match.group(1).split(":"))
                tomorrow = ahora + timedelta(days=1)
                return tomorrow.replace(hour=hora, minute=minuto, second=0, microsecond=0)
        
        # 3. DÍA DE LA SEMANA
        dias = {"lunes":0, "martes":1, "miércoles":2, "jueves":3, "viernes":4, "sábado":5, "domingo":6}
        for nombre, cod in dias.items():
            if nombre in fecha_low:
                match = re.search(r'(\d{1,2}:\d{2})', fecha_low)
                if match:
                    hora, minuto = map(int, match.group(1).split(":"))
                    dias_adelante = (cod - ahora.weekday()) % 7
                    if dias_adelante == 0: dias_adelante = 7
                    target = ahora + timedelta(days=dias_adelante)
                    return target.replace(hour=hora, minute=minuto, second=0, microsecond=0)

        # 4. FECHA ESPECÍFICA (ej: "31 oct, 19:00")
        meses = {
            "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
            "jul": 7, "ago": 8, "sep": 9, "oct": 10, "nov": 11, "dic": 12
        }
        match_esp = re.search(r'(\d{1,2})\s([a-z]{3}).*?(\d{1,2}:\d{2})', fecha_low)
        if match_esp:
            dia = int(match_esp.group(1))
            mes_txt = match_esp.group(2)
            hora_str = match_esp.group(3)
            if mes_txt in meses:
                mes = meses[mes_txt]
                año = ahora.year
                if mes < ahora.month: año += 1
                h, m = map(int, hora_str.split(":"))
                return datetime(año, mes, dia, h, m)

        return fecha 
    except Exception as e:
        return "Error formato"

# --- FUNCIÓN PRINCIPAL ---

def ejecutar_scraper_eventbrite():
    driver = None
    reporte = {
        "nombre": "Eventbrite",
        "estado": "Pendiente",
        "filas_procesadas": 0,
        "error": None,
        "inicio": datetime.now().strftime('%H:%M:%S')
    }
    
    # --- CONFIGURACIÓN AUDITORÍA ---
    df_rechazados = pd.DataFrame(columns=['Nombre', 'Locación', 'Fecha', 'Motivo', 'Linea', 'Fuente', 'Link'])

    def registrar_rechazo(nombre, loc, fecha, motivo, linea, fuente, href):
        nonlocal df_rechazados
        nuevo = pd.DataFrame([{
            'Nombre': nombre, 'Locación': loc, 'Fecha': fecha,
            'Motivo': motivo, 'Linea': str(linea), 'Fuente': fuente,
            'Link': href
        }])
        df_rechazados = pd.concat([df_rechazados, nuevo], ignore_index=True)

    date_keywords = ['lun', 'mar', 'mié', 'jue', 'vie', 'sáb', 'dom', 'mañana', 'hoy', 'ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic']
    
    try:
        driver = iniciar_driver()
        base_url = 'https://www.eventbrite.com.ar/d/argentina--c%C3%B3rdoba/all-events/'
        event_data = []
        seen_links = set()

        for page in range(1, 6):
            print(f"📄 Eventbrite: Procesando página {page}...")
            driver.get(f'{base_url}?page={page}')
            
            try:
                # Esperamos a que aparezca el contenedor de las cards, no solo el h3
                WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 'section.event-card-details'))
                )
                # Scroll lento para disparar el lazy loading de Eventbrite
                for _ in range(3):
                    driver.execute_script("window.scrollBy(0, 400);")
                    time.sleep(0.5)
            except Exception as e: 
                print(f"⚠️ No se detectaron cards en página {page}. Posible cambio de diseño o fin.")
                break

            events = driver.find_elements(By.CSS_SELECTOR, 'article, section.discover-horizontal-event-card, div[class*="Stack_root"]')
            
            for event in events:
                try:
                    # 1. Extracción Básica
                    try:
                        name_el = event.find_element(By.TAG_NAME, 'h3')
                        name = name_el.text.strip()
                        link = event.find_element(By.TAG_NAME, 'a').get_attribute('href')
                    except:
                        continue

                    if not name or link in seen_links: 
                        continue
                    
                    # 2. Extracción de Fecha y Locación vía párrafos
                    paragraphs = event.find_elements(By.TAG_NAME, 'p')
                    date_info, location = 'N/A', 'N/A'

                    if paragraphs:
                        idx_fecha = -1
                        for i, p in enumerate(paragraphs):
                            txt = p.text.strip().lower()
                            if any(kw in txt for kw in date_keywords):
                                idx_fecha = i
                                break
                        
                        if idx_fecha != -1:
                            date_info = paragraphs[idx_fecha].text.strip()
                            if len(paragraphs) > idx_fecha + 1:
                                location = paragraphs[idx_fecha + 1].text.strip()
                        else:
                            location = paragraphs[0].text.strip()

                    # 3. Auditoría inicial: Datos incompletos
                    if date_info == 'N/A' or location == 'N/A':
                        registrar_rechazo(name, location, date_info, "Card con datos insuficientes (Fecha/Locación N/A)", "125", "Eventbrite", link)
                        continue

                    event_data.append({
                        'Nombre': name, 'Fecha': date_info, 'Locación': location,
                        'Precio': "Consultar", 'Origen': link
                    })
                    seen_links.add(link)
                except: 
                    continue

        # --- PROCESAMIENTO ---
        if not event_data:
            reporte["estado"] = "Primera página vacía. Reintentando."
            raise ValueError("No se encontraron datos en Eventbrite")

        df_crudo = pd.DataFrame(event_data)
        
        # 4. Auditoría: Filtrado de Locación (Hoteles MICE)
        keywords_locacion = ['quinto centenario', 'blas pascal', 'quorum', 'sheraton', 'holiday inn']
        mask_locacion = df_crudo['Locación'].str.lower().str.contains('|'.join(keywords_locacion), na=False)
        
        df_rechazados_loc = df_crudo[~mask_locacion]
        for _, row in df_rechazados_loc.iterrows():
            registrar_rechazo(row['Nombre'], row['Locación'], row['Fecha'], "Locación no coincide con Hoteles MICE", "150", "Eventbrite", row['Origen'])

        df_filtrado = df_crudo[mask_locacion].copy()

        # 5. Auditoría: Conversión de Fecha
        if not df_filtrado.empty:
            df_filtrado['Fecha Convertida'] = df_filtrado['Fecha'].apply(convertir_fechas)
            
            # Identificamos fallos (si devuelve string en lugar de datetime o "Error formato")
            mask_fecha_ok = df_filtrado['Fecha Convertida'].apply(lambda x: isinstance(x, datetime))
            
            df_rechazados_fecha = df_filtrado[~mask_fecha_ok]
            for _, row in df_rechazados_fecha.iterrows():
                registrar_rechazo(row['Nombre'], row['Locación'], row['Fecha'], f"Fallo en conversión de fecha: {row['Fecha']}", "165", "Eventbrite", row['Origen'])
            
            df_final_data = df_filtrado[mask_fecha_ok].copy()

            df_final, metricas_eb = aplicar_clasificador(
                df=df_final,
                col_nombre='Nombre',
                col_lugar='Locación',
                col_tipo_evento='tipo de evento',
                col_confianza='confianza_clasificacion'
            )
            log(f"🤖 Eventbrite — Predicciones: {metricas_eb['predicciones']} | Confianza promedio: {metricas_eb['confianza_promedio']}")
            
            if not df_final_data.empty:
                df_final = pd.DataFrame({
                    'Nombre': df_final_data['Nombre'],
                    'Locación': df_final_data['Locación'],
                    'Fecha Convertida': df_final_data['Fecha Convertida'].astype(str),
                    'termina': "", 'tipo de evento': 'M.I.C.E', 'detalle': "", 'alcance': "",
                    'Precio': 0.0, 'fuente': 'eventbrite', 'Origen': df_final_data['Origen'],
                    'Fecha Scrp': datetime.today().strftime('%Y-%m-%d')
                })
                
                subir_a_google_sheets(df_final, 'base_h_scrp_eventbrite', 'Hoja 1')
                reporte["filas_procesadas"] = len(df_final)
                reporte["estado"] = "Exitoso"
            else:
                reporte["estado"] = "Exitoso (Sin eventos válidos tras filtros)"

        # --- SUBIDA FINAL DE AUDITORÍA ---
        if not df_rechazados.empty:
            # Subimos a la pestaña 'Eventbrite' del documento 'Rechazados'
            subir_a_google_sheets(df_rechazados, 'Rechazados', 'Eventos')
            print(f"✅ Auditoría Eventbrite: {len(df_rechazados)} registros subidos.")

    except Exception as e:
        print(f"❌ Error Crítico Eventbrite: {e}")
        reporte["estado"] = "Fallido"
        reporte["error"] = str(e)
        if driver:
            driver.quit()
        raise e
    finally:
        if driver:
            driver.quit()
        reporte["fin"] = datetime.now().strftime('%H:%M:%S')
    return reporte

intentos_maximos = 0
resultado_final = None
log('')
log('EVENTBRITE')
for i in range(1, intentos_maximos + 1):
    try:
        print(f"🚀 Iniciando Eventbrite - Intento {i} de {intentos_maximos}...")
        resultado_final = ejecutar_scraper_eventbrite()
        
        # Si llega aquí, es que funcionó (no hubo raise)
        print(f"✅ Intento {i} completado con éxito.")
        break 

    except Exception as e:
        print(f"❌ Error en intento {i}: {e}")
        
        # Guardamos un reporte provisional por si este es el último fallo
        resultado_final = {
            "nombre": "Eventbrite",
            "estado": "Fallido definitivamente",
            "error": str(e),
            "filas_procesadas": 0,
            "inicio": datetime.now().strftime('%H:%M:%S') # O la hora que prefieras
        }

        if i < intentos_maximos:
            print(f"⚠️ Reintentando en 10 segundos...")
            time.sleep(10)
        else:
            log("🛑 Fallo en eventbrite (Intentos agotados)")

# Ahora, pase lo que pase, resultado_final contiene el diccionario
#print(f"Estado final registrado: {resultado_final['estado']}")
# Aquí puedes usar resultado_final para subirlo a otro lado o mostrarlo
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
def enviar_log_smtp(cuerpo_log, lista_destinatarios):
    """Envía el log acumulado a múltiples correos usando SMTP (reemplaza Gmail API)."""
    try:
        # Configuración desde variables de entorno para seguridad
        remitente = "furrutia@cordobaacelera.com.ar"  # El mail que generó la App Password
        password = os.environ.get('EMAIL_APP_PASSWORD')
        
        if not password:
            log("🔴 Error: No se encontró EMAIL_APP_PASSWORD en los secretos.")
            return

        # Iniciamos la conexión con el servidor SMTP de Gmail
        log("🔗 Conectando al servidor de correo...")
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()  # Cifrado de seguridad
        server.login(remitente, password)

        for destinatario in lista_destinatarios:
            # Creamos el contenedor del mensaje
            message = MIMEMultipart()
            message['To'] = destinatario
            message['From'] = f"Scraper Automático <{remitente}>"
            message['Subject'] = "📊 REPORTE SCRP AGENDA"
            
            # Agregamos el cuerpo del log
            message.attach(MIMEText(cuerpo_log, 'plain'))

            # Envío del correo
            server.send_message(message)
            log(f"📧 Mail enviado a {destinatario}")

        # Cerramos la conexión después de enviar a todos
        server.quit()
        log("✅ Proceso de envío finalizado.")

    except Exception as e:
        log(f"🔴 Error al enviar mail vía SMTP: {e}")



# Llamamos a la función con la lista de correos

log('')
log('Ferias y Congresos')
def ejecutar_scraper_ferias_y_congresos():
    """
    Scraper integral para Ferias y Congresos con auditoría de rechazos
    y gestión de fechas de rango.
    """
    driver = None
    reporte = {
        "nombre": "Ferias y Congresos",
        "estado": "Pendiente",
        "filas_procesadas": 0,
        "error": None,
        "inicio": datetime.now().strftime('%H:%M:%S')
    }
    
    df_rechazados = pd.DataFrame(columns=['Nombre', 'Locación', 'Fecha', 'Motivo', 'Linea', 'Fuente', 'Link'])

    def registrar_rechazo(nombre, loc, fecha, motivo, linea, fuente, href):
        nonlocal df_rechazados
        nuevo = pd.DataFrame([{
            'Nombre': nombre, 
            'Locación': loc, 
            'Fecha': fecha,
            'Motivo': motivo, 
            'Linea': str(linea), 
            'Fuente': fuente,
            'Link': href
        }])
        df_rechazados = pd.concat([df_rechazados, nuevo], ignore_index=True)

    def parsear_rango_fechas(texto_fecha):
        meses = {
            'Enero': 1, 'Febrero': 2, 'Marzo': 3, 'Abril': 4, 'Mayo': 5, 'Junio': 6,
            'Julio': 7, 'Agosto': 8, 'Septiembre': 9, 'Octubre': 10, 'Noviembre': 11, 'Diciembre': 12
        }
        ahora = datetime.now()
        
        try:
            # Captura formatos: "07 al 09 de Febrero" o "19 Enero al 06 de Abril"
            match = re.search(r'(\d+)\s*([a-zA-Z]+)?\s*al\s*(\d+)\s*de?\s*([a-zA-Z]+)', texto_fecha)
            if not match: return None, None
            
            d1, m1_str, d2, m2_str = match.groups()
            m2 = meses[m2_str.capitalize()]
            m1 = meses[m1_str.capitalize()] if m1_str else m2
            
            year = ahora.year
            # Creamos fechas tentativas para este año
            f_ini_temp = ahora.replace(year=year, month=m1, day=int(d1))
            f_fin_temp = ahora.replace(year=year, month=m2, day=int(d2))
            
            # Si el evento terminó antes de hoy, lo pasamos al año siguiente
            if f_fin_temp < ahora:
                year += 1
                f_ini_temp = f_ini_temp.replace(year=year)
                f_fin_temp = f_fin_temp.replace(year=year)
            
            return f_ini_temp.strftime('%Y-%m-%d'), f_fin_temp.strftime('%Y-%m-%d')
        except:
            return None, None

    try:
        driver = iniciar_driver() 
        url_fuente = "https://www.feriasycongresos.com/calendario-de-eventos?busqueda=C%C3%B3rdoba"
        driver.get(url_fuente)
        
        # Espera dinámica (Vue.js)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, "mod-evento")))
        time.sleep(5)

        bloques = driver.find_elements(By.CLASS_NAME, "mod-evento")
        raw_data = []

        for bloque in bloques:
            try:
                nombre = bloque.find_element(By.TAG_NAME, "h1").text
                fecha_raw = bloque.find_element(By.CSS_SELECTOR, ".txt2 .bold").text
                
                try:
                    recinto_raw = bloque.find_element(By.XPATH, ".//span[contains(text(), 'Recinto:')]").find_element(By.XPATH, "parent::*").text
                except:
                    recinto_raw = "No detectado"

                # 1. Filtro Córdoba Capital (Requisito Punto 1)
                lugares_validos = ["Capital, Córdoba", "Arguello, Córdoba"]
                if not any(lugar in recinto_raw for lugar in lugares_validos):
                    registrar_rechazo(
                        nombre=nombre, loc=recinto_raw, fecha=fecha_raw,
                        motivo="El evento no se encuentra en córdoba capital",
                        linea="1367", fuente="Ferias y Congresos", href=url_fuente
                    )
                    print(f'El evento {nombre} no se encuentra en córdoba capital')
                    continue

                # 2. Procesamiento de Rango de Fechas (Requisito Punto 3)
                f_ini, f_fin = parsear_rango_fechas(fecha_raw)
                
                if not f_ini:
                    registrar_rechazo(
                        nombre=nombre, loc=recinto_raw, fecha=fecha_raw,
                        motivo="FALLO DE EXTRACCIÓN de fecha (formato no reconocido)",
                        linea="1381", fuente="Ferias y Congresos", href=url_fuente
                    )
                    continue

                # 3. Construcción de fila válida
                raw_data.append({
                'Eventos': nombre,          # Cambiado a 'Eventos' para consistencia con otros scrapers
                'Lugar': recinto_raw.replace("Recinto:", "").strip(),
                'Comienza': f_ini,          # Usamos nombres estándar para evitar problemas de duplicados
                'Finaliza': f_fin,
                'Tipo de evento': 'M.I.C.E',
                'Detalle': '',
                'Alcance': '',
                'Costo de entrada': '',
                'Fuente': 'Ferias y Congresos',
                'Origen': url_fuente        # Usamos 'Origen' como ID único
                })
            except Exception:
                continue

        df_final = pd.DataFrame(raw_data)
        # --- 4. Formateo y Orden Final ---
        if raw_data:
            print('RAW DATA ENCONTRADA')
            df_final = pd.DataFrame(raw_data)
            print(f"Longitud df_final post raw_data:{len(df_final)}")
            
            # Aseguramos el orden exacto de las columnas antes de enviar
            columnas_ordenadas = [
                'Eventos', 'Lugar', 'Comienza', 'Finaliza', 
                'Tipo de evento', 'Detalle', 'Alcance', 
                'Costo de entrada', 'Fuente', 'Origen'
            ]
            
            # Reindexamos para asegurar el orden y agregamos la fecha de carga
            df_final = df_final[columnas_ordenadas]
            print(f"Longitud df_final post columnas ordenadas{len(df_final)}")
            df_final['fecha de carga'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

            df_final, metricas_fc = aplicar_clasificador(
                df=df_final,
                col_nombre='Eventos',
                col_lugar='Lugar',
                col_tipo_evento='Tipo de evento',
                col_confianza='confianza_clasificacion'
            )
            log(f"🤖 Ferias y Congresos — Predicciones: {metricas_fc['predicciones']} | Confianza promedio: {metricas_fc['confianza_promedio']}")
            
            # Subida a Sheets
            subir_a_google_sheets(df_final, 'Ferias y Congresos (Auto)', 'Hoja 1')
            
            reporte["estado"] = "Exitoso"
            reporte["filas_procesadas"] = len(df_final)

        # Subida de auditoría si hay rechazados
        if not df_rechazados.empty:
            subir_a_google_sheets(df_rechazados, 'Rechazados', 'Eventos')
            print(f"✅ Auditoría: {len(df_rechazados)} eventos rechazados subidos.")

    except Exception as e:
        reporte["estado"] = "Fallido"
        reporte["error"] = str(e)
    finally:
        if driver:
            driver.quit()
        reporte["fin"] = datetime.now().strftime('%H:%M:%S')
        return reporte

# Ejecución
print("Iniciando Ferias y Congresos...")
#ejecutar_scraper_ferias_y_congresos()


log('')
log('Secretaría de Turismo Municipal')
import time
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def ejecutar_scraper_turismo_cba():
    """
    Scraper para Agencia Córdoba Turismo adaptado al formato estándar.
    """
    driver = None
    reporte = {
        "nombre": "Agencia Turismo Cba",
        "estado": "Pendiente",
        "filas_procesadas": 0,
        "error": None,
        "inicio": datetime.now().strftime('%H:%M:%S')
    }
    
    # DataFrame para auditoría de descartes
    df_rechazados = pd.DataFrame(columns=['Nombre', 'Locación', 'Fecha', 'Motivo', 'Linea', 'Fuente', 'Link'])

    def registrar_rechazo(nombre, loc, fecha, motivo, linea, fuente, href):
        nonlocal df_rechazados
        nuevo = pd.DataFrame([{
            'Nombre': nombre, 'Locación': loc, 'Fecha': fecha,
            'Motivo': motivo, 'Linea': str(linea), 'Fuente': fuente, 'Link': href
        }])
        df_rechazados = pd.concat([df_rechazados, nuevo], ignore_index=True)

    def formatear_fecha(fecha_str):
        try:
            if not fecha_str or "N/A" in str(fecha_str): return None
            fecha_str = " ".join(fecha_str.split())
            dt = datetime.strptime(fecha_str, "%d/%m/%Y %H:%M")
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            return None

    try:
        driver = iniciar_driver() # Usa tu función global
        url_agenda = "https://turismo.cordoba.gob.ar/agenda/agenda-turistica"
        exclusiones = ["edenentradas", "ticketek", "quality"]
        
        driver.get(url_agenda)
        print(f"🚀 {reporte['nombre']}: Cargando y expandiendo agenda...")

        # 1. Expandir contenido "Cargar Más"
        while True:
            try:
                boton = WebDriverWait(driver, 8).until(
                    EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'Cargar Más')]"))
                )
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", boton)
                time.sleep(1)
                driver.execute_script("arguments[0].click();", boton)
                time.sleep(3)
            except:
                break # No hay más botón

        # 2. Parsear contenido
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        cards = soup.find_all('div', class_='card')
        eventos_lista = []
        
        for card in cards:
            try:
                # --- Link y Filtros ---
                link_tag = card.find('a', href=True)
                fuente_link = link_tag['href'].lower() if link_tag else url_agenda
                
                # --- Nombre y Locación ---
                nombre = card.find('h4', class_='card-title').get_text(strip=True) if card.find('h4') else "Sin Nombre"
                locacion = card.find('p', class_='lugar').get_text(strip=True) if card.find('p', class_='lugar') else ""
                
                # Filtrado por plataformas ya cubiertas
                if any(p in fuente_link for p in exclusiones):
                    registrar_rechazo(nombre, locacion, "N/A", f"Exclusión: Plataforma externa ({fuente_link})", "67", "Turismo Cba", fuente_link)
                    continue

                # --- Fechas ---
                fechas_p = card.find_all('p', class_='fs-4')
                inicio_raw = fechas_p[0].get_text(" ", strip=True).replace("hs", "").strip() if len(fechas_p) > 0 else ""
                fin_raw = fechas_p[1].get_text(" ", strip=True).replace("hs", "").strip() if len(fechas_p) > 1 else ""
                
                fecha_inicio = formatear_fecha(inicio_raw)
                fecha_fin = formatear_fecha(fin_raw)

                if not fecha_inicio:
                    registrar_rechazo(nombre, locacion, inicio_raw, "Error de formato de fecha o fecha vacía", "78", "Turismo Cba", fuente_link)
                    continue

                # --- Precio ---
                footer_txt = card.find('div', class_='footer').get_text(strip=True) if card.find('div', class_='footer') else ""
                precio = "0" if "Gratuito" in footer_txt else footer_txt.replace("Precio", "").replace("$", "").strip()

                # --- Append al formato final ---
                eventos_lista.append({
                    "Eventos": nombre,
                    "Lugar": locacion,
                    "Comienza": fecha_inicio,
                    "Finaliza": fecha_fin if fecha_fin else fecha_inicio,
                    "Tipo de evento": "Espectáculo",
                    "Detalle": "",
                    "Alcance": "",
                    "Costo de entrada": precio,
                    "Fuente": "Agencia Turismo Cba",
                    "Origen": fuente_link,
                    "Fecha Scrp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

            except Exception as e:
                continue

        # 3. Procesamiento final y subida
        df_final = pd.DataFrame(eventos_lista)

        df_final, metricas_fc = aplicar_clasificador(
            df=df_final,
            col_nombre='Eventos',
            col_lugar='Lugar',
            col_tipo_evento='Tipo de evento',
            col_confianza='confianza_clasificacion'
        )
        log(f"🤖 Ferias y Congresos — Predicciones: {metricas_fc['predicciones']} | Confianza promedio: {metricas_fc['confianza_promedio']}")
                
        if not df_final.empty:
            # Subir a Google Sheets (usando tu función global)
            # Nota: Asegúrate de que el nombre del archivo en Sheets sea el correcto
            subir_a_google_sheets(df_final, 'Turismo CBA (Auto)', 'Hoja 1')
            
            reporte["estado"] = "Exitoso"
            reporte["filas_procesadas"] = len(df_final)
        else:
            reporte["estado"] = "Sin datos"

        # Subir rechazados si existen
        if not df_rechazados.empty:
            subir_a_google_sheets(df_rechazados, 'Rechazados', 'Eventos')

    except Exception as e:
        reporte["estado"] = "Fallido"
        reporte["error"] = str(e)
        print(f"❌ Error en Agencia Turismo Cba: {e}")
    
    finally:
        if driver:
            driver.quit()
        reporte["fin"] = datetime.now().strftime('%H:%M:%S')
        return reporte

#ejecutar_scraper_turismo_cba()



#Importante el orden. Marca jerarquía. El primero se mantiene siempre a la hora de comparar duplicados y así...
dict_fuentes = {
    'Ferias y Congresos': 'Ferias y Congresos (Auto)',
    'Ticketek': 'Ticketek historico (Auto)',
    'Eden Entradas': 'Eden historico (Auto)',
    'Agencia Turismo Cba':'Turismo CBA (Auto)',
    'eventbrite': 'base_h_scrp_eventbrite'
}

def procesar_duplicados_y_normalizar():
    print("🚀 Iniciando proceso de limpieza con Jerarquía de Fuentes...")
    
    # --- CIUDADES DE CÓRDOBA A DETECTAR ---
    ciudades_cordoba = [
        r'r[ií]o\s+cuarto',
        r'villa\s+dolores',
        r'san\s+francisco',
        r'villa\s+mar[ií]a'
    ]
    
    try:
        df_principal = obtener_df_de_sheets("Entradas auto", "Eventos")
        if df_principal.empty: return

        # --- DataFrame para rechazados ---
        df_rechazados = pd.DataFrame(columns=['Nombre', 'Locación', 'Fecha', 'Motivo', 'Linea', 'Fuente', 'Link'])

        def registrar_rechazo(nombre, loc, fecha, motivo, linea, fuente, href):
            nonlocal df_rechazados
            nuevo = pd.DataFrame([{
                'Nombre': nombre, 'Locación': loc, 'Fecha': fecha,
                'Motivo': motivo, 'Linea': str(linea), 'Fuente': fuente, 'Link': href
            }])
            df_rechazados = pd.concat([df_rechazados, nuevo], ignore_index=True)

        # --- VERIFICACIÓN DE CIUDADES ANTES DE PROCESAMIENTO ---
        indices_a_eliminar = []
        
        for idx, row in df_principal.iterrows():
            nombre_evento = str(row.get('Eventos', ''))
            locacion = str(row.get('Lugar', ''))
            texto_combinado = f"{nombre_evento} {locacion}".lower()
            
            # Verificar si menciona alguna ciudad de Córdoba
            ciudad_detectada = None
            for patron in ciudades_cordoba:
                if re.search(patron, texto_combinado, re.IGNORECASE):
                    ciudad_detectada = re.search(patron, texto_combinado, re.IGNORECASE).group(0)
                    break
            
            if ciudad_detectada:
                registrar_rechazo(
                    nombre=nombre_evento,
                    loc=locacion,
                    fecha=row.get('Comienza', 'N/A'),
                    motivo=f"Ciudad distinta de capital detectada: {ciudad_detectada}",
                    linea="1625",
                    fuente=row.get('Fuente', 'Desconocida'),
                    href=row.get('Origen', '')
                )
                
                indices_a_eliminar.append(idx)
                
                # Borrar de la tabla origen
                tabla_origen = dict_fuentes.get(row.get('Fuente'))
                if tabla_origen:
                    print(f"🏙️ Ciudad detectada ({ciudad_detectada}): Eliminando '{nombre_evento}' de {tabla_origen}")
                    borrar_fila_por_origen(tabla_origen, "Hoja 1", row.get('Origen'))

        # Eliminar las filas detectadas del DataFrame principal
        df_principal = df_principal.drop(indices_a_eliminar).reset_index(drop=True)

        # --- 1. NORMALIZACIÓN DE LUGARES ---
        df_equiv = obtener_df_de_sheets("Equiv Lugares", "Hoja 1")
        mapeo_lugares = {}
        if not df_equiv.empty:
            mapeo_lugares = {str(k).lower().strip(): str(v).strip() for k, v in zip(df_equiv.iloc[:, 0], df_equiv.iloc[:, 1])}
        
        df_principal['Lugar_Norm'] = df_principal['Lugar'].apply(
            lambda l: mapeo_lugares.get(str(l).lower().strip(), str(l).lower().strip())
        )

        # --- 2. PROCESAMIENTO DE FECHAS ---
        df_principal['Comienza_DT'] = pd.to_datetime(df_principal['Comienza'], errors='coerce').dt.date

        duplicados_para_registro = []
        indices_ya_agrupados = set()
        
        # Calcular el próximo ID para la hoja Duplicados
        df_hist_dups = obtener_df_de_sheets("Duplicados", "Hoja 1")
        prox_id_num = 1
        if not df_hist_dups.empty and 'id_dup' in df_hist_dups.columns:
            try:
                nums = df_hist_dups['id_dup'].astype(str).str.extract(r'(\d+)').dropna().astype(int)
                if not nums.empty: prox_id_num = int(nums.max()) + 1
            except: prox_id_num = 1

        # --- 3. BUCLE DE DETECCIÓN ---
        prioridad_fuentes = {fuente: i for i, fuente in enumerate(dict_fuentes.keys())}

        for i in range(len(df_principal)):
            if i in indices_ya_agrupados: continue
            
            fila_a = df_principal.iloc[i]
            if pd.isna(fila_a['Comienza_DT']): continue

            grupo_actual_indices = [i]
            for j in range(i + 1, len(df_principal)):
                if j in indices_ya_agrupados: continue
                fila_b = df_principal.iloc[j]
                
                mismo_lugar = (str(fila_a['Lugar_Norm']) == str(fila_b['Lugar_Norm'])) and fila_a['Lugar_Norm'] != ""
                misma_fecha = (fila_a['Comienza_DT'] == fila_b['Comienza_DT'])

                if mismo_lugar and misma_fecha:
                    grupo_actual_indices.append(j)

            if len(grupo_actual_indices) > 1:
                filas_grupo = df_principal.iloc[grupo_actual_indices].copy()
                filas_grupo['prioridad'] = filas_grupo['Fuente'].map(lambda x: prioridad_fuentes.get(x, 99))
                filas_grupo = filas_grupo.sort_values(by='prioridad', ascending=True)
                
                letras = "ABCDEFGHIJKL"
                for idx, (original_idx, row) in enumerate(filas_grupo.iterrows()):
                    indices_ya_agrupados.add(original_idx)
                    
                    ev = row.copy()
                    ev['id_dup'] = f"{prox_id_num}{letras[idx]}"
                    
                    duplicados_para_registro.append(ev.drop(['Lugar_Norm', 'Comienza_DT', 'prioridad'], errors='ignore'))
                    
                    if idx > 0:
                        tabla_dest = dict_fuentes.get(ev['Fuente'])
                        if tabla_dest:
                            print(f"📍 Jerarquía: Manteniendo {filas_grupo.iloc[0]['Fuente']} y borrando {ev['Fuente']}")
                            borrar_fila_por_origen(tabla_dest, "Hoja 1", ev['Origen'])
                
                prox_id_num += 1

        # --- 4. FINALIZAR ---
        if duplicados_para_registro:
            df_final = pd.DataFrame(duplicados_para_registro)
            subir_a_google_sheets(df_final, "Duplicados", "Hoja 1")
            print("✅ Proceso de jerarquía y duplicados finalizado.")
        else:
            print("✨ No se hallaron duplicados para procesar.")

        # --- SUBIR RECHAZADOS ---
        if not df_rechazados.empty:
            subir_a_google_sheets(df_rechazados, 'Rechazados', 'Eventos')
            print(f"✅ Se rechazaron {len(df_rechazados)} eventos por ciudades distintas a capital.")

    except Exception as e:
        print(f"💥 ERROR en procesar_duplicados: {e}")
# --- FUNCIONES DE BORRADO Y LECTURA ---
def borrar_fila_por_origen(nombre_tabla, nombre_hoja, origen_link):
    import os, json, gspread
    import pandas as pd # Asegúrate de que pandas esté importado
    from google.oauth2 import service_account
    
    # --- EXCEPCIÓN SOLICITADA ---
    url_exceptuada = "https://www.feriasycongresos.com/calendario-de-eventos?busqueda=C%C3%B3rdoba"
    if str(origen_link).strip() == url_exceptuada:
        print(f"    🛡️ Excepción: El origen '{origen_link}' está protegido y no será borrado.")
        return
    # ----------------------------

    secreto_json = os.environ.get('GCP_SERVICE_ACCOUNT_JSON')
    if not secreto_json: return

    try:
        info_claves = json.loads(secreto_json)
        creds = service_account.Credentials.from_service_account_info(
            info_claves, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
        )
        client = gspread.authorize(creds)
        sheet = client.open(nombre_tabla).worksheet(nombre_hoja)
        
        data = sheet.get_all_values()
        if len(data) <= 1: return
        
        df_temp = pd.DataFrame(data[1:], columns=data[0])
        
        # --- MEJORA: Detectar cuál es la columna de ID ---
        columnas_posibles = ['Origen', 'href', 'Link', 'URL']
        col_id = next((c for c in columnas_posibles if c in df_temp.columns), None)

        if col_id:
            # Buscamos el link
            match_idx = df_temp.index[df_temp[col_id].astype(str) == str(origen_link)].tolist()
            
            if match_idx:
                # Borra la primera coincidencia
                fila_a_borrar = match_idx[0] + 2
                sheet.delete_rows(fila_a_borrar)
                print(f"    🗑️ Eliminado de '{nombre_tabla}' (columna {col_id}): {origen_link}")
            else:
                print(f"    ⚠️ No se encontró el link {origen_link} en la columna {col_id}")
        else:
            print(f"    ❌ ERROR: No se encontró ninguna columna de ID en {nombre_tabla}")

    except Exception as e:
        print(f"    ❌ Error crítico borrando en '{nombre_tabla}': {e}")
def obtener_df_de_sheets(nombre_tabla, nombre_hoja):
    import os, json, gspread
    from google.oauth2 import service_account
    secreto_json = os.environ.get('GCP_SERVICE_ACCOUNT_JSON')
    if not secreto_json: return pd.DataFrame()
    try:
        info_claves = json.loads(secreto_json)
        creds = service_account.Credentials.from_service_account_info(info_claves, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        client = gspread.authorize(creds)
        sheet = client.open(nombre_tabla).worksheet(nombre_hoja)
        data = sheet.get_all_values()
        if len(data) > 1:
            return pd.DataFrame(data[1:], columns=data[0])
        return pd.DataFrame()
    except:
        return pd.DataFrame()


indices_processed = set()
log('')
log('Detección y procesamiento de duplicados')
procesar_duplicados_y_normalizar()






destinatarios=['furrutia@cordobaacelera.com.ar']
#destinatarios=['furrutia@cordobaacelera.com.ar','meabeldano@cordobaacelera.com.ar','pgonzalez@cordobaacelera.com.ar']
contenido_final_log = log_buffer.getvalue()
enviar_log_smtp(contenido_final_log, destinatarios)
























































































































