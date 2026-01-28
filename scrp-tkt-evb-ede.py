
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
        print(f"Error general: {e}")


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
        print(f"Error en extracción: {e}")
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
                    print(f"Precio 1: {price1_int}, Precio 2: {price2_int}, Suma: {total_price}")
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
            print(f"Lista de precios: {price_list}, Promedio: {avg}")
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

                print(f"Error processing {full_href}: {e}")

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
    df['tipo de evento'] = None
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
    secreto_json = os.environ.get('GCP_SERVICE_ACCOUNT_JSON')
    intentos = 0
    while intentos < retries:
        try:
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            info_claves = json.loads(os.environ.get('GCP_SERVICE_ACCOUNT_JSON'))
            creds = service_account.Credentials.from_service_account_info(info_claves, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
            client = gspread.authorize(creds)
            
            sheet = client.open(nombre_tabla).worksheet(nombre_hoja)
            existing_data = sheet.get_all_values()
            
            # Limpieza inicial del DF que entra
            df_entrada = df.copy()

            if len(existing_data) > 1:
                existing_df = pd.DataFrame(existing_data[1:], columns=existing_data[0])
                combined_df = pd.concat([existing_df, df_entrada], ignore_index=True)
            else:
                combined_df = df_entrada

            if not combined_df.empty:
                # --- LIMPIEZA CRÍTICA ANTES DE SUBIR ---
                # 1. Reemplazar Infinitos por NaN y luego todos los NaN por string vacío
                # Esto elimina el error "Out of range float values"
                combined_df = combined_df.replace([np.inf, -np.inf], np.nan).fillna("")
                
                # 2. Asegurar que no haya objetos Timestamp de Python (pasarlos a string)
                for col in combined_df.columns:
                    if pd.api.types.is_datetime64_any_dtype(combined_df[col]):
                        combined_df[col] = combined_df[col].astype(str)

                # 3. Lógica de duplicados
                columnas_posibles = ['Eventos', 'Nombre', 'title', 'Lugar', 'Locación', 'lugar', 'Origen', 'href', 'Fecha Convertida', 'Comienza']
                subset_duplicados = [c for c in columnas_posibles if c in combined_df.columns]
                
                if subset_duplicados:
                    combined_df = combined_df.drop_duplicates(subset=subset_duplicados, keep='last')
                
                # 4. Ordenar
                col_fecha = next((c for c in ['fecha de carga', 'Fecha Scrp'] if c in combined_df.columns), None)
                if col_fecha:
                    combined_df = combined_df.sort_values(by=col_fecha, ascending=False)

                # 5. Envío a Google
                sheet.clear()
                # Convertimos todo a lista de listas y aseguramos que cada valor sea compatible con JSON
                valores_finales = combined_df.values.tolist()
                sheet.update([combined_df.columns.values.tolist()] + valores_finales, 
                             value_input_option='USER_ENTERED')
                
                print(f"✅ Hoja '{nombre_tabla}' actualizada con éxito.")
                return True 

        except Exception as e:
            intentos += 1
            print(f"⚠️ Intento {intentos} fallido para {nombre_tabla}: {e}")
            if intentos < retries: 
                time.sleep(5)
            else: 
                # Aquí es donde el error "sube" al reporte del scraper
                raise Exception(f"Fallo definitivo en Google Sheets: {str(e)}")
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
            print("No se encontraron artistas. Finalizando tarea Ticketek.")
            return

        # 3. Procesar detalles de cada link
        df_artists2 = process_hrefs(driver, df_artists)
        
        # 4. Limpieza y Reordenamiento
        df_artists2_cleaned = clean_data(df_artists2.copy())
        df_artists2_cleaned['lugar'] = df_artists2_cleaned['lugar'].apply(limpiar_lugar)
        
        df_final = reordenar_y_agregar_columnas(df_artists2_cleaned.copy())
        df_final['finaliza'] = df_final['date']
        df_final['fecha de carga'] = datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        df_final['price_avg'] = df_final['price_avg'].astype(str).str.replace("'", "", regex=False).astype(float)

        # 5. Exportar a Excel (Opcional si vas a Sheets, pero lo mantenemos)
        #fecha_actual = datetime.now().strftime('%Y-%m-%d')
        #ruta_archivo = f"C:/Users//OneDrive/Escritorio/ticketek/scrp_ticketek_{fecha_actual}.xlsx"
        #df_final.to_excel(ruta_archivo, index=False)

        subir_a_google_sheets(df_final,'Ticketek historico (Auto)','Hoja 1')
        reporte["estado"] = "Exitoso"
        reporte["filas_procesadas"] = len(df_final)
    except Exception as e:
        reporte["estado"] = "Fallido"
        reporte["error"] = str(e)
        print(f"❌ Error en Ticketek: {e}")
    finally:
        if driver:
            driver.quit()
        reporte["fin"] = datetime.now().strftime('%H:%M:%S')
        return reporte
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
    # (Mantenemos tu lógica de normalización de fecha compleja aquí...)
    # [Insertar aquí el cuerpo de tu función normalizar_fecha_complejo]
    return [pd.Timestamp.now()] # Simplificado para el ejemplo

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
    driver = None
    reporte = {
        "nombre": "Eden Entradas",
        "estado": "Pendiente",
        "filas_procesadas": 0,
        "error": None,
        "inicio": datetime.now().strftime('%H:%M:%S')
    }
    
    try:
        # 1. Configuración e inicio
        driver = iniciar_driver() # Usamos la función de inicio que definimos antes
        BASE_URL = "https://www.edenentradas.ar"
        driver.get(BASE_URL + "/")
        time.sleep(5)

        # 2. Scrapeo de lista principal
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        eventos_html = soup.find_all('a', class_='grid_element')
        
        data = []
        for evento in eventos_html:
            data.append({
                'Nombre': evento.find('div', class_='item_title').text.strip() if evento.find('div', class_='item_title') else None,
                'Locación': evento.find('strong').text.strip() if evento.find('strong') else None,
                'Fecha': evento.find('span').text.strip() if evento.find('span') else None,
                'href': evento['href']
            })
        
        data_df = pd.DataFrame(data).dropna(subset=['Locación']).drop_duplicates().reset_index(drop=True)

        # 3. Recorrido de detalles para Precios y Ciudad
        for index, row in data_df.iterrows():
            try:
                full_href = f"{BASE_URL}{row['href'].replace('..', '')}"
                driver.get(full_href)
                time.sleep(3)
                
                # Manejo de página intermedia e ingreso a compra para ver precios
                soup_det = BeautifulSoup(driver.page_source, 'html.parser')
                
                # Lógica de detección de ciudad
                cols = soup_det.find_all('div', class_='col-xs-7')
                data_df.loc[index, 'filtro_ciudad'] = ', '.join([e.text.strip() for e in cols]) if cols else ""

                # Click en botones para ver precios (Tipo 1 o Tipo 2)
                try:
                    btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, "div.picker-full button.next, #buyButton")))
                    driver.execute_script("arguments[0].click();", btn)
                    time.sleep(4)
                    data_df.loc[index, 'precio_promedio'] = extraer_promedio_precios(BeautifulSoup(driver.page_source, 'html.parser'))
                except:
                    data_df.loc[index, 'precio_promedio'] = None
            except: continue

        # 4. Filtrado y Normalización
        data_df = data_df[data_df['filtro_ciudad'].str.contains('Córdoba|Cordoba', case=False, na=False)]
        df_norm = procesar_dataframe_complejo(data_df) # Usando tu función de procesamiento
        
        # 5. Formateo Final
        df_final = pd.DataFrame({
            'Eventos': df_norm['Nombre'],
            'Lugar': df_norm['Locación'],
            'Comienza': df_norm['Fecha'],
            'Finaliza': df_norm['Fecha'],
            'Tipo de evento': None,
            'Detalle': None,
            'Alcance': None,
            'Costo de entrada': df_norm['precio_promedio'],
            'Fuente': 'Eden Entradas',
            'Origen': df_norm['href'].str.replace('..', 'https://www.edenentradas.com.ar', regex=False),
            'fecha de carga': datetime.today().strftime('%Y-%m-%d %H:%M:%S')
        }).dropna(subset=['Comienza'])

        # 6. Subida a Google Sheets con nuestra función de reintentos
        subir_a_google_sheets(df_final, 'Eden historico (Auto)', 'Hoja 1')

        reporte["estado"] = "Exitoso"
        reporte["filas_procesadas"] = len(df_final)

    except Exception as e:
        reporte["estado"] = "Fallido"
        reporte["error"] = str(e)
        print(f"❌ Error en Eden: {e}")
    
    finally:
        if driver: driver.quit()
        reporte["fin"] = datetime.now().strftime('%H:%M:%S')
        return reporte
ejecutar_scraper_eden()

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
    # Eliminar texto después de la hora (como " + 1 más")
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
        
        # 3. DÍA DE LA SEMANA (Lunes, Martes...)
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
        # Intentar extraer: día (numero), mes (letras), hora (00:00)
        match_esp = re.search(r'(\d{1,2})\s([a-z]{3}).*?(\d{1,2}:\d{2})', fecha_low)
        if match_esp:
            dia = int(match_esp.group(1))
            mes_txt = match_esp.group(2)
            hora_str = match_esp.group(3)
            
            if mes_txt in meses:
                mes = meses[mes_txt]
                año = ahora.year
                if mes < ahora.month: año += 1 # Si el mes ya pasó, es el año que viene
                
                h, m = map(int, hora_str.split(":"))
                return datetime(año, mes, dia, h, m)

        return fecha # Si nada coincide, devolver original
    except Exception as e:
        print(f"⚠️ Error procesando fecha '{fecha}': {e}")
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
    
    date_keywords = ['lun', 'mar', 'mié', 'jue', 'vie', 'sáb', 'dom', 'mañana', 'hoy', 'ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic']
    
    try:
        driver = iniciar_driver()
        
        base_url = 'https://www.eventbrite.com.ar/d/argentina--c%C3%B3rdoba/all-events/'
        event_data = []
        seen_links = set()
        
        # Tasa de cambio
        try:
            response = requests.get("https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json", timeout=10)
            cambio = response.json()['usd']['ars'] if response.status_code == 200 else 1000
        except: cambio = 1000

        for page in range(1, 6):
            print(f"📄 Página {page}...")
            driver.get(f'{base_url}?page={page}')
            
            try:
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'h3')))
                driver.execute_script("window.scrollBy(0, 800);")
                time.sleep(2)
            except: break

            events = driver.find_elements(By.CSS_SELECTOR, 'article, section.discover-horizontal-event-card, div[class*="Stack_root"]')
            if not events: break
            
            for event in events:
                try:
                    name = event.find_element(By.TAG_NAME, 'h3').text.strip()
                    link = event.find_element(By.TAG_NAME, 'a').get_attribute('href')
                    if not name or link in seen_links: continue
                    
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

                    event_data.append({
                        'Nombre': name, 'Fecha': date_info, 'Locación': location,
                        'Precio': "Consultar", 'Origen': link
                    })
                    seen_links.add(link)
                except: continue

        # --- PROCESAMIENTO ---
        df_crudo = pd.DataFrame(event_data)
        
        # Filtrado
        keywords_locacion = ['quinto centenario', 'blas pascal', 'quorum']
        df_filtrado = df_crudo[df_crudo['Locación'].str.lower().str.contains('|'.join(keywords_locacion), na=False)].copy()
        

        if not df_filtrado.empty:
            # USAR EL NOMBRE CORRECTO DE LA FUNCIÓN
            df_filtrado['Fecha Convertida'] = df_filtrado['Fecha'].apply(convertir_fechas)
            
            df_final = pd.DataFrame({
                'Nombre': df_filtrado['Nombre'],
                'Locación': df_filtrado['Locación'],
                'Fecha Convertida': df_filtrado['Fecha Convertida'].astype(str),
                'termina': "", 'tipo de evento': 'M.I.C.E', 'detalle': "", 'alcance': "",
                'Precio': 0.0, 'fuente': 'eventbrite', 'Origen': df_filtrado['Origen'],
                'Fecha Scrp': datetime.today().strftime('%Y-%m-%d')
            })

            subir_a_google_sheets(df_final, 'base_h_scrp_eventbrite', 'Hoja 1')
            reporte["filas_procesadas"] = len(df_final)
            reporte["estado"] = "Exitoso"
        else:
            reporte["estado"] = "Exitoso (Sin novedades)"

    except Exception as e:
        print(f"❌ Error: {e}")
        reporte["estado"] = "Fallido"
        reporte["error"] = str(e)
    finally:
        if driver:
            driver.quit()
        reporte["fin"] = datetime.now().strftime('%H:%M:%S')
        return reporte

# Ejecutar

ejecutar_scraper_eventbrite()

