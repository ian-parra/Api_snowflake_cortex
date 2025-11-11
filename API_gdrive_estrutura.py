import os
import io
import shutil
from dotenv import load_dotenv 
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import snowflake.connector
from snowflake.connector import connect
from snowflake.snowpark import Session 
from snowflake.snowpark.exceptions import SnowparkSQLException 


load_dotenv('ignore.env') 

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_PATH") 
DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID")
SCOPES = ['https://www.googleapis.com/auth/drive.readonly'] 

SNOWFLAKE_STAGE_NAME = f'@{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.PDF_FILES_STAGE' 
TEMP_UPLOAD_DIR = 'temp_drive_files'



def download_files_from_drive():
    """Busca archivos en la carpeta de Drive y los descarga al directorio temporal."""
    
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES
        )
        service = build('drive', 'v3', credentials=creds)
    except Exception as e:
        print(f"ERROR DE AUTENTICACIÓN DE DRIVE: {e}")
        return []

    print(f"Buscando archivos en Drive ID: {DRIVE_FOLDER_ID}...")

    query = f"'{DRIVE_FOLDER_ID}' in parents and mimeType != 'application/vnd.google-apps.folder' and trashed=false"
    results = service.files().list(q=query, fields='files(id, name)').execute()
    items = results.get('files', [])

    if not items:
        print("No se encontraron archivos nuevos para descargar.")
        return []

    downloaded_files_count = 0
    os.makedirs(TEMP_UPLOAD_DIR, exist_ok=True)
    
    for item in items:
        file_id, file_name = item['id'], item['name']
        local_path = os.path.join(TEMP_UPLOAD_DIR, file_name)
        
        try:
            request = service.files().get_media(fileId=file_id)
            file_handle = io.FileIO(local_path, 'wb')
            downloader = MediaIoBaseDownload(file_handle, request)
            
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            downloaded_files_count += 1
        except Exception as download_err:
            print(f"ERROR: No se pudo descargar {file_name}. Error: {download_err}")
            continue
            
    return os.listdir(TEMP_UPLOAD_DIR)


def upload_to_snowflake(file_names):
    """Sube archivos al Stage de Snowflake usando el comando PUT y llama al SP."""
    if not file_names:
        return False

    try:
        conn = connect(
            user=SNOWFLAKE_USER, password=SNOWFLAKE_PASSWORD, account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE, database=SNOWFLAKE_DATABASE, schema=SNOWFLAKE_SCHEMA
        )
        cursor = conn.cursor()
    except Exception as e:
        print(f"ERROR DE CONEXIÓN A SNOWFLAKE: {e}")
        return False

    try:
        put_command = f"PUT file://{TEMP_UPLOAD_DIR}/* {SNOWFLAKE_STAGE_NAME} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        cursor.execute(put_command)
        print("Carga PUT finalizada. Inicia el procesamiento interno...")
        
        cursor.execute(f"CALL NETFLIX.NET_MOVIES.P_GDRIVE_TO_CORE_LOADER('{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}');")
        print("Llamada al SP de procesamiento interno completada.")
        
    except Exception as e:
        print(f"ERROR EN LA CARGA PUT O LLAMADA AL SP: {e}")

    finally:
        if conn: conn.close()
    return True



def transform_data(session: Session, tabla_destino: str):
    """
    Función de transformación ejecutada DENTRO del Stored Procedure de Snowflake (HANDLER).
    Procesa los archivos que ya están en el Stage.
    """
    
    stage_path = f'@NETFLIX.NET_MOVIES.PDF_FILES_STAGE' 
    
    try: 
        print(f"Iniciando transformación interna en destino: {tabla_destino}")
        
        session.sql(f"""
            COPY INTO NETFLIX.NET_MOVIES.RAW_CSV_DATA
            FROM {stage_path}
            FILE_FORMAT = (TYPE = CSV, SKIP_HEADER = 1)
            PATTERN = '.*[.]csv$'
            ON_ERROR = 'CONTINUE';
        """).collect()
        
        session.sql(f"""
            INSERT INTO NETFLIX.NET_MOVIES.RAW_PDF_TEXT (FILE_NAME, RAW_CONTENT)
            SELECT
                T.RELATIVE_PATH AS FILE_NAME,
                SNOWFLAKE.CORTEX.PARSE_DOCUMENT('{stage_path}', T.RELATIVE_PATH)
            FROM
                DIRECTORY('{stage_path}') AS T
            WHERE
                T.RELATIVE_PATH LIKE '%.pdf';
        """).collect()
        
        return f"Transformación interna con Cortex AI completada para el destino: {tabla_destino}"
    
    except SnowparkSQLException as e:
        return f"Error de procesamiento Snowpark/SQL: {e.message}"
    except Exception as e:
        return f"Error inesperado en transform_data: {e}"



def main():
    print("--- INICIO: Extracción y Carga (E&L) Drive -> Snowflake Stage ---")
    
    files_to_upload = download_files_from_drive()
    
    if files_to_upload:
        upload_to_snowflake(files_to_upload)
    
    if os.path.exists(TEMP_UPLOAD_DIR):
        try:
            shutil.rmtree(TEMP_UPLOAD_DIR) 
            print(f"Directorio temporal '{TEMP_UPLOAD_DIR}' limpiado.")
        except OSError as e:
            print(f"Error limpiando directorio temporal: {e}")

    print("--- PROCESO FINALIZADO ---")


if __name__ == "__main__":
    main()