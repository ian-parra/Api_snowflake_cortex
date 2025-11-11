import os
import io
import shutil
from dotenv import load_dotenv 
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import snowflake.connector
from snowflake.connector import connect

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
        print(f"ERROR DE AUTENTICACIÓN DE DRIVE: Revise GOOGLE_SERVICE_ACCOUNT_PATH y permisos. {e}")
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
        file_id = item['id']
        file_name = item['name']
        local_path = os.path.join(TEMP_UPLOAD_DIR, file_name)

        
        request = service.files().get_media(fileId=file_id)
        file_handle = io.FileIO(local_path, 'wb')
        downloader = MediaIoBaseDownload(file_handle, request)
        
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            
        downloaded_files_count += 1
        
    return os.listdir(TEMP_UPLOAD_DIR)



def upload_to_snowflake(file_names):
    """Sube archivos al Stage de Snowflake usando el comando PUT."""
    if not file_names:
        return

    try:
        conn = connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        cursor = conn.cursor()
    except Exception as e:
        print(f"ERROR DE CONEXIÓN A SNOWFLAKE: Revise credenciales de Snowflake en .env. {e}")
        return

    try:
        print(f"\nSubiendo archivos al Stage: {SNOWFLAKE_STAGE_NAME}...")
        
        put_command = f"PUT file://{TEMP_UPLOAD_DIR}/* {SNOWFLAKE_STAGE_NAME} AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
        
        cursor.execute(put_command)

    except Exception as e:
        print(f"ERROR EN LA CARGA PUT A SNOWFLAKE: {e}")

    finally:
        if 'conn' in locals() and conn:
            conn.close()



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