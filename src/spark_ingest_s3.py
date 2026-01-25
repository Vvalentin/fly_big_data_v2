import subprocess
import os
import io
import boto3
import tarfile
import time
import sys
import fastavro
from dotenv import load_dotenv  # <--- WICHTIG: Import hier oben
from botocore.handlers import disable_signing
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# --- EIGENE IMPORTS ---
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.custom_logger import PipelineLogger

# ==========================================
# WINDOWS SETUP & CONFIG (ANGEPASST)
# ==========================================

# 1. Konfiguration SOFORT laden, damit HADOOP_HOME aus .env verfügbar ist
load_dotenv()

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# 2. Dynamische Hadoop-Logik:
# Prüfen, ob HADOOP_HOME im System ist -> Nein? Dann in .env schauen -> Nein? Dann Fallback.
if 'HADOOP_HOME' not in os.environ:
    env_hadoop = os.getenv('HADOOP_HOME')
    if env_hadoop:
        os.environ['HADOOP_HOME'] = env_hadoop
        print(f"DEBUG: HADOOP_HOME aus .env gesetzt: {env_hadoop}")
    else:
        print("WARNUNG: HADOOP_HOME nicht gefunden. Setze Fallback auf C:\\hadoop")
        os.environ['HADOOP_HOME'] = "C:\\hadoop"

# 3. Bin-Ordner zum PATH hinzufügen
if 'HADOOP_HOME' in os.environ:
    hadoop_bin = os.path.join(os.environ['HADOOP_HOME'], 'bin')
    if hadoop_bin not in os.environ['PATH']:
        os.environ['PATH'] += ";" + hadoop_bin

if 'SPARK_HOME' in os.environ:
    del os.environ['SPARK_HOME']

java_path = r"C:\Program Files\Java\jdk-21"

if os.path.exists(java_path):
    os.environ["JAVA_HOME"] = java_path
    # Wichtig: Das bin-Verzeichnis muss GANZ VORNE in den Path,
    # damit Windows nicht das alte Java 11 findet.
    os.environ["PATH"] = os.path.join(java_path, "bin") + ";" + os.environ["PATH"]
else:
    print(f"WARNUNG: Java Pfad {java_path} nicht gefunden! Prüfe den Pfad.", file=sys.stderr)

load_dotenv()
BUCKET_NAME = os.getenv("BUCKET_NAME", "data-samples")
PREFIX = os.getenv("PREFIX", "states/")
ENDPOINT_URL = os.getenv("S3_ENDPOINT", "https://s3.opensky-network.org")
LOG_FILE = os.getenv("LOG_FILE", "data/pipeline_metrics.json")

# ==========================================
# ⚙️ KONFIGURATION: ANZAHL DATEIEN
# ==========================================
# Hier einstellen, wie viele echte Dateien geladen werden sollen.
# Alle landen in EINEM Parquet-Output.
NUM_FILES_TO_PROCESS = 6
# ==========================================

# Datei für Kommunikation zwischen Spark und Monitor
STAGE_FILE = os.path.join("data", "current_stage.txt")

COLUMNS_ORDER = [
    "time", "icao24", "lat", "lon", "velocity", "heading", "vertrate",
    "callsign", "onground", "alert", "spi", "squawk", "baroaltitude",
    "geoaltitude", "lastposupdate", "lastcontact"
]

def get_s3_client():
    s3 = boto3.client('s3', endpoint_url=ENDPOINT_URL)
    s3.meta.events.register('choose-signer.s3.*', disable_signing)
    return s3

def set_monitor_stage(stage_name):
    """Schreibt den aktuellen Status in eine Datei, die der Monitor liest."""
    try:
        # Ordner erstellen falls nicht da
        os.makedirs(os.path.dirname(STAGE_FILE), exist_ok=True)
        with open(STAGE_FILE, 'w') as f:
            f.write(stage_name)
    except:
        pass

def process_partition(iterator):
    """
    Worker mit ECHTEM STREAMING und SMART SCHEMA MAPPING
    """
    import sys
    import fastavro

    urls = list(iterator)
    if not urls:
        return iter([])

    s3 = get_s3_client()
    row_counter = 0

    # MAPPING: Unser Ziel-Name -> Mögliche Avro-Namen
    AVRO_MAPPING = {
        "lat": ["latitude", "lat"],
        "lon": ["longitude", "lon"],
        "velocity": ["velocity", "speed"],
        "heading": ["heading", "track"],
        "vertrate": ["vertical_rate", "vertrate"],
        "geoaltitude": ["geo_altitude", "altitude", "geoaltitude"]
    }

    print(f"DEBUG [Worker]: Starte Verarbeitung von {len(urls)} Paketen...", file=sys.stderr)

    for key in urls:
        try:
            obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            raw_stream = obj['Body']

            try:
                with tarfile.open(fileobj=raw_stream, mode='r|') as tar:
                    for member in tar:

                        # --- AVRO STRATEGIE (Verbessert) ---
                        if member.name.endswith(".avro"):
                            f = tar.extractfile(member)
                            if f:
                                reader = fastavro.reader(f)
                                for record in reader:
                                    row = []
                                    for col in COLUMNS_ORDER:
                                        # 1. Direkter Versuch
                                        val = record.get(col)

                                        # 2. Fallback Versuch (Synonyme prüfen)
                                        if val is None and col in AVRO_MAPPING:
                                            for alias in AVRO_MAPPING[col]:
                                                val = record.get(alias)
                                                if val is not None:
                                                    break

                                        row.append("" if val is None else str(val))
                                    yield row
                                    row_counter += 1

                        # --- CSV STRATEGIE (Bleibt gleich) ---
                        elif ".csv" in member.name.lower() or "states" in member.name.lower():
                            if member.name.endswith(".avro"): continue
                            f = tar.extractfile(member)
                            if f:
                                for line_bytes in f:
                                    line = line_bytes.decode('utf-8', errors='ignore').strip()
                                    if not line: continue
                                    if "time" in line and "icao24" in line: continue
                                    parts = line.split(",")
                                    yield parts
                                    row_counter += 1

            except Exception as tar_error:
                print(f"DEBUG [Worker]: Stream-Fehler bei {key}: {tar_error}", file=sys.stderr)
        except Exception as e:
            print(f"ERROR bei {key}: {str(e)}", file=sys.stderr)

    # Wichtig: Nichts zurückgeben, da yield genutzt wird

def main():
    logger = PipelineLogger(LOG_FILE)

    # --- 1. MONITOR STARTEN ---
    monitor_script = os.path.join(os.path.dirname(__file__), 'system_monitor.py')
    monitor_process = subprocess.Popen([sys.executable, monitor_script])

    print("--- Monitor-Subprozess gestartet ---")
    set_monitor_stage("1. Setup Spark")

    try:
        logger.log_metric("Setup", "Status", 1, "Boolean", "Starte Spark Session")

        # RAM ERHÖHT AUF 4GB
        spark = SparkSession.builder \
            .appName("OpenSky_Streaming_Ingest") \
            .master("local[*]") \
            .config("spark.executor.memory", "4g") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "200") \
            .getOrCreate()

        sc = spark.sparkContext
        sc.setLogLevel("WARN")

        set_monitor_stage("2. S3 Listing")
        s3_client = get_s3_client()

        logger.log_metric("Listing", "Start", 0, "ts", f"Suche {NUM_FILES_TO_PROCESS} Dateien...")
        print(f"DEBUG [Driver]: Scanne S3 nach {NUM_FILES_TO_PROCESS} Dateien...", file=sys.stderr)

        # --- LOGIK: Sammle Dateien ---
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=PREFIX)
        valid_files = []

        for page in page_iterator:
            if 'Contents' not in page: continue
            for obj in page['Contents']:
                key = obj['Key']
                if not key.endswith('.tar'): continue
                if "/." in key: continue

                has_valid_year = False
                for year in range(2010, 2030):
                    if f"/{year}-" in key or f"states/{year}-" in key:
                        has_valid_year = True
                        break
                if not has_valid_year: continue

                valid_files.append(key)
                if len(valid_files) >= NUM_FILES_TO_PROCESS:
                    break
            if len(valid_files) >= NUM_FILES_TO_PROCESS:
                break

        if not valid_files:
            print("KEINE DATEN GEFUNDEN!")
            return

        print(f"DEBUG [Driver]: {len(valid_files)} Dateien gefunden. Starte Streaming...", file=sys.stderr)
        target_files = valid_files

        # --- PROCESSING ---
        set_monitor_stage(f"3. Lazy Plan ({len(target_files)} Files)")

        num_partitions = max(2, len(target_files))
        rdd_keys = sc.parallelize(target_files, numSlices=num_partitions)
        raw_rdd = rdd_keys.mapPartitions(process_partition)

        # --- WICHTIG: KEIN CACHING MEHR! ---
        # Wir lassen die Daten nur durchfließen.
        # raw_rdd.persist(...)  <-- ENTFERNT

        set_monitor_stage("4. Filter & Schema")
        start_proc = time.time()

        # --- WICHTIG: ERROR CHECK ENTFERNT ---
        # Da wir nicht cachen, würde dieser Check einen doppelten Download auslösen.
        # Wir vertrauen darauf, dass kaputte Zeilen später gefiltert werden.

        # Nur Listen (echte Daten) durchlassen, Fehler-Strings ignorieren
        data_rdd = raw_rdd.filter(lambda x: isinstance(x, list) and len(x) == 16)

        if data_rdd.isEmpty():
            print("Keine Daten gefunden.")
            return

        schema = StructType([
            StructField("time", StringType(), True),
            StructField("icao24", StringType(), True),
            StructField("lat", StringType(), True),
            StructField("lon", StringType(), True),
            StructField("velocity", StringType(), True),
            StructField("heading", StringType(), True),
            StructField("vertrate", StringType(), True),
            StructField("callsign", StringType(), True),
            StructField("onground", StringType(), True),
            StructField("alert", StringType(), True),
            StructField("spi", StringType(), True),
            StructField("squawk", StringType(), True),
            StructField("baroaltitude", StringType(), True),
            StructField("geoaltitude", StringType(), True),
            StructField("lastposupdate", StringType(), True),
            StructField("lastcontact", StringType(), True)
        ])

        from pyspark.sql.functions import expr, col

        set_monitor_stage("5. Create DataFrame")
        df = spark.createDataFrame(data_rdd, schema)

        df = df.withColumn("velocity", expr("try_cast(velocity as double)")) \
            .withColumn("geoaltitude", expr("try_cast(geoaltitude as double)"))

        # Kaputte Zeilen verwerfen
        df = df.filter(col("velocity").isNotNull())

        set_monitor_stage("6. ACTION: Write Parquet")
        output_path = os.path.join("data", "processed", f"run_{int(time.time())}")

        # Hier passiert jetzt alles in einem Rutsch: Download -> Parse -> Filter -> Write
        df.write.mode("overwrite").parquet(output_path)

        duration_proc = time.time() - start_proc

        # Row Count ist billig, da Parquet Metadaten hat
        # (oder wir lesen es kurz neu ein, was schneller ist als raw download)
        logger.log_metric("Processing", "Duration", duration_proc, "Seconds", f"Zeit")
        logger.log_metric("Storage", "Status", 1, "Boolean", f"Gespeichert: {output_path}")

    finally:
        set_monitor_stage("7. Cleanup")
        print("--- Stoppe Monitor ---")
        monitor_process.terminate()
        try:
            monitor_process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            monitor_process.kill()

        if os.path.exists(STAGE_FILE):
            os.remove(STAGE_FILE)

        spark.stop()

if __name__ == "__main__":
    main()