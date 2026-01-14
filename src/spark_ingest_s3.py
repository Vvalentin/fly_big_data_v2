import os
import boto3
import tarfile
import io
import time
import sys
import fastavro  # <--- NEU: Für Avro Support
from botocore.handlers import disable_signing
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from dotenv import load_dotenv

# --- WINDOWS SETUP ---
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['HADOOP_HOME'] = "C:\\hadoop"
hadoop_bin = os.path.join(os.environ['HADOOP_HOME'], 'bin')
if hadoop_bin not in os.environ['PATH']:
    os.environ['PATH'] += ";" + hadoop_bin
if 'SPARK_HOME' in os.environ:
    del os.environ['SPARK_HOME']
# ---------------------

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.custom_logger import PipelineLogger

load_dotenv()
BUCKET_NAME = os.getenv("BUCKET_NAME", "data-samples")
# Wir setzen hier einen Prefix, der idealerweise beide Formate enthält oder wir testen flexibel
PREFIX = os.getenv("PREFIX", "states/")
ENDPOINT_URL = os.getenv("S3_ENDPOINT", "https://s3.opensky-network.org")
LOG_FILE = os.getenv("LOG_FILE", "pipeline_metrics.json")
SCALE_FACTOR = int(os.getenv("SCALE_FACTOR", "1"))

logger = PipelineLogger(LOG_FILE)

# Die Spaltenreihenfolge muss für Avro und CSV identisch sein!
COLUMNS_ORDER = [
    "time", "icao24", "lat", "lon", "velocity", "heading", "vertrate",
    "callsign", "onground", "alert", "spi", "squawk", "baroaltitude",
    "geoaltitude", "lastposupdate", "lastcontact"
]

def get_s3_client():
    s3 = boto3.client('s3', endpoint_url=ENDPOINT_URL)
    s3.meta.events.register('choose-signer.s3.*', disable_signing)
    return s3

def process_partition(iterator):
    """
    Worker, der sowohl CSV als auch AVRO verarbeiten kann.
    """
    import sys
    # Import hier drin zur Sicherheit für die Worker
    import fastavro

    urls = list(iterator)
    s3 = get_s3_client()
    results = []

    print(f"DEBUG [Worker]: Starte Verarbeitung von {len(urls)} Paketen...", file=sys.stderr)

    for key in urls:
        print(f"DEBUG [Worker]: Lade Key: {key}", file=sys.stderr)
        try:
            # 1. Download in RAM
            obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            content_bytes = obj['Body'].read()

            if len(content_bytes) == 0:
                continue

            file_obj = io.BytesIO(content_bytes)

            try:
                # 2. Tar öffnen
                with tarfile.open(fileobj=file_obj, mode='r') as tar:
                    members = tar.getmembers()

                    for member in members:
                        # --- STRATEGIE 1: AVRO ---
                        if member.name.endswith(".avro"):
                            print(f"DEBUG [Worker]: Entpacke AVRO: {member.name}", file=sys.stderr)
                            f = tar.extractfile(member)
                            if f:
                                # fastavro liest binär direkt aus dem File-Objekt
                                reader = fastavro.reader(f)
                                avro_count = 0
                                for record in reader:
                                    # Avro gibt ein Dictionary (Key-Value) zurück.
                                    # Wir müssen es in eine feste Liste wandeln, sortiert nach COLUMNS_ORDER
                                    row = []
                                    for col in COLUMNS_ORDER:
                                        # Wert holen, None zu Leerstring, alles zu String casten für Spark
                                        val = record.get(col)
                                        if val is None:
                                            row.append("")
                                        else:
                                            row.append(str(val))
                                    results.append(row)
                                    avro_count += 1
                                print(f"DEBUG [Worker]: AVRO fertig. {avro_count} Zeilen extrahiert.", file=sys.stderr)

                        # --- STRATEGIE 2: CSV ---
                        elif ".csv" in member.name.lower() or "states" in member.name.lower():
                            # Einfacher Check: Wenn es Avro ist, haben wir es oben schon erwischt.
                            # Wenn nicht Avro und "states" im Namen, versuchen wir es als Text/CSV.
                            if member.name.endswith(".avro"): continue

                            print(f"DEBUG [Worker]: Entpacke CSV/Text: {member.name}", file=sys.stderr)
                            f = tar.extractfile(member)
                            if f:
                                lines = [line.decode('utf-8', errors='ignore').strip() for line in f.readlines()]
                                csv_count = 0
                                if len(lines) > 0:
                                    start_idx = 0
                                    # Header überspringen falls vorhanden
                                    if "time" in lines[0] and "icao24" in lines[0]:
                                        start_idx = 1

                                    for line in lines[start_idx:]:
                                        # Split CSV
                                        parts = line.split(",")
                                        # Wir filtern hier noch nicht hart, das macht Spark später
                                        results.append(parts)
                                        csv_count += 1
                                print(f"DEBUG [Worker]: CSV fertig. {csv_count} Zeilen extrahiert.", file=sys.stderr)

            except Exception as tar_error:
                print(f"DEBUG [Worker]: Datei {key} ist kein valides Tar: {tar_error}", file=sys.stderr)

        except Exception as e:
            print(f"ERROR bei {key}: {str(e)}", file=sys.stderr)

    print(f"DEBUG [Worker]: Beendet. Gebe {len(results)} Zeilen zurück.", file=sys.stderr)
    return iter(results)

def main():
    logger.log_metric("Setup", "Status", 1, "Boolean", "Starte Spark Session")

    spark = SparkSession.builder \
        .appName("OpenSky_Universal_Ingest") \
        .master("local[*]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    s3_client = get_s3_client()

    # --- INTELLIGENTE SUCHE (VERBESSERT) ---
    logger.log_metric("Listing", "Start", 0, "ts", f"Suche erste valide Datei in {BUCKET_NAME}/{PREFIX}...")
    print(f"DEBUG [Driver]: Scanne S3 Prefix '{PREFIX}' nach echten Daten...", file=sys.stderr)

    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=PREFIX)

    found_file = None

    for page in page_iterator:
        if 'Contents' not in page:
            continue

        for obj in page['Contents']:
            key = obj['Key']

            # 1. Muss ein Tar sein
            if not key.endswith('.tar'):
                continue

            # 2. Versteckte Ordner blockieren (zur Sicherheit)
            if "/." in key:
                continue

            # 3. NEU & WICHTIG: Jahres-Check
            # Wir akzeptieren nur Dateien, die ein echtes Jahr im Pfad haben.
            # Das verhindert, dass wir '.2017' (versteckt) erwischen.
            has_valid_year = False
            for year in range(2010, 2030):
                # Wir suchen nach "/2022-" oder "states/2022-"
                if f"/{year}-" in key or f"states/{year}-" in key:
                    has_valid_year = True
                    break

            if not has_valid_year:
                continue

            # TREFFER!
            found_file = key
            break

        if found_file:
            break

    if not found_file:
        print("KEINE VALIDEN DATEN-DATEIEN GEFUNDEN (nur Müll oder leer)!")
        spark.stop()
        return

    print(f"DEBUG [Driver]: Gefunden! Nutze Datei: {found_file}", file=sys.stderr)
    # --- ENDE SUCHE ---

    # Simulation Scaling
    target_files = [found_file] * SCALE_FACTOR
    logger.log_metric("Listing", "FileCount", len(target_files), "Count", f"Dateien: {target_files}")

    num_partitions = max(2, len(target_files))
    rdd_keys = sc.parallelize(target_files, numSlices=num_partitions)

    start_proc = time.time()

    # 1. Verarbeitung
    raw_rdd = rdd_keys.mapPartitions(process_partition)

    # --- PERFORMANCE FIX: CACHING ---
    # Verhindert, dass Spark die Datei 3x herunterlädt!
    raw_rdd.cache()
    # --------------------------------

    # 2. Filtern (Fehler & Spaltenanzahl)
    # Erst Fehler ausgeben (triggert Download 1x und speichert im Cache)
    errors = raw_rdd.filter(lambda x: isinstance(x, str) and "ERROR" in x).collect()
    if errors:
        print(f"WARNUNG: {len(errors)} Fehler aufgetreten (z.B. {errors[0]})")

    # Dann Daten filtern (nutzt Cache -> sehr schnell)
    data_rdd = raw_rdd.filter(lambda x: isinstance(x, list) and len(x) == 16)

    if data_rdd.isEmpty():
        print("!!! WARNUNG: Keine validen Datenzeilen (16 Spalten) gefunden.")
        spark.stop()
        return

    # 3. DataFrame
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

    df = spark.createDataFrame(data_rdd, schema)

    df = df.withColumn("velocity", df["velocity"].cast(DoubleType())) \
        .withColumn("geoaltitude", df["geoaltitude"].cast(DoubleType()))

    row_count = df.count()
    duration_proc = time.time() - start_proc

    logger.log_metric("Processing", "Duration", duration_proc, "Seconds", f"Zeit")
    logger.log_metric("Processing", "RowCount", row_count, "Rows", "Zeilen")

    output_path = os.path.join("data", "processed", f"run_{int(time.time())}")
    df.write.mode("overwrite").parquet(output_path)
    logger.log_metric("Storage", "Status", 1, "Boolean", f"Gespeichert: {output_path}")

    spark.stop()

if __name__ == "__main__":
    main()