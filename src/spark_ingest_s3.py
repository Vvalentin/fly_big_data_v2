import os
import boto3
import tarfile
import io
import time
import sys
import fastavro
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
PREFIX = os.getenv("PREFIX", "states/")
ENDPOINT_URL = os.getenv("S3_ENDPOINT", "https://s3.opensky-network.org")
LOG_FILE = os.getenv("LOG_FILE", "pipeline_metrics.json")
SCALE_FACTOR = int(os.getenv("SCALE_FACTOR", "1"))

logger = PipelineLogger(LOG_FILE)

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
    Worker mit ECHTEM STREAMING (Input UND Output)
    Nutzt 'yield' statt Listen, um RAM-Überlauf zu verhindern.
    """
    import sys
    import fastavro

    urls = list(iterator)
    # Wenn keine URLs da sind, brechen wir sofort ab (Leerer Iterator)
    if not urls:
        return iter([])

    s3 = get_s3_client()

    # Statistik für Logs
    row_counter = 0

    print(f"DEBUG [Worker]: Starte Verarbeitung von {len(urls)} Paketen...", file=sys.stderr)

    for key in urls:
        print(f"DEBUG [Worker]: Starte Stream für: {key}", file=sys.stderr)
        try:
            # 1. Verbindung öffnen
            obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
            raw_stream = obj['Body']

            # 2. Tar im Stream-Modus öffnen
            try:
                with tarfile.open(fileobj=raw_stream, mode='r|') as tar:
                    for member in tar:

                        # --- STRATEGIE 1: AVRO ---
                        if member.name.endswith(".avro"):
                            f = tar.extractfile(member)
                            if f:
                                reader = fastavro.reader(f)
                                for record in reader:
                                    row = []
                                    for col in COLUMNS_ORDER:
                                        val = record.get(col)
                                        row.append("" if val is None else str(val))

                                    # HIER IST DER FIX:
                                    # Wir geben die Zeile SOFORT an Spark weiter.
                                    # Kein Speichern in einer Liste!
                                    yield row
                                    row_counter += 1

                        # --- STRATEGIE 2: CSV ---
                        elif ".csv" in member.name.lower() or "states" in member.name.lower():
                            if member.name.endswith(".avro"): continue

                            f = tar.extractfile(member)
                            if f:
                                for line_bytes in f:
                                    line = line_bytes.decode('utf-8', errors='ignore').strip()
                                    if not line: continue

                                    if "time" in line and "icao24" in line:
                                        continue

                                    parts = line.split(",")
                                    yield parts # <--- FIX: Sofort weitergeben
                                    row_counter += 1

            except Exception as tar_error:
                print(f"DEBUG [Worker]: Stream-Fehler bei {key}: {tar_error}", file=sys.stderr)

        except Exception as e:
            print(f"ERROR bei {key}: {str(e)}", file=sys.stderr)

    print(f"DEBUG [Worker]: Beendet. Habe {row_counter} Zeilen gestreamt.", file=sys.stderr)

def main():
    logger.log_metric("Setup", "Status", 1, "Boolean", "Starte Spark Session")

    spark = SparkSession.builder \
        .appName("OpenSky_Streaming_Ingest") \
        .master("local[*]") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    s3_client = get_s3_client()

    # --- INTELLIGENTE SUCHE ---
    logger.log_metric("Listing", "Start", 0, "ts", f"Suche erste valide Datei in {BUCKET_NAME}/{PREFIX}...")
    print(f"DEBUG [Driver]: Scanne S3 Prefix '{PREFIX}' nach echten Daten...", file=sys.stderr)

    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=BUCKET_NAME, Prefix=PREFIX)

    found_file = None

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

            found_file = key
            break
        if found_file: break

    if not found_file:
        print("KEINE VALIDEN DATEN-DATEIEN GEFUNDEN!")
        spark.stop()
        return

    print(f"DEBUG [Driver]: Gefunden! Nutze Datei: {found_file}", file=sys.stderr)

    target_files = [found_file] * SCALE_FACTOR
    logger.log_metric("Listing", "FileCount", len(target_files), "Count", f"Dateien: {target_files}")

    num_partitions = max(2, len(target_files))
    rdd_keys = sc.parallelize(target_files, numSlices=num_partitions)

    start_proc = time.time()

    # 1. Processing (Streaming)
    raw_rdd = rdd_keys.mapPartitions(process_partition)

    # 2. Caching
    # Da wir streamen, ist Caching jetzt WICHTIGER DENN JE.
    # Es "materialisiert" den Stream im RAM, damit wir ihn mehrfach nutzen können.
    raw_rdd.cache()

    errors = raw_rdd.filter(lambda x: isinstance(x, str) and "ERROR" in x).collect()
    if errors:
        print(f"WARNUNG: {len(errors)} Fehler aufgetreten.")

    data_rdd = raw_rdd.filter(lambda x: isinstance(x, list) and len(x) == 16)

    if data_rdd.isEmpty():
        print("!!! WARNUNG: Keine validen Datenzeilen (16 Spalten) gefunden.")
        spark.stop()
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