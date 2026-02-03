import os
import boto3
import pandas as pd
from dotenv import load_dotenv
from botocore.handlers import disable_signing
import random  # Nur für den Resilience-Test nötig

load_dotenv()

# Konfiguration
BUCKET_NAME = os.getenv("BUCKET_NAME", "data-samples")
ENDPOINT_URL = os.getenv("S3_ENDPOINT", "https://s3.opensky-network.org")
FILE_KEY = "metadata/aircraft-database-complete-2025-08.csv"
OUTPUT_DIR = "data/external"
OUTPUT_PARQUET = "aircraft_database.parquet"


def get_s3_client():
    s3 = boto3.client('s3', endpoint_url=ENDPOINT_URL)
    s3.meta.events.register('choose-signer.s3.*', disable_signing)
    return s3


def main():
    print(f"--- START: Lade {FILE_KEY} herunter ---")
    s3 = get_s3_client()

    try:
        # 1. Download
        print("Lade CSV-Stream...")
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=FILE_KEY)

        # 2. Verarbeitung
        print("Konvertiere zu Parquet...")
        df = pd.read_csv(
            obj['Body'],
            dtype=str,
            quotechar="'",
            encoding='utf-8',
            on_bad_lines='skip'
        )

        # 3. Unnötige Spalten entfernen
        cols = ['icao24', 'manufacturerName', 'model',
                'typecode', 'categoryDescription', 'operator']
        df = df[[c for c in cols if c in df.columns]]  # Sicherer Select

        if 'icao24' in df.columns:
            df = df.drop_duplicates(subset=['icao24'])

        # 3. Speichern
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        out_path = os.path.join(OUTPUT_DIR, OUTPUT_PARQUET)

        df.to_parquet(out_path, index=False)

        print(f"Erfolg! Gespeichert: {out_path}")
        print(f"Anzahl Flugzeuge: {len(df):,}")

    except Exception as e:
        print(f"❌ Fehler: {e}")


if __name__ == "__main__":
    main()
