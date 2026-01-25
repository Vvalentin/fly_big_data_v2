import os
import boto3
import pandas as pd
from dotenv import load_dotenv
from botocore.handlers import disable_signing

load_dotenv()

# S3 Einstellungen
BUCKET_NAME = os.getenv("BUCKET_NAME", "data-samples")
ENDPOINT_URL = os.getenv("S3_ENDPOINT", "https://s3.opensky-network.org")

# Datei-Einstellungen
TARGET_FILENAME = "aircraft-database-complete-2025-08.csv"
OUTPUT_DIR = "data/external"
OUTPUT_PARQUET = "aircraft_database.parquet"

def get_s3_client():
    s3 = boto3.client('s3', endpoint_url=ENDPOINT_URL)
    s3.meta.events.register('choose-signer.s3.*', disable_signing)
    return s3

def main():
    print(f"--- START: Download {TARGET_FILENAME} ---")
    
    s3 = get_s3_client()
    found_key = None

    # 1. DATEI SUCHEN
    print(f"Suche Datei in Bucket '{BUCKET_NAME}'...")
    
    # Check 1: Direkt in metadata/
    candidate = f"metadata/{TARGET_FILENAME}"
    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=candidate)
        found_key = candidate
    except:
        # Check 2: Im Root
        try:
            s3.head_object(Bucket=BUCKET_NAME, Key=TARGET_FILENAME)
            found_key = TARGET_FILENAME
        except:
            pass

    # Fallback: Falls der Name doch leicht anders ist
    if not found_key:
        print("⚠️ Exakter Name nicht gefunden. Suche fuzzy nach '2025-08'...")
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix="metadata/"):
            if 'Contents' not in page: continue
            for obj in page['Contents']:
                if "2025-08" in obj['Key'] and obj['Key'].endswith(".csv"):
                    found_key = obj['Key']
                    break
            if found_key: break

    if not found_key:
        print("Datei absolut nicht gefunden. Prüfe den Namen im S3 Browser.")
        return

    print(f"Gefunden: {found_key}")

    # 2. DOWNLOAD & PARQUET CONVERT
    print("⬇Lade herunter und konvertiere zu Parquet...")
    
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=found_key)
        
        # Direktes Lesen mit Pandas
        df = pd.read_csv(
            obj['Body'],
            dtype=str,            
            quotechar="'",         
            skipinitialspace=True,
            encoding='utf-8',
            on_bad_lines='skip'
        )
        
        # Nur relevante Spalten behalten
        cols_to_keep = ['icao24', 'manufacturerName', 'model', 'typecode', 'categoryDescription', 'operator']
        existing_cols = [c for c in cols_to_keep if c in df.columns]
        if existing_cols:
            df = df[existing_cols]

        # Duplikate bei icao24 entfernen
        if 'icao24' in df.columns:
            df = df.drop_duplicates(subset=['icao24'])

        # 3. SPEICHERN
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        out_path = os.path.join(OUTPUT_DIR, OUTPUT_PARQUET)
        
        df.to_parquet(out_path, index=False)
        
        print(f"Fertig! Gespeichert als: {out_path}")
        print(f"   -> Anzahl Flugzeuge: {len(df):,}")
        
    except Exception as e:
        print(f"❌ Fehler: {e}")

if __name__ == "__main__":
    main()