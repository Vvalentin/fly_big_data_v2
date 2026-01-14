import time
import psutil
import csv
import os
import sys
from datetime import datetime

# Konfiguration
LOG_FILE = "data/system_metrics.csv"
STAGE_FILE = "data/current_stage.txt"
INTERVAL = 0.5

def run_monitor():
    # 1. Init: Dateien vorbereiten
    # Sicherstellen, dass data Ordner existiert
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

    # CSV Header schreiben
    with open(LOG_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'stage', 'cpu_percent', 'ram_percent', 'ram_gb', 'net_sent_mb', 'net_recv_mb'])

    # Baseline für Netzwerk
    last_net = psutil.net_io_counters()

    print(f"--- [Monitor Process] Gestartet. Logge in {LOG_FILE} ---")

    try:
        while True:
            # 2. Stage lesen (vom Spark Driver geschrieben)
            current_stage = "Init"
            if os.path.exists(STAGE_FILE):
                try:
                    with open(STAGE_FILE, 'r') as f:
                        current_stage = f.read().strip()
                except:
                    pass # Falls Datei gerade gesperrt ist, ignorieren

            # 3. Messen
            cpu = psutil.cpu_percent(interval=None)
            ram = psutil.virtual_memory()
            net = psutil.net_io_counters()

            # Netzwerk Diff berechnen
            sent_mb = (net.bytes_sent - last_net.bytes_sent) / (1024 * 1024)
            recv_mb = (net.bytes_recv - last_net.bytes_recv) / (1024 * 1024)
            last_net = net

            # 4. Schreiben
            with open(LOG_FILE, 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    datetime.now().isoformat(),
                    current_stage,
                    cpu,
                    ram.percent,
                    round(ram.used / (1024**3), 2),
                    round(sent_mb, 2),
                    round(recv_mb, 2)
                ])

            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        print("--- [Monitor Process] Beendet ---")

if __name__ == "__main__":
    run_monitor()