import json
import time
import os
from datetime import datetime

class PipelineLogger:
    def __init__(self, log_file):
        self.log_file = log_file
        # Datei initialisieren (leeres Array), falls sie nicht existiert oder leer ist
        if not os.path.exists(log_file) or os.path.getsize(log_file) == 0:
            with open(log_file, 'w') as f:
                json.dump([], f)

    def log_metric(self, stage, metric_name, value, unit, remarks=""):
        entry = {
            "timestamp": time.time(),
            "iso_date": datetime.now().isoformat(),
            "stage": stage,
            "metric": metric_name,
            "value": value,
            "unit": unit,
            "remarks": remarks
        }

        # Atomares Lesen/Schreiben simulieren
        current_logs = []
        if os.path.exists(self.log_file) and os.path.getsize(self.log_file) > 0:
            try:
                with open(self.log_file, 'r') as f:
                    current_logs = json.load(f)
            except json.JSONDecodeError:
                pass

        current_logs.append(entry)

        with open(self.log_file, 'w') as f:
            json.dump(current_logs, f, indent=2)

        print(f"[{stage}] {metric_name}: {value} {unit} ({remarks})")