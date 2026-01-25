# OpenSky Flight Data Pipeline

Dieses Projekt realisiert eine komplette ETL-Pipeline für Flugdaten des OpenSky Networks. Es nutzt Apache Spark für die Verarbeitung massiver Datenströme und Python für die Analyse.

Das Ziel ist es, Rohdaten (States) und Metadaten (Flugzeugtypen) zu laden, zu bereinigen und für analytische Zwecke zu nutzen.

---

## Übersicht der Skripte (src/ Ordner)

Alle Skripte zur Datenaufbereitung befinden sich im `src` Ordner. Sie müssen **vor** der Analyse ausgeführt werden.

### 1. spark_ingest_s3.py (Haupt-ETL-Prozess)
Dies ist das Kernstück der Pipeline.
* **Funktion:** Verbindet sich mit dem OpenSky S3-Bucket und lädt Flugbewegungsdaten (States) aus .tar-Archiven (unterstützt Avro und CSV Formate).
* **Verarbeitung:** Nutzt Apache Spark (PySpark), um die Datenströme zu parsen, Datentypen zu korrigieren (Casting) und fehlerhafte Zeilen zu filtern.
* **Automation:** Startet automatisch den `system_monitor.py` als Hintergrundprozess, um während der Berechnung die Systemlast (CPU/RAM) zu protokollieren.
* **Output:** Speichert die bereinigten Daten als Parquet-Dateien im Ordner `data/processed/run_[TIMESTAMP]`.

### 2. aircraft_type_ingest_s3.py (Stammdaten)
Dieses Skript reichert die Analyse mit Kontext an.
* **Funktion:** Lädt die `aircraft-database-complete` (CSV) herunter.
* **Logik:** Sucht im S3-Bucket nach der aktuellsten Version (z.B. 2025-08).
* **Bereinigung:** Entfernt Duplikate basierend auf der ICAO24-Adresse.
* **Output:** Konvertiert die CSV in eine performante Parquet-Datei unter `data/external/aircraft_database.parquet`.

### 3. system_monitor.py (System-Überwachung)
Ein Hilfsskript für Performance-Analysen.
* **Funktion:** Überwacht in definierten Intervallen CPU-Auslastung, RAM-Verbrauch und Netzwerkverkehr.
* **Wichtig:** Dieses Skript muss **nicht manuell** gestartet werden. Es wird vom Hauptskript automatisch gesteuert.
* **Output:** Schreibt Metriken in `data/system_metrics.csv`.

### 4. custom_logger.py (Logging-Modul)
Ein Hilfsmodul für strukturiertes Logging.
* **Funktion:** Stellt eine Klasse bereit, um Pipeline-Status und Metriken im JSON-Format zu speichern.
* **Output:** Schreibt Logs in `data/pipeline_metrics.json`.

---

## Ablauf der Ausführung

**Schritt 1: Requierments installieren**

* Erstelle eine virtuelle Umgebung (.venv) und installiere darin alle notwendigen Abhängigkeiten mit dem Befehl **pip install -r requirements.txt**

**Schritt 2: Flugdaten (States) verarbeiten**

* Führe dazu die Datei **src/spark_ingest_s3.py** aus.

**Schritt 3: Flugzeug-Datenbank laden**

* Führe die Datei **src/aircraft_type_ingest_s3.py** aus.

**Datenstruktur:**

* Nach erfolgreicher Ausführung sieht die Ordnerstruktur wie folgt aus:
    * `data/processed/`: Enthält die Ordner der einzelnen Spark-Runs mit den Flugdaten (Parquet).
    * `data/external/`: Enthält die `aircraft_database.parquet`.
    * `data/system_metrics.csv`: Performance-Log des letzten Durchlaufs.
    * `data/pipeline_metrics.json`: Detailliertes Log der ETL-Schritte.

**Schritt 4: Notebooks**

* Sobald alle Daten bereitliegen, können die Analysen gestartet werden. Die Jupyter Notebooks befinden sich im Ordner notebooks/ und sind in die Bereiche Data Validation, Download Performance und Use Cases unterteilt.