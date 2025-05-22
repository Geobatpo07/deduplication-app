import os
from pathlib import Path

# Répertoire racine du projet
BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Répertoire de données
DATA_DIR = BASE_DIR / "data"

# Fichiers pour Dedupe
TRAINING_FILE = DATA_DIR / "training.json"
SETTINGS_FILE = DATA_DIR / "dedupe_settings"
DUCKDB_FILE = DATA_DIR / "deduplication.duckdb"
