import sqlite, csv, pathlib

ROOT = pathlib.Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "envs" / "sqlite" / "tiny.db"
DATA_DIR = ROOT / "datasets" / "tiny"