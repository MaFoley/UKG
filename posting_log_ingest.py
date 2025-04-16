import sqlite3
from pathlib import Path
import pandas as pd

DB_PATH = "DataFiles/PostResult/posting_log.db"
LOG_DIR = Path("DataFiles/PostResult")

def get_posted_keys():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS post_log (
        Id INTEGER PRIMARY KEY AUTOINCREMENT,
        EmpNo TEXT NOT NULL,
        WorkDate TEXT NOT NULL,
        PrnCode TEXT NOT NULL,
        Status TEXT,
        Response TEXT,
        SourceFile TEXT
    )
""")
    conn.commit()

    for file in LOG_DIR.glob("PostResults_*.csv"):
        print(f"🧾 Ingesting: {file.name}")
        df = pd.read_csv(file)
        source_name = file.name
        for _, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO post_log (EmpNo, WorkDate, PrnCode, Status, Response, SourceFile)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    str(row["EmpNo"]).strip().zfill(6),
                    str(row["WorkDate"]).strip(),
                    str(row["PrnCode"]).strip(),
                    str(row["Status"]).strip(),
                    str(row["Response"]).strip(),
                    source_name,
                ))
            except Exception as e:
                print(f"Error inserting row from {source_name}: {e}")
    conn.commit()

    cursor.execute("""
    SELECT EmpNo, WorkDate, PrnCode FROM post_log
    WHERE Status = '201'
""")
    posted = {(row[0], row[1], row[2]) for row in cursor.fetchall()}
    conn.close()
    return posted

