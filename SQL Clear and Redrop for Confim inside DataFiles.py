import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = "DataFiles/PostResult/posting_log.db"
LOG_DIR = Path("DataFiles/PostResult")

def reset_posting_log():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Drop and recreate the table
    cursor.execute("DROP TABLE IF EXISTS post_log")
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS post_log (
        Id INTEGER PRIMARY KEY AUTOINCREMENT,
        EmpNo TEXT NOT NULL,
        WorkDate TEXT NOT NULL,
        PrnCode TEXT NOT NULL,
        Status TEXT,
        Response TEXT,
        Time_Id TEXT,
        SourceFile TEXT
    )
""")
    conn.commit()

    # Re-ingest all CSVs
    for file in LOG_DIR.glob("PostResults_*.csv"):
        df = pd.read_csv(file)
        for _, row in df.iterrows():
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO post_log (EmpNo, WorkDate, PrnCode, Status, Response, Time_Id, SourceFile)
                    VALUES (?, ?, ?, ?, ?,?,?)
                """, (
                    str(row["EmpNo"]).strip().zfill(6),
                    str(row["WorkDate"]).strip(),
                    str(row["PrnCode"]).strip(),
                    str(row["Status"]).strip(),
                    str(row["Time_Id"]).strip(),
                    str(row["Response"]).strip(),
                    file.name
                ))
            except Exception as e:
                print(f"⚠️ Error inserting from {file.name}: {e}")
    print(f"🧾 Ingested {len(df)} rows from {file.name}")

    conn.commit()
    conn.close()
    print("✅ Log DB wiped and rebuilt.")

if __name__ == "__main__":
    reset_posting_log()