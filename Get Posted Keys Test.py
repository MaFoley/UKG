from posting_log_ingest import get_posted_keys

posted_entries = get_posted_keys()

if posted_entries:
    print("🔎 Sample posted entry:", next(iter(posted_entries)))
    print(f"🔢 Total posted entries: {len(posted_entries)}")
else:
    print("🫥 No posted entries found.")