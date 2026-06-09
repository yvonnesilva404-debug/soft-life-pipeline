"""Read Supabase export CSV, apply detection, generate UPDATE SQL."""
import csv, sys
sys.path.insert(0, '.')
import pipeline

CSV = r'C:\Users\User\Downloads\softlifecreed-jobs-2026-06-09.csv'

with open(CSV, encoding='utf-8') as f:
    rows = list(csv.DictReader(f))

print(f"Read {len(rows)} jobs from Supabase export")

# Apply detection
updates = []
for row in rows:
    url = (row.get('url') or '').strip()
    if not url:
        continue
    title = (row.get('title') or '').strip()
    location = (row.get('location') or '').strip()
    pt = pipeline._is_part_time_job(title)
    nt = pipeline._is_night_time_job(title)
    nu = pipeline._is_non_us_location(location)
    updates.append((url.replace("'", "''"), pt, nt, nu))

# Count
pt_count = sum(1 for _, pt, _, _ in updates if pt)
nt_count = sum(1 for _, _, nt, _ in updates if nt)
nu_count = sum(1 for _, _, _, nu in updates if nu)
print(f"Detected: Part Time={pt_count}, Night Time={nt_count}, Non-US={nu_count}")

# Generate a SQL file for Supabase UPDATE
sql_path = r'C:\Users\User\Documents\work\soft-life-pipeline\fix_labels.sql'
with open(sql_path, 'w') as f:
    f.write('-- Fix detection labels on all jobs\n')
    f.write('BEGIN;\n')
    for url, pt, nt, nu in updates:
        f.write(f"UPDATE jobs SET part_time={str(pt).lower()}, night_time={str(nt).lower()}, non_us={str(nu).lower()} WHERE url='{url}';\n")
    f.write('COMMIT;\n')

print(f"Wrote {len(updates)} UPDATE statements to {sql_path}")
print(f"\nRun with: supabase db query --linked --file {sql_path}")
