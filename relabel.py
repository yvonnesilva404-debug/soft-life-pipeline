"""Fix detection labels in existing softlife.csv + reject.csv without re-scoring."""
import csv, sys
sys.path.insert(0, '.')
import pipeline

for fname in ['softlife.csv', 'reject.csv']:
    path = 'C:\\Users\\User\\Documents\\work\\soft-life-pipeline\\' + fname
    with open(path, encoding='utf-8') as f:
        rows = list(csv.DictReader(f))

    for row in rows:
        title = (row.get('Title') or '').strip()
        location = (row.get('Location') or '').strip()
        row['Part Time']  = "TRUE" if pipeline._is_part_time_job(title) else ""
        row['Night Time'] = "TRUE" if pipeline._is_night_time_job(title) else ""
        row['Non US']     = "TRUE" if pipeline._is_non_us_location(location) else ""

    fieldnames = rows[0].keys()
    with open(path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    pt = sum(1 for r in rows if r.get('Part Time') == 'TRUE')
    nt = sum(1 for r in rows if r.get('Night Time') == 'TRUE')
    nu = sum(1 for r in rows if r.get('Non US') == 'TRUE')
    print(f"{fname}: {len(rows)} rows | PT={pt} NT={nt} NU={nu}")

print("Done. Now regenerate JSON + SQL and upload.")
