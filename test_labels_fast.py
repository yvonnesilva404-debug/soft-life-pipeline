import csv, sys, pathlib
sys.path.insert(0, '.')
import pipeline

rows = list(csv.DictReader(open('sample30.csv')))

header = f"{'#':3s} {'Title':50s} {'Location':45s} {'PT':4s} {'NT':4s} {'NU':4s}"
print(header)
print('-' * 115)
for i, row in enumerate(rows, 1):
    title = (row.get("Title") or "").strip()
    location = (row.get("Location") or "").strip()
    pt = 'PT' if pipeline._is_part_time_job(title) else ''
    nt = 'NT' if pipeline._is_night_time_job(title) else ''
    nu = 'NU' if pipeline._is_non_us_location(location) else ''
    t = title[:48]
    l = location[:43]
    print(f'{i:3d} {t:50s} {l:45s} {pt:4s} {nt:4s} {nu:4s}')
