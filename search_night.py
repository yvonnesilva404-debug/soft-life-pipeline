import csv

with open(r'C:\Users\User\Downloads\softlifecreed-jobs-2026-06-09.csv', encoding='utf-8') as f:
    rows = list(csv.DictReader(f))

night_hints = ['night', 'evening', 'graveyard', 'overnight', 'shift', 'on-call', 'on call', 'after hours', '2nd shift', '3rd shift', 'midnight']

found = []
for i, row in enumerate(rows):
    title = (row.get('title') or '').lower()
    loc = (row.get('location') or '').lower()
    combined = title + ' ' + loc
    for h in night_hints:
        if h in combined:
            found.append((i, h, row.get('title','')[:60], row.get('location','')[:40]))
            break

print(f"Found {len(found)} jobs with night hints in title/location:")
for i, h, t, l in found:
    print(f"  [{h:12s}] {t:60s} | {l}")
