import csv, sys
sys.path.insert(0, '.')
import fetch as H
import pipeline

with open(r'C:\Users\User\Downloads\softlifecreed-jobs-2026-06-09.csv', encoding='utf-8') as f:
    rows = list(csv.DictReader(f))

# Pick likely night-shift candidates by index
candidates = [56, 75, 132, 152, 155, 160, 178, 186, 196, 273, 300, 301]
# Map indices back
indices = {}
for i, row in enumerate(rows):
    indices[i] = row

for idx in candidates:
    if idx not in indices:
        continue
    row = indices[idx]
    title = (row.get('title') or '').strip()
    url = (row.get('url') or '').strip()
    if not url:
        continue

    print(f"--- #{idx} {title[:65]}")

    # For real URLs, fetch description with timeout
    try:
        job = H._harvest_job_data(url, default_title=title)
        desc = (job.get('description') or '')[:800]
    except Exception as e:
        print(f"  FETCH FAILED: {e}")
        continue

    nt = pipeline._is_night_time_job(title, desc)
    if nt:
        print(f"  >>> NIGHT TIME DETECTED")
    else:
        desc_lower = desc.lower()
        hints = ['night', 'evening', 'overnight', 'graveyard', 'on-call', 'on call',
                 'after hours', '24/7', 'shift', 'weekend', 'rotating']
        found = [h for h in hints if h in desc_lower]
        print(f"  NT={'no' if not found else f'MISSED: {found}'}")
        if found:
            # Show context around first hit
            for h in found:
                pos = desc_lower.find(h)
                start = max(0, pos - 40)
                end = min(len(desc), pos + len(h) + 60)
                snippet = desc[start:end].replace('\n', ' ')
                print(f"    ...{snippet}...")
                break

    print()
