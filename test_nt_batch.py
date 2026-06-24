"""Run night-time detection on real job descriptions from a sample."""
import csv, sys
sys.path.insert(0, '.')
import fetch as H
import pipeline

# Grab 10 random-ish jobs from the download CSV
with open(r'C:\Users\User\Downloads\softlifecreed-jobs-2026-06-09.csv', encoding='utf-8') as f:
    rows = list(csv.DictReader(f))

# Pick some with varied titles (skip first 5, then every ~30th)
samples = []
for i in range(5, len(rows), 31):
    samples.append(rows[i])
    if len(samples) >= 10:
        break

print(f"Checking {len(samples)} jobs with real description fetch...\n")
for row in samples:
    title = (row.get('title') or '').strip()
    url = (row.get('url') or '').strip()
    if not url:
        continue

    print(f"Title: {title[:60]}")
    print(f"  URL: {url[:70]}")

    # Fetch description
    job = H._harvest_job_data(url, default_title=title)
    desc = (job.get('description') or '')[:500]

    # Check night time
    nt_by_title = pipeline._is_night_time_job(title)
    nt_by_both = pipeline._is_night_time_job(title, desc)

    if nt_by_title:
        print(f"  >>> NIGHT (title): YES")
    elif nt_by_both:
        print(f"  >>> NIGHT (description): YES")
    elif desc:
        # Show snippet if something night-related is in desc but missed
        desc_lower = desc.lower()
        hints = ['night', 'evening', 'overnight', 'graveyard', 'shift', 'on-call', 'on call', '24/7', 'rotating']
        found = [h for h in hints if h in desc_lower]
        if found:
            print(f"  Description hints: {found}")
            print(f"  Snippet: {desc[:200]}")
        else:
            print(f"  No night hints found in description")
    else:
        print(f"  No description available")

    print()
