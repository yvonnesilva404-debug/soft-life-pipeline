import csv

with open(r'C:\Users\User\Downloads\softlifecreed-jobs-2026-06-09.csv', encoding='utf-8') as f:
    rows = list(csv.DictReader(f))

keywords = ['nurse', 'support', 'technician', 'associate', 'coordinator',
            'operator', 'advisor', 'advocate', 'assistant', 'representative',
            'clinician', 'therapist', 'medical', 'health', 'care', 'patient',
            'provider', 'pharmacist', 'lab', 'responder', 'dispatch',
            'security', 'safety', 'surveillance', 'monitor', 'operator',
            'veterinary', 'veterinarian', 'hospital', 'clinic',
            'driver', 'logistics', 'warehouse', 'manufacturing',
            'custodian', 'janitor', 'maintenance', 'repair',
            'call center', 'customer service', 'help desk',
            'triage', 'emergency', 'crisis', 'hotline']

seen = set()
for i, row in enumerate(rows):
    title = (row.get('title') or '').lower()
    loc = (row.get('location') or '').lower()
    combined = title
    for kw in keywords:
        if kw in combined:
            key = row.get('title','')[:60]
            if key not in seen:
                seen.add(key)
                print(f'{i:5d} {row.get("title","")[:70]:70s} | {row.get("location","")[:35]}')
            break
