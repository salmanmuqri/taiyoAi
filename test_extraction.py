"""
Test script to verify detailed field extraction from pageInd.html
"""

from bs4 import BeautifulSoup
from models import ProjectDetail
import json

# Read the local HTML file
with open('pageInd.html', 'r', encoding='utf-8') as f:
    html = f.read()

soup = BeautifulSoup(html, 'lxml')

# Parse ALL dt/dd pairs
pds_data = {}
all_dls = soup.find_all('dl', class_='pds')

for dl in all_dls:
    dt_elements = dl.find_all('dt', class_='col-md-3')
    dd_elements = dl.find_all('dd', class_='col-md-9')
    
    for dt, dd in zip(dt_elements, dd_elements):
        key = dt.get_text(strip=True)
        value = dd.get_text(strip=True)
        pds_data[key] = value

print("=" * 80)
print("EXTRACTED FIELDS FROM pageInd.html")
print("=" * 80)

for key, value in pds_data.items():
    # Truncate long values
    display_value = value[:100] + '...' if len(value) > 100 else value
    print(f"{key}: {display_value}")

print("\n" + "=" * 80)
print("KEY FIELDS CHECK")
print("=" * 80)

required_fields = [
    'Project Name',
    'Project Number', 
    'Country / Economy',
    'Project Status',
    'Project Type / Modality of Assistance',
    'Source of Funding / Amount',
    'Sector / Subsector',
    'Impact',
    'Outcome',
    'Outputs',
    'Geographical Location',
    'Responsible ADB Officer',
    'Responsible ADB Department',
    'Responsible ADB Division',
    'Concept Clearance',
    'Last PDS Update'
]

for field in required_fields:
    value = pds_data.get(field, 'NOT FOUND')
    status = "✓" if field in pds_data else "✗"
    print(f"{status} {field}: {value[:50] if isinstance(value, str) else value}")

print("\n" + "=" * 80)
print("TOTAL FIELDS EXTRACTED:", len(pds_data))
print("=" * 80)
