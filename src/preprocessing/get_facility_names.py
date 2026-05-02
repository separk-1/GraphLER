import pandas as pd
import re
import json

# Load your excel
df = pd.read_excel("../../data/0430/processed/ler_structured.xlsx")

def normalize_plant_name(name):
    if not isinstance(name, str):
        return None

    name = name.lower()

    # remove unit info
    name = re.sub(r",?\s*unit\s*\d+", "", name)

    # remove extra spaces
    name = re.sub(r"\s+", " ", name)

    return name.strip()

facility_names = set()

for name in df["Facility Name"]:
    if name != "Not Found":
        cleaned = normalize_plant_name(name)
        if cleaned:
            facility_names.add(cleaned)

# convert to sorted list
facility_names = sorted(facility_names)

# save json
with open("../../data/facility_names.json", "w", encoding="utf-8") as f:
    json.dump({"facility_names": facility_names}, f, indent=2)

print(f"Total facility_names: {len(facility_names)}")