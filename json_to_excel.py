import pandas as pd
import json
import os

# Input / Output paths
input_file = r"D:\Mansi\Other_Project\GRPumps\gr_pumps_06112025_new1.json"
output_base = r"D:\Mansi\Other_Project\GRPumps\output\GRPUMPS_06112025"
output_json = f"{output_base}.json"
output_excel = f"{output_base}.xlsx"

# 1️⃣ Read JSON
with open(input_file, "r", encoding="utf-8") as f:
    data = json.load(f)

# 2️⃣ Normalize release_date (replace [] → None)
for item in data:
    if "release_date" in item and isinstance(item["release_date"], list) and len(item["release_date"]) == 0:
        item["release_date"] = None

for item in data:
    if "extra_textual_info" in item and isinstance(item["extra_textual_info"], str) and "www.grpumps.com/product/pump/" in item['metadata']['data_source_url']:
        item["extra_textual_info"] = None
for item in data:
    if "manuals" in item and isinstance(item["manuals"],list) and item["manuals"] !=[]:
        for i in item["manuals"]:
            if i["description"] == "":
                i["description"] = None
            else:
                pass
for item in data:
    if "attachments" in item and isinstance(item["attachments"],list) and item["attachments"] !=[]:
        for i in item["attachments"]:
            if i["description"] == "":
                i["description"] = None
            else:pass

# 3️⃣ Add 'pdp_url' from metadata if available
for item in data:
    if "metadata" in item and isinstance(item["metadata"], dict):
        item["pdp_url"] = item["metadata"].get("data_source_url", "NA")

# 4️⃣ Convert nested lists/dicts to JSON strings for Excel
processed_data = []
for item in data:
    clean_item = {}
    for key, value in item.items():
        if isinstance(value, (dict, list)):
            clean_item[key] = json.dumps(value, ensure_ascii=False)
        else:
            clean_item[key] = value
    processed_data.append(clean_item)

# 5️⃣ Convert to DataFrame and export to Excel
df = pd.DataFrame(processed_data)
df.to_excel(output_excel, index=False)

# 6️⃣ Save updated JSON too
with open(output_json, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=4)

print(f"✅ JSON saved at: {output_json}")
print(f"✅ Excel saved at: {output_excel}")
