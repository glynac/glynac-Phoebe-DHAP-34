import os
import pytest
from pathlib import Path

# path config 
BASE_DIR = Path(__file__).parent.parent
BRONZE_DIR = BASE_DIR / "config/bronze"
SILVER_DIR = BASE_DIR / "config/silver"

# state initial list
bronze_items = []

def get_bronze_data():
    global bronze_items
    data = []
    if not BRONZE_DIR.exists():
        return data
    for service in sorted([d for d in BRONZE_DIR.iterdir() if d.is_dir()]):
        for yaml_file in sorted(service.glob("*.yaml")):
            data.append({
                "service": service.name,
                "table": yaml_file.stem
            })
    bronze_items = data
    return data

# calculation
counter = {"exists": 0, "missing": 0, "current": 0}

print(f"\n\n{' SERVICE ': <15} | {' TABLE NAME ': <30} | {' STATUS '}")
print("-" * 65)

@pytest.mark.parametrize("item", get_bronze_data())
def test_sync_bronze_to_silver(item):
    service = item['service']
    table = item['table']
    
    table_path = SILVER_DIR / service / table
    exists = table_path.exists() and table_path.is_dir()
    
    if exists:
        status = "✅ EXISTS"
        counter["exists"] += 1
    else:
        status = "❌ MISSING"
        counter["missing"] += 1
    
    # print status per item
    print(f"{service: <15} | {table: <30} | {status}")
    
    # log progress and final summary
    counter["current"] += 1
    if counter["current"] == len(bronze_items):
        total = counter["exists"] + counter["missing"]
        print("\n" + "="*65)
        print(f" FINAL SUMMARY REPORT")
        print("="*65)
        print(f" TOTAL BRONZE TABLES : {total}")
        print(f" ✅ SILVER EXISTS      : {counter['exists']}")
        print(f" ❌ SILVER MISSING     : {counter['missing']}")
        print("="*65)