import json
import urllib.request

url = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
print("Downloading...")
urllib.request.urlretrieve(url, "master.json")
print("Parsing...")

futures = []
options = []

with open("master.json", "r") as f:
    data = json.load(f)
    for item in data:
        if item.get("exch_seg") == "NFO":
            itype = item.get("instrumenttype", "")
            if itype.startswith("FUT") or str(item.get("symbol", "")).endswith("FUT"):
                futures.append(item)
            elif itype.startswith("OPT") or str(item.get("symbol", "")).endswith("CE") or str(item.get("symbol", "")).endswith("PE"):
                options.append(item)

print(f"NFO Futures size: {len(futures)}")
print(f"NFO Options size: {len(options)}")
print("Sample NFO Future:", futures[0] if futures else None)
print("Sample NFO Option:", options[0] if options else None)
