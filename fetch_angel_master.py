import requests
import json
import os
from datetime import datetime
import sys

# Ensure we can import from the parent directory if needed
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
import config

# ==========================================
# ANGEL ONE MICRO-DB GENERATOR (CRON AUTOMATION)
# ==========================================
# CRON JOB: 30 8 * * 1-5 cd /home/ubuntu/relay && python3 fetch_angel_master.py
# ==========================================

URL = "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json"
TEMP_FILE = os.path.join(config.RAW_DATA_DIR, "OpenAPIScripMaster.json")
MCX_FUTURES_OUT = os.path.join(config.ANGEL_SEGMENTS_DIR, "angel_mcx_futures_master.json")
MCX_OPTIONS_OUT = os.path.join(config.ANGEL_SEGMENTS_DIR, "angel_mcx_options_master.json")
NSE_FUTURES_OUT = os.path.join(config.ANGEL_SEGMENTS_DIR, "angel_nse_futures_master.json")
NSE_OPTIONS_OUT = os.path.join(config.ANGEL_SEGMENTS_DIR, "angel_nse_options_master.json")

def run_sync():
    print(f"[{datetime.now()}] Step 1: Downloading 37MB Master File to {config.RAW_DIR}...")
    
    # Ensure directories exist (Safety)
    os.makedirs(config.RAW_DIR, exist_ok=True)
    os.makedirs(config.SEGMENTS_DIR, exist_ok=True)

    try:
        response = requests.get(URL, stream=True)
        response.raise_for_status()
        with open(TEMP_FILE, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
    except Exception as e:
        print(f"[{datetime.now()}] Error downloading file: {e}")
        return

    print(f"[{datetime.now()}] Step 2: Parsing & Segregating Master Data...")
    
    mcx_futures = []
    mcx_options = []
    nse_futures = []
    nse_options = []
    
    try:
        with open(TEMP_FILE, "r") as f:
            data = json.load(f)
            
            for item in data:
                exch = item.get("exch_seg")
                itype = item.get("instrumenttype")
                symbol = str(item.get("symbol", ""))

                # MCX Logic
                if exch == "MCX":
                    if itype == "FUTCOM" or symbol.endswith("FUT"):
                        mcx_futures.append({"name": item.get("name"), "symbol": symbol, "token": item.get("token"), "expiry": item.get("expiry")})
                    elif itype == "OPTCOM" or symbol.endswith("CE") or symbol.endswith("PE"):
                        mcx_options.append({"name": item.get("name"), "symbol": symbol, "token": item.get("token"), "expiry": item.get("expiry"), "strike": item.get("strike")})
                
                # NSE Logic (Filter for Futures & Options)
                elif exch == "NFO":
                    if itype == "OPTSTK" or itype == "OPTIDX":
                        nse_options.append({"name": item.get("name"), "symbol": symbol, "token": item.get("token"), "expiry": item.get("expiry"), "strike": item.get("strike")})
                    elif itype == "FUTSTK" or itype == "FUTIDX":
                        nse_futures.append({"name": item.get("name"), "symbol": symbol, "token": item.get("token"), "expiry": item.get("expiry")})

    except Exception as e:
        print(f"[{datetime.now()}] Error parsing file: {e}")
        return
        
    print(f"[{datetime.now()}]   -> MCX: {len(mcx_futures)} Futures, {len(mcx_options)} Options.")
    print(f"[{datetime.now()}]   -> NSE: {len(nse_futures)} Futures, {len(nse_options)} Options.")

    # Save segments
    with open(MCX_FUTURES_OUT, "w") as f: json.dump(mcx_futures, f)
    with open(MCX_OPTIONS_OUT, "w") as f: json.dump(mcx_options, f)
    with open(NSE_FUTURES_OUT, "w") as f: json.dump(nse_futures, f)
    with open(NSE_OPTIONS_OUT, "w") as f: json.dump(nse_options, f)

    print(f"[{datetime.now()}] Step 3: Cleanup. Deleting raw JSON...")
    if os.path.exists(TEMP_FILE):
        os.remove(TEMP_FILE)
        
    print(f"[{datetime.now()}] Refactored Sync Complete!")

if __name__ == "__main__":
    run_sync()
