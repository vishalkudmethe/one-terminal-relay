import requests
import json
import os
import csv
import io
from datetime import datetime
import config

# ==========================================
# UNIFIED MICRO-DB REFRESHER (CRON READY)
# ==========================================

def save_json(data, broker, filename):
    filepath = os.path.join(config.get_data_dir(broker), filename)
    with open(filepath, "w") as f:
        json.dump(data, f)
    print(f"  -> Saved {len(data)} items to {filename}")

def refresh_angel():
    print(f"\n[{datetime.now()}] Refreshing Angel One...")
    url = config.SCRIP_MASTER_URLS["angel"]
    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        
        nse_futures, nse_options = [], []
        mcx_futures, mcx_options = [], []

        for item in data:
            exch = item.get("exch_seg")
            itype = item.get("instrumenttype")
            symbol = str(item.get("symbol", ""))

            if exch == "MCX":
                if itype == "FUTCOM" or symbol.endswith("FUT"):
                    mcx_futures.append({"name": item.get("name"), "symbol": symbol, "token": item.get("token"), "expiry": item.get("expiry")})
                elif itype == "OPTCOM" or symbol.endswith("CE") or symbol.endswith("PE"):
                    mcx_options.append({"name": item.get("name"), "symbol": symbol, "token": item.get("token"), "expiry": item.get("expiry"), "strike": item.get("strike")})
            elif exch == "NFO":
                if itype in ["OPTSTK", "OPTIDX"]:
                    nse_options.append({"name": item.get("name"), "symbol": symbol, "token": item.get("token"), "expiry": item.get("expiry"), "strike": item.get("strike")})
                elif itype in ["FUTSTK", "FUTIDX"]:
                    nse_futures.append({"name": item.get("name"), "symbol": symbol, "token": item.get("token"), "expiry": item.get("expiry")})
        
        save_json(mcx_futures, "angel", "angel_mcx_futures_master.json")
        save_json(mcx_options, "angel", "angel_mcx_options_master.json")
        save_json(nse_futures, "angel", "angel_nse_futures_master.json")
        save_json(nse_options, "angel", "angel_nse_options_master.json")
    except Exception as e:
        print(f"  [!] Angel Error: {e}")

def refresh_fyers():
    print(f"\n[{datetime.now()}] Refreshing Fyers V3 (CSV to JSON)...")
    for segment, url in config.SCRIP_MASTER_URLS["fyers"].items():
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            reader = csv.reader(io.StringIO(resp.text))
            
            output_data = []
            for row in reader:
                if not row or len(row) < 10: continue
                # Fyers V3 CSV: fyToken, symbolDetails, exchangeToken, ticker, isin, expiry, strike, lot, type, segment, tick
                output_data.append({
                    "token": row[0],
                    "symbol": row[1],
                    "ticker": row[3],
                    "expiry": row[5],
                    "strike": row[6],
                    "lot": row[7],
                    "type": row[8]
                })
            
            # Sub-segmenting
            if segment == "nse_fo":
                futures = [i for i in output_data if "FUT" in i["type"]]
                options = [i for i in output_data if "OPT" in i["type"]]
                save_json(futures, "fyers", "fyers_nse_futures_master.json")
                save_json(options, "fyers", "fyers_nse_options_master.json")
            elif segment == "mcx_fo":
                save_json(output_data, "fyers", "fyers_mcx_master.json")
            else:
                save_json(output_data, "fyers", f"fyers_{segment}_master.json")
                
        except Exception as e:
            print(f"  [!] Fyers Error ({segment}): {e}")

def refresh_upstox():
    print(f"\n[{datetime.now()}] Refreshing Upstox V2 (JSON)...")
    for segment, url in config.SCRIP_MASTER_URLS["upstox"].items():
        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            # Upstox V2 structure is often {"status": "success", "data": [...]}
            items = data.get("data", []) if isinstance(data, dict) else data
            
            processed = []
            for item in items:
                processed.append({
                    "token": item.get("instrument_key"),
                    "symbol": item.get("tradingsymbol") or item.get("symbol"),
                    "name": item.get("name"),
                    "expiry": item.get("expiry"),
                    "strike": item.get("strike_price"),
                    "lot": item.get("lot_size"),
                    "type": item.get("instrument_type")
                })
            
            save_json(processed, "upstox", f"upstox_{segment}_master.json")
        except Exception as e:
            print(f"  [!] Upstox Error ({segment}): {e}")

if __name__ == "__main__":
    refresh_angel()
    refresh_fyers()
    refresh_upstox()
    print(f"\n[{datetime.now()}] All Masters Refreshed Successfully.")
