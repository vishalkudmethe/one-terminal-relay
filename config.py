import os

# ---- Configuration ----
GATEWAY_SECRET = os.getenv("GATEWAY_SECRET", "super-secret-relay-token-change-me")
DATABASE_URL = "sqlite:///./audit_log.db"

BROKER_URLS = {
    "angel": "https://apiconnect.angelbroking.com",
    "angelone": "https://apiconnect.angelbroking.com",
    "binance": "https://api.binance.com",
    "dhan": "https://api.dhan.co",
    "fyers": "https://api-t1.fyers.in",
    "upstox": "https://api.upstox.com",
    "zerodha": "https://api.zerodha.com"
}

# Version for Health Check
VERSION = "2.5.1"

# Data Paths
BASE_RELAY_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_ROOT = os.path.join(BASE_RELAY_DIR, "data", "market_data")

def get_data_dir(broker: str, subfolder: str = "segments"):
    """Helper to get standardized data paths"""
    path = os.path.join(DATA_ROOT, broker, subfolder)
    os.makedirs(path, exist_ok=True)
    return path

# Set specific directory paths
ANGEL_SEGMENTS_DIR = get_data_dir("angel", "segments")
RAW_DATA_DIR = os.path.join(BASE_RELAY_DIR, "data", "raw")
os.makedirs(RAW_DATA_DIR, exist_ok=True)

# Aliases for older scripts
RAW_DIR = RAW_DATA_DIR
SEGMENTS_DIR = ANGEL_SEGMENTS_DIR

SCRIP_MASTER_URLS = {
    "angel": "https://margincalculator.angelbroking.com/OpenAPI_File/files/OpenAPIScripMaster.json",
    "fyers": {
        "nse_fo": "https://public.fyers.in/sym_details/NSE_FO.csv",
        "mcx_fo": "https://public.fyers.in/sym_details/MCX_FO.csv",
        "nse_eq": "https://public.fyers.in/sym_details/NSE_EQ.csv"
    },
    "upstox": {
        "nse_fo": "https://api.upstox.com/v2/market-details/v2/instrument/NSE_FO",
        "mcx_fo": "https://api.upstox.com/v2/market-details/v2/instrument/MCX_FO",
        "nse_eq": "https://api.upstox.com/v2/market-details/v2/instrument/NSE_EQ"
    }
}
