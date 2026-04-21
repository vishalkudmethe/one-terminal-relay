from fastapi import APIRouter, Request, Depends, Query, Header
from fastapi.responses import JSONResponse, StreamingResponse
import httpx
import os
import json
import urllib.parse
import logging
from services.utils import get_db, parse_ot_context, AuditLog, HOP_BY_HOP_HEADERS
import config

logger = logging.getLogger(__name__)
router = APIRouter()
client = httpx.AsyncClient()

@router.get("/search-mcx")
async def search_mcx(type: str = Query("futures"), symbol: str = Query(None)):
    """Search within Angel One MCX Micro-DB Segments"""
    filename = "angel_mcx_futures_master.json" if type == "futures" else "angel_mcx_options_master.json"
    filepath = os.path.join(config.ANGEL_SEGMENTS_DIR, filename)

    if os.path.exists(filepath):
        try:
            with open(filepath, "r") as f:
                data = json.load(f)
                if type == "options" and symbol:
                    data = [item for item in data if item["name"] == symbol.upper()]
                return JSONResponse(data)
        except Exception as e:
            return JSONResponse({"error": "Failed to read data", "details": str(e)}, status_code=500)
    
    # Return empty list instead of 404 to prevent app-side resolve failures
    return JSONResponse([])

@router.get("/search-nse")
async def search_nse(type: str = Query("futures"), symbol: str = Query(None)):
    """Search within Angel One NSE F&O Micro-DB Segments"""
    filename = "angel_nse_futures_master.json" if type == "futures" else "angel_nse_options_master.json"
    filepath = os.path.join(config.ANGEL_SEGMENTS_DIR, filename)

    if os.path.exists(filepath):
        try:
            with open(filepath, "r") as f:
                data = json.load(f)
                if type == "options" and symbol:
                    data = [item for item in data if item["name"] == symbol.upper()]
                return JSONResponse(data)
        except Exception as e:
            return JSONResponse({"error": "Failed to read data", "details": str(e)}, status_code=500)
    
    # Return empty list instead of 404
    return JSONResponse([])

async def handle_angel_request(path: str, request: Request, db, xff, uuid):
    """Core Angel One Request Handler"""
    # Build Downstream URL
    clean_path = urllib.parse.unquote(path).lstrip('/') # Remove only leading slashes
    url = f"{config.BROKER_URLS['angel']}/{clean_path}"
    
    # Audit Log
    try:
        log = AuditLog(broker="angel", endpoint=path, method=request.method, client_ip=xff, device_uuid=uuid)
        db.add(log)
        db.commit()
    except Exception as e:
        logger.error(f"Audit Log Error: {e}")

    # Prepare Headers
    headers = {}
    for k, v in request.headers.items():
        k_low = k.lower()
        if k_low in HOP_BY_HOP_HEADERS or k_low in ["authorization", "x-ot-context"]: continue
        if k_low == "x-broker-authorization": headers["Authorization"] = v
        else: headers[k] = v

    # Proxy Call
    body = await request.body()
    
    # ── Broker-Specific Normalization: Angel One -EQ logic ─────────────
    # If placing an order or searching, ensure NSE equity has -EQ suffix
    if request.method == "POST" and body:
        try:
            payload = json.loads(body)
            changed = False
            
            # 1. Order Placement Logic
            if "tradingsymbol" in payload and payload.get("exchange") == "NSE":
                symbol = payload["tradingsymbol"]
                if not symbol.endswith("-EQ") and not any(idx in symbol for idx in ["NIFTY", "BANKNIFTY"]):
                    payload["tradingsymbol"] = f"{symbol}-EQ"
                    changed = True
            
            # 2. Search Scrip Logic
            if "searchscrip" in payload and payload.get("exchange") == "NSE":
                query = payload["searchscrip"]
                # Only append -EQ if it's a specific symbol (no weird chars, alphanumeric)
                if query.isalnum() and not query.endswith("-EQ") and not any(idx in query for idx in ["NIFTY", "BANKNIFTY"]):
                     payload["searchscrip"] = f"{query}-EQ"
                     changed = True
            
            if changed:
                body = json.dumps(payload).encode('utf-8')
                headers["Content-Length"] = str(len(body))
        except:
            pass # Non-JSON body or other parse error

    try:
        resp = await client.request(
            method=request.method, url=url, headers=headers, content=body, 
            params={k: v for k, v in request.query_params.items() if k not in ("broker", "path")},
            follow_redirects=False, timeout=30.0
        )

        final_headers = {k: v for k, v in resp.headers.items() if k.lower() not in HOP_BY_HOP_HEADERS}
        return StreamingResponse(iter([await resp.aread()]), status_code=resp.status_code, headers=final_headers)
    except httpx.ConnectError:
        return JSONResponse({"error": "Failed to connect to Angel One"}, status_code=502)
    except Exception as e:
        return JSONResponse({"error": "Internal Relay Error", "details": str(e)}, status_code=500)
