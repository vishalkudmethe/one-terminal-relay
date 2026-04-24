from fastapi import APIRouter, Request, Depends, Query, Header
from fastapi.responses import JSONResponse, StreamingResponse
import httpx
import os
import json
import urllib.parse
from services.utils import get_db, parse_ot_context, AuditLog, HOP_BY_HOP_HEADERS
import config

router = APIRouter()
client = httpx.AsyncClient()

@router.get("/search-mcx")
async def search_mcx(type: str = Query("futures"), symbol: str = Query(None)):
    """Search within Angel One MCX Micro-DB Segments"""
    from services.token_manager import token_manager
    if type == "futures":
        data = token_manager.get_angel_mcx_futures()
    else:
        data = token_manager.get_angel_mcx_options()
        if symbol:
            data = [item for item in data if item.get("name") == symbol.upper()]
    return JSONResponse(data)

@router.get("/search-nse")
async def search_nse(type: str = Query("futures"), symbol: str = Query(None)):
    """Search within Angel One NSE (F&O and Equity) Micro-DB Segments"""
    from services.token_manager import token_manager
    if type == "futures":
        data = token_manager.get_angel_nse_futures()
        return JSONResponse(data)
    # Options and Equity omitted for brevity as they are fetched differently 
    # and we mainly use /search-nse for futures in NFO bootstrapper.
    return JSONResponse([])

@router.get("/search-bse")
async def search_bse(type: str = Query("equity"), symbol: str = Query(None)):
    """Search within Angel One BSE Micro-DB Segments"""
    filename = "angel_bse_equity_master.json"
    filepath = os.path.join(config.ANGEL_SEGMENTS_DIR, filename)
    if os.path.exists(filepath):
        try:
            with open(filepath, "r") as f:
                data = json.load(f)
                return JSONResponse(data)
        except Exception as e:
            return JSONResponse({"error": "Failed to read data", "details": str(e)}, status_code=500)
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

        resp_body = await resp.aread()
        
        # Capture feedToken on successful login
        if "loginByPassword" in path and resp.status_code == 200:
            try:
                data = json.loads(resp_body)
                if data.get("status") is True and data.get("data"):
                    f_token = data["data"].get("feedToken")
                    c_code = data["data"].get("clientcode") or headers.get("X-User-ID")
                    a_key = headers.get("X-PrivateKey") or headers.get("X-Api-Key")
                    if f_token and c_code and a_key:
                        from services.indian_stock_brokers.angel_ws import update_angel_creds
                        # Using c_code as the key because it matches the user_id in the WebSocket connection
                        update_angel_creds(c_code, c_code, f_token, a_key)
            except: pass

        final_headers = {k: v for k, v in resp.headers.items() if k.lower() not in HOP_BY_HOP_HEADERS}
        return StreamingResponse(iter([resp_body]), status_code=resp.status_code, headers=final_headers)
    except httpx.ConnectError:
        return JSONResponse({"error": "Failed to connect to Angel One"}, status_code=502)
    except Exception as e:
        return JSONResponse({"error": "Internal Relay Error", "details": str(e)}, status_code=500)
