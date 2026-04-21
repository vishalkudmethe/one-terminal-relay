from fastapi import APIRouter, Request, Query
from fastapi.responses import JSONResponse, StreamingResponse
import httpx
import os
import json
import urllib.parse
from services.utils import AuditLog, HOP_BY_HOP_HEADERS
import config

router = APIRouter()

client = httpx.AsyncClient()

async def handle_fyers_request(path: str, request: Request, db, xff, uuid):
    """Core Fyers Request Handler (V3 Optimized)"""
    # Build Downstream URL
    clean_path = urllib.parse.unquote(path).strip('/')
    # Note: config.BROKER_URLS['fyers'] is set to https://api-t1.fyers.in
    url = f"{config.BROKER_URLS['fyers']}/{clean_path}"
    
    # Audit Log
    log = AuditLog(broker="fyers", endpoint=path, method=request.method, client_ip=xff, device_uuid=uuid)
    db.add(log); db.commit()

    # Prepare Headers
    headers = {}
    for k, v in request.headers.items():
        k_low = k.lower()
        if k_low in HOP_BY_HOP_HEADERS or k_low in ["authorization", "x-ot-context"]: continue
        if k_low == "x-broker-authorization": headers["Authorization"] = v
        else: headers[k] = v

    # Proxy Call (Hybrid Redirects)
    should_follow = "generate-authcode" in path.lower() or "auth/login" in path.lower()
    
    body = await request.body()
    resp = await client.request(
        method=request.method, url=url, headers=headers, content=body, 
        params={k: v for k, v in request.query_params.items() if k not in ("broker", "path")},
        follow_redirects=should_follow, timeout=30.0
    )

    final_headers = {k: v for k, v in resp.headers.items() if k.lower() not in HOP_BY_HOP_HEADERS}
    return StreamingResponse(iter([await resp.aread()]), status_code=resp.status_code, headers=final_headers)

@router.get("/search-segments")
async def search_segments(segment: str = Query("nse_fo"), type: str = Query(None)):
    """Search within Fyers Micro-DB Segments"""
    filename = f"fyers_{segment.lower()}_master.json"
    filepath = os.path.join(config.get_data_dir("fyers"), filename)

    if os.path.exists(filepath):
        try:
            with open(filepath, "r") as f:
                data = json.load(f)
                if type:
                    data = [item for item in data if item.get("type") == type.upper()]
                return JSONResponse(data)
        except Exception as e:
            return JSONResponse({"error": "Failed to read data", "details": str(e)}, status_code=500)
    
    return JSONResponse([], status_code=404)
