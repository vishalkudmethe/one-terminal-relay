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

async def handle_upstox_request(path: str, request: Request, db, xff, uuid):
    """Core Upstox Request Handler (V2 Optimized)"""
    # Build Downstream URL
    clean_path = urllib.parse.unquote(path).strip('/')
    url = f"{config.BROKER_URLS['upstox']}/{clean_path}"
    
    # Audit Log
    log = AuditLog(broker="upstox", endpoint=path, method=request.method, client_ip=xff, device_uuid=uuid)
    db.add(log); db.commit()

    # Prepare Headers
    headers = {}
    for k, v in request.headers.items():
        k_low = k.lower()
        if k_low in HOP_BY_HOP_HEADERS or k_low in ["authorization", "x-ot-context"]: continue
        if k_low == "x-broker-authorization": headers["Authorization"] = v
        else: headers[k] = v

    # Upstox V2 often requires specific parameters (like instrument_key)
    # We pass them through directly from the query_params
    upstream_params = {k: v for k, v in request.query_params.items() if k not in ("broker", "path")}

    # Proxy Call
    body = await request.body()
    resp = await client.request(
        method=request.method, url=url, headers=headers, content=body, 
        params=upstream_params,
        follow_redirects=False, timeout=30.0
    )


    final_headers = {k: v for k, v in resp.headers.items() if k.lower() not in HOP_BY_HOP_HEADERS}
    return StreamingResponse(iter([await resp.aread()]), status_code=resp.status_code, headers=final_headers)

@router.get("/search-segments")
async def search_segments(segment: str = Query("nse_fo"), type: str = Query(None)):
    """Search within Upstox Micro-DB Segments"""
    filename = f"upstox_{segment.lower()}_master.json"
    filepath = os.path.join(config.get_data_dir("upstox"), filename)

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
