from fastapi import APIRouter, Request, Query
from fastapi.responses import JSONResponse, StreamingResponse
import httpx
import urllib.parse
from services.utils import AuditLog, HOP_BY_HOP_HEADERS
import config

router = APIRouter()
client = httpx.AsyncClient()

async def handle_dhan_request(path: str, request: Request, db, xff, uuid):
    clean_path = urllib.parse.unquote(path).strip('/')
    url = f"https://api.dhan.co/{clean_path}"

    log = AuditLog(broker="dhan", endpoint=path, method=request.method, client_ip=xff, device_uuid=uuid)
    db.add(log); db.commit()

    headers = {}
    for k, v in request.headers.items():
        k_low = k.lower()
        if k_low in HOP_BY_HOP_HEADERS or k_low in ["authorization", "x-ot-context"]: continue
        if k_low == "x-broker-authorization": headers["Authorization"] = v
        else: headers[k] = v

    body = await request.body()
    resp = await client.request(
        method=request.method, url=url, headers=headers, content=body,
        params={k: v for k, v in request.query_params.items() if k not in ("broker", "path")},
        follow_redirects=True, timeout=30.0
    )

    final_headers = {k: v for k, v in resp.headers.items() if k.lower() not in HOP_BY_HOP_HEADERS}
    return StreamingResponse(iter([await resp.aread()]), status_code=resp.status_code, headers=final_headers)

@router.get("/health")
async def dhan_health():
    return {"broker": "dhan", "status": "handler_active"}
