from fastapi import APIRouter, Request, Depends, Header
from fastapi.responses import JSONResponse
import httpx
import json
import logging
from typing import Optional
from .token_manager import token_manager
from services.utils import get_db, parse_ot_context, AuditLog, HOP_BY_HOP_HEADERS
from services.indian_stock_brokers import angel_handler, fyers_handler, upstox_handler
import config

router = APIRouter()
logger = logging.getLogger(__name__)
client = httpx.AsyncClient()

@router.post("/place")
async def place_unified_order(
    request: Request,
    db = Depends(get_db),
    x_ot_context: Optional[str] = Header(None, alias="X-OT-Context"),
    authorization: Optional[str] = Header(None)
):
    """
    Unified Order Placement Endpoint
    Expects JSON: { "broker": "angel", "uId": "NSE:RELIANCE", "qty": 1, "side": "BUY", "type": "LIMIT", "price": 2500.0, "product": "DELIVERY" }
    """
    # 1. Auth Check (Reuse Gateway Secret)
    if not authorization or not authorization.startswith("Bearer "):
        return JSONResponse(status_code=401, content={"error": "Unauthorized - Missing Token"})
    if authorization.split("Bearer ")[1] != config.GATEWAY_SECRET:
        return JSONResponse(status_code=403, content={"error": "Forbidden - Invalid Token"})

    # 2. Context Parsing
    ctx = parse_ot_context(x_ot_context)
    xff, uuid = ctx.get("XFF"), ctx.get("UUID")
    
    try:
        body = await request.json()
        broker = body.get("broker", "").lower()
        uId = body.get("uId")
        
        if not broker or not uId:
            return JSONResponse(status_code=400, content={"error": "Missing 'broker' or 'uId' in request body"})

        # 3. Resolve uId to Native Token
        native_token = token_manager.get_native_token(uId, broker)
        if not native_token or native_token == "0" or native_token == "":
            logger.warning(f"UNIFIED_ORDER_RESOLUTION_FAILURE: uId={uId} broker={broker}")
            return JSONResponse(status_code=422, content={"error": f"Safety Violation: Could not resolve valid native token for uId '{uId}' on broker '{broker}'"})

        logger.info(f"UNIFIED_ORDER_RESOLUTION_SUCCESS: uId={uId} -> broker={broker} token={native_token}")

        metadata = token_manager.get_metadata(uId)
        symbol = metadata.get('symbol', uId.split(':')[-1]) if metadata else uId.split(':')[-1]
        exch = metadata.get('exch', uId.split(':')[0]) if metadata else uId.split(':')[0]

        # 4. Transform to Broker-Specific Payload
        broker_payload = {}
        target_path = ""

        if broker in ["angel", "angelone"]:
            target_path = "rest/secure/angelbroking/order/v1/placeOrder"
            broker_payload = {
                "variety": "NORMAL",
                "tradingsymbol": symbol if exch != "NSE" else f"{symbol}-EQ",
                "symboltoken": native_token,
                "transactiontype": body.get("side", "BUY"),
                "exchange": exch,
                "ordertype": body.get("type", "LIMIT"),
                "producttype": body.get("product", "DELIVERY"),
                "duration": "DAY",
                "price": str(body.get("price", 0)),
                "squareoff": "0",
                "stoploss": "0",
                "quantity": str(body.get("qty", 1))
            }
        elif broker == "upstox":
            target_path = "v2/order/place"
            broker_payload = {
                "quantity": body.get("qty", 1),
                "product": body.get("product", "DELIVERY"),
                "validity": "DAY",
                "price": body.get("price", 0),
                "tag": "oneterminal",
                "instrument_token": native_token,
                "order_type": body.get("type", "LIMIT"),
                "transaction_type": body.get("side", "BUY"),
                "disclosed_quantity": 0,
                "trigger_price": 0,
                "is_amo": False
            }
        else:
            return JSONResponse(status_code=501, content={"error": f"Unified order not yet implemented for '{broker}'"})

        # 5. Route to specific broker handler for execution
        # We simulate a new request with the broker-specific payload
        if broker in ["angel", "angelone"]:
             # Create a mock request object or call handler directly
             # For simplicity, we call handle_angel_request but we need to inject the new body
             # Since handle_angel_request reads await request.body(), we might need a wrapper
             pass
        
        # Actually, let's just perform the call here to avoid complex request wrapping
        url = f"{config.BROKER_URLS[broker]}/{target_path}"
        
        # Prepare Headers (reuse logic from handlers)
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        for k, v in request.headers.items():
            if k.lower() == "x-broker-authorization":
                headers["Authorization"] = v
        
        resp = await client.post(url, headers=headers, json=broker_payload, timeout=30.0)
        return JSONResponse(status_code=resp.status_code, content=resp.json())

    except Exception as e:
        logger.error(f"Unified Order Error: {e}")
        return JSONResponse(status_code=500, content={"error": "Internal Relay Error", "details": str(e)})
