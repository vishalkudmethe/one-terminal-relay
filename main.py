from fastapi import FastAPI, Request, Depends, Query, Header, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import uvicorn
import asyncio
import logging
import json
import config
from services.utils import get_db, parse_ot_context, SessionLocal, init_db
from services.currency_service import CurrencyService
from services.ws_manager import manager
from services.unified_ws_manager import unified_manager
from services.indian_stock_brokers import angel_handler, fyers_handler, upstox_handler
from services.international_brokers import alpaca_handler, nium_handler

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title=f"One Terminal Cloud Relay v{config.VERSION} (Modular)")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include handlers that have their own sub-routers
app.include_router(angel_handler.router, prefix="/angel", tags=["AngelOne"])
app.include_router(fyers_handler.router, prefix="/fyers", tags=["Fyers"])
app.include_router(upstox_handler.router, prefix="/upstox", tags=["Upstox"])
app.include_router(alpaca_handler.router, prefix="/v1/alpaca", tags=["Alpaca"])
app.include_router(nium_handler.router, prefix="/v1/nium", tags=["Nium"])

@app.websocket("/ws/{user_id}")
@app.websocket("/ws/{user_id}/")
async def websocket_endpoint(websocket: WebSocket, user_id: str):
    # Normalize ID: strip and uppercase to match broker conventions (e.g. Angel One)
    user_id = user_id.strip().upper()
    await unified_manager.connect_client(user_id, websocket)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                action = msg.get("action")
                broker = msg.get("broker")
                symbols = msg.get("symbols", [])
                logger.info(f"WS Action: {action} for {user_id} ({broker}) with {len(symbols)} symbols")
                
                if action == "subscribe":
                    token = msg.get("token") # Access/JWT Token
                    await unified_manager.subscribe(user_id, broker, symbols, token)
                elif action == "unsubscribe":
                    pass # TODO: Implement unsubscribe
            except Exception as e:
                logger.error(f"WS Message Error: {e}")
    except WebSocketDisconnect:
        unified_manager.disconnect_client(user_id, websocket)

async def currency_sync_task():
    """Background task to fetch USD/INR rate every 1 hour."""
    while True:
        rate = await CurrencyService.fetch_usd_inr_rate()
        if rate > 0:
            db = SessionLocal()
            try:
                CurrencyService.update_rate_in_db(db, rate)
            finally:
                db.close()
        await asyncio.sleep(3600)  # 1 Hour

@app.on_event("startup")
async def startup_event():
    logger.info("Initializing Database...")
    init_db()
    logger.info("Starting background tasks...")
    asyncio.create_task(currency_sync_task())
    logger.info("Currency Sync Task started.")
    # asyncio.create_task(alpaca_handler.alpaca_sse_listener())
    logger.info("Alpaca SSE Listener started.")

@app.get("/v1/alpaca/exchange-rate")
async def get_exchange_rate(db = Depends(get_db)):
    rate = CurrencyService.get_rate_from_db(db)
    return {"rate": rate, "currency": "USD_INR", "buffer": 1.5}

@app.get("/")
async def health():
    return {
        "status": "Live", 
        "version": config.VERSION, 
        "architecture": "Modular (Router-Handler)",
        "handlers": ["angel", "fyers", "upstox", "binance"]
    }

@app.api_route("/relay", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def universal_relay(
    request: Request,
    broker: str = Query(..., description="Broker name"),
    path: str = Query(..., description="Target API path"),
    db = Depends(get_db),
    x_ot_context: Optional[str] = Header(None, alias="X-OT-Context"),
    authorization: Optional[str] = Header(None)
):
    # 1. Auth Check (Gateway Secret)
    if not authorization or not authorization.startswith("Bearer "):
        return JSONResponse(status_code=401, content={"error": "Unauthorized - Missing Token"})
    if authorization.split("Bearer ")[1] != config.GATEWAY_SECRET:
        return JSONResponse(status_code=403, content={"error": "Forbidden - Invalid Token"})

    # 2. Context Parsing
    ctx = parse_ot_context(x_ot_context)
    xff, uuid = ctx.get("XFF"), ctx.get("UUID")
    if not xff or not uuid:
        return JSONResponse(status_code=400, content={"error": "Missing X-OT-Context (XFF=...;UUID=...)"})

    # 3. Route to Specific Handlers
    if broker in ["angel", "angelone"]:
        return await angel_handler.handle_angel_request(path, request, db, xff, uuid)
    elif broker == "fyers":
        return await fyers_handler.handle_fyers_request(path, request, db, xff, uuid)
    elif broker == "upstox":
        return await upstox_handler.handle_upstox_request(path, request, db, xff, uuid)
    else:
        # Placeholder for other brokers (Generic Relay)
        return JSONResponse(status_code=501, content={"error": f"Handler for '{broker}' not implemented yet."})

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
