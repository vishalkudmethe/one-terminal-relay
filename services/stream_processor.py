import asyncio
import logging
from services.indian_stock_brokers.angel_ws import angel_client
from services.indian_stock_brokers.fyers_ws import fyers_client
from services.indian_stock_brokers.upstox_ws import upstox_client
from services.indian_stock_brokers.zerodha_ws import zerodha_client
from services.indian_stock_brokers.groww_ws import groww_client

logger = logging.getLogger(__name__)

def get_broker_client(user_id: str, broker: str, token: str, manager):
    """Factory: returns the correct upstream WebSocket coroutine for a given broker."""
    if broker in ["angel", "angelone"]:
        return angel_client(user_id, token, manager)
    elif broker == "fyers":
        return fyers_client(user_id, token, manager)
    elif broker == "upstox":
        return upstox_client(user_id, token, manager)
    elif broker == "zerodha":
        return zerodha_client(user_id, token, manager)
    elif broker == "groww":
        return groww_client(user_id, token, manager)
    else:
        logger.warning(f"[StreamProcessor] No client found for broker: {broker}")
        return None
