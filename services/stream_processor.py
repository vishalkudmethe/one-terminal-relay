import asyncio
import logging
from services.indian_stock_brokers.fyers_ws import fyers_client
from services.indian_stock_brokers.upstox_ws import upstox_client
from services.indian_stock_brokers.angel_ws import angel_client

logger = logging.getLogger(__name__)

def get_broker_client(user_id: str, broker: str, token: str, manager):
    """Factory to return the correct sub-client."""
    if broker == "fyers":
        return fyers_client(user_id, token, manager)
    elif broker == "upstox":
        return upstox_client(user_id, token, manager)
    elif broker in ["angel", "angelone"]:
        return angel_client(user_id, token, manager)
    return None
