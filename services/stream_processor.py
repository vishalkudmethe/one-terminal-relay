import asyncio
import logging
from services.indian_stock_brokers.angel_ws import angel_client
from services.indian_stock_brokers.fyers_ws import fyers_client
from services.indian_stock_brokers.upstox_ws import upstox_client
from services.indian_stock_brokers.zerodha_ws import zerodha_client
from services.indian_stock_brokers.groww_ws import groww_client
from services.indian_stock_brokers.icici_ws import icici_client
from services.indian_stock_brokers.kotak_ws import kotak_client
from services.indian_stock_brokers.hdfc_ws import hdfc_client
from services.indian_stock_brokers.dhan_ws import dhan_client
from services.indian_stock_brokers.motilal_ws import motilal_client
from services.indian_stock_brokers.anandrathi_ws import anandrathi_client
from services.indian_stock_brokers.prabhudas_ws import prabhudas_client
from services.indian_stock_brokers.axis_ws import axis_client
from services.indian_stock_brokers.iifl_ws import iifl_client
from services.indian_stock_brokers.nuvama_ws import nuvama_client
from services.indian_stock_brokers.profitmart_ws import profitmart_client
from services.indian_stock_brokers.religare_ws import religare_client

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
    elif broker == "icici":
        return icici_client(user_id, token, manager)
    elif broker == "kotak":
        return kotak_client(user_id, token, manager)
    elif broker == "hdfc":
        return hdfc_client(user_id, token, manager)
    elif broker == "dhan":
        return dhan_client(user_id, token, manager)
    elif broker == "motilal":
        return motilal_client(user_id, token, manager)
    elif broker == "anandrathi":
        return anandrathi_client(user_id, token, manager)
    elif broker == "prabhudas":
        return prabhudas_client(user_id, token, manager)
    elif broker == "axis":
        return axis_client(user_id, token, manager)
    elif broker == "iifl":
        return iifl_client(user_id, token, manager)
    elif broker == "nuvama":
        return nuvama_client(user_id, token, manager)
    elif broker == "profitmart":
        return profitmart_client(user_id, token, manager)
    elif broker == "religare":
        return religare_client(user_id, token, manager)
    else:
        logger.warning(f"[StreamProcessor] No client found for broker: {broker}")
        return None
