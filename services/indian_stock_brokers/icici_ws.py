import asyncio
import json
import logging
import websockets
import httpx

logger = logging.getLogger(__name__)

# ICICI Breeze WebSocket
# Token format: "EXCHANGE_CODE.DATA_LEVEL!STOCK_TOKEN" e.g. "4.1!38071"
# Exchange codes: 4=NSE, 1=BSE, 13=NFO, 11=MCX
# Tick payload is JSON with keys: symbol, ltp, prev_close, etc.
BREEZE_WSS_URL = "wss://livefeeds.icicidirect.com"
BREEZE_EXCHANGE_MAP = {"NSE": "4", "BSE": "1", "NFO": "13", "MCX": "11"}

async def _icici_data_client(user_id: str, token: str, manager):
    """ICICI Breeze Live Feed Client — JSON WebSocket
    
    Token format: "api_key:session_token"
    Native token stored as "EXCHANGE_CODE.1!STOCK_TOKEN" (e.g. "4.1!38071")
    """
    parts = token.split(':')
    api_key = parts[0] if len(parts) >= 2 else token
    session_token = parts[1] if len(parts) >= 2 else token

    while True:
        try:
            headers = {
                "Authorization": f"Bearer {session_token}",
                "X-API-KEY": api_key
            }
            async with websockets.connect(BREEZE_WSS_URL, extra_headers=headers) as ws:
                logger.info(f"[ICICI Breeze] Connected for {user_id}")

                # Subscribe using native stock_token format (e.g. "4.1!38071")
                native_tokens = manager.get_native_subscriptions(user_id, "icici")
                if native_tokens:
                    await ws.send(json.dumps({
                        "task": "cn",
                        "channel": ",".join(native_tokens),
                        "token": session_token,
                        "user": api_key,
                        "acctid": api_key
                    }))
                    logger.info(f"[ICICI Breeze] Subscribed {len(native_tokens)} instruments for {user_id}")

                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        data = json.loads(message)

                        # Breeze tick structure
                        symbol_id = data.get("symbol_id") or data.get("stock_code") or ""
                        ltp = float(data.get("last") or data.get("ltp") or 0)
                        cp = float(data.get("prev_close") or data.get("close") or 0)

                        if ltp > 0 and symbol_id:
                            await manager.broadcast_tick(
                                user_id, "icici", symbol_id, ltp, cp=cp
                            )

                    except asyncio.TimeoutError:
                        # Breeze heartbeat
                        await ws.send(json.dumps({"task": "hb"}))
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"[ICICI Breeze] Connection closed for {user_id}, triggering failover...")
                        await manager.failover(user_id, "icici")
                        break
                    except Exception as e:
                        logger.error(f"[ICICI Breeze] Stream error: {e}")
                        break

        except Exception as e:
            logger.error(f"[ICICI Breeze] Connection failed for {user_id}: {e}")
            await asyncio.sleep(5)


async def icici_client(user_id: str, token: str, manager):
    """ICICI Breeze Client Entry Point"""
    await _icici_data_client(user_id, token, manager)
