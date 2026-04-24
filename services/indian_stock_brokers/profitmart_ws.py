import asyncio
import json
import logging
import websockets

logger = logging.getLogger(__name__)

# Profitmart API WebSocket
# Native token format: "SEGMENT:TOKEN" e.g. "NSE:2885"
PROFITMART_WSS_URL = "wss://api.profitmart.in/ws"

async def _profitmart_data_client(user_id: str, token: str, manager):
    """Profitmart API Live Feed Client
    
    Token format: "access_token"
    Native token stored as "SEGMENT:TOKEN" e.g. "NSE:2885"
    """
    while True:
        try:
            headers = {
                "Authorization": f"Bearer {token}"
            }
            async with websockets.connect(PROFITMART_WSS_URL, extra_headers=headers) as ws:
                logger.info(f"[Profitmart] Connected for {user_id}")

                # Build subscription from native tokens
                native_keys = manager.get_native_subscriptions(user_id, "profitmart")
                
                if native_keys:
                    await ws.send(json.dumps({
                        "action": "subscribe",
                        "mode": "ltp",
                        "instruments": native_keys
                    }))
                    logger.info(f"[Profitmart] Subscribed {len(native_keys)} instruments for {user_id}")

                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        data = json.loads(message)

                        msg_type = data.get("type") or data.get("action")
                        
                        if msg_type == "tick" or "ltp" in data:
                            # Parse Profitmart JSON tick
                            sec_id = str(data.get("instrument") or data.get("token") or "")
                            
                            ltp = float(data.get("ltp") or data.get("last_price") or 0)
                            cp = float(data.get("close") or data.get("prev_close") or 0)

                            if ltp > 0 and sec_id:
                                await manager.broadcast_tick(
                                    user_id, "profitmart", sec_id, ltp, cp=cp
                                )

                    except asyncio.TimeoutError:
                        await ws.send(json.dumps({"action": "heartbeat"}))
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"[Profitmart] Connection closed for {user_id}, triggering failover...")
                        await manager.failover(user_id, "profitmart")
                        break
                    except Exception as e:
                        logger.error(f"[Profitmart] Stream error: {e}")
                        break

        except Exception as e:
            logger.error(f"[Profitmart] Connection failed for {user_id}: {e}")
            await asyncio.sleep(5)


async def profitmart_client(user_id: str, token: str, manager):
    """Profitmart Client Entry Point"""
    await _profitmart_data_client(user_id, token, manager)
