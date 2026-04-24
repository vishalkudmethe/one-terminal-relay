import asyncio
import json
import logging
import websockets

logger = logging.getLogger(__name__)

# Axis Direct API WebSocket
# Native token format: "SEGMENT:TOKEN" e.g. "NSE:2885"
AXIS_WSS_URL = "wss://stream.axisdirect.in/ws"

async def _axis_data_client(user_id: str, token: str, manager):
    """Axis Direct RAPID API Live Feed Client
    
    Token format: "access_token"
    Native token stored as "SEGMENT:TOKEN" e.g. "NSE:2885"
    """
    while True:
        try:
            headers = {
                "Authorization": f"Bearer {token}"
            }
            async with websockets.connect(AXIS_WSS_URL, extra_headers=headers) as ws:
                logger.info(f"[Axis Direct] Connected for {user_id}")

                # Build subscription from native tokens
                native_keys = manager.get_native_subscriptions(user_id, "axis")
                
                if native_keys:
                    await ws.send(json.dumps({
                        "action": "subscribe",
                        "mode": "ltp",
                        "instruments": native_keys
                    }))
                    logger.info(f"[Axis Direct] Subscribed {len(native_keys)} instruments for {user_id}")

                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        data = json.loads(message)

                        msg_type = data.get("type") or data.get("action")
                        
                        if msg_type == "tick" or "ltp" in data:
                            # Parse Axis JSON tick
                            sec_id = str(data.get("instrument") or data.get("token") or "")
                            
                            ltp = float(data.get("ltp") or data.get("last_price") or 0)
                            cp = float(data.get("close") or data.get("prev_close") or 0)

                            if ltp > 0 and sec_id:
                                await manager.broadcast_tick(
                                    user_id, "axis", sec_id, ltp, cp=cp
                                )

                    except asyncio.TimeoutError:
                        await ws.send(json.dumps({"action": "heartbeat"}))
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"[Axis Direct] Connection closed for {user_id}, triggering failover...")
                        await manager.failover(user_id, "axis")
                        break
                    except Exception as e:
                        logger.error(f"[Axis Direct] Stream error: {e}")
                        break

        except Exception as e:
            logger.error(f"[Axis Direct] Connection failed for {user_id}: {e}")
            await asyncio.sleep(5)


async def axis_client(user_id: str, token: str, manager):
    """Axis Direct Client Entry Point"""
    await _axis_data_client(user_id, token, manager)
