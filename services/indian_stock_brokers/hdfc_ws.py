import asyncio
import json
import logging
import websockets
import httpx

logger = logging.getLogger(__name__)

# HDFC Sky API WebSocket
# Native token format: "SEGMENT:instrument_token" e.g. "NSE:3045"
# Uses standard JSON subscription format
HDFC_WSS_URL = "wss://livefeeds.hdfcsec.com/feed"

async def _hdfc_data_client(user_id: str, token: str, manager):
    """HDFC Sky Live Feed Client — JSON WebSocket
    
    Token format: "api_key:access_token"
    Native token stored as "SEGMENT:instrument_token" e.g. "NSE:3045"
    """
    parts = token.split(':')
    api_key = parts[0] if len(parts) >= 2 else token
    access_token = parts[1] if len(parts) >= 2 else token

    while True:
        try:
            headers = {
                "Authorization": f"Bearer {access_token}",
                "X-API-KEY": api_key
            }
            async with websockets.connect(HDFC_WSS_URL, extra_headers=headers) as ws:
                logger.info(f"[HDFC Sky] Connected for {user_id}")

                # Subscribe using native tokens (SEGMENT:instrument_token format)
                native_tokens = manager.get_native_subscriptions(user_id, "hdfc")
                if native_tokens:
                    await ws.send(json.dumps({
                        "action": "subscribe",
                        "mode": "ltp",
                        "instruments": native_tokens
                    }))
                    logger.info(f"[HDFC Sky] Subscribed {len(native_tokens)} instruments for {user_id}")

                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        data = json.loads(message)

                        msg_type = data.get("type") or data.get("a", "")

                        if msg_type == "tick" or "ltp" in data:
                            # Parse HDFC tick
                            instrument = data.get("instrument") or data.get("token", "")
                            ltp = float(data.get("ltp") or data.get("last_price") or 0)
                            cp = float(data.get("close") or data.get("prev_close") or 0)

                            if ltp > 0 and instrument:
                                await manager.broadcast_tick(
                                    user_id, "hdfc", instrument, ltp, cp=cp
                                )
                        elif msg_type in ["heartbeat", "hb", "ping"]:
                            await ws.send(json.dumps({"type": "pong"}))

                    except asyncio.TimeoutError:
                        await ws.send(json.dumps({"type": "heartbeat"}))
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"[HDFC Sky] Connection closed for {user_id}, triggering failover...")
                        await manager.failover(user_id, "hdfc")
                        break
                    except Exception as e:
                        logger.error(f"[HDFC Sky] Stream error: {e}")
                        break

        except Exception as e:
            logger.error(f"[HDFC Sky] Connection failed for {user_id}: {e}")
            await asyncio.sleep(5)


async def hdfc_client(user_id: str, token: str, manager):
    """HDFC Sky Client Entry Point"""
    await _hdfc_data_client(user_id, token, manager)
