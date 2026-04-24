import asyncio
import json
import logging
import websockets

logger = logging.getLogger(__name__)

# Motilal Oswal MOAPI WebSocket
# Native token format: "EXCHANGE:scripcode" e.g. "NSE:11915"
MOTILAL_WSS_URL = "wss://openapi.motilaloswal.com/stream"

async def _motilal_data_client(user_id: str, token: str, manager):
    """Motilal Oswal Live Feed Client
    
    Token format: "client_id:access_token"
    Native token stored as "EXCHANGE:scripcode" e.g. "NSE:3045"
    """
    parts = token.split(':')
    client_id = parts[0] if len(parts) >= 2 else ""
    access_token = parts[1] if len(parts) >= 2 else token

    while True:
        try:
            headers = {
                "Authorization": f"Bearer {access_token}"
            }
            async with websockets.connect(MOTILAL_WSS_URL, extra_headers=headers) as ws:
                logger.info(f"[Motilal Oswal] Connected for {user_id}")

                # 1. MOAPI requires a login packet on connect
                await ws.send(json.dumps({
                    "Method": "Login",
                    "ClientId": client_id,
                    "AuthToken": access_token
                }))
                
                # Build subscription from native tokens
                native_keys = manager.get_native_subscriptions(user_id, "motilal")
                instruments = []
                for key in native_keys:
                    if ':' in key:
                        seg, tok = key.split(':', 1)
                        instruments.append({"Exchange": seg, "ScripCode": tok})

                if instruments:
                    # MOAPI JSON subscription payload format
                    await ws.send(json.dumps({
                        "Method": "Subscribe",
                        "Mode": "LTP",
                        "Instruments": instruments
                    }))
                    logger.info(f"[Motilal Oswal] Subscribed {len(instruments)} instruments for {user_id}")

                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        data = json.loads(message)

                        msg_type = data.get("MessageType") or data.get("Method")
                        
                        if msg_type == "Tick" or "LTP" in data:
                            # Parse MOAPI JSON tick
                            sec_id = str(data.get("ScripCode") or "")
                            seg_str = data.get("Exchange") or "NSE"
                            
                            ltp = float(data.get("LTP") or data.get("LastTradedPrice") or 0)
                            cp = float(data.get("Close") or data.get("PreviousClose") or 0)

                            if ltp > 0 and sec_id:
                                native_key = f"{seg_str}:{sec_id}"
                                await manager.broadcast_tick(
                                    user_id, "motilal", native_key, ltp, cp=cp
                                )

                    except asyncio.TimeoutError:
                        await ws.send(json.dumps({"Method": "Heartbeat"}))
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"[Motilal Oswal] Connection closed for {user_id}, triggering failover...")
                        await manager.failover(user_id, "motilal")
                        break
                    except Exception as e:
                        logger.error(f"[Motilal Oswal] Stream error: {e}")
                        break

        except Exception as e:
            logger.error(f"[Motilal Oswal] Connection failed for {user_id}: {e}")
            await asyncio.sleep(5)


async def motilal_client(user_id: str, token: str, manager):
    """Motilal Oswal Client Entry Point"""
    await _motilal_data_client(user_id, token, manager)
