import asyncio
import json
import logging
import websockets
import httpx

logger = logging.getLogger(__name__)

# Kotak Neo WebSocket uses Socket.IO-like protocol over standard WebSocket
# Native token format: "EXCHANGE_SEGMENT:instrument_token" e.g. "nse_cm:12345"
KOTAK_WSS_BASE = "wss://mlhsm.kotakneo.com"

async def _kotak_data_client(user_id: str, token: str, manager):
    """Kotak Neo Live Feed Client — Socket.IO-like JSON WebSocket
    
    Token format: "consumer_key:access_token:sid"
    Native token stored as "exchange_segment:instrument_token" e.g. "nse_cm:11915"
    """
    parts = token.split(':')
    consumer_key = parts[0] if len(parts) >= 3 else ""
    access_token = parts[1] if len(parts) >= 3 else token
    sid = parts[2] if len(parts) >= 3 else ""
    
    wss_url = f"{KOTAK_WSS_BASE}/hs/ws/v2.1/quotes/callback/{consumer_key}/{sid}"

    while True:
        try:
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Sid": sid,
                "Auth": access_token,
                "User-Agent": "KotakNeo-OneTerminal/2.5"
            }
            async with websockets.connect(wss_url, extra_headers=headers) as ws:
                logger.info(f"[Kotak Neo] Connected for {user_id}")

                # Build subscription from native tokens
                # Native: "nse_cm:12345" -> {"instrument_token": "12345", "exchange_segment": "nse_cm"}
                native_keys = manager.get_native_subscriptions(user_id, "kotak")
                instruments = []
                for key in native_keys:
                    if ':' in key:
                        seg, tok = key.split(':', 1)
                        instruments.append({"instrument_token": tok, "exchange_segment": seg})

                if instruments:
                    await ws.send(json.dumps({
                        "type": "quotes",
                        "scrips": instruments,
                        "channelnum": "1"
                    }))
                    logger.info(f"[Kotak Neo] Subscribed {len(instruments)} instruments for {user_id}")

                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        
                        # Kotak delivers JSON ticks
                        if isinstance(message, str) and message.startswith('{'):
                            data = json.loads(message)
                            
                            # Kotak tick structure
                            tok = str(data.get("tk") or data.get("instrument_token") or "")
                            seg = data.get("e") or data.get("exchange_segment") or "nse_cm"
                            ltp = float(data.get("ltp") or data.get("lp") or 0)
                            cp = float(data.get("c") or data.get("prev_close") or 0)

                            if ltp > 0 and tok:
                                native_key = f"{seg}:{tok}"
                                await manager.broadcast_tick(
                                    user_id, "kotak", native_key, ltp, cp=cp
                                )
                        elif message == "ping":
                            await ws.send("pong")

                    except asyncio.TimeoutError:
                        await ws.send(json.dumps({"type": "ping"}))
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"[Kotak Neo] Connection closed for {user_id}, triggering failover...")
                        await manager.failover(user_id, "kotak")
                        break
                    except Exception as e:
                        logger.error(f"[Kotak Neo] Stream error: {e}")
                        break

        except Exception as e:
            logger.error(f"[Kotak Neo] Connection failed for {user_id}: {e}")
            await asyncio.sleep(5)


async def kotak_client(user_id: str, token: str, manager):
    """Kotak Neo Client Entry Point"""
    await _kotak_data_client(user_id, token, manager)
