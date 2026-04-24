import asyncio
import json
import logging
import struct
import websockets

logger = logging.getLogger(__name__)

# DhanHQ API WebSocket
# Documentation suggests a binary protocol, but for standard integrations 
# many use the JSON fallback or the official SDK.
# Native token format: "ExchangeSegment:SecurityId" e.g. "NSE_EQ:1333"
DHAN_WSS_URL = "wss://api-feed.dhan.co"

async def _dhan_data_client(user_id: str, token: str, manager):
    """DhanHQ Live Feed Client
    
    Token format: "client_id:access_token"
    Native token stored as "ExchangeSegment:SecurityId" e.g. "NSE_EQ:1333"
    """
    parts = token.split(':')
    client_id = parts[0] if len(parts) >= 2 else ""
    access_token = parts[1] if len(parts) >= 2 else token

    while True:
        try:
            # Dhan authentication usually requires passing token as a query param or auth header
            wss_url = f"{DHAN_WSS_URL}?token={access_token}&clientId={client_id}"
            
            async with websockets.connect(wss_url) as ws:
                logger.info(f"[Dhan] Connected for {user_id}")

                # Build subscription from native tokens
                native_keys = manager.get_native_subscriptions(user_id, "dhan")
                instruments = []
                for key in native_keys:
                    if ':' in key:
                        seg, tok = key.split(':', 1)
                        # Dhan specific mapping
                        exch_code = 1 if seg in ["NSE_EQ", "NSE_FO"] else 2 if seg in ["BSE_EQ"] else 3 if seg == "MCX_FO" else 1
                        instruments.append({"ExchangeSegment": exch_code, "SecurityId": tok})

                if instruments:
                    # Dhan JSON subscription payload format
                    await ws.send(json.dumps({
                        "RequestCode": 15,
                        "InstrumentCount": len(instruments),
                        "InstrumentList": instruments
                    }))
                    logger.info(f"[Dhan] Subscribed {len(instruments)} instruments for {user_id}")

                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        
                        if isinstance(message, bytes):
                            # Dhan Binary Parsing (Stub - requires actual struct unpack based on annexure)
                            # E.g., packet length is 50 bytes for Ticker Data
                            # byte 0: Type, bytes 4-8: SecurityId, etc.
                            # We will rely on JSON for now if available.
                            pass
                        elif isinstance(message, str):
                            data = json.loads(message)
                            
                            # Parse Dhan JSON tick
                            sec_id = str(data.get("SecurityId") or data.get("security_id") or "")
                            seg_id = data.get("ExchangeSegment") or 1
                            seg_str = "NSE_EQ" if seg_id == 1 else "BSE_EQ" if seg_id == 2 else "MCX_FO"
                            
                            ltp = float(data.get("LTP") or data.get("last_traded_price") or 0)
                            cp = float(data.get("Close") or data.get("previous_close") or 0)

                            if ltp > 0 and sec_id:
                                native_key = f"{seg_str}:{sec_id}"
                                await manager.broadcast_tick(
                                    user_id, "dhan", native_key, ltp, cp=cp
                                )

                    except asyncio.TimeoutError:
                        await ws.send(json.dumps({"RequestCode": 0})) # Ping
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"[Dhan] Connection closed for {user_id}, triggering failover...")
                        await manager.failover(user_id, "dhan")
                        break
                    except Exception as e:
                        logger.error(f"[Dhan] Stream error: {e}")
                        break

        except Exception as e:
            logger.error(f"[Dhan] Connection failed for {user_id}: {e}")
            await asyncio.sleep(5)


async def dhan_client(user_id: str, token: str, manager):
    """DhanHQ Client Entry Point"""
    await _dhan_data_client(user_id, token, manager)
