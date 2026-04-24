import asyncio
import json
import logging
import struct
import websockets
import httpx

logger = logging.getLogger(__name__)

async def _upstox_data_client(user_id: str, token: str, manager):
    """Upstox V3 Market Data Feed — Strict Protobuf (Binary) Parser
    
    Uses the GET /authorize endpoint to obtain a short-lived WSS URL,
    then subscribes via instrument_key (native token) arrays.
    On connection loss, triggers a silent failover via manager.failover().
    """
    auth_url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
    
    while True:
        try:
            # 1. Get authorized WSS URL
            async with httpx.AsyncClient() as client:
                res = await client.get(
                    auth_url,
                    headers={"Authorization": f"Bearer {token}", "Api-Version": "3.0"}
                )
                if res.status_code != 200:
                    logger.warning(f"[Upstox] Auth failed for {user_id}: {res.status_code}")
                    await asyncio.sleep(5)
                    continue
                wss_url = res.json()['data']['authorizedRedirectUri']
            
            # 2. Connect and subscribe using native instrument_keys
            async with websockets.connect(wss_url, extra_headers={"Authorization": f"Bearer {token}"}) as ws:
                logger.info(f"[Upstox] Connected for {user_id}")
                
                native_keys = manager.get_native_subscriptions(user_id, "upstox")
                if native_keys:
                    await ws.send(json.dumps({
                        "guid": f"ot_{user_id}",
                        "method": "sub",
                        "data": {"mode": "full", "instrumentKeys": native_keys}
                    }))
                    logger.info(f"[Upstox] Subscribed {len(native_keys)} instruments for {user_id}")
                
                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        
                        if isinstance(message, bytes):
                            # Upstox V3 Protobuf-Lite Decoding
                            # Strategy: scan for instrument_key strings in binary payload,
                            # then extract LTP (tag 0x09 = double) and CP (tag 0x21 = double)
                            # following the Upstox LTPC Protobuf schema.
                            native_keys_local = manager.get_native_subscriptions(user_id, "upstox")
                            for key in native_keys_local:
                                key_bytes = key.encode('ascii')
                                if key_bytes not in message:
                                    continue
                                
                                idx = message.find(key_bytes)
                                # Scan forward from position for LTP double tag (0x09 in field 1 of LTPC)
                                search_region = message[idx:idx + 200]
                                ltp_tag_pos = search_region.find(b'\x09')
                                if ltp_tag_pos != -1 and len(search_region) >= ltp_tag_pos + 9:
                                    ltp = struct.unpack('<d', search_region[ltp_tag_pos+1:ltp_tag_pos+9])[0]
                                    
                                    # CP is tag 4 field (0x21) immediately after LTP in LTPC message
                                    cp = 0.0
                                    cp_tag_pos = search_region.find(b'\x21', ltp_tag_pos)
                                    if cp_tag_pos != -1 and len(search_region) >= cp_tag_pos + 9:
                                        cp = struct.unpack('<d', search_region[cp_tag_pos+1:cp_tag_pos+9])[0]
                                    
                                    if ltp > 0:
                                        await manager.broadcast_tick(
                                            user_id, "upstox", key, ltp, cp=cp
                                        )
                        else:
                            # JSON control messages (heartbeat, etc.)
                            data = json.loads(message)
                            if data.get('type') == 'heartbeat':
                                await ws.send(json.dumps({"type": "heartbeat"}))
                    
                    except asyncio.TimeoutError:
                        # Send ping to keep alive
                        await ws.ping()
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"[Upstox] Connection closed for {user_id}, triggering failover...")
                        await manager.failover(user_id, "upstox")
                        break
                    except Exception as e:
                        logger.error(f"[Upstox] Stream error for {user_id}: {e}")
                        break
        
        except Exception as e:
            logger.error(f"[Upstox] Connection failed for {user_id}: {e}")
            await asyncio.sleep(5)


async def _upstox_portfolio_client(user_id: str, token: str, manager):
    """Upstox V3 Portfolio (Orders/Positions) Client"""
    auth_url = "https://api.upstox.com/v3/feed/portfolio-stream-feed/authorize"
    
    while True:
        try:
            async with httpx.AsyncClient() as client:
                res = await client.get(auth_url, headers={"Authorization": f"Bearer {token}", "Api-Version": "3.0"})
                if res.status_code != 200:
                    await asyncio.sleep(5)
                    continue
                wss_url = res.json()['data']['authorizedRedirectUri']
            
            async with websockets.connect(wss_url, extra_headers={"Authorization": f"Bearer {token}"}) as ws:
                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=10.0)
                        data = json.loads(message)
                        await manager.broadcast_to_user(user_id, {
                            "broker": "upstox", "type": "PORTFOLIO", "data": data
                        })
                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        logger.error(f"[Upstox] Portfolio stream error: {e}")
                        break
        except Exception as e:
            logger.error(f"[Upstox] Portfolio connection failed: {e}")
            await asyncio.sleep(5)


async def upstox_client(user_id: str, token: str, manager):
    """Upstox Client Entry Point"""
    await asyncio.gather(
        _upstox_data_client(user_id, token, manager),
        _upstox_portfolio_client(user_id, token, manager)
    )
