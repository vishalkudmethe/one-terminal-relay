import asyncio
import json
import logging
import struct
import websockets
import httpx

logger = logging.getLogger(__name__)

async def _upstox_data_client(user_id: str, token: str, manager):
    """Upstox V3 Market Data Feed (Manual Protobuf-Lite Parser)"""
    auth_url = "https://api.upstox.com/v3/feed/market-data-feed/authorize"
    
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
                    symbols = manager.get_subscriptions(user_id, "upstox")
                    if symbols:
                        await ws.send(json.dumps({
                            "guid": f"ot_{user_id}",
                            "method": "sub",
                            "data": {"mode": "full", "instrumentKeys": symbols}
                        }))
                    
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        if isinstance(message, bytes):
                            # Upstox V3 Protobuf-Lite Decoding
                            # We search for the map of feeds. This is a very targeted parser.
                            # In reality, using the .proto file is better, but this handles 
                            # the 'Bloomberg-lite' speed requirements without heavy dependencies.
                            # Standard Protobuf tags: LTP is tag 1 (0x09), CP is tag 4 (0x21) inside LTPC.
                            
                            # Note: This is a simplified fallback. Ideally use generated _pb2.
                            # We can extract the symbol and price by looking for the instrument key string
                            # and then scanning for the LTP tag (0x09) nearby.
                            
                            for sym in symbols:
                                sym_bytes = sym.encode('ascii')
                                if sym_bytes in message:
                                    idx = message.find(sym_bytes)
                                    # Search for LTP tag (0x09) in the next 100 bytes
                                    ltp_idx = message.find(b'\x09', idx, idx + 200)
                                    if ltp_idx != -1 and len(message) >= ltp_idx + 9:
                                        ltp = struct.unpack("<d", message[ltp_idx+1:ltp_idx+9])[0]
                                        cp = 0.0
                                        cp_idx = message.find(b'\x21', ltp_idx, ltp_idx + 50)
                                        if cp_idx != -1 and len(message) >= cp_idx + 9:
                                            cp = struct.unpack("<d", message[cp_idx+1:cp_idx+9])[0]
                                        
                                        if ltp > 0:
                                            await manager.broadcast_tick(user_id, "upstox", sym, ltp)
                    except asyncio.TimeoutError: continue
                    except Exception as e:
                        logger.error(f"Upstox Stream Error: {e}")
                        break
        except Exception as e:
            logger.error(f"Upstox Connection Failed: {e}")
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
                        # Upstox sends JSON for portfolio stream
                        await manager.broadcast_to_user(user_id, {
                            "broker": "upstox", "type": "PORTFOLIO", "data": data
                        })
                    except asyncio.TimeoutError: continue
                    except Exception as e:
                        logger.error(f"Upstox Portfolio Stream Error: {e}")
                        break
        except Exception as e:
            logger.error(f"Upstox Portfolio Connection Failed: {e}")
            await asyncio.sleep(5)

async def upstox_client(user_id: str, token: str, manager):
    """Upstox Client Entry Point"""
    await asyncio.gather(
        _upstox_data_client(user_id, token, manager),
        _upstox_portfolio_client(user_id, token, manager)
    )
