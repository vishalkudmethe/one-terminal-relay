import asyncio
import json
import logging
import struct
import websockets

logger = logging.getLogger(__name__)

async def _fyers_data_client(user_id: str, token: str, manager):
    """Fyers V3 Market Data Client (Binary)"""
    url = "wss://api-t1.fyers.in/socket/v3/data"
    app_id = token.split(':')[0] if ':' in token else ""
    access_token = token.split(':')[1] if ':' in token else token

    while True:
        try:
            async with websockets.connect(url) as ws:
                # 1. Authentication
                await ws.send(json.dumps({
                    "t": "authentication",
                    "v": 2,
                    "app_id": app_id,
                    "access_token": access_token
                }))
                
                while True:
                    symbols = manager.get_subscriptions(user_id, "fyers")
                    if symbols:
                        await ws.send(json.dumps({
                            "t": "sub", "v": 1, 
                            "auth_passthrough": True, 
                            "symbols": symbols, "nsflag": 1
                        }))
                    
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=5.0)
                        if isinstance(message, bytes):
                            if len(message) < 12: continue
                            topic_id = struct.unpack(">H", message[0:2])[0]
                            if 7200 <= topic_id <= 7208:
                                sym_len = message[2]
                                symbol = message[3:3+sym_len].decode('ascii')
                                offset = 3 + sym_len
                                ltp = struct.unpack("<f", message[offset:offset+4])[0]
                                cp = struct.unpack("<f", message[offset+4:offset+8])[0] if topic_id == 7208 and len(message) >= offset + 8 else 0.0
                                
                                if ltp > 0:
                                    await manager.broadcast_tick(user_id, "fyers", symbol, ltp)
                        else:
                            data = json.loads(message)
                            if data.get('s') == 'ok': logger.info(f"Fyers Auth Success for {user_id}")
                    except asyncio.TimeoutError: continue
                    except Exception as e:
                        logger.error(f"Fyers Stream Error: {e}")
                        break
        except Exception as e:
            logger.error(f"Fyers Connection Failed: {e}")
            await asyncio.sleep(5)
async def _fyers_order_client(user_id: str, token: str, manager):
    """Fyers V3 Order Stream Client"""
    url = "wss://api-t1.fyers.in/socket/v3/order"
    access_token = token.split(':')[1] if ':' in token else token

    while True:
        try:
            async with websockets.connect(url) as ws:
                await ws.send(json.dumps({
                    "t": "authentication", "v": 2, "access_token": access_token
                }))
                while True:
                    await ws.send(json.dumps({"t": "sub", "v": 1, "topic": "orders"}))
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=10.0)
                        data = json.loads(message)
                        if data.get('t') == 'ol': # Order Update
                            await manager.broadcast_to_user(user_id, {
                                "broker": "fyers", "type": "ORDER", "data": data
                            })
                    except asyncio.TimeoutError: continue
                    except Exception as e:
                        logger.error(f"Fyers Order Stream Error: {e}")
                        break
        except Exception as e:
            logger.error(f"Fyers Order Connection Failed: {e}")
            await asyncio.sleep(5)

async def fyers_client(user_id: str, token: str, manager):
    """Fyers Client Entry Point"""
    await asyncio.gather(
        _fyers_data_client(user_id, token, manager),
        _fyers_order_client(user_id, token, manager)
    )
