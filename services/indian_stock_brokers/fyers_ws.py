import asyncio
import json
import logging
import struct
import websockets

logger = logging.getLogger(__name__)

async def _fyers_data_client(user_id: str, token: str, manager):
    """Fyers V3 Market Data Client (Binary Parser)
    
    Authenticates using app_id:access_token format.
    Subscribes using fytoken (native token) arrays resolved from manager.get_native_subscriptions().
    On connection loss, triggers a silent failover via manager.failover().
    """
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
                
                # 2. Wait for auth confirmation
                auth_msg = await asyncio.wait_for(ws.recv(), timeout=10.0)
                auth_data = json.loads(auth_msg)
                if auth_data.get('s') != 'ok':
                    logger.error(f"[Fyers] Auth failed for {user_id}: {auth_data}")
                    await asyncio.sleep(5)
                    continue
                logger.info(f"[Fyers] Auth success for {user_id}")
                
                # 3. Subscribe using native fytokens
                native_tokens = manager.get_native_subscriptions(user_id, "fyers")
                if native_tokens:
                    await ws.send(json.dumps({
                        "t": "sub", "v": 1,
                        "auth_passthrough": True,
                        "symbols": native_tokens,
                        "nsflag": 1
                    }))
                    logger.info(f"[Fyers] Subscribed {len(native_tokens)} instruments for {user_id}")
                
                # 4. Listen for ticks
                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        
                        if isinstance(message, bytes):
                            if len(message) < 12:
                                continue
                            
                            # Fyers Binary Format:
                            # [0:2] = topic_id (big-endian uint16)
                            # [2]   = symbol string length
                            # [3:3+sym_len] = fytoken string
                            # [3+sym_len:]  = float32 fields (ltp, cp, open, high, low, close, etc.)
                            topic_id = struct.unpack(">H", message[0:2])[0]
                            
                            # Topic IDs 7200-7208 are market data ticks
                            if 7200 <= topic_id <= 7208:
                                sym_len = message[2]
                                if 3 + sym_len > len(message):
                                    continue
                                    
                                fytoken = message[3:3+sym_len].decode('ascii')
                                offset = 3 + sym_len
                                
                                if len(message) < offset + 4:
                                    continue
                                
                                ltp = struct.unpack(">f", message[offset:offset+4])[0]
                                
                                # CP (close price / prev close) is typically the 2nd field
                                cp = 0.0
                                if len(message) >= offset + 8:
                                    cp = struct.unpack(">f", message[offset+4:offset+8])[0]
                                
                                if ltp > 0:
                                    await manager.broadcast_tick(
                                        user_id, "fyers", fytoken, ltp, cp=cp
                                    )
                        else:
                            # JSON messages (heartbeat, subscription confirmations, errors)
                            data = json.loads(message)
                            msg_type = data.get('t')
                            if msg_type == 'hb':  # Heartbeat
                                await ws.send(json.dumps({"t": "hb"}))
                            elif msg_type == 'error':
                                logger.error(f"[Fyers] Server error for {user_id}: {data}")
                    
                    except asyncio.TimeoutError:
                        # Send heartbeat ping
                        await ws.send(json.dumps({"t": "hb"}))
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"[Fyers] Connection closed for {user_id}, triggering failover...")
                        await manager.failover(user_id, "fyers")
                        break
                    except Exception as e:
                        logger.error(f"[Fyers] Stream error for {user_id}: {e}")
                        break
        
        except Exception as e:
            logger.error(f"[Fyers] Connection failed for {user_id}: {e}")
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
                        if data.get('t') == 'ol':  # Order Update
                            await manager.broadcast_to_user(user_id, {
                                "broker": "fyers", "type": "ORDER", "data": data
                            })
                    except asyncio.TimeoutError:
                        continue
                    except Exception as e:
                        logger.error(f"[Fyers] Order stream error: {e}")
                        break
        except Exception as e:
            logger.error(f"[Fyers] Order connection failed: {e}")
            await asyncio.sleep(5)


async def fyers_client(user_id: str, token: str, manager):
    """Fyers Client Entry Point"""
    await asyncio.gather(
        _fyers_data_client(user_id, token, manager),
        _fyers_order_client(user_id, token, manager)
    )
