import asyncio
import json
import logging
import struct
import websockets

logger = logging.getLogger(__name__)

async def angel_client(user_id: str, token: str, manager):
    """Angel One SmartAPI WebSocket Client"""
    if ':' not in token:
        logger.error(f"Invalid Angel token format for {user_id}")
        return
        
    client_id, feed_token, api_key = token.split(':')
    url = f"wss://smartapisocket.angelone.in/smart-stream?clientCode={client_id}&feedToken={feed_token}&apiKey={api_key}"

    while True:
        try:
            async with websockets.connect(url) as ws:
                last_subscribed = set()
                
                while True:
                    # 1. Dynamic Subscription Check
                    symbols_with_exchanges = manager.get_subscriptions(user_id, "angel")
                    current_symbols = set(symbols_with_exchanges)
                    
                    if current_symbols != last_subscribed and symbols_with_exchanges:
                        exch_map = {}
                        for s in symbols_with_exchanges:
                            if ':' in s:
                                ex, tk = s.split(':')
                                ex_id = _map_angel_exchange(ex)
                                if ex_id not in exch_map: exch_map[ex_id] = []
                                exch_map[ex_id].append(tk)
                        
                        # 37: token_list = [{"exchangeType": k, "tokens": v} for k, v in exch_map.items()]
                        # Batched Subscription (Angel Limit: 50 per request, 2000 per connection)
                        # We use 40 to be safe as requested by user.
                        all_tokens = []
                        for ex_id, tokens in exch_map.items():
                            for t in tokens:
                                all_tokens.append((ex_id, t))
                        
                        # Chunk into batches of 40
                        num_batches = (len(all_tokens) + 39) // 40
                        for i in range(0, len(all_tokens), 40):
                            batch = all_tokens[i:i+40]
                            batch_map = {}
                            for ex_id, tk in batch:
                                if ex_id not in batch_map: batch_map[ex_id] = []
                                batch_map[ex_id].append(tk)
                            
                            token_list = [{"exchangeType": k, "tokens": v} for k, v in batch_map.items()]
                            logger.info(f"DEBUG_SUB: [Angel] Batch {i//40 + 1}: {token_list}")
                            await ws.send(json.dumps({
                                "correlationId": f"ot_{user_id}_{i}",
                                "action": 1, # SUBSCRIBE
                                "params": {"mode": 3, "tokenList": token_list}
                            }))
                            await asyncio.sleep(0.2) # Additive delay for stability
                        
                        last_subscribed = current_symbols
                        logger.info(f"Angel subscribed to {len(symbols_with_exchanges)} symbols in {num_batches} batches for {user_id}")
                    
                    # 2. Message Loop
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        if isinstance(message, bytes) and len(message) > 2:
                            mode = message[0]
                            # 1 byte Mode, 1 byte Exchange, 25 bytes Token
                            token = message[2:27].decode('ascii', errors='ignore').strip('\x00').strip()
                            
                            ltp = 0.0
                            cp = 0.0
                            
                            if mode == 1 and len(message) >= 31: # LTP Mode
                                ltp_raw = struct.unpack("<I", message[27:31])[0]
                                ltp = ltp_raw / 100.0
                            elif mode == 3 and len(message) >= 51: # Snap Quote Mode
                                ltp_raw = struct.unpack("<I", message[43:47])[0]
                                ltp = ltp_raw / 100.0
                                if len(message) >= 123:
                                    cp_raw = struct.unpack("<I", message[115:119])[0]
                                    cp = cp_raw / 100.0
                            
                            if ltp > 0:
                                # if message[1] in [1, 3]: # Log NSE/BSE specifically for debugging
                                #     logger.debug(f"Angel Stock Match: Exch {message[1]}, Token {token}, LTP {ltp}")
                                
                                await manager.broadcast_to_user(user_id, {
                                    "broker": "angel", 
                                    "token": token,
                                    "symbol": token,
                                    "lp": round(ltp, 2), 
                                    "ltp": round(ltp, 2),
                                    "cp": round(cp, 2),
                                    "type": "UPDATE"
                                })
                    except asyncio.TimeoutError:
                        # Keep-alive check
                        await ws.ping()
                        continue
                    except Exception as e:
                        logger.error(f"Angel Stream Error: {e}")
                        break
        except Exception as e:
            logger.error(f"Angel Connection Failed: {e}")
            await asyncio.sleep(5)

def _map_angel_exchange(ex: str) -> int:
    mapping = {'NSE': 1, 'NFO': 2, 'BSE': 3, 'BFO': 4, 'MCX': 5}
    return mapping.get(ex.upper(), 1)
