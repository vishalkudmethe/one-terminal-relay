import asyncio
import json
import logging
import websockets

logger = logging.getLogger(__name__)

# Anand Rathi API (XTS Symphony Fintech)
# XTS uses Socket.IO based feeds, but they also expose raw websocket endpoints.
# Native token format: "exchangeSegment|instrumentID" e.g. "NSECM|2885"
ANANDRATHI_WSS_URL = "wss://xts.anandrathi.com/apimarketdata/socket.io/?EIO=3&transport=websocket"

async def _anandrathi_data_client(user_id: str, token: str, manager):
    """Anand Rathi XTS Live Feed Client
    
    Token format: "token"
    Native token stored as "exchangeSegment|instrumentID" e.g. "NSECM|2885"
    """
    while True:
        try:
            # XTS Socket.IO connection requires sending the token in the initial connect sequence
            async with websockets.connect(ANANDRATHI_WSS_URL) as ws:
                logger.info(f"[AnandRathi] Connected for {user_id}")

                # Build subscription from native tokens
                native_keys = manager.get_native_subscriptions(user_id, "anandrathi")
                instruments = []
                for key in native_keys:
                    if '|' in key:
                        seg, tok = key.split('|', 1)
                        # XTS expects specific exchange codes
                        exchange_segment = 1 if seg == "NSECM" else 2 if seg == "NSEFO" else 51 if seg == "MCXFO" else 1
                        instruments.append({"exchangeSegment": exchange_segment, "exchangeInstrumentID": tok})

                if instruments:
                    # Send standard XTS subscription payload
                    await ws.send(json.dumps({
                        "instruments": instruments,
                        "xtsMessageCode": 1501 # 1501 = TouchLine Event (LTP)
                    }))
                    logger.info(f"[AnandRathi] Subscribed {len(instruments)} instruments for {user_id}")

                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        
                        # Handle Socket.IO wrapping if necessary (e.g. starts with '42')
                        if isinstance(message, str):
                            if message.startswith('0'): # ping/pong
                                pass
                            
                            # Clean XTS message format
                            try:
                                # Sometimes it's wrapped in socket.io framing like '42["message", {...}]'
                                if message.startswith('42'):
                                    raw_data = json.loads(message[2:])[1]
                                else:
                                    raw_data = json.loads(message)
                                
                                # XTS specific parsing
                                if "LastTradedPrice" in raw_data:
                                    tok = str(raw_data.get("ExchangeInstrumentID") or "")
                                    seg = raw_data.get("ExchangeSegment", 1)
                                    seg_str = "NSECM" if seg == 1 else "NSEFO" if seg == 2 else "MCXFO"
                                    
                                    ltp = float(raw_data.get("LastTradedPrice", 0))
                                    cp = float(raw_data.get("Close", 0))

                                    if ltp > 0 and tok:
                                        native_key = f"{seg_str}|{tok}"
                                        await manager.broadcast_tick(
                                            user_id, "anandrathi", native_key, ltp, cp=cp
                                        )
                            except:
                                pass

                    except asyncio.TimeoutError:
                        await ws.send("2") # Socket.IO ping
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"[AnandRathi] Connection closed for {user_id}, triggering failover...")
                        await manager.failover(user_id, "anandrathi")
                        break
                    except Exception as e:
                        logger.error(f"[AnandRathi] Stream error: {e}")
                        break

        except Exception as e:
            logger.error(f"[AnandRathi] Connection failed for {user_id}: {e}")
            await asyncio.sleep(5)


async def anandrathi_client(user_id: str, token: str, manager):
    """Anand Rathi Client Entry Point"""
    await _anandrathi_data_client(user_id, token, manager)
