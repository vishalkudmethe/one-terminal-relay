import asyncio
import json
import logging
import websockets

logger = logging.getLogger(__name__)

# IIFL Markets API WebSocket
# Native token format: "Exch,ExchType,ScripCode" e.g. "N,C,2885"
IIFL_WSS_URL = "wss://apstream.indiainfoline.com/apigateway/MarketData/ws"

async def _iifl_data_client(user_id: str, token: str, manager):
    """IIFL Markets API Live Feed Client
    
    Token format: "AppKey:SubscriptionKey:AccessToken"
    Native token stored as "Exch,ExchType,ScripCode" e.g. "N,C,2885"
    """
    parts = token.split(':')
    app_key = parts[0] if len(parts) >= 3 else ""
    sub_key = parts[1] if len(parts) >= 3 else ""
    access_token = parts[2] if len(parts) >= 3 else token

    while True:
        try:
            headers = {
                "Ocp-Apim-Subscription-Key": sub_key,
                "Authorization": access_token
            }
            async with websockets.connect(IIFL_WSS_URL, extra_headers=headers) as ws:
                logger.info(f"[IIFL] Connected for {user_id}")

                # Build subscription from native tokens
                native_keys = manager.get_native_subscriptions(user_id, "iifl")
                instruments = []
                for key in native_keys:
                    if ',' in key:
                        exch, exch_type, scrip_code = key.split(',', 2)
                        instruments.append({
                            "Exchange": exch,
                            "ExchangeType": exch_type,
                            "ScripCode": scrip_code
                        })

                if instruments:
                    await ws.send(json.dumps({
                        "RequestCode": "Subscribe",
                        "AppKey": app_key,
                        "Count": len(instruments),
                        "MarketData": instruments
                    }))
                    logger.info(f"[IIFL] Subscribed {len(instruments)} instruments for {user_id}")

                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=30.0)
                        data = json.loads(message)

                        msg_type = data.get("MessageType") or data.get("Event")
                        
                        if msg_type == "Touchline" or "LastTradedPrice" in data:
                            # Parse IIFL JSON tick
                            scrip_code = str(data.get("ScripCode") or "")
                            exch = data.get("Exchange", "N")
                            exch_type = data.get("ExchangeType", "C")
                            
                            ltp = float(data.get("LastTradedPrice") or 0)
                            cp = float(data.get("PreviousClose") or 0)

                            if ltp > 0 and scrip_code:
                                native_key = f"{exch},{exch_type},{scrip_code}"
                                await manager.broadcast_tick(
                                    user_id, "iifl", native_key, ltp, cp=cp
                                )

                    except asyncio.TimeoutError:
                        await ws.send(json.dumps({"RequestCode": "HeartBeat"}))
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"[IIFL] Connection closed for {user_id}, triggering failover...")
                        await manager.failover(user_id, "iifl")
                        break
                    except Exception as e:
                        logger.error(f"[IIFL] Stream error: {e}")
                        break

        except Exception as e:
            logger.error(f"[IIFL] Connection failed for {user_id}: {e}")
            await asyncio.sleep(5)


async def iifl_client(user_id: str, token: str, manager):
    """IIFL Client Entry Point"""
    await _iifl_data_client(user_id, token, manager)
