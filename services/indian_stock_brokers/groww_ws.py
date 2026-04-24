import asyncio
import json
import logging
import websockets

logger = logging.getLogger(__name__)

GROWW_WSS_URL = "wss://growwapi.groww.in/feed/v1"

async def _groww_data_client(user_id: str, token: str, manager):
    """Groww Market Data Feed — JSON WebSocket Client
    
    Uses exchange_token + segment pairs for subscription.
    Native tokens stored as "SEGMENT:exchange_token" (e.g. "NSE:2885").
    On connection loss, triggers a silent failover via manager.failover().
    """
    while True:
        try:
            headers = {
                "Authorization": f"Bearer {token}",
                "x-app-id": "groww-trader"
            }
            async with websockets.connect(GROWW_WSS_URL, extra_headers=headers) as ws:
                logger.info(f"[Groww] Connected for {user_id}")

                # Build subscription list from native tokens
                # Native token format: "SEGMENT:exchange_token" e.g. "NSE:2885"
                native_keys = manager.get_native_subscriptions(user_id, "groww")
                sub_list = []
                for key in native_keys:
                    if ':' in key:
                        segment, exchange_token = key.split(':', 1)
                        sub_list.append({
                            "exchange_token": int(exchange_token),
                            "segment": segment
                        })

                if sub_list:
                    await ws.send(json.dumps({
                        "a": "subscribe",
                        "v": sub_list
                    }))
                    logger.info(f"[Groww] Subscribed {len(sub_list)} instruments for {user_id}")

                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=30.0)

                        data = json.loads(message)
                        msg_type = data.get('type') or data.get('a')

                        if msg_type == 'tick' or 'payload' in data:
                            payload = data.get('payload') or data
                            exchange_token = str(payload.get('exchange_token', ''))
                            segment = payload.get('segment', 'NSE')
                            ltp = float(payload.get('ltp') or payload.get('last_price') or 0)
                            cp = float(payload.get('close_price') or payload.get('prev_close') or 0)

                            if ltp > 0 and exchange_token:
                                native_key = f"{segment}:{exchange_token}"
                                await manager.broadcast_tick(
                                    user_id, "groww", native_key, ltp, cp=cp
                                )

                        elif msg_type == 'heartbeat' or msg_type == 'hb':
                            await ws.send(json.dumps({"a": "heartbeat"}))

                        elif msg_type == 'error':
                            logger.error(f"[Groww] Server error for {user_id}: {data}")

                    except asyncio.TimeoutError:
                        await ws.send(json.dumps({"a": "heartbeat"}))
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"[Groww] Connection closed for {user_id}, triggering failover...")
                        await manager.failover(user_id, "groww")
                        break
                    except Exception as e:
                        logger.error(f"[Groww] Stream error for {user_id}: {e}")
                        break

        except Exception as e:
            logger.error(f"[Groww] Connection failed for {user_id}: {e}")
            await asyncio.sleep(5)


async def groww_client(user_id: str, token: str, manager):
    """Groww Client Entry Point"""
    await _groww_data_client(user_id, token, manager)
