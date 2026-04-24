import asyncio
import json
import logging
import os
import struct
import websockets

logger = logging.getLogger(__name__)

# Relay-side Angel One credentials (set as env vars on AWS EC2)
# These are the RELAY's own Angel One trading account, used to stream market data.
# The mobile never needs to send credentials for data streaming.
_ANGEL_CLIENT_ID = os.getenv("ANGEL_CLIENT_ID", "")
_ANGEL_FEED_TOKEN = os.getenv("ANGEL_FEED_TOKEN", "")
_ANGEL_API_KEY = os.getenv("ANGEL_API_KEY", "")

# Runtime credential cache (captured from proxy logins)
# user_id -> (client_id, feed_token, api_key)
_RUNTIME_CREDS = {}

def update_angel_creds(user_id: str, client_id: str, feed_token: str, api_key: str):
    """Called by angel_handler.py when a successful login is detected."""
    _RUNTIME_CREDS[user_id] = (client_id, feed_token, api_key)
    logger.info(f"[Angel] Captured runtime credentials for user: {user_id}")


# ─────────────────────────────────────────────────────────────────────────────
# Binary Packet Layout (Official Angel One SmartAPI WebSocket v2)
# Source: github.com/angel-one/smartapi-python/blob/main/SmartApi/smartWebSocketV2.py
#
# Byte  |  Size  | Format | Field
# ──────|────────|────────|──────────────────────────────────────────────────
#   0   |   1    |   B    | subscription_mode (1=LTP, 2=QUOTE, 3=SNAP_QUOTE)
#   1   |   1    |   B    | exchange_type
#  2-26 |  25    |  str   | token (null-padded ASCII)
# 27-34 |   8    |   q    | sequence_number     (int64)
# 35-42 |   8    |   q    | exchange_timestamp  (int64)
# 43-50 |   8    |   q    | last_traded_price   (int64, divide by 100)  ← LTP
# ── Mode 2 (QUOTE) and Mode 3 (SNAP_QUOTE) only ──────────────────────────────
# 51-58 |   8    |   q    | last_traded_quantity
# 59-66 |   8    |   q    | average_traded_price
# 67-74 |   8    |   q    | volume_trade_for_the_day                    ← VOL
# 75-82 |   8    |   d    | total_buy_quantity  (float64)
# 83-90 |   8    |   d    | total_sell_quantity (float64)
# 91-98 |   8    |   q    | open_price_of_the_day  (int64, /100)
# 99-106|   8    |   q    | high_price_of_the_day  (int64, /100)
# 107-114|  8    |   q    | low_price_of_the_day   (int64, /100)
# 115-122|  8    |   q    | closed_price           (int64, /100)        ← CP
# ─────────────────────────────────────────────────────────────────────────────


def _parse_angel_binary(message: bytes) -> tuple[float, float, int]:
    """
    Parse Angel One binary tick packet.
    Returns (ltp, cp, volume). All fields default to 0.0/0 on parse failure.

    Strictly follows the official Angel One SDK binary layout.
    Uses <q (little-endian int64) for all price/qty fields — NOT <I.
    """
    ltp = 0.0
    cp = 0.0
    volume = 0

    if len(message) < 2:
        return ltp, cp, volume

    mode = message[0]

    # ── Mode 1: LTP Only (min 51 bytes) ──────────────────────────────────────
    if mode == 1:
        if len(message) >= 51:
            ltp_raw = struct.unpack("<q", message[43:51])[0]
            ltp = ltp_raw / 100.0

    # ── Mode 2: Quote (min 123 bytes) ────────────────────────────────────────
    elif mode == 2:
        if len(message) >= 123:
            ltp_raw = struct.unpack("<q", message[43:51])[0]
            ltp = ltp_raw / 100.0
            volume = struct.unpack("<q", message[67:75])[0]
            cp_raw = struct.unpack("<q", message[115:123])[0]
            cp = cp_raw / 100.0

    # ── Mode 3: Snap Quote (min 123 bytes) ───────────────────────────────────
    elif mode == 3:
        if len(message) >= 123:
            ltp_raw = struct.unpack("<q", message[43:51])[0]
            ltp = ltp_raw / 100.0
            volume = struct.unpack("<q", message[67:75])[0]
            cp_raw = struct.unpack("<q", message[115:123])[0]
            cp = cp_raw / 100.0

    return ltp, cp, volume


async def _heartbeat(ws):
    """
    Manual Angel-protocol heartbeat.
    Angel One expects a TEXT 'ping' message every ~10s and responds with 'pong'.
    The websockets library's built-in ping_interval sends a WS PROTOCOL Ping
    frame which Angel does not respond to — causing ping_timeout drops.
    """
    while True:
        try:
            await asyncio.sleep(10)
            await ws.send("ping")
            logger.debug("[Angel] Heartbeat 'ping' sent")
        except Exception:
            break  # WS is closing; let the outer loop handle reconnect


async def angel_client(user_id: str, token: str, manager):
    """
    Angel One SmartAPI WebSocket Client (Relay-Side).
    
    Credential resolution order:
    1. Runtime cache (captured from user login)
    2. Mobile-provided token (passed in subscribe)
    3. Relay environment variables (ANGEL_CLIENT_ID, etc.)
    """
    
    while True:
        # Resolve credentials at the start of every connection attempt
        client_id, feed_token, api_key = None, None, None

        # 1. Check Runtime Cache
        if user_id in _RUNTIME_CREDS:
            client_id, feed_token, api_key = _RUNTIME_CREDS[user_id]
            logger.info(f"[Angel] Using captured runtime credentials for {user_id}")

        # 2. Check Mobile-provided token (legacy/manual)
        if not client_id and token and token.count(':') == 2:
            client_id, feed_token, api_key = token.split(':')
            logger.info(f"[Angel] Using mobile-provided credentials for {user_id}")

        # 3. Fallback to Relay Environment Variables
        if not client_id:
            client_id = _ANGEL_CLIENT_ID
            feed_token = _ANGEL_FEED_TOKEN
            api_key = _ANGEL_API_KEY
            if client_id:
                logger.info(f"[Angel] Using relay-wide environment credentials for {user_id}")

        if not client_id or not feed_token or not api_key:
            logger.error(
                f"[Angel] Cannot start stream for {user_id}: Missing credentials. "
                "Login via the app or set relay environment variables."
            )
            await asyncio.sleep(10)  # Wait for a login to happen
            continue

        url = (
            f"wss://smartapisocket.angelone.in/smart-stream"
            f"?clientCode={client_id}&feedToken={feed_token}&apiKey={api_key}"
        )

        try:
            # ping_interval=None: Disable library auto-ping (WS protocol frames).
            # Manual heartbeat sends Angel-compatible TEXT "ping" every 10s instead.
            async with websockets.connect(url, ping_interval=None, ping_timeout=None) as ws:
                last_subscribed = set()
                hb_task = asyncio.create_task(_heartbeat(ws))

                try:
                    while True:
                        # 1. Dynamic Subscription Check
                        uIds_to_subscribe = manager.get_subscriptions(user_id, "angel")
                        current_symbols = set(uIds_to_subscribe)

                        if current_symbols != last_subscribed and uIds_to_subscribe:
                            from services.token_manager import token_manager
                            exch_map = {}
                            unresolved = []

                            for uId in uIds_to_subscribe:
                                if ':' not in uId:
                                    continue
                                parts = uId.split(':')
                                ex = parts[0]
                                potential_token = parts[1]

                                # CRITICAL: Resolve uId -> native angel numeric token
                                # e.g. "NSE:RELIANCE" -> "3045"
                                native_token = token_manager.get_native_token(uId, "angel")
                                
                                # Fallback: If the app sent EXCHANGE:TOKEN (e.g. NSE:1660)
                                if not native_token and potential_token.isdigit():
                                    native_token = potential_token
                                    
                                if not native_token:
                                    unresolved.append(uId)
                                    continue

                                ex_id = _map_angel_exchange(ex)
                                if ex_id not in exch_map:
                                    exch_map[ex_id] = []
                                exch_map[ex_id].append(native_token)

                            if unresolved:
                                logger.warning(
                                    f"[Angel] {len(unresolved)} uIds not in TokenManager: "
                                    f"{unresolved[:5]}{'...' if len(unresolved) > 5 else ''}"
                                )

                            if exch_map:
                                # Batched Subscription (Angel Limit: 50/request, 2000/connection)
                                all_tokens = []
                                for ex_id, tokens in exch_map.items():
                                    for t in tokens:
                                        all_tokens.append((ex_id, t))

                                num_batches = (len(all_tokens) + 39) // 40
                                for i in range(0, len(all_tokens), 40):
                                    batch = all_tokens[i:i + 40]
                                    batch_map = {}
                                    for ex_id, tk in batch:
                                        if ex_id not in batch_map:
                                            batch_map[ex_id] = []
                                        batch_map[ex_id].append(tk)

                                    token_list = [{"exchangeType": k, "tokens": v} for k, v in batch_map.items()]
                                    logger.info(f"DEBUG_SUB: [Angel] Batch {i // 40 + 1}/{num_batches}: {token_list}")
                                    await ws.send(json.dumps({
                                        "correlationId": f"ot_{user_id}_{i}",
                                        "action": 1,  # SUBSCRIBE
                                        "params": {"mode": 3, "tokenList": token_list}
                                    }))
                                    await asyncio.sleep(0.2)  # Stagger batches

                                logger.info(
                                    f"[Angel] Subscribed {len(all_tokens)} tokens in {num_batches} batches for {user_id}. "
                                    f"Skipped {len(unresolved)} unresolved."
                                )

                            last_subscribed = current_symbols

                        # 2. Message Loop
                        try:
                            message = await asyncio.wait_for(ws.recv(), timeout=30.0)
                            
                            # Angel sends "pong" as a text response to our "ping"
                            if isinstance(message, str):
                                if message == "pong":
                                    logger.debug("[Angel] Heartbeat 'pong' received")
                                continue  # Ignore all text messages (pong, info, etc.)

                            if isinstance(message, bytes) and len(message) > 2:
                                mode = message[0]
                                # Binary layout: 1 byte Mode | 1 byte Exchange | 25 bytes Token
                                native_token = message[2:27].decode('ascii', errors='ignore').strip('\x00').strip()

                                ltp, cp, volume = _parse_angel_binary(message)

                                if ltp > 0:
                                    logger.debug(f"DEBUG_TICK: [Angel] mode={mode} token={native_token} LTP={ltp} CP={cp} VOL={volume}")
                                    await manager.broadcast_tick(user_id, "angel", native_token, ltp, volume=volume, cp=cp)

                        except asyncio.TimeoutError:
                            # 30s silence — market is closed or no data; heartbeat keeps TCP alive
                            logger.debug("[Angel] recv timeout (30s) — awaiting next tick")
                            continue
                        except Exception as e:
                            logger.error(f"[Angel] Stream Error: {e}")
                            break

                finally:
                    hb_task.cancel()
                    try:
                        await hb_task
                    except asyncio.CancelledError:
                        pass

        except Exception as e:
            logger.error(f"[Angel] Connection Failed: {e}")
            await asyncio.sleep(5)


def _map_angel_exchange(ex: str) -> int:
    mapping = {'NSE': 1, 'NFO': 2, 'BSE': 3, 'BFO': 4, 'MCX': 5}
    return mapping.get(ex.upper(), 1)
