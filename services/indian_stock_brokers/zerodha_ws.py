import asyncio
import json
import logging
import struct
import websockets

logger = logging.getLogger(__name__)

# Zerodha Kite Ticker Binary Format (LTP Mode):
# Message is a packed array of ticks.
# Header: 2 bytes (num_packets, big-endian uint16)
# Per packet:
#   2 bytes: packet_length (big-endian uint16)
#   4 bytes: instrument_token (big-endian uint32)
#   4 bytes: last_price (big-endian uint32) -> actual = value / 100.0
#
# Quote/Full mode packets are longer (44 bytes, 184 bytes) but we use LTP for max speed.

KITE_WSS_URL = "wss://ws.kite.trade"

async def _zerodha_data_client(user_id: str, token: str, manager):
    """Zerodha Kite Ticker — Custom Binary Packet Parser (LTP Mode)
    
    Uses instrument_token (int) arrays resolved from get_native_subscriptions().
    On connection loss, triggers a silent failover via manager.failover().
    
    Token format expected: "api_key:access_token"
    """
    parts = token.split(':')
    api_key = parts[0] if len(parts) >= 2 else token
    access_token = parts[1] if len(parts) >= 2 else token
    
    wss_url = f"{KITE_WSS_URL}?api_key={api_key}&access_token={access_token}"

    while True:
        try:
            async with websockets.connect(wss_url) as ws:
                logger.info(f"[Zerodha] Connected for {user_id}")

                # Subscribe using native integer instrument_tokens
                native_tokens = manager.get_native_subscriptions(user_id, "zerodha")
                if native_tokens:
                    int_tokens = [int(t) for t in native_tokens if t.isdigit()]
                    # Set mode to LTP for max throughput
                    await ws.send(json.dumps({
                        "a": "subscribe",
                        "v": int_tokens
                    }))
                    await ws.send(json.dumps({
                        "a": "mode",
                        "v": ["ltp", int_tokens]
                    }))
                    logger.info(f"[Zerodha] Subscribed {len(int_tokens)} instruments for {user_id}")

                while True:
                    try:
                        message = await asyncio.wait_for(ws.recv(), timeout=30.0)

                        if isinstance(message, bytes):
                            if len(message) < 2:
                                continue

                            num_packets = struct.unpack(">H", message[0:2])[0]
                            offset = 2

                            for _ in range(num_packets):
                                if offset + 2 > len(message):
                                    break
                                pkt_len = struct.unpack(">H", message[offset:offset+2])[0]
                                offset += 2

                                if offset + pkt_len > len(message):
                                    break

                                pkt = message[offset:offset+pkt_len]
                                offset += pkt_len

                                if len(pkt) < 8:
                                    continue

                                instrument_token = struct.unpack(">I", pkt[0:4])[0]
                                # Kite encodes prices as integers * 100
                                last_price = struct.unpack(">I", pkt[4:8])[0] / 100.0

                                if last_price > 0:
                                    await manager.broadcast_tick(
                                        user_id, "zerodha", str(instrument_token), last_price
                                    )
                        else:
                            # JSON control messages
                            data = json.loads(message)
                            msg_type = data.get('type')
                            if msg_type == 'error':
                                logger.error(f"[Zerodha] Server error for {user_id}: {data}")

                    except asyncio.TimeoutError:
                        # Send heartbeat ping
                        await ws.send("ping")
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"[Zerodha] Connection closed for {user_id}, triggering failover...")
                        await manager.failover(user_id, "zerodha")
                        break
                    except Exception as e:
                        logger.error(f"[Zerodha] Stream error for {user_id}: {e}")
                        break

        except Exception as e:
            logger.error(f"[Zerodha] Connection failed for {user_id}: {e}")
            await asyncio.sleep(5)


async def zerodha_client(user_id: str, token: str, manager):
    """Zerodha Client Entry Point"""
    await _zerodha_data_client(user_id, token, manager)
