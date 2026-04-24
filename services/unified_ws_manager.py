import asyncio
import json
import logging
import time
from typing import Dict, Set, Any, Optional
from fastapi import WebSocket
from services.token_manager import token_manager
try:
    from proto import market_data_pb2
except ImportError:
    market_data_pb2 = None

logger = logging.getLogger(__name__)

class UnifiedWSManager:
    def __init__(self):
        # user_id -> set of active client web sockets (mobile/web)
        self.client_connections: Dict[str, Set[WebSocket]] = {}
        
        # user_id -> {broker_name -> broker_client_task}
        self.broker_tasks: Dict[str, Dict[str, asyncio.Task]] = {}
        
        # user_id -> {broker_name -> {symbols_set}}
        self.subscriptions: Dict[str, Dict[str, Set[str]]] = {}
        
        # user_id -> {uId -> {'ltp': last_price, 'v': last_vol}}
        # Used for Delta Filtering to save bandwidth
        self.last_broadcasted: Dict[str, Dict[str, Dict[str, float]]] = {}

        # user_id -> {broker_name -> {native_token -> requested_uId}}
        # This solves the "Name Mismatch" problem (e.g. app asks for MCX:GOLD, broker says MCX:GOLD26MARFUT)
        self._token_to_uId_map: Dict[str, Dict[str, Dict[str, str]]] = {}

    async def connect_client(self, user_id: str, websocket: WebSocket):
        await websocket.accept()
        if user_id not in self.client_connections:
            self.client_connections[user_id] = set()
            self._token_to_uId_map[user_id] = {}
        self.client_connections[user_id].add(websocket)
        logger.info(f"Client {user_id} connected. Total clients: {len(self.client_connections)}")

    def disconnect_client(self, user_id: str, websocket: WebSocket):
        if user_id in self.client_connections:
            self.client_connections[user_id].discard(websocket)
            if not self.client_connections[user_id]:
                del self.client_connections[user_id]
                if user_id in self._token_to_uId_map:
                    del self._token_to_uId_map[user_id]
                self._stop_all_broker_tasks(user_id)
        logger.info(f"Client {user_id} disconnected.")

    def _stop_all_broker_tasks(self, user_id: str):
        if user_id in self.broker_tasks:
            for broker, task in self.broker_tasks[user_id].items():
                task.cancel()
            del self.broker_tasks[user_id]
        if user_id in self.subscriptions:
            del self.subscriptions[user_id]

    async def subscribe(self, user_id: str, broker: str, symbols: list, token: str):
        """Subscribe to symbols for a specific broker and user."""
        if user_id not in self.subscriptions:
            self.subscriptions[user_id] = {}
        if broker not in self.subscriptions[user_id]:
            self.subscriptions[user_id][broker] = set()
        
        self.subscriptions[user_id][broker].update(symbols)
        
        # Build local uId map for this broker
        if user_id not in self._token_to_uId_map:
            self._token_to_uId_map[user_id] = {}
        if broker not in self._token_to_uId_map[user_id]:
            self._token_to_uId_map[user_id][broker] = {}
            
        for uId in symbols:
            native = token_manager.get_native_token(uId, broker)
            if not native and ':' in uId and uId.split(':')[1].isdigit():
                native = uId.split(':')[1]
            if native:
                self._token_to_uId_map[user_id][broker][str(native)] = uId
        
        # Start or update the broker-specific background client
        await self._ensure_broker_client(user_id, broker, token)

    async def _ensure_broker_client(self, user_id: str, broker: str, token: str):
        if user_id not in self.broker_tasks:
            self.broker_tasks[user_id] = {}
            
        if broker not in self.broker_tasks[user_id] or self.broker_tasks[user_id][broker].done():
            # Start a new task for this broker
            from services.stream_processor import get_broker_client
            client_coro = get_broker_client(user_id, broker, token, self)
            if client_coro:
                self.broker_tasks[user_id][broker] = asyncio.create_task(client_coro)
                logger.info(f"Started {broker} client for {user_id}")

    async def broadcast_tick(self, user_id: str, broker: str, native_token: str, ltp: float, volume: int = 0, cp: float = 0.0):
        """
        Normalization Engine:
        1. Resolve uId (Preferring client's requested name, then TokenManager)
        2. Enrich data (math for change_percent)
        3. Delta Filtering (only send if price/volume changed)
        4. Broadcast as Protobuf
        """
        # 1. Resolve uId
        # Priority: Map what the client ASKED for, then fallback to global mapping
        uId = self._token_to_uId_map.get(user_id, {}).get(broker, {}).get(str(native_token))
        if not uId:
            uId = token_manager.get_uId(broker, native_token)
            
        if not uId:
            return

        # 2. Delta Filtering
        if user_id not in self.last_broadcasted:
            self.last_broadcasted[user_id] = {}
        
        last = self.last_broadcasted[user_id].get(uId, {'ltp': 0.0, 'v': 0})
        if last['ltp'] == ltp and last['v'] == volume:
            return # Skip redundant update
            
        # Update cache
        self.last_broadcasted[user_id][uId] = {'ltp': ltp, 'v': volume}

        # 3. Enrich Data
        meta = token_manager.get_metadata(uId) or {}
        
        # Use broker provided cp if available (Snap Quote), else fallback to DynamoDB meta
        final_cp = cp if cp > 0 else meta.get('cp', 0.0)
        
        chg = round(ltp - final_cp, 2) if final_cp > 0 else 0.0
        chgp = round((chg / final_cp * 100), 2) if final_cp > 0 else 0.0

        # 4. Protobuf Serialization
        exp_seq = token_manager.get_expiry_sequence(uId)
        
        if market_data_pb2:
            tick_kwargs = {
                "uId": uId,
                "ltp": float(ltp),
                "v": int(volume),
                "ts": int(time.time() * 1000)
            }
            if final_cp > 0:
                tick_kwargs["chg"] = float(chg)
                tick_kwargs["chgp"] = float(chgp)
                tick_kwargs["cp"] = float(final_cp)
            
            if exp_seq is not None:
                tick_kwargs["exp_seq"] = int(exp_seq)
                
            tick = market_data_pb2.TickUpdate(**tick_kwargs)
            data = tick.SerializeToString()
            await self._broadcast_binary(user_id, data)
        else:
            # Fallback to JSON if proto not generated yet
            await self.broadcast_to_user(user_id, {
                "uId": uId,
                "ltp": ltp,
                "chg": chg,
                "chgp": chgp,
                "v": volume,
                "exp_seq": exp_seq,
                "type": "TICK"
            })

    async def _broadcast_binary(self, user_id: str, data: bytes):
        """Send binary Protobuf data to all active user connections."""
        if user_id in self.client_connections:
            for ws in self.client_connections[user_id]:
                try:
                    await ws.send_bytes(data)
                except Exception as e:
                    logger.error(f"Error broadcasting binary to {user_id}: {e}")

    async def broadcast_to_user(self, user_id: str, message: Any):
        """Send a JSON message to all active client connections for this user."""
        if user_id in self.client_connections:
            payload = json.dumps(message)
            for ws in self.client_connections[user_id]:
                try:
                    await ws.send_text(payload)
                except Exception as e:
                    logger.error(f"Error broadcasting JSON to {user_id}: {e}")

    def get_subscriptions(self, user_id: str, broker: str) -> list:
        return list(self.subscriptions.get(user_id, {}).get(broker, []))

# Singleton instance
unified_manager = UnifiedWSManager()
