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

        # --- Failover Architecture ---
        # user_id -> broker_name (the PRIMARY broker for this user session)
        # Only ticks from the primary broker are pushed to the UI.
        # Secondary brokers are kept warm in the background for instant failover.
        self.primary_broker: Dict[str, str] = {}

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

    async def subscribe(self, user_id: str, broker: str, symbols: list, token: str, is_primary: bool = True):
        """Subscribe to symbols for a specific broker and user.
        
        Args:
            is_primary: If True, this broker's ticks will be pushed to the UI.
                        If False, the broker is kept warm (standby) for failover only.
        """
        if user_id not in self.subscriptions:
            self.subscriptions[user_id] = {}
        if broker not in self.subscriptions[user_id]:
            self.subscriptions[user_id][broker] = set()
        
        self.subscriptions[user_id][broker].update(symbols)
        
        # Set the primary broker if this subscription is marked as primary
        if is_primary:
            old_primary = self.primary_broker.get(user_id, "none")
            self.primary_broker[user_id] = broker
            if old_primary != broker:
                logger.info(f"[Failover] Primary broker for {user_id} SET to: {broker} (was: {old_primary})")
        else:
            # Only set primary if not already set
            if user_id not in self.primary_broker:
                self.primary_broker[user_id] = broker
            logger.info(f"[Failover] Broker {broker} for {user_id} started in WARM STANDBY mode.")
        
        # Build local uId map for this broker (uId -> native_token and native_token -> uId)
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
        1. Gate: Only process ticks from the PRIMARY broker. Secondary stays warm but silent.
        2. Resolve uId (Preferring client's requested name, then TokenManager)
        3. Enrich data (math for change_percent)
        4. Delta Filtering (only send if price/volume changed)
        5. Broadcast as Protobuf
        """
        # 1. Failover Gate — only push to UI if this broker is the primary
        current_primary = self.primary_broker.get(user_id)
        if current_primary and current_primary != broker:
            # Secondary broker is receiving ticks but is kept silent.
            # This allows it to maintain a "warm" connection ready for instant failover.
            return

        # 2. Resolve uId
        # Priority: Map what the client ASKED for, then fallback to global mapping
        uId = self._token_to_uId_map.get(user_id, {}).get(broker, {}).get(str(native_token))
        if not uId:
            uId = token_manager.get_uId(broker, native_token)
            
        if not uId:
            return

        # 3. Delta Filtering
        if user_id not in self.last_broadcasted:
            self.last_broadcasted[user_id] = {}
        
        last = self.last_broadcasted[user_id].get(uId, {'ltp': 0.0, 'v': 0})
        if last['ltp'] == ltp and last['v'] == volume:
            return # Skip redundant update
            
        # Update cache
        self.last_broadcasted[user_id][uId] = {'ltp': ltp, 'v': volume}

        # 4. Enrich Data
        meta = token_manager.get_metadata(uId) or {}
        
        # Use broker provided cp if available (Snap Quote), else fallback to DynamoDB meta
        final_cp = cp if cp > 0 else meta.get('cp', 0.0)
        
        chg = round(ltp - final_cp, 2) if final_cp > 0 else 0.0
        chgp = round((chg / final_cp * 100), 2) if final_cp > 0 else 0.0

        # 5. Protobuf Serialization
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

    async def failover(self, user_id: str, failed_broker: str):
        """Perform a silent failover: promote the next available warm broker to primary."""
        subs = self.broker_tasks.get(user_id, {})
        # Find a warm broker that is still running
        for broker, task in subs.items():
            if broker != failed_broker and not task.done():
                old = self.primary_broker.get(user_id)
                self.primary_broker[user_id] = broker
                logger.warning(f"[Failover] SILENT FAILOVER triggered for {user_id}: {old} -> {broker}")
                return
        logger.error(f"[Failover] No warm standby broker available for {user_id}. Primary {failed_broker} failed.")

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
        """Returns the list of uIds subscribed by the user for this broker."""
        return list(self.subscriptions.get(user_id, {}).get(broker, []))

    def get_native_subscriptions(self, user_id: str, broker: str) -> list:
        """Returns the list of native tokens for the given broker.
        Used by upstream WebSocket clients to subscribe to broker-specific feeds.
        """
        token_map = self._token_to_uId_map.get(user_id, {}).get(broker, {})
        return list(token_map.keys())

# Singleton instance
unified_manager = UnifiedWSManager()
