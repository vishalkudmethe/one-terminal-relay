import asyncio
import json
import logging
from typing import Dict, Set, Any, Optional
from fastapi import WebSocket

logger = logging.getLogger(__name__)

class UnifiedWSManager:
    def __init__(self):
        # user_id -> set of active client web sockets (mobile/web)
        self.client_connections: Dict[str, Set[WebSocket]] = {}
        
        # user_id -> {broker_name -> broker_client_task}
        self.broker_tasks: Dict[str, Dict[str, asyncio.Task]] = {}
        
        # user_id -> {broker_name -> {symbols_set}}
        self.subscriptions: Dict[str, Dict[str, Set[str]]] = {}

    async def connect_client(self, user_id: str, websocket: WebSocket):
        await websocket.accept()
        if user_id not in self.client_connections:
            self.client_connections[user_id] = set()
        self.client_connections[user_id].add(websocket)
        logger.info(f"Client {user_id} connected. Total clients: {len(self.client_connections)}")

    def disconnect_client(self, user_id: str, websocket: WebSocket):
        if user_id in self.client_connections:
            self.client_connections[user_id].discard(websocket)
            if not self.client_connections[user_id]:
                del self.client_connections[user_id]
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

    async def broadcast_to_user(self, user_id: str, message: Any):
        """Send a message to all active client connections for a user."""
        if user_id in self.client_connections:
            msg_str = json.dumps(message) if not isinstance(message, str) else message
            for ws in self.client_connections[user_id]:
                try:
                    await ws.send_text(msg_str)
                except Exception as e:
                    logger.error(f"Error broadcasting to {user_id}: {e}")

    def get_subscriptions(self, user_id: str, broker: str) -> list:
        return list(self.subscriptions.get(user_id, {}).get(broker, []))

# Singleton instance
unified_manager = UnifiedWSManager()
