import boto3
import logging
import config
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)

class TokenManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(TokenManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
            
        self.table_name = config.DYNAMODB_TABLE_NAME
        self.dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')
        self.table = self.dynamodb.Table(self.table_name)
        
        # Caches
        # broker -> native_token -> uId
        self._broker_to_uId: Dict[str, Dict[str, str]] = {
            'angel': {}, 'upstox': {}, 'fyers': {}
        }
        
        # uId -> broker -> native_token
        self._uId_to_broker_token: Dict[str, Dict[str, str]] = {}
        
        # uId -> metadata
        self._uId_metadata: Dict[str, Dict[str, Any]] = {}
        
        # Base Symbol Resolution (e.g. MCX:GOLD -> current active contract uId)
        # format: { "MCX:GOLD": "MCX:GOLD26APR2024FUT" }
        self._base_to_current: Dict[str, str] = {}
        
        self._initialized = True
        logger.info(f"TokenManager initialized (DynamoDB Only): {self.table_name}")

    def eager_load(self):
        """Eagerly load all mappings from DynamoDB. Local JSON fallback DECOMMISSIONED."""
        logger.info("Starting DynamoDB scan for token mappings...")
        
        try:
            response = self.table.scan()
            items = response.get('Items', [])
            
            while True:
                for item in items:
                    self._process_item(item)
                
                if 'LastEvaluatedKey' not in response:
                    break
                response = self.table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                items = response.get('Items', [])
                
            logger.info(f"DynamoDB eager load complete. Cached {len(self._uId_metadata)} unique IDs.")
        except Exception as e:
            logger.error(f"Critical: DynamoDB eager load failed: {e}")
            raise e

    def _process_item(self, item: dict):
        uId = item['uId']
        broker = item['broker_name']
        token = str(item['native_token'])
        
        # 1. Global Metadata
        if uId not in self._uId_metadata:
            self._uId_metadata[uId] = {
                'symbol': item.get('full_symbol') or item.get('symbol'),
                'name': item.get('symbol'),
                'exch': item.get('exch'),
                'itype': item.get('itype'),
                'expiry': item.get('expiry'),
                'expiry_seq': item.get('expiry_sequence'),
                'cp': float(item.get('cp', 0))
            }
        
        # 2. Broker Mappings
        if broker not in self._broker_to_uId:
            self._broker_to_uId[broker] = {}
        self._broker_to_uId[broker][token] = uId
        
        if uId not in self._uId_to_broker_token:
            self._uId_to_broker_token[uId] = {}
        self._uId_to_broker_token[uId][broker] = token
        
        # 3. Base Symbol Mapping (for F&O)
        if item.get('is_current_expiry') is True:
            base_key = f"{item.get('exch')}:{item.get('symbol')}"
            self._base_to_current[base_key] = uId

    def get_base_uId(self, base_key: str) -> Optional[str]:
        return self._base_to_current.get(base_key)

    def get_angel_mcx_futures(self):
        result = []
        for uId, broker_map in self._uId_to_broker_token.items():
            if "angel" in broker_map:
                meta = self._uId_metadata.get(uId, {})
                if meta.get("exch") == "MCX" and meta.get("itype") == "FUTURES":
                    result.append({
                        "token": broker_map["angel"],
                        "symbol": meta.get("symbol"),
                        "name": meta.get("name"),
                        "expiry": meta.get("expiry")
                    })
        return result

    def get_angel_mcx_options(self):
        result = []
        for uId, broker_map in self._uId_to_broker_token.items():
            if "angel" in broker_map:
                meta = self._uId_metadata.get(uId, {})
                if meta.get("exch") == "MCX" and meta.get("itype") == "OPTIONS":
                    result.append({
                        "token": broker_map["angel"],
                        "symbol": meta.get("symbol"),
                        "name": meta.get("name"),
                        "expiry": meta.get("expiry")
                    })
        return result

    def get_angel_nse_futures(self):
        result = []
        for uId, broker_map in self._uId_to_broker_token.items():
            if "angel" in broker_map:
                meta = self._uId_metadata.get(uId, {})
                if meta.get("exch") in ["NSE", "NFO"] and meta.get("itype") == "FUTURES":
                    result.append({
                        "token": broker_map["angel"],
                        "symbol": meta.get("symbol"),
                        "name": meta.get("name"),
                        "expiry": meta.get("expiry")
                    })
        return result

    def get_uId(self, broker: str, native_token: str) -> Optional[str]:
        return self._broker_to_uId.get(broker, {}).get(str(native_token))

    def get_metadata(self, uId: str) -> Optional[Dict[str, Any]]:
        return self._uId_metadata.get(uId)

    def get_expiry_sequence(self, uId: str) -> Optional[int]:
        meta = self.get_metadata(uId)
        if meta:
            return meta.get('expiry_seq')
        return None

    def get_native_token(self, uId: str, broker: str) -> Optional[str]:
        # Priority 1: Exact match (e.g. NFO:NIFTY26APR26FUT)
        token = self._uId_to_broker_token.get(uId, {}).get(broker)
        if token: return token
        
        # Priority 2: Base match (e.g. MCX:GOLD -> resolve to current contract)
        current_uId = self._base_to_current.get(uId)
        if current_uId:
            return self._uId_to_broker_token.get(current_uId, {}).get(broker)
            
        return None

# Singleton instance
token_manager = TokenManager()
