import requests
import json
import os
import csv
import io
import boto3
import logging
from datetime import datetime, date
from decimal import Decimal
import config

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MONTH_MAP = {
    'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
    'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12
}

class MasterMigrator:
    def __init__(self):
        self.table_name = config.DYNAMODB_TABLE_NAME
        self.dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')
        self.table = self.dynamodb.Table(self.table_name)
        self.all_items = []

    def normalize_expiry(self, expiry_str):
        """Standardize expiry to DDMMMYYYY (e.g. 26APR2026)"""
        if not expiry_str: return ""
        e = str(expiry_str).strip().upper()
        return e.replace("-", "").replace(" ", "")

    def parse_expiry_date(self, expiry_str):
        """Parse Angel expiry string (e.g. '26APR2026') to a date object. Returns None on failure."""
        if not expiry_str:
            return None
        e = self.normalize_expiry(expiry_str)
        try:
            return datetime.strptime(e, "%d%b%Y").date()
        except Exception:
            # Try shorter format like 26APR26
            try:
                return datetime.strptime(e, "%d%b%y").date()
            except Exception:
                return None

    def generate_uId(self, exch, symbol, itype, expiry=None, strike=None, option_type=None):
        exch = exch.upper()
        symbol = symbol.upper()
        
        if itype == "EQUITY":
            return f"{exch}:{symbol}"
        
        exp = self.normalize_expiry(expiry)
        
        if itype == "FUTURES":
            return f"{exch}:{symbol}{exp}FUT"
        
        if itype == "OPTIONS":
            s = str(strike).split('.')[0] if strike else ""
            ot = option_type.upper() if option_type else ""
            return f"{exch}:{symbol}{exp}{s}{ot}"
        
        return f"{exch}:{symbol}"

    def wipe_derivatives(self):
        """Delete all MCX and NFO futures/options entries from DynamoDB before re-sync."""
        logger.info("Wiping existing MCX and NFO derivative entries from DynamoDB...")
        
        to_delete = []
        
        # Scan for all MCX and NFO items
        for exch_target in ["MCX", "NFO"]:
            response = self.table.scan(
                FilterExpression=boto3.dynamodb.conditions.Attr('exch').eq(exch_target)
            )
            to_delete.extend(response.get('Items', []))
            while 'LastEvaluatedKey' in response:
                response = self.table.scan(
                    FilterExpression=boto3.dynamodb.conditions.Attr('exch').eq(exch_target),
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                to_delete.extend(response.get('Items', []))

        if not to_delete:
            logger.info("No existing derivative entries found to wipe.")
            return

        logger.info(f"Deleting {len(to_delete)} existing derivative entries...")
        with self.table.batch_writer() as batch:
            for item in to_delete:
                batch.delete_item(Key={
                    'uId': item['uId'],
                    'broker_name': item['broker_name']
                })
        logger.info(f"Wipe complete. Deleted {len(to_delete)} items.")

    def batch_push(self):
        if not self.all_items:
            logger.warning("No items to push.")
            return
        
        # Deduplicate by (uId, broker_name)
        seen = set()
        unique_items = []
        for item in self.all_items:
            key = (item['uId'], item['broker_name'])
            if key not in seen:
                unique_items.append(item)
                seen.add(key)
        
        logger.info(f"Starting BatchWriteItem for {len(unique_items)} unique items...")
        
        with self.table.batch_writer() as batch:
            for item in unique_items:
                clean_item = {}
                for k, v in item.items():
                    if v is None or v == "": continue
                    if isinstance(v, float):
                        clean_item[k] = Decimal(str(v))
                    elif isinstance(v, date) and not isinstance(v, datetime):
                        clean_item[k] = v.isoformat()
                    else:
                        clean_item[k] = v
                batch.put_item(Item=clean_item)
        
        logger.info("Batch push complete.")

    def _apply_triple_month_filter(self, items, exchange):
        """
        Groups items by base symbol and assigns expiry_sequence 1/2/3 (Near/Next/Far).
        - Only future-dated contracts are considered.
        - For NFO: contracts with lotsize == 0 are skipped (Liquid-Only Option B).
        - Only top 3 are kept; the rest are discarded.
        Returns only the filtered items that should be stored.
        """
        today = date.today()
        groups = {}  # key: symbol, value: list of (expiry_date, item)

        for item in items:
            if item.get("itype") != "FUTURES":
                continue

            expiry_dt = self.parse_expiry_date(item.get("expiry"))
            if expiry_dt is None or expiry_dt < today:
                continue  # skip expired or unparseable

            # Liquid-Only Guard (Option B): skip NFO contracts with zero lot size
            if exchange == "NFO":
                lotsize = item.get("lotsize", 0)
                try:
                    if int(lotsize) == 0:
                        logger.debug(f"Skipping zero-lotsize NFO contract: {item.get('uId')}")
                        continue
                except (ValueError, TypeError):
                    pass  # if lotsize can't be parsed, allow it through

            symbol = item.get("symbol", "")
            if symbol not in groups:
                groups[symbol] = []
            groups[symbol].append((expiry_dt, item))

        kept = []
        for symbol, pairs in groups.items():
            # Sort by expiry date ascending
            pairs.sort(key=lambda x: x[0])
            
            # Take only top 3
            for seq, (expiry_dt, item) in enumerate(pairs[:3], start=1):
                item["expiry_sequence"] = seq
                item["is_current_expiry"] = (seq == 1)
                kept.append(item)
            
            skipped = len(pairs) - 3
            if skipped > 0:
                logger.info(f"[Triple-Month] {exchange}:{symbol} — kept 3, discarded {skipped} extra contract(s).")

        return kept

    def process_angel(self):
        logger.info("Fetching Angel One master...")
        resp = requests.get(config.SCRIP_MASTER_URLS["angel"], timeout=60)
        data = resp.json()
        
        equity_items = []
        mcx_futures = []
        nfo_futures = []
        options_items = []

        for item in data:
            exch = item.get("exch_seg")
            itype = item.get("instrumenttype")
            base_symbol = item.get("name")
            token = str(item.get("token"))

            if not base_symbol:
                continue

            # Classify instrument type
            if itype in ["OPTSTK", "OPTIDX", "OPTCOM"]:
                u_itype = "OPTIONS"
            elif itype in ["FUTSTK", "FUTIDX", "FUTCOM"]:
                u_itype = "FUTURES"
            elif exch in ["NSE", "BSE"] and itype == "SYMBOL":
                u_itype = "EQUITY"
            else:
                continue  # skip unknown types

            # Option type detection
            raw_sym = item.get("symbol", "")
            if "CE" in raw_sym:
                opt_type = "CE"
            elif "PE" in raw_sym:
                opt_type = "PE"
            else:
                opt_type = ""

            uId = self.generate_uId(
                exch,
                base_symbol,
                u_itype,
                expiry=item.get("expiry"),
                strike=item.get("strike"),
                option_type=opt_type
            )

            record = {
                "uId": uId,
                "broker_name": "angel",
                "native_token": token,
                "symbol": base_symbol,
                "full_symbol": item.get("symbol"),
                "exch": exch,
                "itype": u_itype,
                "expiry": item.get("expiry"),
                "strike": item.get("strike"),
                "lotsize": item.get("lotsize", 0),
                "cp": 0.0
            }

            if u_itype == "EQUITY":
                equity_items.append(record)
            elif u_itype == "FUTURES" and exch == "MCX":
                mcx_futures.append(record)
            elif u_itype == "FUTURES" and exch in ["NSE", "NFO", "BSE"]:
                # Normalize all equity futures to NFO
                record["exch"] = "NFO"
                nfo_futures.append(record)
            elif u_itype == "OPTIONS":
                options_items.append(record)

        # Apply Triple-Month filter to MCX and NFO futures
        logger.info(f"Applying Triple-Month filter to {len(mcx_futures)} MCX futures...")
        filtered_mcx = self._apply_triple_month_filter(mcx_futures, "MCX")
        
        logger.info(f"Applying Triple-Month filter to {len(nfo_futures)} NFO futures (lot-size guard active)...")
        filtered_nfo = self._apply_triple_month_filter(nfo_futures, "NFO")

        logger.info(
            f"Angel One processed: {len(equity_items)} equities, "
            f"{len(filtered_mcx)} MCX futures (from {len(mcx_futures)}), "
            f"{len(filtered_nfo)} NFO futures (from {len(nfo_futures)}), "
            f"{len(options_items)} options."
        )

        self.all_items.extend(equity_items)
        self.all_items.extend(filtered_mcx)
        self.all_items.extend(filtered_nfo)
        # Note: Options are NOT wiped/re-imported here to avoid massive load.
        # They are fetched on-demand via the /angel/search-mcx?type=options endpoint.

    def run(self):
        # Step 1: Wipe existing derivatives
        self.wipe_derivatives()
        
        # Step 2: Fetch and filter
        self.process_angel()
        
        # Step 3: Push clean data
        self.batch_push()
        
        # Summary
        eq   = len([i for i in self.all_items if i['itype'] == "EQUITY"])
        fut  = len([i for i in self.all_items if i['itype'] == "FUTURES"])
        n1   = len([i for i in self.all_items if i.get('expiry_sequence') == 1])
        n2   = len([i for i in self.all_items if i.get('expiry_sequence') == 2])
        n3   = len([i for i in self.all_items if i.get('expiry_sequence') == 3])
        print(f"\n✅ Import Complete:")
        print(f"   Equities : {eq}")
        print(f"   Futures  : {fut} total → Near:{n1} | Next:{n2} | Far:{n3}")

if __name__ == "__main__":
    migrator = MasterMigrator()
    migrator.run()
