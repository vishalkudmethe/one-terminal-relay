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

    def wipe_all(self):
        """Delete ALL entries from DynamoDB before re-sync to ensure a 100% clean slate."""
        logger.info(f"Initiating Complete Wipe of DynamoDB table: {self.table_name}")
        
        # We need concurrent.futures for parallel wipe
        import concurrent.futures
        
        to_delete = []
        
        # Scan entire table (ProjectionExpression to save bandwidth)
        response = self.table.scan(ProjectionExpression="uId, broker_name")
        to_delete.extend(response.get('Items', []))
        while 'LastEvaluatedKey' in response:
            response = self.table.scan(
                ProjectionExpression="uId, broker_name",
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            to_delete.extend(response.get('Items', []))

        if not to_delete:
            logger.info("Table is already empty.")
            return

        logger.info(f"Found {len(to_delete)} items. Deleting in parallel batches...")
        
        # Split into chunks of 25 (DynamoDB max batch size for write)
        chunks = [to_delete[i:i + 25] for i in range(0, len(to_delete), 25)]
        
        def delete_batch(chunk):
            with self.table.batch_writer() as batch:
                for item in chunk:
                    batch.delete_item(Key={'uId': item['uId'], 'broker_name': item['broker_name']})
                    
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(delete_batch, chunks)
            
        logger.info(f"Complete Wipe successful. Deleted {len(to_delete)} items.")

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
    def process_upstox(self):
        logger.info("Fetching Upstox master...")
        import urllib.request, gzip, io, csv
        url = "https://assets.upstox.com/market-quote/instruments/exchange/complete.csv.gz"
        try:
            r = urllib.request.urlopen(url)
            csv_data = gzip.GzipFile(fileobj=io.BytesIO(r.read())).read().decode('utf-8')
            reader = csv.DictReader(io.StringIO(csv_data))
        except Exception as e:
            logger.error(f"Failed to fetch Upstox master: {e}")
            return
            
        equity_items = []
        mcx_futures = []
        nfo_futures = []
        options_items = []

        for row in reader:
            exch = row.get("exchange", "").upper()
            if exch not in ["NSE_EQ", "BSE_EQ", "NSE_FO", "MCX_FO"]:
                continue
            
            raw_itype = row.get("instrument_type", "").upper()
            if raw_itype == "EQUITY":
                u_itype = "EQUITY"
            elif raw_itype in ["FUTCOM", "FUTSTK", "FUTIDX"] or "FUT" in raw_itype:
                u_itype = "FUTURES"
            elif raw_itype in ["OPTCOM", "OPTSTK", "OPTIDX"] or "OPT" in raw_itype:
                u_itype = "OPTIONS"
            else:
                continue

            std_exch = "NSE"
            if exch == "BSE_EQ": std_exch = "BSE"
            elif exch == "MCX_FO": std_exch = "MCX"
            elif exch == "NSE_FO": std_exch = "NFO"

            base_symbol = row.get("name", "") if row.get("name", "") else row.get("tradingsymbol", "")
            
            expiry_raw = row.get("expiry", "")
            expiry_str = ""
            if expiry_raw:
                try:
                    dt = datetime.strptime(expiry_raw, "%Y-%m-%d")
                    expiry_str = dt.strftime("%d%b%Y").upper()
                except:
                    pass

            uId = self.generate_uId(
                std_exch,
                base_symbol,
                u_itype,
                expiry=expiry_str,
                strike=row.get("strike"),
                option_type=row.get("option_type")
            )

            record = {
                "uId": uId,
                "broker_name": "upstox",
                "native_token": row.get("instrument_key"),
                "symbol": base_symbol,
                "full_symbol": row.get("tradingsymbol"),
                "exch": std_exch,
                "itype": u_itype,
                "expiry": expiry_str,
                "strike": row.get("strike"),
                "lotsize": row.get("lot_size", 0),
                "cp": float(row.get("last_price", 0.0) or 0)
            }

            if u_itype == "EQUITY":
                equity_items.append(record)
            elif u_itype == "FUTURES" and std_exch == "MCX":
                mcx_futures.append(record)
            elif u_itype == "FUTURES" and std_exch == "NFO":
                nfo_futures.append(record)
            elif u_itype == "OPTIONS":
                options_items.append(record)

        logger.info(f"Applying Triple-Month filter to {len(mcx_futures)} Upstox MCX futures...")
        filtered_mcx = self._apply_triple_month_filter(mcx_futures, "MCX")
        
        logger.info(f"Applying Triple-Month filter to {len(nfo_futures)} Upstox NFO futures...")
        filtered_nfo = self._apply_triple_month_filter(nfo_futures, "NFO")

        logger.info(
            f"Upstox processed: {len(equity_items)} equities, "
            f"{len(filtered_mcx)} MCX futures, "
            f"{len(filtered_nfo)} NFO futures, "
            f"{len(options_items)} options."
        )

        self.all_items.extend(equity_items)
        self.all_items.extend(filtered_mcx)
        self.all_items.extend(filtered_nfo)

    def process_fyers(self):
        logger.info("Fetching Fyers master...")
        import urllib.request, csv, io
        
        urls = {
            "NSE_EQ": "https://public.fyers.in/sym_details/NSE_CM.csv",
            "NSE_FO": "https://public.fyers.in/sym_details/NSE_FO.csv",
            "MCX_FO": "https://public.fyers.in/sym_details/MCX_FO.csv"
        }
        
        equity_items = []
        mcx_futures = []
        nfo_futures = []
        options_items = []

        for category, url in urls.items():
            try:
                r = urllib.request.urlopen(url)
                csv_data = r.read().decode('utf-8')
                reader = csv.reader(io.StringIO(csv_data))
                
                for row in reader:
                    if len(row) < 16:
                        continue
                        
                    fytoken = row[0]
                    name = row[1]
                    lotsize = row[3]
                    expiry_raw = row[7] # Timestamp or Date string
                    symbol_str = row[9] # NSE:RELIANCE-EQ or NSE:NIFTY24APR22000CE
                    
                    # Determine type from symbol
                    if "-EQ" in symbol_str:
                        u_itype = "EQUITY"
                        base_symbol = name
                        std_exch = "NSE"
                    elif "FUT" in symbol_str and "MCX:" in symbol_str:
                        u_itype = "FUTURES"
                        base_symbol = name.split()[0] if name else ""
                        std_exch = "MCX"
                    elif "FUT" in symbol_str and "NSE:" in symbol_str:
                        u_itype = "FUTURES"
                        base_symbol = name.split()[0] if name else ""
                        std_exch = "NFO"
                    elif "CE" in symbol_str or "PE" in symbol_str:
                        u_itype = "OPTIONS"
                        base_symbol = name.split()[0] if name else ""
                        std_exch = "NFO" if "NSE:" in symbol_str else "MCX"
                    else:
                        continue
                        
                    expiry_str = ""
                    # Fyers usually sends timestamps for expiry
                    if expiry_raw and expiry_raw != 'None' and expiry_raw.isdigit():
                        try:
                            dt = datetime.fromtimestamp(int(expiry_raw))
                            expiry_str = dt.strftime("%d%b%Y").upper()
                        except:
                            pass
                            
                    uId = self.generate_uId(
                        std_exch,
                        base_symbol,
                        u_itype,
                        expiry=expiry_str
                    )

                    record = {
                        "uId": uId,
                        "broker_name": "fyers",
                        "native_token": fytoken,
                        "symbol": base_symbol,
                        "full_symbol": symbol_str,
                        "exch": std_exch,
                        "itype": u_itype,
                        "expiry": expiry_str,
                        "lotsize": lotsize,
                        "cp": 0.0
                    }

                    if u_itype == "EQUITY":
                        equity_items.append(record)
                    elif u_itype == "FUTURES" and std_exch == "MCX":
                        mcx_futures.append(record)
                    elif u_itype == "FUTURES" and std_exch == "NFO":
                        nfo_futures.append(record)
                    elif u_itype == "OPTIONS":
                        options_items.append(record)
            except Exception as e:
                logger.error(f"Failed to fetch Fyers {category}: {e}")

        logger.info(f"Applying Triple-Month filter to {len(mcx_futures)} Fyers MCX futures...")
        filtered_mcx = self._apply_triple_month_filter(mcx_futures, "MCX")
        
        logger.info(f"Applying Triple-Month filter to {len(nfo_futures)} Fyers NFO futures...")
        filtered_nfo = self._apply_triple_month_filter(nfo_futures, "NFO")

        logger.info(
            f"Fyers processed: {len(equity_items)} equities, "
            f"{len(filtered_mcx)} MCX futures, "
            f"{len(filtered_nfo)} NFO futures, "
            f"{len(options_items)} options."
        )

        self.all_items.extend(equity_items)
        self.all_items.extend(filtered_mcx)
        self.all_items.extend(filtered_nfo)
    def process_zerodha(self):
        logger.info("Fetching Zerodha/Kite master...")
        import urllib.request, csv, io
        url = "https://api.kite.trade/instruments"
        try:
            r = urllib.request.urlopen(url)
            csv_data = r.read().decode('utf-8')
            reader = csv.DictReader(io.StringIO(csv_data))
        except Exception as e:
            logger.error(f"Failed to fetch Zerodha master: {e}")
            return

        equity_items = []
        mcx_futures = []
        nfo_futures = []
        options_items = []

        for row in reader:
            exch = row.get("exchange", "").upper()
            segment = row.get("segment", "").upper()
            raw_itype = row.get("instrument_type", "").upper()

            # Only process Indian market exchanges
            if exch not in ["NSE", "BSE", "NFO", "MCX"]:
                continue

            if raw_itype == "EQ":
                u_itype = "EQUITY"
            elif raw_itype in ["FUT", "FUTCOM", "FUTIDX", "FUTSTK"]:
                u_itype = "FUTURES"
            elif raw_itype in ["CE", "PE", "OPTIDX", "OPTSTK", "OPTCOM"]:
                u_itype = "OPTIONS"
            else:
                continue

            std_exch = exch
            if exch == "NFO": std_exch = "NFO"
            elif exch == "MCX": std_exch = "MCX"
            elif exch == "NSE" and u_itype == "EQUITY": std_exch = "NSE"

            base_symbol = row.get("name", "") or row.get("tradingsymbol", "")
            instrument_token = row.get("instrument_token", "")
            
            expiry_raw = row.get("expiry", "")
            expiry_str = ""
            if expiry_raw:
                try:
                    dt = datetime.strptime(expiry_raw, "%Y-%m-%d")
                    expiry_str = dt.strftime("%d%b%Y").upper()
                except:
                    pass

            opt_type = ""
            if raw_itype == "CE":
                opt_type = "CE"
            elif raw_itype == "PE":
                opt_type = "PE"
            elif u_itype == "OPTIONS":
                sym = row.get("tradingsymbol", "")
                opt_type = "CE" if sym.endswith("CE") else "PE" if sym.endswith("PE") else ""

            uId = self.generate_uId(
                std_exch, base_symbol, u_itype,
                expiry=expiry_str,
                strike=row.get("strike"),
                option_type=opt_type
            )

            record = {
                "uId": uId,
                "broker_name": "zerodha",
                "native_token": instrument_token,
                "symbol": base_symbol,
                "full_symbol": row.get("tradingsymbol"),
                "exch": std_exch,
                "itype": u_itype,
                "expiry": expiry_str,
                "strike": row.get("strike"),
                "lotsize": row.get("lot_size", 0),
                "cp": float(row.get("last_price", 0) or 0)
            }

            if u_itype == "EQUITY":
                equity_items.append(record)
            elif u_itype == "FUTURES" and std_exch == "MCX":
                mcx_futures.append(record)
            elif u_itype == "FUTURES" and std_exch == "NFO":
                nfo_futures.append(record)
            elif u_itype == "OPTIONS":
                options_items.append(record)

        logger.info(f"Applying Triple-Month filter to {len(mcx_futures)} Zerodha MCX futures...")
        filtered_mcx = self._apply_triple_month_filter(mcx_futures, "MCX")
        logger.info(f"Applying Triple-Month filter to {len(nfo_futures)} Zerodha NFO futures...")
        filtered_nfo = self._apply_triple_month_filter(nfo_futures, "NFO")

        logger.info(
            f"Zerodha processed: {len(equity_items)} equities, "
            f"{len(filtered_mcx)} MCX futures, {len(filtered_nfo)} NFO futures."
        )
        self.all_items.extend(equity_items)
        self.all_items.extend(filtered_mcx)
        self.all_items.extend(filtered_nfo)

    def process_groww(self):
        logger.info("Fetching Groww master...")
        import urllib.request, csv, io
        # Groww public instrument CSV
        url = "https://growwapi.groww.in/v1/historical/instrument?exchange=NSE&segment=CASH"
        
        equity_items = []
        mcx_futures = []
        nfo_futures = []

        for segment_code, segment_name, std_exch in [
            ("CASH", "NSE_EQ", "NSE"),
            ("FNO", "NSE_FO", "NFO"),
            ("COMMODITY", "MCX_FO", "MCX"),
        ]:
            try:
                seg_url = f"https://growwapi.groww.in/v1/historical/instrument?exchange=NSE&segment={segment_code}"
                if segment_code == "COMMODITY":
                    seg_url = "https://growwapi.groww.in/v1/historical/instrument?exchange=MCX&segment=COMMODITY"
                
                req = urllib.request.Request(seg_url, headers={"User-Agent": "OneTerminal/2.5"})
                r = urllib.request.urlopen(req, timeout=20)
                data_bytes = r.read()
                
                # Groww returns JSON array
                import json as _json
                instruments = _json.loads(data_bytes.decode('utf-8'))
                if not isinstance(instruments, list):
                    instruments = instruments.get("data", [])

                for item in instruments:
                    exchange_token = str(item.get("exchange_token") or item.get("exchangeToken", ""))
                    trading_symbol = item.get("trading_symbol") or item.get("tradingSymbol", "")
                    name = item.get("name") or item.get("company_name", trading_symbol)
                    raw_itype = (item.get("instrument_type") or item.get("instrumentType", "")).upper()
                    expiry_raw = item.get("expiry") or item.get("expiry_date", "")
                    lotsize = item.get("lot_size") or item.get("lotSize", 1)
                    
                    if raw_itype in ["", "EQ", "EQUITY"]:
                        u_itype = "EQUITY"
                    elif "FUT" in raw_itype:
                        u_itype = "FUTURES"
                    elif raw_itype in ["CE", "PE"] or "OPT" in raw_itype:
                        u_itype = "OPTIONS"
                    else:
                        continue

                    expiry_str = ""
                    if expiry_raw:
                        for fmt in ["%Y-%m-%d", "%d-%m-%Y", "%d%b%Y"]:
                            try:
                                dt = datetime.strptime(str(expiry_raw), fmt)
                                expiry_str = dt.strftime("%d%b%Y").upper()
                                break
                            except:
                                pass

                    uId = self.generate_uId(std_exch, name or trading_symbol, u_itype, expiry=expiry_str)

                    # Groww native token: stored as "SEGMENT:exchange_token"
                    native_token = f"{segment_code}:{exchange_token}"

                    record = {
                        "uId": uId,
                        "broker_name": "groww",
                        "native_token": native_token,
                        "symbol": name or trading_symbol,
                        "full_symbol": trading_symbol,
                        "exch": std_exch,
                        "itype": u_itype,
                        "expiry": expiry_str,
                        "lotsize": lotsize,
                        "cp": 0.0
                    }

                    if u_itype == "EQUITY":
                        equity_items.append(record)
                    elif u_itype == "FUTURES" and std_exch == "MCX":
                        mcx_futures.append(record)
                    elif u_itype == "FUTURES" and std_exch == "NFO":
                        nfo_futures.append(record)

            except Exception as e:
                logger.warning(f"Groww segment {segment_code} failed: {e} — skipping.")

        logger.info(f"Applying Triple-Month filter to {len(mcx_futures)} Groww MCX futures...")
        filtered_mcx = self._apply_triple_month_filter(mcx_futures, "MCX")
        logger.info(f"Applying Triple-Month filter to {len(nfo_futures)} Groww NFO futures...")
        filtered_nfo = self._apply_triple_month_filter(nfo_futures, "NFO")

        logger.info(
            f"Groww processed: {len(equity_items)} equities, "
            f"{len(filtered_mcx)} MCX futures, {len(filtered_nfo)} NFO futures."
        )
        self.all_items.extend(equity_items)
        self.all_items.extend(filtered_mcx)
        self.all_items.extend(filtered_nfo)

    def process_icici(self):
        logger.info("Fetching ICICI Breeze master...")
        import urllib.request, csv, io
        url = "https://api.icicidirect.com/breezeapi/documents/index.html#instruments"
        # ICICI Breeze requires auth for the actual CSV endpoints.
        # This is a stub that should be replaced with the authenticated fetch
        # when an active session is provided via config/database.
        logger.info("ICICI Master fetch requires auth. Stub executed.")

    def process_kotak(self):
        logger.info("Fetching Kotak Neo master...")
        # Kotak Neo requires auth to fetch the masterscrip paths via:
        # /script-details/1.0/masterscrip/file-paths
        logger.info("Kotak Neo Master fetch requires auth. Stub executed.")

    def process_dhan(self):
        logger.info("Fetching DhanHQ master...")
        import urllib.request, csv, io
        # DhanHQ public instrument CSV
        url = "https://images.dhan.co/api-data/api-scrip-master.csv"
        try:
            r = urllib.request.urlopen(url)
            csv_data = r.read().decode('utf-8')
            reader = csv.DictReader(io.StringIO(csv_data))
        except Exception as e:
            logger.error(f"Failed to fetch Dhan master: {e}")
            return

        equity_items = []
        mcx_futures = []
        nfo_futures = []

        for row in reader:
            exch_id = row.get("SEM_EXM_EXCH_ID", "").upper()
            segment = row.get("SEM_SEGMENT", "").upper()
            
            # Map Dhan exchanges
            std_exch = ""
            if exch_id == "NSE":
                std_exch = "NSE" if segment == "EQ" else "NFO"
            elif exch_id == "MCX":
                std_exch = "MCX"
            else:
                continue

            raw_itype = row.get("SEM_INSTRUMENT_NAME", "").upper()
            if raw_itype == "EQUITIES" or raw_itype == "EQ":
                u_itype = "EQUITY"
            elif "FUT" in raw_itype:
                u_itype = "FUTURES"
            elif "OPT" in raw_itype:
                u_itype = "OPTIONS"
            else:
                continue

            base_symbol = row.get("SEM_CUSTOM_SYMBOL", "") or row.get("SEM_TRADING_SYMBOL", "")
            security_id = row.get("SEM_SMST_SECURITY_ID", "")
            
            expiry_raw = row.get("SEM_EXPIRY_DATE", "")
            expiry_str = ""
            if expiry_raw and u_itype != "EQUITY":
                try:
                    # Dhan format typically: "2024-03-28 14:30:00"
                    dt = datetime.strptime(expiry_raw.split()[0], "%Y-%m-%d")
                    expiry_str = dt.strftime("%d%b%Y").upper()
                except:
                    pass

            uId = self.generate_uId(
                std_exch, base_symbol, u_itype,
                expiry=expiry_str,
                strike=row.get("SEM_STRIKE_PRICE"),
                option_type=row.get("SEM_OPTION_TYPE")
            )

            # Native token for Dhan WS: "ExchangeSegment:SecurityId"
            seg_str = "NSE_EQ" if std_exch == "NSE" and u_itype == "EQUITY" else "NSE_FO" if std_exch == "NFO" else "MCX_FO"
            native_token = f"{seg_str}:{security_id}"

            record = {
                "uId": uId,
                "broker_name": "dhan",
                "native_token": native_token,
                "symbol": base_symbol,
                "full_symbol": row.get("SEM_TRADING_SYMBOL"),
                "exch": std_exch,
                "itype": u_itype,
                "expiry": expiry_str,
                "strike": row.get("SEM_STRIKE_PRICE"),
                "lotsize": row.get("SEM_LOT_SIZE", 0),
                "cp": 0.0
            }

            if u_itype == "EQUITY":
                equity_items.append(record)
            elif u_itype == "FUTURES" and std_exch == "MCX":
                mcx_futures.append(record)
            elif u_itype == "FUTURES" and std_exch == "NFO":
                nfo_futures.append(record)

        logger.info(f"Applying Triple-Month filter to {len(mcx_futures)} Dhan MCX futures...")
        filtered_mcx = self._apply_triple_month_filter(mcx_futures, "MCX")
        logger.info(f"Applying Triple-Month filter to {len(nfo_futures)} Dhan NFO futures...")
        filtered_nfo = self._apply_triple_month_filter(nfo_futures, "NFO")

        logger.info(
            f"Dhan processed: {len(equity_items)} equities, "
            f"{len(filtered_mcx)} MCX futures, {len(filtered_nfo)} NFO futures."
        )
        self.all_items.extend(equity_items)
        self.all_items.extend(filtered_mcx)
        self.all_items.extend(filtered_nfo)

    def process_motilal(self):
        logger.info("Fetching Motilal Oswal master...")
        # Motilal Oswal MOAPI requires auth to hit their getscripsbyexchangename API
        logger.info("Motilal Oswal Master fetch requires auth. Stub executed.")

    def process_xts(self):
        logger.info("Fetching Universal XTS master...")
        # XTS requires auth to hit their master endpoint (dynamic base URL)
        logger.info("Universal XTS Master fetch requires auth. Stub executed.")

    def process_prabhudas(self):
        logger.info("Fetching Prabhudas Lilladher master...")
        # Prabhudas Lilladher requires auth to hit their master endpoint
        logger.info("Prabhudas Lilladher Master fetch requires auth. Stub executed.")

    def process_axis(self):
        logger.info("Fetching Axis Direct master...")
        # Axis Direct requires auth to hit their master endpoint
        logger.info("Axis Direct Master fetch requires auth. Stub executed.")

    def process_iifl(self):
        logger.info("Fetching IIFL Markets master...")
        import urllib.request, csv, io
        # IIFL Markets public instrument CSV
        url = "http://content.indiainfoline.com/IIFLTT/Scripmaster.csv"
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            r = urllib.request.urlopen(req)
            csv_data = r.read().decode('utf-8')
            reader = csv.DictReader(io.StringIO(csv_data))
        except Exception as e:
            logger.error(f"Failed to fetch IIFL master: {e}")
            return

        equity_items = []
        mcx_futures = []
        nfo_futures = []

        for row in reader:
            exch_id = row.get("Exch", "").upper()
            segment = row.get("ExchType", "").upper()
            
            # Map IIFL exchanges
            std_exch = ""
            if exch_id == "N":
                std_exch = "NSE" if segment == "C" else "NFO"
            elif exch_id == "M":
                std_exch = "MCX"
            else:
                continue

            # In IIFL, ExchType "C" is Cash/Equity, "D" is Derivative, "U" is Currency, "Y" is Commodity
            if segment == "C":
                u_itype = "EQUITY"
            elif segment in ["D", "Y"]:
                # Check CpType to differentiate Futures vs Options
                if row.get("CpType", "") == "XX":
                    u_itype = "FUTURES"
                else:
                    u_itype = "OPTIONS"
            else:
                continue

            base_symbol = row.get("Name", "")
            security_id = row.get("ScripCode", "")
            
            expiry_raw = row.get("Expiry", "")
            expiry_str = ""
            if expiry_raw and u_itype != "EQUITY":
                try:
                    # IIFL expiry format usually resembles "20240328" or "28-Mar-2024"
                    # Attempt simple parsing if it exists
                    if '-' in expiry_raw:
                        dt = datetime.strptime(expiry_raw, "%d-%b-%Y")
                    else:
                        dt = datetime.strptime(expiry_raw[:8], "%Y%m%d")
                    expiry_str = dt.strftime("%d%b%Y").upper()
                except:
                    pass

            uId = self.generate_uId(
                std_exch, base_symbol, u_itype,
                expiry=expiry_str,
                strike=row.get("StrikeRate"),
                option_type=row.get("CpType")
            )

            # Native token for IIFL WS: "Exch,ExchType,ScripCode" e.g. "N,C,2885"
            native_token = f"{exch_id},{segment},{security_id}"

            record = {
                "uId": uId,
                "broker_name": "iifl",
                "native_token": native_token,
                "symbol": base_symbol,
                "full_symbol": base_symbol,
                "exch": std_exch,
                "itype": u_itype,
                "expiry": expiry_str,
                "strike": row.get("StrikeRate"),
                "lotsize": 0, # Note: LotSize might require a separate fetch or mapping in IIFL
                "cp": 0.0
            }

            if u_itype == "EQUITY":
                equity_items.append(record)
            elif u_itype == "FUTURES" and std_exch == "MCX":
                mcx_futures.append(record)
            elif u_itype == "FUTURES" and std_exch == "NFO":
                nfo_futures.append(record)

        logger.info(f"Applying Triple-Month filter to {len(mcx_futures)} IIFL MCX futures...")
        filtered_mcx = self._apply_triple_month_filter(mcx_futures, "MCX")
        logger.info(f"Applying Triple-Month filter to {len(nfo_futures)} IIFL NFO futures...")
        filtered_nfo = self._apply_triple_month_filter(nfo_futures, "NFO")

        logger.info(
            f"IIFL processed: {len(equity_items)} equities, "
            f"{len(filtered_mcx)} MCX futures, {len(filtered_nfo)} NFO futures."
        )
        self.all_items.extend(equity_items)
        self.all_items.extend(filtered_mcx)
        self.all_items.extend(filtered_nfo)

    def process_nuvama(self):
        logger.info("Fetching Nuvama master...")
        # Nuvama requires auth to hit their master endpoint
        logger.info("Nuvama Master fetch requires auth. Stub executed.")

    def process_profitmart(self):
        logger.info("Fetching Profitmart master...")
        # Profitmart requires auth to hit their master endpoint
        logger.info("Profitmart Master fetch requires auth. Stub executed.")

    def process_religare(self):
        logger.info("Fetching Religare master...")
        # Religare requires auth to hit their master endpoint
        logger.info("Religare Master fetch requires auth. Stub executed.")

    def run(self):
        # Step 1: Complete Wipe
        self.wipe_all()
        
        # Step 2: Fetch and filter (Angel first as baseline, then all others)
        self.process_angel()
        self.process_fyers()
        self.process_upstox()
        self.process_zerodha()
        self.process_groww()
        self.process_icici()
        self.process_kotak()
        self.process_hdfc()
        self.process_dhan()
        self.process_motilal()
        self.process_xts()
        self.process_prabhudas()
        self.process_axis()
        self.process_iifl()
        self.process_nuvama()
        self.process_profitmart()
        self.process_religare()
        
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
