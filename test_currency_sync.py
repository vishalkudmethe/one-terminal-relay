import asyncio
import sys
import os

# Add parent dir to path to import services
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from services.currency_service import CurrencyService
from services.utils import SessionLocal

async def test_sync():
    print("Testing Currency Sync Task...")
    rate = await CurrencyService.fetch_usd_inr_rate()
    if rate > 0:
        print(f"Success: Fetched Rate = {rate}")
        db = SessionLocal()
        try:
            CurrencyService.update_rate_in_db(db, rate)
            cached_rate = CurrencyService.get_rate_from_db(db)
            print(f"Success: Cached Rate in DB = {cached_rate}")
        finally:
            db.close()
    else:
        print("Failed: Could not fetch rate.")

if __name__ == "__main__":
    asyncio.run(test_sync())
