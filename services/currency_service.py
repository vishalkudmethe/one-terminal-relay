import httpx
import logging
from datetime import datetime
from sqlalchemy.orm import Session
from services.utils import GlobalSettings

logger = logging.getLogger(__name__)

class CurrencyService:
    @staticmethod
    async def fetch_usd_inr_rate() -> float:
        """Fetch latest USD to INR rate from a public API."""
        url = "https://open.er-api.com/v6/latest/USD"
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url, timeout=10.0)
                response.raise_for_status()
                data = response.json()
                rate = data["rates"].get("INR")
                if rate:
                    logger.info(f"Fetched USD/INR Rate: {rate}")
                    return float(rate)
                else:
                    logger.error("INR not found in exchange rate response")
                    return 0.0
        except Exception as e:
            logger.error(f"Error fetching exchange rate: {e}")
            return 0.0

    @staticmethod
    def update_rate_in_db(db: Session, rate: float):
        """Persist the exchange rate to the database."""
        if rate <= 0:
            return
        
        setting = db.query(GlobalSettings).filter(GlobalSettings.key == "USD_INR_RATE").first()
        if not setting:
            setting = GlobalSettings(key="USD_INR_RATE", value=str(rate))
            db.add(setting)
        else:
            setting.value = str(rate)
            setting.updated_at = datetime.utcnow()
        
        db.commit()

    @staticmethod
    def get_markup_from_db(db: Session) -> float:
        """Retrieve FX Markup percent from DB (default to 2.0%)."""
        setting = db.query(GlobalSettings).filter(GlobalSettings.key == "FX_MARKUP_PERCENT").first()
        if setting:
            return float(setting.value)
        return 2.0 # Default 2%

    @staticmethod
    def get_rate_from_db(db: Session, apply_markup: bool = True) -> float:
        """Retrieve the cached exchange rate, optionally applying the admin markup."""
        setting = db.query(GlobalSettings).filter(GlobalSettings.key == "USD_INR_RATE").first()
        base_rate = float(setting.value) if setting else 83.0
        
        if not apply_markup:
            return base_rate
            
        markup = CurrencyService.get_markup_from_db(db)
        # Apply markup (e.g., 2% over mid-market)
        return base_rate * (1 + (markup / 100))
