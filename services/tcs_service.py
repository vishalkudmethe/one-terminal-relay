import logging
from sqlalchemy.orm import Session
from datetime import datetime
from .utils import LRSAccount, GlobalSettings

logger = logging.getLogger(__name__)

# RBI LRS TCS Rules (Post 2023)
TCS_THRESHOLD_INR = 700000  # ₹7 Lakh
TCS_RATE = 0.20             # 20% for investments

class TcsService:
    @staticmethod
    def get_user_lrs_usage(db: Session, user_id: str) -> float:
        """Fetch cumulative LRS usage in INR for the current financial year."""
        account = db.query(LRSAccount).filter(LRSAccount.user_id == user_id).first()
        if not account:
            return 0.0
        
        # Financial Year Check (April 1st Reset)
        # Simplified: Just return the stored usage. In a real app, logic would reset on April 1.
        return account.annual_usage_usd * 83.0 # Approximate INR conversion if stored in USD

    @staticmethod
    def calculate_tcs(db: Session, user_id: str, amount_inr: float) -> float:
        """
        Calculate TCS for a new remittance.
        Rule: 20% TCS on amounts exceeding the ₹7L annual threshold.
        """
        prior_usage = TcsService.get_user_lrs_usage(db, user_id)
        current_total = prior_usage + amount_inr

        if current_total <= TCS_THRESHOLD_INR:
            return 0.0
        
        # If prior usage already crossed threshold, tax the full current amount
        if prior_usage >= TCS_THRESHOLD_INR:
            return amount_inr * TCS_RATE
        
        # If it crosses mid-transaction, tax only the part above threshold
        taxable_amount = current_total - TCS_THRESHOLD_INR
        return taxable_amount * TCS_RATE

    @staticmethod
    def update_usage(db: Session, user_id: str, amount_inr: float, amount_usd: float):
        """Update the user's LRS usage ledger."""
        account = db.query(LRSAccount).filter(LRSAccount.user_id == user_id).first()
        if not account:
            account = LRSAccount(user_id=user_id, annual_usage_usd=0.0)
            db.add(account)
        
        account.annual_usage_usd += amount_usd
        db.commit()
