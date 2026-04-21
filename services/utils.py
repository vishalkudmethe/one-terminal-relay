from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
import config

# ---- Database Setup (SEBI Compliance) ----
engine = create_engine(config.DATABASE_URL, connect_args={"check_same_thread": False, "timeout": 15.0})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class AuditLog(Base):
    __tablename__ = "audit_logs"
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    broker = Column(String, index=True)
    endpoint = Column(String)
    method = Column(String)
    client_ip = Column(String)
    device_uuid = Column(String)

class GlobalSettings(Base):
    __tablename__ = "global_settings"
    key = Column(String, primary_key=True)
    value = Column(String)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class AlpacaAccount(Base):
    __tablename__ = "alpaca_accounts"
    user_id = Column(String, primary_key=True, index=True)
    alpaca_account_id = Column(String, index=True, nullable=True)
    status = Column(String, default="PENDING") # PENDING, APPROVED, REJECTED
    email = Column(String)
    enrollment_data = Column(String) # JSON blob for KYC
    credentials_sent = Column(Integer, default=0) # 0 or 1
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class InstantFundingLedger(Base):
    __tablename__ = "instant_funding_ledger"
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(String, index=True)
    amount_usd = Column(Float)
    amount_inr = Column(Float)
    exchange_rate = Column(Float)
    markup_applied = Column(Float)
    payment_ref = Column(String) # Transaction ID / Screenshot Ref
    status = Column(String, default="PENDING_CLEARANCE") # PENDING_CLEARANCE, CLEARED, BOOSTED, REJECTED
    boost_id = Column(String, nullable=True) # Alpaca Transfer ID
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class LRSAccount(Base):
    __tablename__ = "lrs_accounts"
    user_id = Column(String, primary_key=True, index=True)
    pan_number = Column(String, index=True)
    nium_customer_id = Column(String, index=True, nullable=True)
    annual_usage_usd = Column(Float, default=0.0)
    last_reset_date = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)

class NiumPayout(Base):
    __tablename__ = "nium_payouts"
    id = Column(Integer, primary_key=True, index=True)
    ledger_id = Column(Integer, index=True) # ID from instant_funding_ledger
    system_ref_num = Column(String, index=True)
    payout_id = Column(String, index=True)
    amount_usd = Column(Float)
    tcs_collected = Column(Float)
    status = Column(String, default="INITIATED") # INITIATED, PENDING_COMPLIANCE, SENT, FAILED
    error_message = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

def init_db():
    Base.metadata.create_all(bind=engine)
    print("Database Initialized (AuditLogs, Alpaca, Nium tables verified).")

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

def parse_ot_context(ctx: str):
    if not ctx: return {}
    try:
        return {k.strip().upper(): v.strip() for k, v in (p.split('=') for p in ctx.split(';') if '=' in p)}
    except:
        return {}

HOP_BY_HOP_HEADERS = {
    "host", "content-length", "connection", "keep-alive", 
    "proxy-authenticate", "proxy-authorization", "te", 
    "trailers", "transfer-encoding", "upgrade", "content-encoding"
}
