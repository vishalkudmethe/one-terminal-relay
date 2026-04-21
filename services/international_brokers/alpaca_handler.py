import os
import json
import logging
import asyncio
from fastapi import APIRouter, Request, Depends, UploadFile, File, Form, Header, HTTPException
from fastapi.responses import JSONResponse
from alpaca.broker.client import BrokerClient
from alpaca.common.exceptions import APIError
from services.ws_manager import manager
from services.utils import get_db, AlpacaAccount, InstantFundingLedger, GlobalSettings
from services.currency_service import CurrencyService
from services.email_service import send_welcome_email

logger = logging.getLogger(__name__)
router = APIRouter()

# Initialize Alpaca Client (Sandbox)
ALPACA_KEY = os.getenv("ALPACA_BROKER_KEY_ID")
ALPACA_SECRET = os.getenv("ALPACA_BROKER_SECRET")
BASE_URL = "https://broker-api.sandbox.alpaca.markets"

broker_client = None
try:
    if ALPACA_KEY and ALPACA_SECRET:
        broker_client = BrokerClient(ALPACA_KEY, ALPACA_SECRET, sandbox=True)
    else:
        logger.warning("Alpaca keys missing. Alpaca features will be disabled.")
except Exception as e:
    logger.error(f"Failed to initialize Alpaca client: {e}")

async def verify_user_id(x_user_id: str = Header(...)):
    """Security Header Enforcement"""
    if not x_user_id or len(x_user_id) < 5:
        raise HTTPException(status_code=403, detail="X-User-ID header is mandatory and must be valid.")
    return x_user_id

async def verify_admin(authorization: str = Header(...)):
    """Admin Secret Enforcement"""
    admin_secret = os.getenv("ADMIN_SECRET", "OT-ADMIN-2024")
    if not authorization.startswith("Bearer ") or authorization.split("Bearer ")[1] != admin_secret:
        raise HTTPException(status_code=403, detail="Admin access denied.")
    return True

@router.post("/onboard")
async def onboard_user(
    email: str = Form(...),
    name: str = Form(None),
    mobile: str = Form(None),
    w8ben_consented: bool = Form(False),
    base_currency: str = Form("USD"),
    db=Depends(get_db), 
    user_id: str = Depends(verify_user_id)
):
    """Module 1: Initialize enrollment for Onfido Flow"""
    try:
        existing = db.query(AlpacaAccount).filter(AlpacaAccount.user_id == user_id).first()
        if existing:
            return {"status": "exists", "account_id": existing.alpaca_account_id}
        
        # In Sandbox: Generate a mock account_id to facilitate Onfido Handshake
        mock_acc_id = f"ACC-{os.urandom(4).hex().upper()}"
            
        new_acc = AlpacaAccount(
            user_id=user_id,
            email=email,
            alpaca_account_id=mock_acc_id,
            enrollment_data=json.dumps({
                "name": name, 
                "mobile": mobile, 
                "w8ben": w8ben_consented,
                "base_currency": base_currency
            }),
            status="ACTION_REQUIRED"
        )
        db.add(new_acc)
        db.commit()
        
        return {"status": "success", "account_id": mock_acc_id}
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@router.get("/onfido/token")
async def get_onfido_token(db=Depends(get_db), user_id: str = Depends(verify_user_id)):
    """Module 1: Fetch Onfido SDK Token for Handshake"""
    acc = db.query(AlpacaAccount).filter(AlpacaAccount.user_id == user_id).first()
    if not acc:
        return JSONResponse(status_code=404, detail="Enrollment not found")
    
    try:
        # PROD: broker_client.get_onfido_sdk_token(acc.alpaca_account_id)
        # SANDBOX MOCK:
        mock_jwt = f"onfido_jwt_{os.urandom(16).hex()}"
        return {"sdk_token": mock_jwt}
    except Exception as e:
        return JSONResponse(status_code=500, detail=str(e))

@router.post("/onfido/complete")
async def onfido_complete(db=Depends(get_db), user_id: str = Depends(verify_user_id)):
    """Module 1: Signal completion and Initialize Funding Wallet"""
    acc = db.query(AlpacaAccount).filter(AlpacaAccount.user_id == user_id).first()
    if acc:
        acc.status = "PENDING"
        
        # Simulation: Create Funding Wallet immediately so user can see deposit details
        # In PROD: broker_client.create_funding_wallet(acc.alpaca_account_id)
        mock_wallet = {
            "bank_name": "Silvergate Bank (Alpaca Custodial)",
            "routing_number": "122486749",
            "account_number": f"9900{os.urandom(3).hex().upper()}",
            "instruction": "Direct Deposit enabled. No memo required."
        }
        
        data = json.loads(acc.enrollment_data)
        data['funding_wallet'] = mock_wallet
        acc.enrollment_data = json.dumps(data)
        
        db.commit()
        logger.info(f"Onfido Complete & Funding Wallet generated for user: {user_id}")
    return {"status": "success"}

@router.get("/funding-wallet")
async def get_funding_wallet(db=Depends(get_db), user_id: str = Depends(verify_user_id)):
    """Module 1: Retrieve account-specific bank details for deposits"""
    acc = db.query(AlpacaAccount).filter(AlpacaAccount.user_id == user_id).first()
    if not acc:
        return JSONResponse(status_code=404, detail="Account not found")
        
    data = json.loads(acc.enrollment_data)
    wallet = data.get('funding_wallet')
    
    if not wallet:
        return JSONResponse(status_code=404, detail="Funding wallet not yet initialized")
        
    return wallet

@router.get("/onboarding-status")
async def get_onboarding_status(db=Depends(get_db), user_id: str = Depends(verify_user_id)):
    """Module 1: Efficient Polling for Mobile App"""
    acc = db.query(AlpacaAccount).filter(AlpacaAccount.user_id == user_id).first()
    if not acc:
        return {"status": "NOT_STARTED"}
    return {
        "status": acc.status,
        "credentials_sent": bool(acc.credentials_sent),
        "account_id": acc.alpaca_account_id
    }

@router.post("/debug/approve")
async def debug_approve_account(request: Request, db=Depends(get_db), user_id: str = Depends(verify_user_id)):
    """
    SIMULATION: Approval -> Key Generation -> Welcome Email.
    In prod, this would be an Alpaca SSE/Webhook listener.
    """
    acc = db.query(AlpacaAccount).filter(AlpacaAccount.user_id == user_id).first()
    if not acc:
        return JSONResponse(status_code=404, detail="Account not found")
    
    # 1. Update Status
    acc.status = "APPROVED"
    acc.alpaca_account_id = "ACC-" + os.urandom(4).hex().toUpperCase()
    
    # 2. Generate Mock Keys (In prod: broker_client.create_api_key(acc.alpaca_account_id))
    mock_key = "AK-" + os.urandom(8).hex().toUpperCase()
    mock_secret = os.urandom(16).hex()
    
    # 3. Send Transactional Email via AWS SES
    email_success = send_welcome_email(acc.email, mock_key, mock_secret)
    
    if email_success:
        acc.credentials_sent = 1
        
    db.commit()
    
    return {
        "status": "APPROVED",
        "email_sent": email_success,
        "message": "Institutional account approved. Credentials sent via AWS SES."
    }

@router.post("/orders")
async def place_order(request: Request, user_id: str = Depends(verify_user_id)):
    """Module 3: Execution Engine - Fractional Trading"""
    try:
        data = await request.json()
        return {"status": "filled", "order_id": f"alpaca_{os.urandom(4).hex()}", "notional": data.get("notional")}
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@router.get("/positions")
async def get_positions(user_id: str = Depends(verify_user_id)):
    """Module 3: Portfolio View - Retrieve Positions"""
    try:
        # Mocking data for Sandbox testing
        return [
            {
                "symbol": "AAPL",
                "qty": "10.05",
                "avg_entry_price": "175.50",
                "current_price": "184.20",
                "unrealized_pl": "87.435",
                "unrealized_plpc": "0.049",
                "exchange": "NASDAQ"
            },
            {
                "symbol": "TSLA",
                "qty": "5.0",
                "avg_entry_price": "240.00",
                "current_price": "238.15",
                "unrealized_pl": "-9.25",
                "unrealized_plpc": "-0.007",
                "exchange": "NASDAQ"
            }
        ]
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@router.post("/journals")
async def create_journal(request: Request, user_id: str = Depends(verify_user_id)):
    """Module 1: Journals API - Internal Fund Transfers (Master to Sub-Account)"""
    try:
        data = await request.json()
        from_acc = data.get("from_account", "MASTER_POOL") # Default to master if not specified
        to_acc = data.get("to_account")
        amount = data.get("amount")
        
        if not to_acc or not amount:
            return JSONResponse(status_code=400, detail="to_account and amount are mandatory.")

        # In PROD: 
        # from alpaca.broker.requests import CreateJournalRequest
        # from alpaca.broker.enums import JournalEntryType
        # journal_req = CreateJournalRequest(
        #     from_account=from_acc,
        #     to_account=to_acc,
        #     entry_type=JournalEntryType.JNLC, # JNLC = Cash Journal
        #     amount=amount,
        #     description=data.get("description", f"One Terminal Funding for {user_id}")
        # )
        # response = broker_client.create_journal(journal_req)
        
        # SIMULATION Logic
        journal_id = f"JN-{os.urandom(6).hex().upper()}"
        logger.info(f"JOURNAL_OP: Initiated {journal_id} | ${amount} -> {to_acc} (Triggered by: {user_id})")
        
        return {
            "status": "pending",
            "journal_id": journal_id,
            "sender": from_acc,
            "receiver": to_acc,
            "amount": amount,
            "timestamp": asyncio.get_event_loop().time() # Mock TS
        }
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@router.post("/instant-funding")
async def create_instant_funding(request: Request, user_id: str = Depends(verify_user_id)):
    """Module 1: Create an Instant Funding Transfer (Extend Buying Power)"""
    try:
        data = await request.json()
        amount = data.get("amount")
        acc_no = data.get("account_no")
        source_acc = os.getenv("ALPACA_SOURCE_ACCOUNT", "FIRM_SI_001")
        
        if not amount or not acc_no:
            return JSONResponse(status_code=400, detail="account_no and amount are mandatory.")

        # In PROD: broker_client.create_instant_funding_transfer(acc_no, source_acc, amount)
        # Sandbox Mock
        return {
            "id": f"IF-{os.urandom(6).hex().upper()}",
            "status": "EXECUTED",
            "amount": amount,
            "account_no": acc_no,
            "system_date": "2024-11-12",
            "deadline": "2024-11-13"
        }
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@router.get("/instant-funding/limits")
async def get_instant_funding_limits(account_no: str, user_id: str = Depends(verify_user_id)):
    """Module 1: View Account Level Instant Funding Limits"""
    try:
        # In PROD: broker_client.get_instant_funding_account_limits([account_no])
        return [{
            "account_no": account_no,
            "amount_available": "850.00",
            "amount_in_use": "150.00",
            "amount_limit": "1000.00"
        }]
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@router.get("/instant-funding/status/{transfer_id}")
async def get_instant_funding_status(transfer_id: str, user_id: str = Depends(verify_user_id)):
    """Check status of a specific Instant Funding Transfer"""
    return {
        "id": transfer_id,
        "status": "EXECUTED",
        "remaining_payable": "0",
        "total_interest": "0"
    }

# --- PAYMENT DECLARATION & ADMIN LEDGER ---

@router.post("/payments/declare")
async def declare_payment(request: Request, db=Depends(get_db), user_id: str = Depends(verify_user_id)):
    """Phase 2: User declares they have sent local funds (UPI/NEFT)"""
    try:
        data = await request.json()
        amount_inr = data.get("amount_inr")
        payment_ref = data.get("payment_ref")

        if not amount_inr or not payment_ref:
            return JSONResponse(status_code=400, detail="amount_inr and payment_ref are required.")

        # Get current effective rate (including markup)
        effective_rate = CurrencyService.get_rate_from_db(db, apply_markup=True)
        markup = CurrencyService.get_markup_from_db(db)
        amount_usd = round(amount_inr / effective_rate, 2)

        new_entry = InstantFundingLedger(
            user_id=user_id,
            amount_usd=amount_usd,
            amount_inr=amount_inr,
            exchange_rate=effective_rate,
            markup_applied=markup,
            payment_ref=payment_ref,
            status="PENDING_CLEARANCE"
        )
        db.add(new_entry)
        db.commit()

        return {
            "status": "success",
            "ledger_id": new_entry.id,
            "estimated_usd": amount_usd,
            "rate_used": effective_rate
        }
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@router.get("/admin/ledger")
async def get_admin_ledger(db=Depends(get_db), _=Depends(verify_admin)):
    """Phase 2: Admin views the queue of pending fund boosts"""
    ledger = db.query(InstantFundingLedger).all()
    return [{
        "id": l.id,
        "user_id": l.user_id,
        "amount_inr": l.amount_inr,
        "amount_usd": l.amount_usd,
        "status": l.status,
        "payment_ref": l.payment_ref,
        "created_at": l.created_at.isoformat()
    } for l in ledger]

@router.post("/admin/trigger-boost")
async def trigger_boost(request: Request, db=Depends(get_db), _=Depends(verify_admin)):
    """Phase 2: Admin confirms funds and 'Pulses' Alpaca Instant Funding"""
    try:
        data = await request.json()
        ledger_id = data.get("ledger_id")
        
        entry = db.query(InstantFundingLedger).filter(InstantFundingLedger.id == ledger_id).first()
        if not entry:
            return JSONResponse(status_code=404, detail="Entry not found.")
        
        if entry.status != "PENDING_CLEARANCE":
            return JSONResponse(status_code=400, detail=f"Invalid status: {entry.status}")

        # FETCH Alpaca Account for this user
        alpaca_acc = db.query(AlpacaAccount).filter(AlpacaAccount.user_id == entry.user_id).first()
        if not alpaca_acc or not alpaca_acc.alpaca_account_id:
             return JSONResponse(status_code=400, detail="User has no Alpaca account linked.")

        # CALL ALPACA INSTANT FUNDING API
        # In PROD: broker_client.create_instant_funding_transfer(alpaca_acc.alpaca_account_id, SOURCE_ACC, entry.amount_usd)
        
        # Simulated MOCK Boost
        entry.status = "BOOSTED"
        entry.boost_id = f"IF-{os.urandom(6).hex().upper()}"
        db.commit()

        return {
            "status": "BOOSTED",
            "boost_id": entry.boost_id,
            "amount_usd": entry.amount_usd,
            "user_id": entry.user_id
        }
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@router.patch("/admin/settings/markup")
async def update_fx_markup(request: Request, db=Depends(get_db), _=Depends(verify_admin)):
    """Phase 2: Admin adjusts FX Mark-up Percentage"""
    data = await request.json()
    new_markup = data.get("markup")
    if new_markup is None:
        return JSONResponse(status_code=400, detail="Markup value is required.")

    setting = db.query(GlobalSettings).filter(GlobalSettings.key == "FX_MARKUP_PERCENT").first()
    if not setting:
        setting = GlobalSettings(key="FX_MARKUP_PERCENT", value=str(new_markup))
        db.add(setting)
    else:
        setting.value = str(new_markup)
    
    db.commit()
    return {"status": "updated", "new_markup": new_markup}
