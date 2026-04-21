import os
import logging
from fastapi import APIRouter, Request, Depends, Header, HTTPException
from fastapi.responses import JSONResponse
from services.utils import get_db, LRSAccount, InstantFundingLedger, AlpacaAccount, NiumPayout
from services.nium_service import NiumService
from services.tcs_service import TcsService
from services.international_brokers.alpaca_handler import verify_user_id, verify_admin

logger = logging.getLogger(__name__)
router = APIRouter()
nium = NiumService()

@router.post("/onboard-remitter")
async def onboard_remitter(request: Request, db=Depends(get_db), user_id: str = Depends(verify_user_id)):
    """Module 4: PAN collection and Nium Customer Onboarding"""
    try:
        data = await request.json()
        pan = data.get("pan")
        name = data.get("name") # User's legal name

        if not pan or len(pan) != 10:
            return JSONResponse(status_code=400, detail="Valid 10-character PAN is required.")

        # 1. Onboard to Nium
        customer_hash = await nium.onboard_remitter(user_id, pan, name)
        if not customer_hash:
            return JSONResponse(status_code=500, detail="Nium onboarding failed.")

        # 2. Register Alpaca Beneficiary immediately
        ben_hash = await nium.add_alpaca_beneficiary(customer_hash)

        # 3. Save to Local DB
        lrs_acc = db.query(LRSAccount).filter(LRSAccount.user_id == user_id).first()
        if not lrs_acc:
            lrs_acc = LRSAccount(user_id=user_id, pan_number=pan, nium_customer_id=customer_hash)
            db.add(lrs_acc)
        else:
            lrs_acc.pan_number = pan
            lrs_acc.nium_customer_id = customer_hash
        
        db.commit()

        return {
            "status": "SUCCESS",
            "customer_id": customer_hash,
            "beneficiary_ready": ben_hash is not None
        }
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@router.post("/trigger-swift")
async def trigger_swift_payout(request: Request, db=Depends(get_db), _=Depends(verify_admin)):
    """Module 4: Admin-triggered SWIFT wire to Alpaca via Nium"""
    try:
        data = await request.json()
        ledger_id = data.get("ledger_id")

        # 1. Fetch Ledger Entry
        entry = db.query(InstantFundingLedger).filter(InstantFundingLedger.id == ledger_id).first()
        if not entry or entry.status != "BOOSTED":
            return JSONResponse(status_code=400, detail="Invalid ledger entry or status.")

        # 2. Fetch User's Nium & Alpaca Data
        lrs_acc = db.query(LRSAccount).filter(LRSAccount.user_id == entry.user_id).first()
        alpaca_acc = db.query(AlpacaAccount).filter(AlpacaAccount.user_id == entry.user_id).first()

        if not lrs_acc or not lrs_acc.nium_customer_id:
            return JSONResponse(status_code=400, detail="User not onboarded on Nium.")
        if not alpaca_acc or not alpaca_acc.alpaca_account_id:
            return JSONResponse(status_code=400, detail="User has no Alpaca Account ID.")

        # 3. Calculate TCS
        tcs_amount_inr = TcsService.calculate_tcs(db, entry.user_id, entry.amount_inr)
        
        # 4. Initiate SWIFT Payout
        # For now, we assume the beneficiary is already created/cached
        # In a full impl, we'd fetch or create the beneficiary_hash_id
        ben_hash_id = "MOCK_BEN_HASH" # Should be stored/fetched
        
        payout_result = await nium.initiate_swift_payout(
            lrs_acc.nium_customer_id,
            ben_hash_id,
            entry.amount_usd,
            alpaca_acc.alpaca_account_id # 9-digit ID for FFC
        )

        # 5. Record Output
        new_payout = NiumPayout(
            ledger_id=ledger_id,
            system_ref_num=payout_result.get("ref", "PENDING"),
            amount_usd=entry.amount_usd,
            tcs_collected=tcs_amount_inr,
            status=payout_result["status"],
            error_message=payout_result.get("error")
        )
        db.add(new_payout)
        
        if payout_result["status"] == "SUCCESS":
            TcsService.update_usage(db, entry.user_id, entry.amount_inr, entry.amount_usd)
            entry.status = "SETTLED" # Mark the funding as fully settled
        
        db.commit()

        return payout_result
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
