import os
import httpx
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class NiumService:
    def __init__(self):
        self.base_url = os.getenv("NIUM_BASE_URL", "https://api.sandbox.nium.com")
        self.client_id = os.getenv("NIUM_CLIENT_ID")
        self.client_secret = os.getenv("NIUM_CLIENT_SECRET")
        self.program_id = os.getenv("NIUM_PROGRAM_ID")
        self.broker_code = os.getenv("ALPACA_WIRE_BROKER_CODE", "LCPA")

    async def _get_headers(self):
        # In a real app, logic for OAuth2 token or API Key auth goes here
        return {
            "x-client-id": self.client_id,
            "x-client-secret": self.client_secret,
            "Content-Type": "application/json"
        }

    async def onboard_remitter(self, user_id: str, pan: str, name: str) -> Optional[str]:
        """Module 4: Onboard a user as a Remitter on Nium (India LRS flow)"""
        url = f"{self.base_url}/api/v1/client/{self.program_id}/customer"
        
        payload = {
            "businessDetails": {
                "individualDetails": {
                    "firstName": name.split()[0] if name else "User",
                    "lastName": name.split()[-1] if ' ' in name else "OT",
                }
            },
            "customerType": "INDIVIDUAL",
            "complianceStatus": "IN_PROGRESS",
            "taxDetails": [
                {
                    "taxId": pan,
                    "taxIssuer": "IN"
                }
            ]
        }

        try:
            async with httpx.AsyncClient() as client:
                headers = await self._get_headers()
                response = await client.post(url, headers=headers, json=payload)
                data = response.json()
                if response.status_code in [200, 201]:
                    return data.get("customerHashId")
                else:
                    logger.error(f"Nium Onboarding Error: {data}")
                    return None
        except Exception as e:
            logger.error(f"Nium Request Failed: {e}")
            return None

    async def add_alpaca_beneficiary(self, customer_hash_id: str) -> Optional[str]:
        """Module 4: Register Alpaca's BMO Harris account as a beneficiary for this customer"""
        url = f"{self.base_url}/api/v1/client/{self.program_id}/customer/{customer_hash_id}/beneficiary"
        
        payload = {
            "beneficiaryName": "Alpaca Securities LLC",
            "beneficiaryAccountType": "INDIVIDUAL",
            "beneficiaryAddress": "111 W. Monroe Street",
            "beneficiaryCity": "Chicago",
            "beneficiaryCountryCode": "US",
            "beneficiaryPostcode": "60603",
            "beneficiaryAccountId": "1636877", # Alpaca Global Pool Account
            "beneficiaryBankName": "BMO Harris Bank",
            "beneficiarySwiftCode": "HATRUS44",
            "payoutMethod": "SWIFT",
            "destinationCurrency": "USD"
        }

        try:
            async with httpx.AsyncClient() as client:
                headers = await self._get_headers()
                response = await client.post(url, headers=headers, json=payload)
                data = response.json()
                if response.status_code in [200, 201]:
                    return data.get("beneficiaryHashId")
                return None
        except Exception as e:
            return None

    async def initiate_swift_payout(
        self, 
        customer_hash_id: str, 
        beneficiary_hash_id: str, 
        amount_usd: float, 
        alpaca_acc_no: str
    ) -> Dict[str, Any]:
        """Module 4: Trigger the actual money move with FFC instruction"""
        url = f"{self.base_url}/api/v1/client/{self.program_id}/customer/{customer_hash_id}/payout"
        
        # FFC Reference Logic (as requested)
        memo = f"FFC {self.broker_code} {alpaca_acc_no}"

        payload = {
            "beneficiary": {
                "beneficiaryHashId": beneficiary_hash_id
            },
            "payoutAmount": amount_usd,
            "payoutCurrency": "USD",
            "purposeCode": "S0001", # Indian investment abroad – in equity capital
            "remittanceInstruction": memo,
            "sourceOfFunds": "Personal Savings"
        }

        try:
            async with httpx.AsyncClient() as client:
                headers = await self._get_headers()
                response = await client.post(url, headers=headers, json=payload)
                data = response.json()
                if response.status_code in [200, 201]:
                    return {"status": "SUCCESS", "ref": data.get("systemReferenceNumber")}
                return {"status": "FAILED", "error": data.get("message", "Unknown Nium Error")}
        except Exception as e:
            return {"status": "ERROR", "error": str(e)}
