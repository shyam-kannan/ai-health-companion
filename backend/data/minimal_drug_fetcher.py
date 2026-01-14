"""
Minimal drug data fetcher - ONLY the fields users actually need.

Based on real use case: User shows medicine to camera, wants to know:
- What is it?
- What's it for?
- How do I use it?
- What should I watch out for?
"""

import asyncio
import httpx
import pandas as pd
import logging
import os
from typing import List, Dict, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MinimalOpenFDAClient:
    """Fetch drug data with minimal, focused fields"""
    
    BASE_URL = "https://api.fda.gov/drug"
    
    def __init__(self):
        self.session = httpx.AsyncClient(timeout=30.0)
    
    async def fetch_drug_labels_page(self, skip: int = 0, limit: int = 100) -> Dict:
        """Fetch one page of drug labels"""
        endpoint = f"{self.BASE_URL}/label.json"
        params = {"limit": limit, "skip": skip}
        
        try:
            response = await self.session.get(endpoint, params=params)
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching labels: {e}")
            return {"results": []}
    
    async def fetch_all_labels(self, num_pages: int = 20) -> List[Dict]:
        """Fetch multiple pages (default: 2000 drugs)"""
        all_drugs = []
        
        for page in range(num_pages):
            skip = page * 100
            data = await self.fetch_drug_labels_page(skip=skip)
            
            if "results" in data:
                all_drugs.extend(data["results"])
                logger.info(f"✅ Page {page+1}/{num_pages}: {len(all_drugs)} total drugs")
            
            await asyncio.sleep(0.5)  # Be nice to API
        
        return all_drugs
    
    async def close(self):
        await self.session.aclose()


class MinimalDrugProcessor:
    """
    Extract ONLY the essential fields.
    
    Philosophy: Keep it simple. Users want quick, clear info.
    """
    
    @staticmethod
    def safe_get(data: Dict, key: str, default: str = "") -> str:
        """Safely extract first item from list field"""
        value = data.get(key)
        if value is None:
            return default
        if isinstance(value, list):
            return value[0] if value else default
        return str(value)
    
    @staticmethod
    def safe_join(data: Dict, key: str) -> str:
        """Join list into comma-separated string"""
        value = data.get(key, [])
        if isinstance(value, list):
            return ", ".join(str(v) for v in value if v)
        return ""
    
    @staticmethod
    def process_drug(raw_drug: Dict) -> Optional[Dict]:
        """
        Extract only essential fields.
        NOW WITH STRICT FILTERING - only keep useful drugs!
        """
        
        openfda = raw_drug.get("openfda", {})
        
        # === IDENTIFICATION ===
        brand_name = MinimalDrugProcessor.safe_get(openfda, "brand_name", "Unknown")
        generic_name = MinimalDrugProcessor.safe_get(openfda, "generic_name")
        manufacturer = MinimalDrugProcessor.safe_get(openfda, "manufacturer_name")
        
        # === STRICT FILTERING - Skip if missing critical info ===
        
        # Filter 1: Must have a real brand name
        if brand_name == "Unknown" or not brand_name:
            return None
        
        # Filter 2: Must have openfda data (not empty)
        if not openfda or len(openfda) == 0:
            return None
        
        # Filter 3: Must have product type
        product_type = MinimalDrugProcessor.safe_get(openfda, "product_type")
        if not product_type:
            return None
        
        # Filter 4: Focus on common drug types (skip veterinary, devices, etc.)
        common_types = [
            "HUMAN OTC DRUG",
            "HUMAN PRESCRIPTION DRUG"
        ]
        if product_type not in common_types:
            return None
        
        # NDC codes (for barcode scanning)
        product_ndc = MinimalDrugProcessor.safe_join(openfda, "product_ndc")
        package_ndc = MinimalDrugProcessor.safe_join(openfda, "package_ndc")
        
        # Filter 5: Must have at least one NDC code
        if not product_ndc and not package_ndc:
            return None
        
        # Active ingredients
        active_ingredients = MinimalDrugProcessor.safe_join(openfda, "substance_name")
        if not active_ingredients:
            active_ingredients = MinimalDrugProcessor.safe_get(raw_drug, "active_ingredient")
        
        # Filter 6: Must have active ingredients
        if not active_ingredients:
            return None
        
        # Route (oral, topical, etc.)
        route = MinimalDrugProcessor.safe_join(openfda, "route")
        
        # === PURPOSE ===
        purpose = MinimalDrugProcessor.safe_get(raw_drug, "purpose")
        indications = MinimalDrugProcessor.safe_get(raw_drug, "indications_and_usage")
        
        # Filter 7: Must have purpose OR indications
        if not purpose and not indications:
            return None
        
        # === USAGE ===
        dosage = MinimalDrugProcessor.safe_get(raw_drug, "dosage_and_administration")
        
        # === SAFETY ===
        warnings = MinimalDrugProcessor.safe_get(raw_drug, "warnings")
        do_not_use = MinimalDrugProcessor.safe_get(raw_drug, "do_not_use")
        stop_use = MinimalDrugProcessor.safe_get(raw_drug, "stop_use")
        ask_doctor = MinimalDrugProcessor.safe_get(raw_drug, "ask_doctor")
        when_using = MinimalDrugProcessor.safe_get(raw_drug, "when_using")
        side_effects = MinimalDrugProcessor.safe_get(raw_drug, "adverse_reactions")
        
        # === STORAGE ===
        storage = MinimalDrugProcessor.safe_get(raw_drug, "storage_and_handling")
        keep_away = MinimalDrugProcessor.safe_get(raw_drug, "keep_out_of_reach_of_children")
        
        return {
            # Identification
            "brand_name": brand_name,
            "generic_name": generic_name,
            "manufacturer": manufacturer,
            "product_type": product_type,
            "product_ndc": product_ndc,
            "package_ndc": package_ndc,
            "active_ingredients": active_ingredients,
            "route": route,
            
            # What it's for
            "purpose": purpose,
            "indications": indications,
            
            # How to use
            "dosage": dosage,
            
            # Safety warnings
            "warnings": warnings,
            "do_not_use": do_not_use,
            "stop_use": stop_use,
            "ask_doctor": ask_doctor,
            "when_using": when_using,
            "side_effects": side_effects,
            
            # Storage
            "storage": storage,
            "keep_away_children": keep_away,
        }
    @staticmethod
    def create_search_text(drug: Dict) -> str:
        """
        Create text for vector search.
        
        When user shows medicine to camera, we'll search for:
        - Brand name
        - Generic name
        - Active ingredients
        - Purpose
        """
        parts = [
            f"Brand: {drug['brand_name']}",
            f"Generic: {drug['generic_name']}" if drug['generic_name'] else "",
            f"Ingredients: {drug['active_ingredients']}" if drug['active_ingredients'] else "",
            f"Purpose: {drug['purpose']}" if drug['purpose'] else "",
            f"Uses: {drug['indications']}" if drug['indications'] else "",
        ]
        
        text = "\n".join(filter(None, parts))
        
        # Limit length
        if len(text) > 2000:
            text = text[:2000] + "..."
        
        return text



async def build_minimal_database(num_pages: int = 20):
    """
    Build minimal drug database.
    
    Args:
        num_pages: Number of pages to fetch (100 drugs per page)
                  Default 20 = 2000 drugs
    """
    
    client = MinimalOpenFDAClient()
    
    try:
        # Fetch drug labels
        print("="*60)
        print("FETCHING DRUG DATA")
        print("="*60)
        print(f"Fetching {num_pages} pages (~{num_pages * 100} drugs)")
        print("This will take about 10 minutes...")
        print()
        
        raw_drugs = await client.fetch_all_labels(num_pages=num_pages)
        
        # Process drugs
        print("\n⚙️  Processing drugs...")
        processed = []
        skipped = 0
        
        for raw_drug in raw_drugs:
            drug = MinimalDrugProcessor.process_drug(raw_drug)
            if drug:
                drug["search_text"] = MinimalDrugProcessor.create_search_text(drug)
                processed.append(drug)
            else:
                skipped += 1
        
        print(f"✅ Processed: {len(processed)} drugs")
        print(f"⏭️  Skipped: {skipped} (insufficient data)")
        
        # Save
        os.makedirs("../data/processed", exist_ok=True)
        
        df = pd.DataFrame(processed)
        df.to_csv("../data/processed/drugs_minimal.csv", index=False)
        df.to_json("../data/processed/drugs_minimal.json", orient="records", indent=2)
        
        # Summary
        print("\n" + "="*60)
        print("✅ DATABASE COMPLETE!")
        print("="*60)
        print(f"📊 Total drugs: {len(df)}")
        print(f"📁 Saved to: data/processed/drugs_minimal.csv")
        print()
        print("📋 Sample drugs:")
        for i, row in df.head(10).iterrows():
            print(f"   {i+1}. {row['brand_name']} - {row['purpose'][:50] if row['purpose'] else 'N/A'}...")
        print("="*60)
        
        return df
        
    finally:
        await client.close()


if __name__ == "__main__":
    # Fetch 20 pages = 2000 drugs
    # Increase num_pages if you want more (max ~2500 pages = 250,000 drugs)
    asyncio.run(build_minimal_database(num_pages=50))