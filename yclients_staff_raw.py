#!/usr/bin/env python3
"""
YCLIENTS Staff Raw Data Fetcher
Fetches raw staff data from YCLIENTS API and saves to MongoDB 'company' collection
"""
import asyncio
import sys
import os
import json
from datetime import datetime
from typing import Dict, Any, Optional, List

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from integrations.yclients.yclients_wrapper import YClientsAPI, YClientsAPIError
from db.db_man import DatabaseManager
from observability.logging.centralized_logging import setup_centralized_logger
from utils.date_formatter import get_current_time
from config.config import (
    YCLIENTS_PARTNER_TOKEN, YCLIENTS_USER_TOKEN,
    YCLIENTS_TIMEOUT, YCLIENTS_MAX_RETRIES, YCLIENTS_BACKOFF_FACTOR
)

# Initialize logger
logger, _ = setup_centralized_logger("yclients_staff_raw")

class YClientsStaffRawFetcher:
    """Fetches raw staff data from YCLIENTS API and saves to MongoDB"""
    
    def __init__(self, partner_token: str, user_token: Optional[str] = None):
        """
        Initialize YCLIENTS staff data fetcher
        
        Args:
            partner_token: YCLIENTS partner token
            user_token: Optional YCLIENTS user token
        """
        self.partner_token = partner_token
        self.user_token = user_token
        self.db_manager = DatabaseManager()
        
    async def get_salon_ids_from_db(self) -> List[int]:
        """
        Get all salon IDs from the 'salons' collection
        
        Returns:
            List of salon IDs
        """
        try:
            salons_collection = self.db_manager.db['salons']
            
            loop = asyncio.get_event_loop()
            salon_docs = await loop.run_in_executor(
                None,
                lambda: list(salons_collection.find({}, {'_id': 1}))
            )
            
            salon_ids = [int(doc['_id']) for doc in salon_docs]
            logger.info(f"Found {len(salon_ids)} salons in database: {salon_ids}")
            return salon_ids
            
        except Exception as e:
            logger.error(f"Error fetching salon IDs from database: {e}")
            return []
        
    async def fetch_and_save_staff_data_for_salon(self, salon_id: int) -> bool:
        """
        Fetch raw staff data for a single salon from YCLIENTS API
        
        Args:
            salon_id: YCLIENTS salon ID
            
        Returns:
            bool: Success status
        """
        try:
            logger.info(f"Fetching raw staff data for salon {salon_id}...")
            
            # Create API instance for this salon
            api = YClientsAPI(
                company_id=salon_id,
                partner_token=self.partner_token,
                user_token=self.user_token,
                timeout=YCLIENTS_TIMEOUT,
                max_retries=YCLIENTS_MAX_RETRIES,
                backoff_factor=YCLIENTS_BACKOFF_FACTOR,
                logger=logger
            )
            
            # Fetch raw staff data from YCLIENTS API
            staff_response = api.list_staff()
            
            if not staff_response:
                logger.warning(f"No staff data received from YCLIENTS API for salon {salon_id}")
                return False
            
            # 1) Show prettified JSON in terminal
            print(f"\n{'='*60}")
            print(f"RAW STAFF DATA FOR SALON {salon_id}")
            print(f"{'='*60}")
            print(json.dumps(staff_response, indent=2, ensure_ascii=False))
            print(f"{'='*60}\n")
            
            # 2) Update salon document in 'salons' collection using upsert
            current_time = get_current_time()
            adjusted_time = self.db_manager._adjust_time_for_storage(current_time)
            salons_collection = self.db_manager.db['salons']
            
            update_doc = {
                '$set': {
                    'staff': staff_response,
                    'staff_updated_at': adjusted_time
                }
            }
            
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: salons_collection.update_one(
                    {'_id': str(salon_id)},
                    update_doc,
                    upsert=True
                )
            )
            
            if result.upserted_id:
                logger.info(f"Created new salon document with staff data for salon {salon_id}")
            elif result.modified_count > 0:
                logger.info(f"Updated salon document with staff data for salon {salon_id}")
            else:
                logger.info(f"No changes needed for salon {salon_id} staff data")
            
            # Log summary of data
            if isinstance(staff_response, dict) and 'data' in staff_response:
                staff_count = len(staff_response['data']) if isinstance(staff_response['data'], list) else 0
                logger.info(f"Saved {staff_count} staff members data for salon {salon_id}")
            
            # Close API connection
            api.close()
            return True
            
        except YClientsAPIError as e:
            logger.error(f"YCLIENTS API error while fetching staff data for salon {salon_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error fetching and saving staff data for salon {salon_id}: {e}")
            return False
    
    async def fetch_and_save_all_salons_staff_data(self, salon_ids: List[int]) -> bool:
        """
        Fetch raw staff data for all salons
        
        Args:
            salon_ids: List of YCLIENTS salon IDs
            
        Returns:
            bool: Success status (True if all salons processed successfully)
        """
        if not salon_ids:
            logger.warning("No salon IDs provided")
            return False
            
        logger.info(f"Fetching staff data for {len(salon_ids)} salons: {salon_ids}")
        
        success_count = 0
        total_count = len(salon_ids)
        
        for salon_id in salon_ids:
            try:
                success = await self.fetch_and_save_staff_data_for_salon(salon_id)
                if success:
                    success_count += 1
                else:
                    logger.error(f"Failed to fetch staff data for salon {salon_id}")
            except Exception as e:
                logger.error(f"Exception while processing salon {salon_id}: {e}")
        
        logger.info(f"Staff data fetch completed: {success_count}/{total_count} salons successful")
        return success_count == total_count
    
    async def cleanup(self):
        """Clean up resources"""
        try:
            self.db_manager.close()
            logger.info("Cleaned up YCLIENTS staff fetcher resources")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

async def main():
    """Main function to fetch and save YCLIENTS staff data for all salons"""
    # Get YCLIENTS API configuration from config
    partner_token = YCLIENTS_PARTNER_TOKEN
    user_token = YCLIENTS_USER_TOKEN
    
    if not partner_token:
        logger.error("YCLIENTS_PARTNER_TOKEN not configured in config.py")
        return False
    
    fetcher = YClientsStaffRawFetcher(
        partner_token=partner_token,
        user_token=user_token
    )
    
    try:
        # Get salon IDs from database
        salon_ids = await fetcher.get_salon_ids_from_db()
        
        if not salon_ids:
            logger.error("No salon IDs found in database. Run yclients_salons.py first to populate salons collection.")
            return False
        
        logger.info(f"Processing {len(salon_ids)} salons: {salon_ids}")
        
        success = await fetcher.fetch_and_save_all_salons_staff_data(salon_ids)
        if success:
            logger.info("YCLIENTS staff data fetch completed successfully for all salons")
        else:
            logger.error("YCLIENTS staff data fetch failed for some salons")
        return success
    finally:
        await fetcher.cleanup()

if __name__ == "__main__":
    print("YCLIENTS Staff Raw Data Fetcher")
    print("Fetches raw staff data from YCLIENTS API for salons")
    print("- Reads salon IDs from MongoDB 'salons' collection")
    print("- Shows prettified JSON in terminal")
    print("- Updates MongoDB 'salons' collection with staff data")
    print()
    
    success = asyncio.run(main())
    sys.exit(0 if success else 1)