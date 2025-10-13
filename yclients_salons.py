"""
YCLIENTS Salons Data Fetcher
Fetches salon information from YCLIENTS booking forms and saves to MongoDB 'salons' collection
"""
import asyncio
import sys
import os
import json
import requests
from datetime import datetime
from typing import Dict, Any, Optional, List, Set

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from db.db_man import DatabaseManager
from observability.logging.centralized_logging import setup_centralized_logger
from utils.date_formatter import get_current_time
from config.config import (
    BOOKING_FORMS, YCLIENTS_PARTNER_TOKEN, YCLIENTS_USER_TOKEN,
    YCLIENTS_TIMEOUT, YCLIENTS_MAX_RETRIES, YCLIENTS_BACKOFF_FACTOR
)

# Initialize logger
logger, _ = setup_centralized_logger("yclients_salons")

class YClientsSalonsFetcher:
    """Fetches salon information from YCLIENTS booking forms and company API"""
    
    def __init__(self, partner_token: str, user_token: Optional[str] = None):
        """
        Initialize YCLIENTS salons data fetcher
        
        Args:
            partner_token: YCLIENTS partner token
            user_token: Optional YCLIENTS user token
        """
        self.partner_token = partner_token
        self.user_token = user_token
        self.db_manager = DatabaseManager()
        self.base_url = "https://api.yclients.com/api/v1"
        
        # Setup session for HTTP requests
        self.session = requests.Session()
        self.session.headers.update({
            'Accept': 'application/vnd.yclients.v2+json',
            'Authorization': f'Bearer {self.partner_token}'
        })
    
    def _make_request(self, url: str, use_user_token: bool = False) -> Optional[Dict[str, Any]]:
        """
        Make HTTP request to YCLIENTS API
        
        Args:
            url: API endpoint URL
            use_user_token: Whether to include user token in authorization
            
        Returns:
            JSON response data or None if failed
        """
        headers = self.session.headers.copy()
        if use_user_token and self.user_token:
            headers['Authorization'] = f'Bearer {self.partner_token}, User {self.user_token}'
        
        try:
            response = self.session.get(url, headers=headers, timeout=YCLIENTS_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            return None
    
    def fetch_salon_ids_from_form(self, form_id: int) -> Set[int]:
        """
        Fetch salon IDs from a booking form
        
        Args:
            form_id: YCLIENTS form ID
            
        Returns:
            Set of unique salon IDs
        """
        salon_ids = set()
        
        try:
            logger.info(f"Fetching salon IDs from form {form_id}...")
            
            url = f"{self.base_url}/bookform/{form_id}"
            response_data = self._make_request(url)
            
            if not response_data:
                logger.warning(f"No data received from form {form_id}")
                return salon_ids
            
            # Extract salon IDs from online_sales_links
            if 'data' in response_data and 'online_sales_links' in response_data['data']:
                for link in response_data['data']['online_sales_links']:
                    if 'salon_ids' in link and isinstance(link['salon_ids'], list):
                        for salon_id in link['salon_ids']:
                            salon_ids.add(int(salon_id))
                            logger.debug(f"Found salon ID {salon_id} in form {form_id}")
            
            logger.info(f"Found {len(salon_ids)} unique salon IDs in form {form_id}: {salon_ids}")
            
        except Exception as e:
            logger.error(f"Error fetching salon IDs from form {form_id}: {e}")
        
        return salon_ids
    
    def fetch_all_salon_ids(self, form_ids: List[int]) -> Set[int]:
        """
        Fetch all unique salon IDs from multiple booking forms
        
        Args:
            form_ids: List of YCLIENTS form IDs
            
        Returns:
            Set of all unique salon IDs
        """
        all_salon_ids = set()
        
        for form_id in form_ids:
            salon_ids = self.fetch_salon_ids_from_form(form_id)
            all_salon_ids.update(salon_ids)
        
        logger.info(f"Total unique salon IDs found: {len(all_salon_ids)} - {all_salon_ids}")
        return all_salon_ids
    
    async def fetch_and_save_salon_info(self, salon_id: int) -> bool:
        """
        Fetch salon information from YCLIENTS API and save to MongoDB
        
        Args:
            salon_id: Salon ID to fetch info for
            
        Returns:
            bool: Success status
        """
        try:
            logger.info(f"Fetching salon info for salon {salon_id}...")
            
            # Fetch salon info from company API
            url = f"{self.base_url}/company/{salon_id}/"
            salon_info = self._make_request(url, use_user_token=True)
            
            if not salon_info:
                logger.warning(f"No salon info received for salon {salon_id}")
                return False
            
            # 1) Show prettified JSON in terminal
            print(f"\\n{'='*60}")
            print(f"SALON INFO FOR SALON {salon_id}")
            print(f"{'='*60}")
            print(json.dumps(salon_info, indent=2, ensure_ascii=False))
            print(f"{'='*60}\\n")
            
            # 2) Save to MongoDB 'salons' collection using upsert
            current_time = get_current_time()
            adjusted_time = self.db_manager._adjust_time_for_storage(current_time)
            salons_collection = self.db_manager.db['salons']
            
            update_doc = {
                '$set': {
                    'salon_info': salon_info,
                    'updated_at': adjusted_time
                },
                '$setOnInsert': {
                    'created_at': adjusted_time,
                    'salon_id': salon_id
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
                logger.info(f"Created new salon document for salon {salon_id}")
            elif result.modified_count > 0:
                logger.info(f"Updated existing salon document for salon {salon_id}")
            else:
                logger.info(f"No changes needed for salon {salon_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error fetching and saving salon info for salon {salon_id}: {e}")
            return False
    
    async def fetch_and_save_all_salons_info(self, salon_ids: Set[int]) -> bool:
        """
        Fetch salon information for all salon IDs
        
        Args:
            salon_ids: Set of salon IDs to process
            
        Returns:
            bool: Success status (True if all salons processed successfully)
        """
        if not salon_ids:
            logger.warning("No salon IDs provided")
            return False
        
        logger.info(f"Fetching salon info for {len(salon_ids)} salons: {salon_ids}")
        
        success_count = 0
        total_count = len(salon_ids)
        
        for salon_id in salon_ids:
            try:
                success = await self.fetch_and_save_salon_info(salon_id)
                if success:
                    success_count += 1
                else:
                    logger.error(f"Failed to fetch salon info for salon {salon_id}")
            except Exception as e:
                logger.error(f"Exception while processing salon {salon_id}: {e}")
        
        logger.info(f"Salon info fetch completed: {success_count}/{total_count} salons successful")
        return success_count == total_count
    
    async def cleanup(self):
        """Clean up resources"""
        try:
            self.session.close()
            self.db_manager.close()
            logger.info("Cleaned up YCLIENTS salons fetcher resources")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

async def main():
    """Main function to fetch and save YCLIENTS salon data"""
    # Get YCLIENTS API configuration from config
    form_ids = BOOKING_FORMS
    partner_token = YCLIENTS_PARTNER_TOKEN
    user_token = YCLIENTS_USER_TOKEN
    
    if not form_ids:
        logger.error("BOOKING_FORMS not configured in config.py (should be comma-separated list)")
        return False
    
    if not partner_token:
        logger.error("YCLIENTS_PARTNER_TOKEN not configured in config.py")
        return False
    
    logger.info(f"Processing {len(form_ids)} booking forms: {form_ids}")
    
    fetcher = YClientsSalonsFetcher(
        partner_token=partner_token,
        user_token=user_token
    )
    
    try:
        # Step 1: Fetch all unique salon IDs from booking forms
        all_salon_ids = fetcher.fetch_all_salon_ids(form_ids)
        
        if not all_salon_ids:
            logger.error("No salon IDs found in any booking forms")
            return False
        
        # Step 2: Fetch and save salon information for all salon IDs
        success = await fetcher.fetch_and_save_all_salons_info(all_salon_ids)
        
        if success:
            logger.info("YCLIENTS salon data fetch completed successfully for all salons")
        else:
            logger.error("YCLIENTS salon data fetch failed for some salons")
        
        return success
        
    finally:
        await fetcher.cleanup()

if __name__ == "__main__":
    print("YCLIENTS Salons Data Fetcher")
    print("Fetches salon information from YCLIENTS booking forms")
    print("- Extracts salon IDs from booking forms")
    print("- Fetches detailed salon information")
    print("- Shows prettified JSON in terminal")
    print("- Stores in MongoDB 'salons' collection")
    print()
    
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
