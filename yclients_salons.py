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

from db_man import DatabaseManager
from logging_utils import setup_logger
from utils import get_current_time
from config import YCLIENTS_TIMEOUT, YCLIENTS_MAX_RETRIES, YCLIENTS_BACKOFF_FACTOR
from profile_manager import ProfileManager

# Initialize logger
logger, _ = setup_logger("yclients_salons.log", "yclients_salons", "INFO", "DEBUG")

class YClientsSalonsFetcher:
    """Fetches salon information from YCLIENTS booking forms and company API"""

    def __init__(self, profile_name: Optional[str] = None):
        """
        Initialize YCLIENTS salons data fetcher

        Args:
            profile_name: Name of the profile to use (uses default if None)
        """
        self.profile_manager = ProfileManager()
        self.profile = self.profile_manager.get_profile(profile_name)

        if not self.profile:
            raise ValueError(f"Profile '{profile_name}' not found")

        self.partner_token = self.profile['yclients']['partner_token']
        self.user_token = self.profile['yclients'].get('user_token')
        self.booking_forms = self.profile['yclients']['booking_forms']

        self.db_manager = DatabaseManager()
        self.base_url = "https://api.yclients.com/api/v1"

        # Setup session for HTTP requests
        self.session = requests.Session()

        # Configure proxy if enabled
        proxy_settings = self.profile_manager.get_proxy_settings(profile_name)
        if proxy_settings:
            proxies = {
                'http': f"http://{proxy_settings['username']}:{proxy_settings['password']}@{proxy_settings['host']}:{proxy_settings['port']}",
                'https': f"http://{proxy_settings['username']}:{proxy_settings['password']}@{proxy_settings['host']}:{proxy_settings['port']}"
            }
            self.session.proxies.update(proxies)
            logger.info(f"Using proxy: {proxy_settings['host']}:{proxy_settings['port']}")

        self.session.headers.update({
            'Accept': 'application/vnd.yclients.v2+json',
            'Authorization': f'Bearer {self.partner_token}'
        })

        logger.info(f"Initialized YCLIENTS fetcher for profile: {self.profile['name']}")
    
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

async def main(profile_name: Optional[str] = None):
    """Main function to fetch and save YCLIENTS salon data"""
    try:
        logger.info("Starting YCLIENTS salon data fetch process")

        # Initialize fetcher with profile
        fetcher = YClientsSalonsFetcher(profile_name)

        form_ids = fetcher.booking_forms
        if not form_ids:
            logger.error("No booking forms configured in profile")
            return False

        logger.info(f"Processing {len(form_ids)} booking forms: {form_ids}")

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

    except Exception as e:
        logger.error(f"Error in main function: {e}")
        return False
    finally:
        if 'fetcher' in locals():
            await fetcher.cleanup()

if __name__ == "__main__":
    import argparse
    import time

    parser = argparse.ArgumentParser(description="YCLIENTS Salons Data Fetcher")
    parser.add_argument('--company', help='Company name to process (from profiles)')
    parser.add_argument('--list-profiles', action='store_true', help='List available profiles')

    args = parser.parse_args()

    if args.list_profiles:
        pm = ProfileManager()
        print("Available profiles:")
        for name, profile in pm.get_all_profiles().items():
            proxy_status = "with proxy" if profile.get('proxy', {}).get('use_proxy', False) else "no proxy"
            print(f"- {name}: {profile.get('name', 'Unnamed')} ({proxy_status})")
        print(f"\nDefault profile: {pm.default_profile}")
        sys.exit(0)

    print("YCLIENTS Salons Data Fetcher")
    print("Fetches salon information from YCLIENTS booking forms")
    print("- Extracts salon IDs from booking forms")
    print("- Fetches detailed salon information")
    print("- Shows prettified JSON in terminal")
    print("- Stores in MongoDB 'salons' collection")
    print()

    pm = ProfileManager()

    if args.company:
        # Find profile by company name
        profile_name = None
        for name, profile in pm.get_all_profiles().items():
            if profile.get('name') == args.company:
                profile_name = name
                break

        if not profile_name:
            print(f"Error: Company '{args.company}' not found in profiles")
            sys.exit(1)

        print(f"Processing company: {args.company} (profile: {profile_name})")
        success = asyncio.run(main(profile_name))
        sys.exit(0 if success else 1)
    else:
        # Process all companies with 10-second pauses
        all_profiles = pm.get_all_profiles()
        print(f"Processing all {len(all_profiles)} companies with 10-second pauses...")

        overall_success = True
        for i, (profile_name, profile) in enumerate(all_profiles.items()):
            company_name = profile.get('name', profile_name)
            print(f"\n{'='*60}")
            print(f"Processing company {i+1}/{len(all_profiles)}: {company_name}")
            print(f"{'='*60}")

            try:
                success = asyncio.run(main(profile_name))
                if not success:
                    overall_success = False
                    print(f"Warning: Failed to process company {company_name}")
            except Exception as e:
                print(f"Error processing company {company_name}: {e}")
                overall_success = False

            # Wait 10 seconds before next company (except for the last one)
            if i < len(all_profiles) - 1:
                print(f"Waiting 10 seconds before next company...")
                time.sleep(10)

        print(f"\n{'='*60}")
        if overall_success:
            print("All companies processed successfully!")
        else:
            print("Some companies failed to process completely")
        print(f"{'='*60}")

        sys.exit(0 if overall_success else 1)
