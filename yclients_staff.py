#!/usr/bin/env python3
"""
YCLIENTS Staff Raw Data Fetcher
Fetches raw staff data from YCLIENTS API and saves to MongoDB 'salons' collection
"""
import asyncio
import sys
import os
import json
import requests
from datetime import datetime
from typing import Dict, Any, Optional, List

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from db_man import DatabaseManager
from logging_utils import setup_logger
from utils import get_current_time
from config import YCLIENTS_TIMEOUT, YCLIENTS_MAX_RETRIES, YCLIENTS_BACKOFF_FACTOR
from profile_manager import ProfileManager
from yclients_wrapper import YClientsAPI, YClientsAPIError

# Initialize logger
logger, _ = setup_logger("yclients_staff.log", "yclients_staff", "INFO", "DEBUG")

class YClientsStaffRawFetcher:
    """Fetches raw staff data from YCLIENTS API and saves to MongoDB"""

    def __init__(self, profile_name: Optional[str] = None):
        """
        Initialize YCLIENTS staff data fetcher

        Args:
            profile_name: Name of the profile to use (uses default if None)
        """
        self.profile_manager = ProfileManager()
        self.profile = self.profile_manager.get_profile(profile_name)

        if not self.profile:
            raise ValueError(f"Profile '{profile_name}' not found")

        self.company_name = self.profile['name']
        self.company_timezone = self.profile.get('timezone', 'UTC')
        self.salon_ids = self.profile.get('salon_ids', [])
        self.partner_token = self.profile['yclients']['partner_token']
        self.user_token = self.profile['yclients'].get('user_token')

        # Use company name as database name (sanitize it for MongoDB)
        db_name = self.company_name.lower().replace(' ', '_').replace('-', '_')
        self.db_manager = DatabaseManager(project_name=db_name, timezone=self.company_timezone)
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

        logger.info(f"Initialized YCLIENTS staff fetcher for company: {self.company_name} (database: {db_name})")
        logger.info(f"Profile salon_ids: {self.salon_ids}")
        
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

            # Print raw response for debugging
            print(f"Raw API response for {url}:")
            print(response.text)
            print("-" * 40)

            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            return None
        
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
            self.session.close()
            self.db_manager.close()
            logger.info("Cleaned up YCLIENTS staff fetcher resources")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

async def main(profile_name: Optional[str] = None):
    """Main function to fetch and save YCLIENTS staff data for all salons"""
    try:
        logger.info("Starting YCLIENTS staff data fetch process")

        # Initialize fetcher with profile
        fetcher = YClientsStaffRawFetcher(profile_name)

        # Use salon_ids directly from profile
        if not fetcher.salon_ids:
            logger.error("No salon_ids configured in profile")
            return False

        salon_ids = fetcher.salon_ids
        logger.info(f"Processing {len(salon_ids)} salons from profile: {salon_ids}")

        success = await fetcher.fetch_and_save_all_salons_staff_data(salon_ids)
        if success:
            logger.info("YCLIENTS staff data fetch completed successfully for all salons")
        else:
            logger.error("YCLIENTS staff data fetch failed for some salons")
        return success
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        return False
    finally:
        if 'fetcher' in locals():
            await fetcher.cleanup()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="YCLIENTS Staff Raw Data Fetcher")
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

    print("YCLIENTS Staff Raw Data Fetcher")
    print("Fetches raw staff data from YCLIENTS API for salons")
    print("- Uses salon IDs from profile configuration")
    print("- Shows prettified JSON in terminal")
    print("- Updates MongoDB 'salons' collection with staff data")
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

        import time
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