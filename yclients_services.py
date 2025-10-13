#!/usr/bin/env python3
"""
YCLIENTS Services Raw Data Fetcher
Fetches raw services data and categories from YCLIENTS API and saves to MongoDB 'salons' collection
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

from db_man import DatabaseManager
from logging_utils import setup_logger
from utils import get_current_time
from config import YCLIENTS_TIMEOUT, YCLIENTS_MAX_RETRIES, YCLIENTS_BACKOFF_FACTOR
from profile_manager import ProfileManager
from yclients_wrapper import YClientsAPI, YClientsAPIError

# Initialize logger
logger, _ = setup_logger("yclients_services.log", "yclients_services", "INFO", "DEBUG")

class YClientsServicesRawFetcher:
    """Fetches raw services data and categories from YCLIENTS API and saves to MongoDB"""

    def __init__(self, profile_name: Optional[str] = None):
        """
        Initialize YCLIENTS services data fetcher

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

        logger.info(f"Initialized YCLIENTS services fetcher for company: {self.company_name} (database: {db_name})")
        
    async def get_salon_ids_from_profile_or_db(self) -> List[int]:
        """
        Get salon IDs from profile first, then fallback to database

        Returns:
            List of salon IDs
        """
        # First try to get salon IDs from profile
        if self.salon_ids:
            logger.info(f"Using {len(self.salon_ids)} salon IDs from profile: {self.salon_ids}")
            return self.salon_ids

        # Fallback to database if no salon IDs in profile
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
        
    async def fetch_and_save_services_data_for_salon(self, salon_id: int) -> bool:
        """
        Fetch raw services data and categories for a single salon from YCLIENTS API
        
        Args:
            salon_id: YCLIENTS salon ID
            
        Returns:
            bool: Success status
        """
        try:
            logger.info(f"Fetching raw services data and categories for salon {salon_id}...")
            
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
            
            # Fetch raw services data from YCLIENTS API
            services_response = api.list_services()
            
            if not services_response:
                logger.warning(f"No services data received from YCLIENTS API for salon {salon_id}")
                return False
                
            # Debug: Check if services response already contains categories
            if isinstance(services_response, dict):
                logger.debug(f"Services response keys: {list(services_response.keys())}")
                if 'data' in services_response and isinstance(services_response['data'], dict):
                    logger.debug(f"Services data keys: {list(services_response['data'].keys())}")
                    if 'categories' in services_response['data']:
                        logger.info(f"Categories found in services response for salon {salon_id}")
                    else:
                        logger.info(f"No categories in services response for salon {salon_id}, will fetch separately")
            
            # Also fetch company services for complete data
            company_services_response = None
            try:
                company_services_response = api.list_company_services()
                logger.info(f"Also fetched complete company services data for salon {salon_id}")
            except YClientsAPIError as e:
                logger.warning(f"Could not fetch company services for salon {salon_id} (may require user token): {e}")
            
            # Fetch service categories
            categories_response = None
            try:
                categories_response = api.list_service_categories()
                if categories_response:
                    logger.info(f"Also fetched service categories data for salon {salon_id}")
                    logger.debug(f"Categories response structure: {list(categories_response.keys()) if isinstance(categories_response, dict) else 'Not a dict'}")
                else:
                    logger.warning(f"Empty categories response for salon {salon_id}")
            except YClientsAPIError as e:
                logger.warning(f"Could not fetch service categories for salon {salon_id}: {e}")
            
            # Prepare complete raw data
            complete_raw_data = {
                "book_services": services_response,
                "company_services": company_services_response
            }
            
            # Prepare categories data - extract from services response or use separate fetch
            categories_data = None
            
            # First try to extract categories from services response  
            if isinstance(services_response, dict) and 'data' in services_response:
                data = services_response['data']
                if isinstance(data, dict) and 'categories' in data:
                    categories_data = data['categories']
                    logger.info(f"Extracted categories from services response for salon {salon_id}")
                elif isinstance(data, dict) and 'category' in data:
                    categories_data = data['category']
                    logger.info(f"Extracted category from services response for salon {salon_id}")
            
            # If no categories in services response, use separate fetch
            if not categories_data and categories_response:
                categories_data = categories_response
                logger.info(f"Using separately fetched categories for salon {salon_id}")
            
            # Debug: Log what we found
            if categories_data:
                logger.info(f"Categories data type: {type(categories_data)}")
                if isinstance(categories_data, list):
                    logger.info(f"Found {len(categories_data)} categories")
                elif isinstance(categories_data, dict):
                    logger.info(f"Categories data keys: {list(categories_data.keys())}")
            else:
                logger.warning(f"No categories data found at all for salon {salon_id}")
            
            # 1) Show prettified JSON in terminal
            print(f"\n{'='*60}")
            print(f"RAW SERVICES DATA FOR SALON {salon_id}")
            print(f"{'='*60}")
            print(json.dumps(complete_raw_data, indent=2, ensure_ascii=False))
            print(f"{'='*60}\n")
            
            # Show categories data if available
            if categories_data:
                print(f"\n{'='*60}")
                print(f"RAW CATEGORIES DATA FOR SALON {salon_id}")
                print(f"{'='*60}")
                print(json.dumps(categories_data, indent=2, ensure_ascii=False))
                print(f"{'='*60}\n")
            
            # 2) Update salon document in 'salons' collection using upsert
            current_time = get_current_time(self.company_timezone)
            adjusted_time = self.db_manager._adjust_time_for_storage(current_time)
            salons_collection = self.db_manager.db['salons']
            
            update_doc = {
                '$set': {
                    'services': complete_raw_data,
                    'services_updated_at': adjusted_time
                }
            }
            
            # Add categories if available
            if categories_data:
                update_doc['$set']['categories'] = categories_data
                update_doc['$set']['categories_updated_at'] = adjusted_time
                logger.info(f"Adding categories to database for salon {salon_id}")
            else:
                logger.warning(f"No categories data available for salon {salon_id}")
            
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
                logger.info(f"Created new salon document with services and categories data for salon {salon_id}")
            elif result.modified_count > 0:
                logger.info(f"Updated salon document with services and categories data for salon {salon_id}")
            else:
                logger.info(f"No changes needed for salon {salon_id} services and categories data")
            
            # Log summary of data
            if isinstance(services_response, dict) and 'data' in services_response:
                if 'services' in services_response['data']:
                    services_count = len(services_response['data']['services'])
                    logger.info(f"Saved {services_count} services from book_services endpoint for salon {salon_id}")
                else:
                    logger.info(f"Saved services data from book_services endpoint for salon {salon_id}")
            
            if company_services_response and isinstance(company_services_response, dict) and 'data' in company_services_response:
                company_services_count = len(company_services_response['data']) if isinstance(company_services_response['data'], list) else 0
                logger.info(f"Saved {company_services_count} services from company_services endpoint for salon {salon_id}")
            
            if categories_response and isinstance(categories_response, dict) and 'data' in categories_response:
                categories_count = len(categories_response['data']) if isinstance(categories_response['data'], list) else 0
                logger.info(f"Saved {categories_count} service categories for salon {salon_id}")
            
            # Close API connection
            api.close()
            return True
            
        except YClientsAPIError as e:
            logger.error(f"YCLIENTS API error while fetching services data for salon {salon_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"Error fetching and saving services data for salon {salon_id}: {e}")
            return False
    
    async def fetch_and_save_all_salons_services_data(self, salon_ids: List[int]) -> bool:
        """
        Fetch raw services data and categories for all salons
        
        Args:
            salon_ids: List of YCLIENTS salon IDs
            
        Returns:
            bool: Success status (True if all salons processed successfully)
        """
        if not salon_ids:
            logger.warning("No salon IDs provided")
            return False
            
        logger.info(f"Fetching services data and categories for {len(salon_ids)} salons: {salon_ids}")
        
        success_count = 0
        total_count = len(salon_ids)
        
        for salon_id in salon_ids:
            try:
                success = await self.fetch_and_save_services_data_for_salon(salon_id)
                if success:
                    success_count += 1
                else:
                    logger.error(f"Failed to fetch services data for salon {salon_id}")
            except Exception as e:
                logger.error(f"Exception while processing salon {salon_id}: {e}")
        
        logger.info(f"Services data and categories fetch completed: {success_count}/{total_count} salons successful")
        return success_count == total_count
    
    async def cleanup(self):
        """Clean up resources"""
        try:
            self.db_manager.close()
            logger.info("Cleaned up YCLIENTS services fetcher resources")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

async def main(profile_name: Optional[str] = None):
    """Main function to fetch and save YCLIENTS services data and categories for all salons"""
    try:
        logger.info("Starting YCLIENTS services data fetch process")

        # Initialize fetcher with profile
        fetcher = YClientsServicesRawFetcher(profile_name)

        # Get salon IDs from profile or database
        salon_ids = await fetcher.get_salon_ids_from_profile_or_db()

        if not salon_ids:
            logger.error("No salon IDs found in profile or database. Run yclients_salons.py first to populate salons collection or add salon_ids to profile.")
            return False

        logger.info(f"Processing {len(salon_ids)} salons: {salon_ids}")

        success = await fetcher.fetch_and_save_all_salons_services_data(salon_ids)
        if success:
            logger.info("YCLIENTS services data and categories fetch completed successfully for all salons")
        else:
            logger.error("YCLIENTS services data and categories fetch failed for some salons")
        return success
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        return False
    finally:
        if 'fetcher' in locals():
            await fetcher.cleanup()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="YCLIENTS Services Raw Data Fetcher")
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

    print("YCLIENTS Services Raw Data Fetcher")
    print("Fetches raw services data and categories from YCLIENTS API for salons")
    print("- Reads salon IDs from profile or MongoDB 'salons' collection")
    print("- Shows prettified JSON in terminal")
    print("- Updates MongoDB 'salons' collection with services data and categories")
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