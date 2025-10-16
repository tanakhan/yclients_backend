#!/usr/bin/env python3
"""
YCLIENTS Full Data Sync Script
Fetches complete salon information, services, and staff data from YCLIENTS API
Combines functionality from yclients_salons.py, yclients_services.py, and yclients_staff.py
"""
import asyncio
import sys
import os
import json
import requests
from datetime import datetime
from typing import Dict, Any, Optional, List

# Add project root to Python path for absolute imports compatible with crontab
project_root = os.path.abspath(os.path.dirname(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from db_man import DatabaseManager
from logging_utils import setup_logger
from utils import get_current_time
from config import YCLIENTS_TIMEOUT, YCLIENTS_MAX_RETRIES, YCLIENTS_BACKOFF_FACTOR
from profile_manager import ProfileManager
from yclients_wrapper import YClientsAPI, YClientsAPIError

# Initialize logger
logger, _ = setup_logger("yclients_full_sync.log", "yclients_full_sync", "INFO", "DEBUG")

class YClientsFullDataSyncer:
    """Complete YCLIENTS data syncer combining salons, services, and staff fetching"""

    def __init__(self, profile_name: Optional[str] = None, verbose: bool = False):
        """
        Initialize YCLIENTS full data syncer

        Args:
            profile_name: Name of the profile to use (uses default if None)
            verbose: Whether to print raw API responses
        """
        self.verbose = verbose
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

        logger.info(f"Initialized YCLIENTS full syncer for company: {self.company_name} (database: {db_name})")
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

            # Print raw response for debugging only if verbose mode is enabled
            if self.verbose:
                print(f"Raw API response for {url}:")
                print(response.text)
                print("-" * 40)

            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {url}: {e}")
            return None

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

            # Show prettified JSON in terminal if verbose
            if self.verbose:
                print(f"\\n{'='*60}")
                print(f"SALON INFO FOR SALON {salon_id}")
                print(f"{'='*60}")
                print(json.dumps(salon_info, indent=2, ensure_ascii=False))
                print(f"{'='*60}\\n")

            # Save to MongoDB 'salons' collection using upsert
            current_time = get_current_time(self.company_timezone)
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

            # Show prettified JSON in terminal if verbose
            if self.verbose:
                print(f"\\n{'='*60}")
                print(f"RAW SERVICES DATA FOR SALON {salon_id}")
                print(f"{'='*60}")
                print(json.dumps(complete_raw_data, indent=2, ensure_ascii=False))
                print(f"{'='*60}\\n")

            # Show categories data if available and verbose
            if categories_data and self.verbose:
                print(f"\\n{'='*60}")
                print(f"RAW CATEGORIES DATA FOR SALON {salon_id}")
                print(f"{'='*60}")
                print(json.dumps(categories_data, indent=2, ensure_ascii=False))
                print(f"{'='*60}\\n")

            # Update salon document in 'salons' collection using upsert
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

            # Show prettified JSON in terminal if verbose
            if self.verbose:
                print(f"\\n{'='*60}")
                print(f"RAW STAFF DATA FOR SALON {salon_id}")
                print(f"{'='*60}")
                print(json.dumps(staff_response, indent=2, ensure_ascii=False))
                print(f"{'='*60}\\n")

            # Update salon document in 'salons' collection using upsert
            current_time = get_current_time(self.company_timezone)
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

    async def generate_simplified_data_for_salon(self, salon_id: int) -> Dict[str, Any]:
        """
        Generate simplified data structure for a specific salon

        Args:
            salon_id: The salon ID to generate data for

        Returns:
            Dict containing simplified salon, staff, and service data for this salon
        """
        try:
            logger.info(f"Generating simplified data structure for salon {salon_id}")

            # Get the salon document from database
            salons_collection = self.db_manager.db['salons']

            loop = asyncio.get_event_loop()
            salon_doc = await loop.run_in_executor(
                None,
                lambda: salons_collection.find_one({'_id': str(salon_id)})
            )

            if not salon_doc:
                logger.warning(f"No salon document found for salon {salon_id}")
                return {}

            simplified_data = {
                "_id": str(salon_id),
                "id": str(salon_id),
            }

            # Extract salon info
            if 'salon_info' in salon_doc and 'data' in salon_doc['salon_info']:
                salon_data = salon_doc['salon_info']['data']
                simplified_data['salon_info'] = {
                    salon_data.get('title', ''): {
                        'id': salon_data.get('id'),
                        'phone': salon_data.get('phone', ''),
                        'address': salon_data.get('address', '')
                    }
                }

            # Extract staff data
            if 'staff' in salon_doc and 'data' in salon_doc['staff']:
                staff_list = []
                for staff in salon_doc['staff']['data']:
                    staff_list.append({
                        staff.get('name', ''): {
                            'id': staff.get('id')
                        }
                    })
                simplified_data['staff_name_to_id'] = staff_list

            # Extract services data
            service_name_to_id = {}
            if 'services' in salon_doc and 'company_services' in salon_doc['services'] and 'data' in salon_doc['services']['company_services']:
                for service in salon_doc['services']['company_services']['data']:
                    service_name = service.get('title', '')
                    if service_name:
                        service_info = {
                            'id': service.get('id'),
                            'price': service.get('price_min', 0),
                            'category_id': service.get('category_id')
                        }

                        if service_name not in service_name_to_id:
                            service_name_to_id[service_name] = []
                        service_name_to_id[service_name].append(service_info)

            simplified_data['service_name_to_id'] = service_name_to_id

            # Extract categories data
            if 'categories' in salon_doc and 'data' in salon_doc['categories']:
                category_list = []
                for category in salon_doc['categories']['data']:
                    category_list.append({
                        category.get('title', ''): {
                            'category_id': category.get('id')
                        }
                    })
                simplified_data['category_name_to_id'] = category_list

            logger.info(f"Successfully generated simplified data structure for salon {salon_id}")
            return simplified_data

        except Exception as e:
            logger.error(f"Error generating simplified data for salon {salon_id}: {e}")
            return {}

    async def save_simplified_data(self, simplified_data: Dict[str, Any]) -> bool:
        """
        Save simplified data to prompts collection

        Args:
            simplified_data: The simplified data structure to save

        Returns:
            bool: Success status
        """
        try:
            if not simplified_data:
                logger.warning("No simplified data to save")
                return False

            logger.info("Saving simplified data to prompts collection")

            # Save to prompts collection
            prompts_collection = self.db_manager.db['prompts']
            current_time = get_current_time(self.company_timezone)
            adjusted_time = self.db_manager._adjust_time_for_storage(current_time)

            update_doc = {
                '$set': {
                    **simplified_data,
                    'updated_at': adjusted_time
                },
                '$setOnInsert': {
                    'created_at': adjusted_time
                }
            }

            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                lambda: prompts_collection.update_one(
                    {'_id': simplified_data.get('_id')},
                    update_doc,
                    upsert=True
                )
            )

            if result.upserted_id:
                logger.info("Created new prompts document")
            elif result.modified_count > 0:
                logger.info("Updated existing prompts document")
            else:
                logger.info("No changes needed for prompts document")

            return True

        except Exception as e:
            logger.error(f"Error saving simplified data: {e}")
            return False

    async def run_full_sync(self) -> bool:
        """
        Run complete sync for all salon IDs: fetch salon info, services, and staff data

        Returns:
            bool: Success status (True if all operations completed successfully)
        """
        if not self.salon_ids:
            logger.error("No salon_ids configured in profile")
            return False

        salon_ids = self.salon_ids
        logger.info(f"Starting full sync for {len(salon_ids)} salons: {salon_ids}")

        overall_success = True

        # Phase 1: Fetch salon information
        logger.info("="*80)
        logger.info("PHASE 1: FETCHING SALON INFORMATION")
        logger.info("="*80)

        for salon_id in salon_ids:
            try:
                logger.info(f"Processing salon {salon_id} - Phase 1/3: Salon Info")
                success = await self.fetch_and_save_salon_info(salon_id)
                if not success:
                    logger.error(f"Failed to fetch salon info for salon {salon_id}")
                    overall_success = False
                else:
                    logger.info(f"Successfully fetched salon info for salon {salon_id}")
            except Exception as e:
                logger.error(f"Exception during salon info fetch for salon {salon_id}: {e}")
                overall_success = False

        # Phase 2: Fetch services data
        logger.info("\\n" + "="*80)
        logger.info("PHASE 2: FETCHING SERVICES DATA")
        logger.info("="*80)

        for salon_id in salon_ids:
            try:
                logger.info(f"Processing salon {salon_id} - Phase 2/3: Services")
                success = await self.fetch_and_save_services_data_for_salon(salon_id)
                if not success:
                    logger.error(f"Failed to fetch services data for salon {salon_id}")
                    overall_success = False
                else:
                    logger.info(f"Successfully fetched services data for salon {salon_id}")
            except Exception as e:
                logger.error(f"Exception during services fetch for salon {salon_id}: {e}")
                overall_success = False

        # Phase 3: Fetch staff data
        logger.info("\\n" + "="*80)
        logger.info("PHASE 3: FETCHING STAFF DATA")
        logger.info("="*80)

        for salon_id in salon_ids:
            try:
                logger.info(f"Processing salon {salon_id} - Phase 3/3: Staff")
                success = await self.fetch_and_save_staff_data_for_salon(salon_id)
                if not success:
                    logger.error(f"Failed to fetch staff data for salon {salon_id}")
                    overall_success = False
                else:
                    logger.info(f"Successfully fetched staff data for salon {salon_id}")
            except Exception as e:
                logger.error(f"Exception during staff fetch for salon {salon_id}: {e}")
                overall_success = False

        # Phase 4: Generate and save simplified data for each salon
        logger.info("\\n" + "="*80)
        logger.info("PHASE 4: GENERATING SIMPLIFIED DATA FOR EACH SALON")
        logger.info("="*80)

        for salon_id in salon_ids:
            try:
                logger.info(f"Generating simplified data for salon {salon_id}")
                simplified_data = await self.generate_simplified_data_for_salon(salon_id)
                if simplified_data:
                    success = await self.save_simplified_data(simplified_data)
                    if success:
                        logger.info(f"Successfully generated and saved simplified data for salon {salon_id}")
                    else:
                        logger.error(f"Failed to save simplified data for salon {salon_id}")
                        overall_success = False
                else:
                    logger.error(f"Failed to generate simplified data for salon {salon_id}")
                    overall_success = False
            except Exception as e:
                logger.error(f"Exception during simplified data generation for salon {salon_id}: {e}")
                overall_success = False

        # Final summary
        logger.info("\\n" + "="*80)
        if overall_success:
            logger.info("FULL SYNC COMPLETED SUCCESSFULLY")
            logger.info(f"All data fetched and saved for {len(salon_ids)} salons: {salon_ids}")
        else:
            logger.error("FULL SYNC COMPLETED WITH ERRORS")
            logger.error("Some salons may have incomplete data")
        logger.info("="*80)

        return overall_success

    async def cleanup(self):
        """Clean up resources"""
        try:
            self.session.close()
            self.db_manager.close()
            logger.info("Cleaned up YCLIENTS full syncer resources")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

async def main(profile_name: Optional[str] = None, verbose: bool = False):
    """Main function to run full YCLIENTS data sync"""
    try:
        logger.info("Starting YCLIENTS full data sync process")

        # Initialize syncer with profile and verbose flag
        syncer = YClientsFullDataSyncer(profile_name, verbose)

        success = await syncer.run_full_sync()
        if success:
            logger.info("YCLIENTS full data sync completed successfully")
        else:
            logger.error("YCLIENTS full data sync failed for some operations")
        return success
    except Exception as e:
        logger.error(f"Error in main function: {e}")
        return False
    finally:
        if 'syncer' in locals():
            await syncer.cleanup()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="YCLIENTS Full Data Sync")
    parser.add_argument('--company', help='Company name to process (from profiles)')
    parser.add_argument('--list-profiles', action='store_true', help='List available profiles')
    parser.add_argument('--verbose', '-v', action='store_true', help='Print raw API responses and JSON data')

    args = parser.parse_args()

    if args.list_profiles:
        pm = ProfileManager()
        print("Available profiles:")
        for name, profile in pm.get_all_profiles().items():
            proxy_status = "with proxy" if profile.get('proxy', {}).get('use_proxy', False) else "no proxy"
            print(f"- {name}: {profile.get('name', 'Unnamed')} ({proxy_status})")
        print(f"\\nDefault profile: {pm.default_profile}")
        sys.exit(0)

    print("YCLIENTS Full Data Sync")
    print("Fetches complete salon information, services, and staff data")
    print("- Uses salon IDs from profile configuration")
    print("- Updates MongoDB 'salons' collection with all data")
    print("- Generates simplified data structure for 'prompts' collection")
    print("- Shows prettified JSON responses in terminal")
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
        success = asyncio.run(main(profile_name, args.verbose))
        sys.exit(0 if success else 1)
    else:
        # Process all companies with 10-second pauses
        all_profiles = pm.get_all_profiles()
        print(f"Processing all {len(all_profiles)} companies with 10-second pauses...")

        import time
        overall_success = True
        for i, (profile_name, profile) in enumerate(all_profiles.items()):
            company_name = profile.get('name', profile_name)
            print(f"\\n{'='*60}")
            print(f"Processing company {i+1}/{len(all_profiles)}: {company_name}")
            print(f"{'='*60}")

            try:
                success = asyncio.run(main(profile_name, args.verbose))
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

        print(f"\\n{'='*60}")
        if overall_success:
            print("All companies processed successfully!")
        else:
            print("Some companies failed to process completely")
        print(f"{'='*60}")

        sys.exit(0 if overall_success else 1)