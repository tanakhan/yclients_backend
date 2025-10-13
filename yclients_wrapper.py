"""
YClients API wrapper – basic client for common booking flows.

Features implemented:
1. list_branches() - Get all company branches
2. list_staff() - Get staff members with optional filtering
3. list_services() - Get services with optional filtering
4. search_clients() - Advanced client search with filters
5. find_client() - Convenience method for common client searches
6. client_visits() - Get client visit/appointment history with flexible phone processing
7. get_client_last_visit_info() - Get comprehensive info about client's last visit
8. book_appointment() - Create new appointments
9. cancel_appointment() - Cancel existing appointments
10. reschedule_appointment() - Reschedule existing appointments
11. available_days() - Get available booking dates
12. available_times() - Get available time slots

The wrapper automatically sets required `Accept`/`Content‑Type` headers and supports partner & user tokens,
basic retry with exponential back‑off, flexible phone number processing, and raises a dedicated 
`YClientsAPIError` on any error returned by the platform.

Usage example:
    >>> api = YClientsAPI(company_id=4564,
    ...                   partner_token=os.getenv("YCLIENTS_PARTNER_TOKEN"),
    ...                   user_token=os.getenv("YCLIENTS_USER_TOKEN"))
    >>> print(api.list_branches())
    >>> print(api.list_staff())
    >>> clients = api.find_client(name="John", phone="123")
    >>> visits = api.client_visits(phone="+7 (912) 345-67-89")
    >>> last_visit = api.get_client_last_visit_info("912 345 6789")
"""

import logging
import time
from typing import Any, Dict, List, Optional

import requests

__all__ = ["YClientsAPI", "YClientsAPIError"]


class YClientsAPIError(Exception):
    """Raised for any transport or logical error returned by YCLIENTS."""


class YClientsAPI:
    """Light‑weight synchronous wrapper over the YCLIENTS REST API (v2)."""

    BASE_URL = "https://api.yclients.com/api/v1"

    def __init__(
        self,
        company_id: int,
        partner_token: str,
        user_token: Optional[str] = None,
        *,
        timeout: int = 10,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.company_id = company_id
        self.partner_token = partner_token
        self.user_token = user_token
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.session = requests.Session()
        self._default_headers = {
            "Accept": "application/vnd.yclients.v2+json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.partner_token}",
        }
        self.log = logger or logging.getLogger(self.__class__.__name__)

    # ----------------------------------------------------- private helpers
    def _normalize_phone(self, phone: str) -> str:
        """
        Normalize phone number by removing all non-digit characters.
        
        Args:
            phone: Phone number in any format
            
        Returns:
            str: Phone number with only digits
            
        Raises:
            ValueError: If phone number contains no digits
            
        Example:
            >>> api._normalize_phone("+7 (912) 345-67-89")
            "79123456789"
            >>> api._normalize_phone("922 661 1768")
            "9226611768"
        """
        import re
        clean_phone = re.sub(r'[^\d]', '', phone)
        if not clean_phone:
            raise ValueError("Phone number must contain at least one digit")
        return clean_phone
    
    def _find_client_by_phone(self, phone: str) -> Optional[Dict[str, Any]]:
        """
        Find client by phone number with smart matching logic.
        
        Handles different phone number formats and finds exact or partial matches.
        Uses the same logic as test_client_history.py for consistency.
        
        Args:
            phone: Phone number in any format
            
        Returns:
            Client dictionary if found, None otherwise
            
        Raises:
            ValueError: If phone number is invalid
            YClientsAPIError: If API request fails
        """
        clean_phone = self._normalize_phone(phone)
        
        # Use last 7 digits for initial search to get candidates
        search_digits = clean_phone[-7:] if len(clean_phone) >= 7 else clean_phone
        clients = self.find_client(phone=search_digits)
        
        if not isinstance(clients, dict) or 'data' not in clients or not clients['data']:
            self.log.warning(f"No client found with phone: {phone}")
            return None
        
        # Find exact or partial matches
        exact_matches = []
        for client in clients['data']:
            client_phone = client.get('phone', '')
            clean_client_phone = self._normalize_phone(client_phone) if client_phone else ''
            
            # Check for exact match
            if clean_client_phone == clean_phone:
                exact_matches.append(client)
            # Also check if the input is contained at the end (for partial numbers)
            elif clean_client_phone.endswith(clean_phone) and len(clean_phone) >= 7:
                exact_matches.append(client)
        
        if not exact_matches:
            self.log.warning(f"No exact phone match found for: {phone}")
            return None
        
        # Use the first exact match
        selected_client = exact_matches[0]
        
        if len(exact_matches) > 1:
            self.log.info(f"Found {len(exact_matches)} matches for phone {phone}, using first one")
        
        return selected_client

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        use_user_token: bool = False,
    ) -> Any:
        """Low‑level HTTP helper with simple 429 retry and uniform error handling."""
        url = f"{self.BASE_URL}{path}"
        headers = self._default_headers.copy()
        if use_user_token and self.user_token:
            # For operations requiring user token, use both partner and user tokens
            headers["Authorization"] = f"Bearer {self.partner_token}, User {self.user_token}"
        
        retries = 0
        while True:
            try:
                resp = self.session.request(
                    method,
                    url,
                    params=params,
                    json=json,
                    headers=headers,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                raise YClientsAPIError(f"Network error: {exc}") from exc

            if resp.status_code == 429 and retries < self.max_retries:
                retries += 1
                sleep_for = self.backoff_factor * (2 ** (retries - 1))
                self.log.warning("Rate‑limited (429). Retrying in %.1fs", sleep_for)
                time.sleep(sleep_for)
                continue

            if 200 <= resp.status_code < 300:
                data = resp.json() if resp.content else {}
                if isinstance(data, dict) and data.get("success") is False:
                    raise YClientsAPIError(data.get("meta") or data)
                return data

            raise YClientsAPIError(f"YClients error {resp.status_code}: {resp.text}")

    # ----------------------------------------------------- Branch endpoints
    def list_branches(
        self, 
        *, 
        active_only: bool = True,
        group_id: Optional[int] = None,
        my_companies: bool = False
    ) -> Any:
        """
        Return a list of all branches (companies) available for the current
        partner token.

        Args:
            active_only: If True (default) keep only branches where disabled == False.
            group_id: Filter by group ID (for chains)
            my_companies: If True, get only companies belonging to current user
            
        Returns:
            List of company/branch dictionaries or API response dict.
        """
        params: Dict[str, Any] = {}
        if group_id:
            params["group_id"] = group_id
        if my_companies:
            params["my"] = 1
            
        response = self._request("GET", "/companies", params=params)
        
        # Handle different response formats
        if isinstance(response, dict):
            if "data" in response:
                # Standard API response format
                branches = response["data"]
            elif "success" in response and isinstance(response.get("data"), list):
                # Alternative format
                branches = response["data"]
            elif isinstance(response, list):
                # Direct list response
                branches = response
            else:
                # Return original response if format is unexpected
                return response
        elif isinstance(response, list):
            # Direct list response
            branches = response
        else:
            return response
        
        # Filter active branches if requested
        if active_only and isinstance(branches, list):
            branches = [c for c in branches if not c.get("disabled", False)]
        
        # Return in standard format
        if isinstance(response, dict) and "data" in response:
            return {
                "success": response.get("success", True),
                "data": branches,
                "meta": response.get("meta", [])
            }
        else:
            return branches

    # ----------------------------------------------------- Staff endpoints
    def list_staff(
        self,
        *,
        service_ids: Optional[List[int]] = None,
        date_time: Optional[str] = None,
    ) -> Any:
        """Return staff list optionally filtered by service/date (GET /book_staff)."""
        params: Dict[str, Any] = {}
        if service_ids:
            for sid in service_ids:
                params.setdefault("service_ids[]", []).append(sid)
        if date_time:
            params["datetime"] = date_time
        return self._request("GET", f"/book_staff/{self.company_id}", params=params)

    def get_staff(self, staff_id: int) -> Any:
        """Detailed info for a single staff member (GET /staff/{company_id}/{id})."""
        return self._request("GET", f"/staff/{self.company_id}/{staff_id}")

    # ----------------------------------------------------- Service endpoints
    def list_services(
        self,
        *,
        staff_id: Optional[int] = None,
        date_time: Optional[str] = None,
    ) -> Any:
        """Return services list optionally filtered by staff/date (GET /book_services)."""
        params: Dict[str, Any] = {}
        if staff_id:
            params["staff_id"] = staff_id
        if date_time:
            params["datetime"] = date_time
        
        response = self._request("GET", f"/book_services/{self.company_id}", params=params)
        
        # YClients returns services in data.services array, not directly in data
        if isinstance(response, dict) and "data" in response and "services" in response["data"]:
            # Restructure to match expected format
            return {
                "success": response.get("success", True),
                "data": response["data"]["services"],
                "meta": response.get("meta", [])
            }
        
        return response

    def get_service(self, service_id: int) -> Any:
        """Detailed info for a single service (GET /services/{company_id}/{id})."""
        return self._request("GET", f"/services/{self.company_id}/{service_id}")

    def list_company_services(self) -> Any:
        """
        Get all services for the company with complete information including category_id and staff.
        This is the recommended method for getting complete service data with categories.
        
        Uses: GET /company/{company_id}/services
        
        Returns:
            Complete services data with category_id, staff info, prices, etc.
        """
        return self._request("GET", f"/company/{self.company_id}/services", use_user_token=True)

    def list_service_categories(
        self, 
        *, 
        include_services: bool = False,
        company_id: Optional[int] = None
    ) -> Any:
        """
        Get service categories for a company.
        
        Args:
            include_services: If True, include nested services in each category
            company_id: Override company ID (uses instance company_id if not provided)
            
        Uses: GET /company/{company_id}/service_categories
        
        Returns:
            List of service categories with optional nested services
        """
        target_company_id = company_id or self.company_id
        params: Dict[str, Any] = {}
        if include_services:
            params["include"] = "services"
            
        return self._request("GET", f"/company/{target_company_id}/service_categories", params=params, use_user_token=True)

    def list_chain_service_categories(
        self, 
        chain_id: int,
        *, 
        include_services: bool = False
    ) -> Any:
        """
        Get service categories for an entire chain.
        
        Args:
            chain_id: Chain/group ID
            include_services: If True, include nested services in each category
            
        Uses: GET /chain/{chain_id}/service_categories
        
        Returns:
            List of service categories across all chain locations
        """
        params: Dict[str, Any] = {}
        if include_services:
            params["include"] = "services"
            
        return self._request("GET", f"/chain/{chain_id}/service_categories", params=params, use_user_token=True)

    def list_services_by_staff(
        self, 
        staff_id: int,
        *, 
        date_time: Optional[str] = None
    ) -> Any:
        """
        Get services that a specific staff member can provide.
        
        Args:
            staff_id: Staff member ID
            date_time: Optional datetime filter
            
        Uses: GET /book_services/{company_id}?staff_id={staff_id}
        
        Returns:
            Services data filtered by staff member, includes categories
        """
        params: Dict[str, Any] = {"staff_id": staff_id}
        if date_time:
            params["datetime"] = date_time
        
        response = self._request("GET", f"/book_services/{self.company_id}", params=params)
        
        # YClients returns services in data.services array, not directly in data
        if isinstance(response, dict) and "data" in response and "services" in response["data"]:
            # Restructure to match expected format
            return {
                "success": response.get("success", True),
                "data": response["data"]["services"],
                "categories": response["data"].get("categories", []),
                "meta": response.get("meta", [])
            }
        
        return response

    # ----------------------------------------------------- Client endpoints
    def get_client(self, client_id: int) -> Any:
        """
        Get detailed client information by ID.
        
        Args:
            client_id: Client ID to retrieve
            
        Returns:
            Detailed client information
            
        Raises:
            YClientsAPIError: If API request fails
            
        Example:
            >>> client = api.get_client(12345)
            >>> print(f"Client: {client['name']} {client['surname']}")
            >>> print(f"Phone: {client['phone']}")
        """
        return self._request("GET", f"/client/{self.company_id}/{client_id}", use_user_token=True)

    def search_clients(
        self,
        *,
        filters: Optional[List[Dict[str, Any]]] = None,
        page: int = 1,
        page_size: int = 25,
        fields: Optional[List[str]] = None,
        order_by: Optional[str] = None,
        order_by_direction: str = "ASC",
    ) -> Any:
        """
        Search for clients with optional filters and field selection.
        
        Args:
            filters: List of filter dictionaries
            page: Page number (default: 1)
            page_size: Number of results per page (default: 25)
            fields: List of fields to return. If None, returns common useful fields.
            order_by: Field to order by
            order_by_direction: Order direction ("ASC" or "DESC")
            
        Returns:
            Search results with client data
        """
        # If no fields specified, use common useful fields
        if fields is None:
            fields = [
                'id', 'name', 'surname', 'patronymic', 'phone', 'email', 
                'card', 'visits_count', 'spent', 'balance', 'discount', 
                'sex', 'birth_date', 'created', 'last_visit_date'
            ]
        
        payload: Dict[str, Any] = {
            "page": page,
            "page_size": page_size,
            "operation": "AND",
            "fields": fields,
        }
        if filters:
            payload["filters"] = filters
        if order_by:
            payload["order_by"] = order_by
            payload["order_by_direction"] = order_by_direction
        return self._request(
            "POST",
            f"/company/{self.company_id}/clients/search",
            json=payload,
            use_user_token=True,
        )

    def find_client(
        self,
        *,
        name: Optional[str] = None,
        phone: Optional[str] = None,
        email: Optional[str] = None,
        page: int = 1,
        page_size: int = 25,
    ) -> Any:
        """
        Convenient method to search for clients by common criteria.
        
        Args:
            name: Client name (partial match)
            phone: Phone number (partial match)
            email: Email address (partial match)
            page: Page number (default: 1)
            page_size: Number of results per page (default: 25)
            
        Returns:
            Search results with client data
        """
        filters = []
        
        if name:
            filters.append({"name": name})
        if phone:
            filters.append({"phone": phone})
        if email:
            filters.append({"email": email})
            
        return self.search_clients(
            filters=filters,
            page=page,
            page_size=page_size
        )

    def client_visits(
        self,
        *,
        phone: Optional[str] = None,
        client_id: Optional[int] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        page_from: Optional[str] = None,
        page_to: Optional[str] = None,
        include_services: bool = True,
        include_staff: bool = True,
    ) -> Any:
        """
        Get visit (appointment) history for a specific client.
        
        Uses YClients endpoint: POST /company/{company_id}/clients/visits/search
        
        Args:
            phone: Client phone number (required if client_id not provided)
                  Should be digits only, e.g. "79123456789" (without + or country code)
            client_id: Client ID (required if phone not provided)
            from_date: Start date filter (ISO format: YYYY-MM-DD)
            to_date: End date filter (ISO format: YYYY-MM-DD)
            page_from: Pagination cursor for next pages (from previous response meta)
            page_to: Pagination cursor for next pages (from previous response meta)
            include_services: Whether to include service details in response
            include_staff: Whether to include staff details in response
            
        Returns:
            Visit history data with pagination info
            
        Raises:
            ValueError: If neither phone nor client_id is provided
            YClientsAPIError: If API request fails
            
        Example:
            >>> # Get visits by phone (digits only)
            >>> visits = api.client_visits(phone="79123456789")
            >>> 
            >>> # Get visits by client ID with date range
            >>> visits = api.client_visits(
            ...     client_id=12345,
            ...     from_date="2024-01-01",
            ...     to_date="2024-12-31"
            ... )
        """
        if not (phone or client_id):
            raise ValueError("Either phone or client_id must be provided")
        
        # Normalize phone number if provided
        clean_phone = None
        if phone and not client_id:
            clean_phone = self._normalize_phone(phone)
        
        # Log the request for debugging
        self.log.debug(
            f"Getting client visits for {'phone=' + clean_phone if clean_phone else 'client_id=' + str(client_id)}"
        )
        
        endpoint = f"/company/{self.company_id}/clients/visits/search"
        payload: Dict[str, Any] = {
            "client_id": client_id,
            "client_phone": clean_phone if not client_id else None,
        }
        
        # Add optional date filters
        if from_date:
            payload["from"] = from_date
        if to_date:
            payload["to"] = to_date
            
        # Add pagination cursors
        if page_from:
            payload["from_cursor"] = page_from
        if page_to:
            payload["to_cursor"] = page_to
            
        # Add optional includes
        if include_services:
            payload["include_services"] = 1
        if include_staff:
            payload["include_staff"] = 1
        
        return self._request("POST", endpoint, json=payload, use_user_token=True)

    def get_client_last_visit_info(self, phone: str) -> Optional[Dict[str, Any]]:
        """
        Get information about the client's last visit including staff details.
        
        Args:
            phone: Client phone number (any format accepted)
            
        Returns:
            Dictionary with last visit information or None if not found:
            {
                "id": int,                    # Staff member ID
                "name": str,                  # Staff member name
                "specialization": str,        # Staff specialization
                "last_visit_date": str,       # Date of last visit
                "last_visit_id": int,         # Visit record ID
                "attendance": int             # Attendance status (1=attended, 0=missed, etc.)
            }
            
        Raises:
            ValueError: If phone number is invalid
            YClientsAPIError: If API request fails
            
        Example:
            >>> visit_info = api.get_client_last_visit_info("+79123456789")
            >>> if visit_info:
            ...     print(f"Last master: {visit_info['name']} (ID: {visit_info['id']})")
            ...     print(f"Last visit: {visit_info['last_visit_date']}")
            ...     print(f"Attendance: {'Attended' if visit_info['attendance'] == 1 else 'Missed'}")
        """
        if not phone:
            raise ValueError("Phone number is required")
        
        clean_phone = self._normalize_phone(phone)
        self.log.debug(f"Getting last visit info for client with phone: {clean_phone}")
        
        try:
            # Step 1: Find client by phone using smart matching
            exact_match = self._find_client_by_phone(phone)
            if not exact_match:
                return None
            
            client_id = exact_match.get('id')
            
            # Step 2: Get client visits with staff information
            visits = self.client_visits(client_id=client_id, include_staff=True)
            
            if not isinstance(visits, dict) or 'data' not in visits:
                self.log.warning(f"Failed to get visits for client ID: {client_id}")
                return None
            
            records = visits['data'].get('records', [])
            
            if not records:
                self.log.info(f"No visit records found for client ID: {client_id}")
                return None
            
            # Step 3: Find the most recent visit with staff information
            # Sort by date (newest first)
            sorted_records = sorted(records, key=lambda x: x.get('date', ''), reverse=True)
            
            for record in sorted_records:
                staff = record.get('staff')
                if staff and staff.get('id') and staff.get('name'):
                    # Found the most recent visit with valid staff info
                    last_staff = {
                        "id": staff.get('id'),
                        "name": staff.get('name'),
                        "specialization": staff.get('specialization', ''),
                        "last_visit_date": record.get('date', ''),
                        "last_visit_id": record.get('id'),
                        "attendance": record.get('attendance')
                    }
                    
                    self.log.debug(f"Found last visit info - staff: {last_staff['name']} (ID: {last_staff['id']})")
                    return last_staff
            
            # No visits with staff information found
            self.log.info(f"No visits with staff information found for client ID: {client_id}")
            return None
            
        except Exception as e:
            self.log.error(f"Error getting last visit info for phone {phone}: {e}")
            raise

    # ----------------------------------------------------- Booking endpoints
    def book_appointment(
        self,
        *,
        phone: str,
        fullname: str,
        email: str,
        appointments: List[Dict[str, Any]],
        code: Optional[str] = None,
        notify_by_sms: int = 0,
        notify_by_email: int = 0,
        comment: Optional[str] = None,
        api_id: Optional[str] = None,
        custom_fields: Optional[Dict[str, Any]] = None,
    ) -> Any:
        payload: Dict[str, Any] = {
            "phone": phone,
            "fullname": fullname,
            "email": email,
            "appointments": appointments,
        }
        if code:
            payload["code"] = code
        if notify_by_sms:
            payload["notify_by_sms"] = notify_by_sms
        if notify_by_email:
            payload["notify_by_email"] = notify_by_email
        if comment:
            payload["comment"] = comment
        if api_id:
            payload["api_id"] = api_id
        if custom_fields:
            payload["custom_fields"] = custom_fields
        return self._request("POST", f"/book_record/{self.company_id}", json=payload)

    def cancel_appointment(
        self,
        record_id: int,
        *,
        include_consumables: int = 0,
        include_finance_transactions: int = 0,
    ) -> None:
        params: Dict[str, Any] = {}
        if include_consumables:
            params["include_consumables"] = include_consumables
        if include_finance_transactions:
            params["include_finance_transactions"] = include_finance_transactions
        self._request("DELETE", f"/record/{self.company_id}/{record_id}", params=params)

    def reschedule_appointment(
        self,
        record_id: int,
        *,
        new_datetime_iso: str,
        comment: Optional[str] = None,
    ) -> Any:
        payload = {"datetime": new_datetime_iso}
        if comment:
            payload["comment"] = comment
        return self._request("PUT", f"/book_record/{self.company_id}/{record_id}", json=payload)

    # ----------------------------------------------------- Availability endpoints
    def available_days(
        self,
        *,
        service_ids: Optional[List[int]] = None,
        staff_id: Optional[int] = None,
        date: Optional[str] = None,
    ) -> Any:
        params: Dict[str, Any] = {}
        if service_ids:
            for sid in service_ids:
                params.setdefault("service_ids[]", []).append(sid)
        if staff_id is not None:
            params["staff_id"] = staff_id
        if date:
            params["date"] = date
        return self._request("GET", f"/book_dates/{self.company_id}", params=params)

    def available_times(
        self,
        *,
        staff_id: int,
        date_iso: str,
        service_ids: Optional[List[int]] = None,
    ) -> Any:
        params: Dict[str, Any] = {}
        if service_ids:
            for sid in service_ids:
                params.setdefault("service_ids[]", []).append(sid)
        return self._request("GET", f"/book_times/{self.company_id}/{staff_id}/{date_iso}", params=params)

    # ----------------------------------------------------- Comprehensive data collection
    def build_complete_service_catalog(
        self,
        *,
        company_id: Optional[int] = None,
        include_staff_details: bool = True
    ) -> Dict[str, Any]:
        """
        Build a complete service catalog with categories, services, and staff information.
        
        This method combines multiple API calls to create a comprehensive data structure
        following the roadmap provided in the YClients documentation.
        
        Args:
            company_id: Override company ID (uses instance company_id if not provided)
            include_staff_details: Whether to include detailed staff information
            
        Returns:
            Complete catalog with structure:
            {
                "company_id": int,
                "company_title": str,
                "categories": [
                    {
                        "category_id": int,
                        "title": str,
                        "services": [
                            {
                                "service_id": int,
                                "title": str,
                                "price_min": int,
                                "price_max": int,
                                "staff": [
                                    {"id": int, "seance_length": int}
                                ]
                            }
                        ]
                    }
                ]
            }
        """
        target_company_id = company_id or self.company_id
        
        self.log.info(f"Building complete service catalog for company {target_company_id}")
        
        try:
            # Step 1: Get company info if needed
            company_info = {"id": target_company_id, "title": f"Company {target_company_id}"}
            
            # Step 2: Get service categories with nested services
            self.log.debug("Fetching service categories...")
            categories_response = self.list_service_categories(
                include_services=True, 
                company_id=target_company_id
            )
            
            # Step 3: Get all company services for complete data
            self.log.debug("Fetching all company services...")
            services_response = self.list_company_services() if target_company_id == self.company_id else \
                               self._request("GET", f"/company/{target_company_id}/services", use_user_token=True)
            
            # Process categories data
            categories_data = []
            if isinstance(categories_response, dict) and "data" in categories_response:
                categories_raw = categories_response["data"]
            elif isinstance(categories_response, list):
                categories_raw = categories_response
            else:
                self.log.warning("Unexpected categories response format")
                categories_raw = []
            
            # Process services data
            services_data = []
            if isinstance(services_response, dict) and "data" in services_response:
                services_data = services_response["data"]
            elif isinstance(services_response, list):
                services_data = services_response
            
            # Create service lookup by category_id
            services_by_category = {}
            for service in services_data:
                category_id = service.get("category_id")
                if category_id:
                    if category_id not in services_by_category:
                        services_by_category[category_id] = []
                    
                    # Format service data
                    service_data = {
                        "service_id": service["id"],
                        "title": service.get("title", ""),
                        "price_min": service.get("price_min", 0),
                        "price_max": service.get("price_max", 0),
                        "duration_seconds": service.get("seance_length"),
                        "staff": service.get("staff", [])
                    }
                    
                    services_by_category[category_id].append(service_data)
            
            # Build categories with services
            for category in categories_raw:
                category_id = category.get("id")
                category_data = {
                    "category_id": category_id,
                    "title": category.get("title", ""),
                    "services": services_by_category.get(category_id, [])
                }
                categories_data.append(category_data)
            
            # Build final result
            result = {
                "company_id": target_company_id,
                "company_title": company_info["title"],
                "categories": categories_data,
                "total_categories": len(categories_data),
                "total_services": len(services_data)
            }
            
            self.log.info(f"Built catalog with {len(categories_data)} categories and {len(services_data)} services")
            return result
            
        except Exception as e:
            self.log.error(f"Error building complete service catalog: {e}")
            raise YClientsAPIError(f"Failed to build complete service catalog: {e}")

    def get_services_with_categories(self, *, manual_categories: Optional[Dict[int, str]] = None) -> Dict[str, Any]:
        """
        Convenience method to get services with category names populated.
        
        This method fixes the issue where list_services() returns empty category_name.
        Due to YClients API access restrictions, it uses manual category mapping as fallback.
        
        ⚠️  IMPORTANT: Default manual_categories mapping is COMPANY-SPECIFIC (ID 953897).
        Category IDs are unique to each YClients company - you MUST provide your own
        mapping when using this wrapper with different companies.
        
        Args:
            manual_categories: Optional manual mapping of category_id -> category_name
                              If not provided, uses built-in mapping for specific company.
                              For other companies, you MUST provide your own mapping.
        
        Returns:
            Services list with category names properly populated
            
        Example:
            # For your own company, provide custom mapping:
            api.get_services_with_categories(manual_categories={
                12345: "Your Category Name",
                67890: "Another Category"
            })
        """
        self.log.debug("Fetching services with category names...")
        
        # Default manual categories - COMPANY-SPECIFIC MAPPING
        # ⚠️  WARNING: This mapping is tailored for a specific laser hair removal studio
        # ⚠️  Category IDs and names are unique to each YClients company
        # ⚠️  When using this wrapper with other companies, you MUST provide
        # ⚠️  your own manual_categories mapping or update these defaults
        #
        # Current mapping is for Company ID 953897 (laser hair removal studio):
        if manual_categories is None:
            manual_categories = {
                14181354: "Женская лазерная эпиляция",    # Female laser hair removal
                14181355: "Мужская лазерная эпиляция",     # Male laser hair removal  
                18489550: "Диодное омоложение",            # Diode rejuvenation (cosmetic)
                # Add more categories as discovered for THIS specific company
            }
        
        try:
            # Try to get categories via API first
            categories_map = {}
            try:
                categories_response = self.list_service_categories()
                if isinstance(categories_response, dict) and "data" in categories_response:
                    for category in categories_response["data"]:
                        categories_map[category["id"]] = category.get("title", "")
                    self.log.debug(f"Successfully fetched {len(categories_map)} categories via API")
            except YClientsAPIError as e:
                if "403" in str(e) or "Недостаточно прав" in str(e):
                    self.log.info("Category API access restricted (403), using manual mapping")
                    categories_map = manual_categories.copy()
                else:
                    raise
            
            # Fallback to manual mapping if API returned empty
            if not categories_map and manual_categories:
                self.log.info("API returned no categories, using manual mapping")
                categories_map = manual_categories.copy()
            
            # Get complete services data
            services_response = self.list_company_services()
            
            if not isinstance(services_response, dict) or "data" not in services_response:
                return services_response
            
            # Add category names to services
            for service in services_response["data"]:
                category_id = service.get("category_id")
                if category_id in categories_map:
                    service["category_name"] = categories_map[category_id]
                else:
                    service["category_name"] = f"Category {category_id}"  # Fallback name
                    self.log.debug(f"Unknown category_id {category_id}, using fallback name")
            
            return services_response
            
        except Exception as e:
            self.log.error(f"Error getting services with categories: {e}")
            raise YClientsAPIError(f"Failed to get services with categories: {e}")

    # ----------------------------------------------------- Cleanup
    def close(self) -> None:
        self.session.close()
