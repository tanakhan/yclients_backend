from pymongo import MongoClient
import asyncio
import logging
from config import db_name, db_collection_name, logger_base_name, MONGODB_USAGE_COLLECTION, TIMEZONE
from logging_utils import setup_logger
from utils import get_current_time
import pytz
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional

# Get the logger for this module
logger, _ = setup_logger("db_man.log", "db_manager", "INFO", "DEBUG")
db_message_length_limiter = 50

# Database-specific settings for messenger nodes
MESSENGER_USERNAME_PREFIX = ""  # No prefix for messenger document IDs
MESSENGER_CONTACT_FORM_MANDATORY = False  # Contact forms not mandatory for messengers
CONTACT_FORM_EMAIL_MANDATORY = False  # Email not mandatory for simplicity

# Get the configured timezone
try:
    CONFIGURED_TIMEZONE = pytz.timezone(TIMEZONE)
    # Calculate the UTC offset in hours for the current timezone
    now = datetime.now(CONFIGURED_TIMEZONE)
    UTC_OFFSET_HOURS = now.utcoffset().total_seconds() / 3600
    logger.info(f"Using timezone {TIMEZONE} with UTC offset of {UTC_OFFSET_HOURS} hours")
except (pytz.exceptions.UnknownTimeZoneError, NameError):
    logger.warning(f"Unknown timezone or TIMEZONE not defined in config. Defaulting to UTC.")
    CONFIGURED_TIMEZONE = pytz.UTC
    UTC_OFFSET_HOURS = 0

# Track global instances for cleanup
_db_instances = []

class DatabaseManager:
    def __init__(self, connection_string='mongodb://localhost:27017/', project_name: Optional[str] = None, timezone: Optional[str] = None):
        """Initialize database connection"""
        self.mongo_client = MongoClient(connection_string,
                                   maxPoolSize=10,
                                   serverSelectionTimeoutMS=5000)

        # Use project-specific database if provided, otherwise use default
        self.project_name = project_name or db_name
        self.db = self.mongo_client[self.project_name]
        self.chats = self.db[db_collection_name]
        self.usage = self.db[MONGODB_USAGE_COLLECTION]  # Use config variable

        # Configure timezone for this instance
        self.timezone = timezone or TIMEZONE
        try:
            self.configured_timezone = pytz.timezone(self.timezone)
            # Calculate the UTC offset in hours for the current timezone
            now = datetime.now(self.configured_timezone)
            self.utc_offset_hours = now.utcoffset().total_seconds() / 3600
            logger.info(f"DatabaseManager initialized with database: {self.project_name}, timezone: {self.timezone}, offset: {self.utc_offset_hours}h")
        except (pytz.exceptions.UnknownTimeZoneError, NameError):
            logger.warning(f"Unknown timezone {self.timezone}, defaulting to UTC.")
            self.configured_timezone = pytz.UTC
            self.utc_offset_hours = 0

        # Add instance to global tracking list
        global _db_instances
        if self not in _db_instances:
            _db_instances.append(self)
            logger.debug(f"Added DatabaseManager instance to global tracking (total: {len(_db_instances)})")
    
    def switch_project(self, project_name: str):
        """
        Switch to a different project database
        
        Args:
            project_name: Name of the project database to switch to
        """
        self.project_name = project_name
        self.db = self.mongo_client[project_name]
        self.chats = self.db[db_collection_name]
        self.usage = self.db[MONGODB_USAGE_COLLECTION]
        logger.info(f"Switched to project database: {project_name}")
    
    def ensure_project_database(self, project_name: str):
        """
        Ensure a project database exists (MongoDB creates it automatically on first write)
        
        Args:
            project_name: Name of the project database
        """
        try:
            # MongoDB creates databases automatically on first write operation
            # We just need to switch to it
            if project_name != self.project_name:
                self.switch_project(project_name)
            logger.debug(f"Ensured project database: {project_name}")
        except Exception as e:
            logger.error(f"Error ensuring project database {project_name}: {e}")
    
    def _adjust_time_for_storage(self, dt):
        """
        Pre-adjust a datetime for storage in MongoDB to compensate for UTC conversion.
        This adds the UTC offset so that when MongoDB converts to UTC, it will represent the correct local time.

        Args:
            dt: Datetime object to adjust

        Returns:
            Adjusted datetime object (still with timezone info, but time value adjusted)
        """
        if dt is None:
            return None

        # Use instance-specific timezone offset
        adjusted = dt + timedelta(hours=self.utc_offset_hours)

        logger.debug(f"Original time: {dt.isoformat()}, Adjusted for storage: {adjusted.isoformat()} (timezone: {self.timezone})")
        return adjusted
    
    async def save_usage_data(self, model: str, input_tokens: int, output_tokens: int, 
                             response_time: int, timestamp=None, 
                             status: str = "success", error_message: str = None):
        """
        Save LLM usage data to MongoDB asynchronously
        
        Args:
            model: Name of the LLM model used
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens
            response_time: Response time in milliseconds
            timestamp: Optional timestamp (defaults to current time in configured timezone)
            status: Request status - "success", "timeout", or "error"
            error_message: Optional error details when status is "error"
            
        Returns:
            Boolean indicating success/failure
        """
        try:
            # Log the attempt to save data with detailed information
            logger.info(f"Attempting to save usage data for model {model} with status={status}, input_tokens={input_tokens}, output_tokens={output_tokens}")
            
            # Use current time in configured timezone if timestamp not provided
            if timestamp is None:
                timestamp = get_current_time(self.timezone)
            
            # Adjust the time value before storing
            adjusted_timestamp = self._adjust_time_for_storage(timestamp)
            
            logger.debug(f"Timestamp for usage data: {timestamp.isoformat()}, adjusted to: {adjusted_timestamp.isoformat()}")
            
            usage_data = {
                'timestamp': adjusted_timestamp,  # Store pre-adjusted time
                'model': model,
                'in': input_tokens,
                'out': output_tokens,
                'response_time': response_time,
                'status': status
            }
            
            # Add error message if provided
            if error_message and status == "error":
                usage_data['error_message'] = error_message
                logger.debug(f"Added error message to usage data: {error_message[:100]}...")
            
            logger.debug(f"Prepared usage data: {usage_data}")
            
            # Run MongoDB operation in a separate thread to avoid blocking
            logger.debug(f"Executing MongoDB insert operation for collection: {MONGODB_USAGE_COLLECTION}")
            loop = asyncio.get_event_loop()
            insert_result = await loop.run_in_executor(None, lambda: self.usage.insert_one(usage_data))
            
            # Log the MongoDB operation result
            logger.info(f"MongoDB insert result: {insert_result.inserted_id}")
            logger.info(f"Successfully saved usage data for model {model}: status={status}, {input_tokens} in, {output_tokens} out, {response_time}ms")
            return True
        except Exception as e:
            logger.error(f"Error saving usage data: {str(e)}")
            
            # More detailed error logging
            import traceback
            logger.error(f"Exception traceback: {traceback.format_exc()}")
            
            # Try to log database connection status
            try:
                # Check if MongoDB is connected
                server_info = self.mongo_client.server_info()
                logger.error(f"MongoDB connection seems working, server info: {server_info.get('version', 'unknown')}")
            except Exception as conn_err:
                logger.error(f"MongoDB connection error: {str(conn_err)}")
            
            return False
            
    async def update_contact_info(self, user_id: int, contact_info: Dict[str, Any], username: str = None) -> bool:
        """
        Update contact information for an existing chat
        
        Args:
            user_id: User ID
            contact_info: Contact form data with fields:
                - name: Contact name (will be stored as messenger_name in DB)
                - phone: Contact phone number
            username: Username or document ID (for messengers: "tg_bot@username" or "whatsapp@phone")
            
        Returns:
            bool: Success status
        """
        try:
            # Get document ID - for messengers, username is already the full document ID
            if username:
                user_doc_id = username
            else:
                user_doc_id = str(user_id)
            
            # Get current time for created_at if needed
            current_time = get_current_time(self.timezone)
            adjusted_time = self._adjust_time_for_storage(current_time)
            
            # Prepare update document
            update_doc = {
                '$set': {
                    'messenger_name': contact_info.get('name'),
                    'phone': contact_info.get('phone')
                },
                '$setOnInsert': {
                    'created_at': adjusted_time  # Only set when creating new document
                }
            }
            
            # Run MongoDB operation with upsert to ensure created_at is set
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, lambda: self.chats.update_one(
                {'_id': user_doc_id},
                update_doc,
                upsert=True  # Create document if it doesn't exist
            ))
            
            if result.modified_count > 0:
                logger.info(f"Updated contact info for user {user_doc_id}")
                return True
            else:
                logger.warning(f"No document found to update contact info for user {user_doc_id}")
                return False
                
        except Exception as e:
            logger.error(f"Error updating contact info: {e}")
            return False

    async def save_message(self, user_id: int, role: str, text: str, username: str = None, contact_info: Dict[str, Any] = None):
        """
        Save message to MongoDB asynchronously
        
        Args:
            user_id: User ID (can be int or string for messenger compatibility)
            role: Message role (user, assistant, system, service)
            text: Message text
            username: Username or document ID (for messengers: "tg_bot@username" or "whatsapp@phone")
            contact_info: Optional contact form data with fields:
                - name: Contact name (will be stored as messenger_name in DB)
                - phone: Contact phone number
        """
        try:
            # Determine document ID - for messengers, username is already the full document ID
            if username:
                # For messenger nodes, username is already formatted as "interface_type@identifier"
                user_doc_id = username
            else:
                # Fallback to user_id if no username provided
                user_doc_id = str(user_id)
                logger.warning(f"No username provided for user_id {user_id}, using user_id as document ID")
            
            # Get current time and adjust for storage
            current_time = get_current_time(self.timezone)
            adjusted_time = self._adjust_time_for_storage(current_time)
            
            message_item = {
                'role': role,
                'text': text,
                'timestamp': adjusted_time  # Store pre-adjusted time
            }
            
            # Prepare update document
            update_doc = {
                '$set': {
                    'user_id': user_id,
                    'last_activity': adjusted_time  # Store pre-adjusted time
                },
                '$setOnInsert': {
                    'created_at': adjusted_time  # Only set when creating new document
                },
                '$push': {
                    'messages': message_item
                }
            }
            
            # If contact info is provided, add it to the document
            if contact_info:
                # Add messenger_name and phone
                update_doc['$set']['messenger_name'] = contact_info.get('name')
                update_doc['$set']['phone'] = contact_info.get('phone')
                
                # Add email if provided and mandatory
                if CONTACT_FORM_EMAIL_MANDATORY and contact_info.get('email'):
                    update_doc['$set']['email'] = contact_info.get('email')
                
                # Log contact info update
                logger.info(f"Updating contact info for user {user_doc_id}: messenger_name={contact_info.get('name')}, phone={contact_info.get('phone')}")
            
            # Run MongoDB operation in a separate thread to avoid blocking
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, lambda: self.chats.update_one(
                {'_id': user_doc_id},
                update_doc,
                upsert=True  # Create document if it doesn't exist
            ))
            
            truncated_text = text[:db_message_length_limiter]
            logger.info(f"Saved {role} message to MongoDB for user {user_doc_id}: {truncated_text}... (time: {current_time.isoformat()}, adjusted: {adjusted_time.isoformat()})") 
            return True
        except Exception as e:
            logger.error(f"MongoDB error: {e}")
            return False
    
    async def get_chat_history(self, username: str, n: int):
        """Retrieve chat history for a user from MongoDB"""
        try:
            # For messenger nodes, username is already the full document ID (e.g., "tg_bot@username")
            # For legacy support, add @ prefix if it's a simple username
            if username and '@' in username and not username.startswith('@'):
                # This is already a messenger document ID like "tg_bot@username"
                user_doc_id = username
            else:
                # Legacy format - add @ prefix
                user_doc_id = f"@{username}" if username and not username.startswith('@') else username
            
            loop = asyncio.get_event_loop()
            user_doc = await loop.run_in_executor(
                None, 
                lambda: self.chats.find_one({'_id': user_doc_id})
            )
            
            if user_doc and 'messages' in user_doc:
                # Get the last n messages
                messages = user_doc['messages'][-n:] if len(user_doc['messages']) > n else user_doc['messages']
                
                # Format messages for OpenAI chat completion
                formatted_messages = []
                for msg in messages:
                    # Map roles for OpenAI compatibility
                    role = msg['role']
                    if role == 'bot':
                        role = 'assistant'
                    elif role == 'user':
                        role = 'user'
                    elif role == 'admin':
                        # Keep admin role as is - it will be handled by display formatters
                        role = 'admin'
                    elif role == 'service':
                        # Keep service role as is - it will be handled by display formatters
                        role = 'service'
                    else:
                        logger.warning(f"Unknown role in message: {role}, keeping as is")
                    
                    formatted_messages.append({
                        'role': role,
                        'content': msg['text']
                    })
                
                # Log the formatted history for debugging
                logger.debug(f"Retrieved {len(formatted_messages)} messages for user {user_doc_id}")
                for i, msg in enumerate(formatted_messages):
                    logger.debug(f"Message {i+1}: Role={msg['role']}, Content={msg['content'][:100]}...")
                
                return formatted_messages
            
            logger.debug(f"No messages found for user {user_doc_id}")
            return []
        except Exception as e:
            logger.error(f"Error retrieving chat history: {e}")
            return []
    
    async def clear_collection(self):
        """Clear all documents from the collection"""
        try:
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(None, lambda: self.chats.delete_many({}))
            deleted_count = result.deleted_count
            logger.info(f"Cleared collection {db_collection_name}: {deleted_count} documents deleted")
            return deleted_count
        except Exception as e:
            logger.error(f"Error clearing collection: {e}")
            return 0

    async def get_all_chats(self):
        """Retrieve all chat documents from the collection"""
        try:
            loop = asyncio.get_event_loop()
            
            # Run MongoDB operation in a separate thread to avoid blocking
            chats_cursor = await loop.run_in_executor(
                None, 
                lambda: self.chats.find({})
            )
            
            # Convert cursor to list
            chats_list = await loop.run_in_executor(
                None,
                lambda: list(chats_cursor)
            )
            
            logger.info(f"Retrieved {len(chats_list)} chat documents from MongoDB")
            return chats_list
        except Exception as e:
            logger.error(f"Error retrieving all chats: {e}")
            return []

    async def get_chat_by_id(self, chat_id: str):
        """
        Get a specific chat by ID
        
        Args:
            chat_id: Chat identifier
            
        Returns:
            dict: Chat document or None if not found
        """
        try:
            # Convert to string to ensure compatibility
            chat_id_str = str(chat_id)
            
            # Correctly use self.chats instead of self.collection
            loop = asyncio.get_event_loop()
            chat = await loop.run_in_executor(
                None,
                lambda: self.chats.find_one({"_id": chat_id_str})
            )
            
            if chat:
                logger.debug(f"Retrieved chat document for ID {chat_id_str}")
            else:
                logger.warning(f"No chat document found for ID {chat_id_str}")
                
            return chat
        except Exception as e:
            logger.error(f"Error getting chat by ID {chat_id}: {str(e)}")
            return None

    def close(self):
        """Close MongoDB connection"""
        try:
            # Remove from global instances list
            global _db_instances
            if self in _db_instances:
                _db_instances.remove(self)
                logger.debug(f"Removed DatabaseManager instance from global tracking (remaining: {len(_db_instances)})")
            
            # AI client shutdown notification removed (was legacy from another project)
            
            # Close the connection
            self.mongo_client.close()
            logger.info("MongoDB connection closed")
        except Exception as e:
            logger.error(f"Error during database shutdown: {e}")

def close_all_connections():
    """Close all active database connections"""
    global _db_instances
    logger.info(f"Closing all database connections ({len(_db_instances)} active)...")
    
    for db_instance in list(_db_instances):  # Use a copy of the list since we're modifying it
        try:
            db_instance.close()
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")
    
    _db_instances.clear()
    logger.info("All database connections closed")