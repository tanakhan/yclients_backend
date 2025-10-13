# Configuration for YCLIENTS backend

# Database settings
db_name = "yclients_db"
db_collection_name = "chats"
MONGODB_USAGE_COLLECTION = "usage"
logger_base_name = "yclients"

# Timezone settings
TIMEZONE = "Asia/Bangkok"

# YCLIENTS API settings - Replace with actual values
BOOKING_FORMS = [12345, 67890]  # List of booking form IDs
YCLIENTS_PARTNER_TOKEN = "your_partner_token_here"
YCLIENTS_USER_TOKEN = "your_user_token_here"  # Optional
YCLIENTS_TIMEOUT = 10  # seconds
YCLIENTS_MAX_RETRIES = 3
YCLIENTS_BACKOFF_FACTOR = 2