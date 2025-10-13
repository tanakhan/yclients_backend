# Configuration for YCLIENTS backend
import os

# Load environment variables from .env file if it exists
def load_env_file():
    """Load environment variables from .env file"""
    env_file = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_file):
        with open(env_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip().strip('"').strip("'")

load_env_file()

# Mode detection from .env (check first letter: 'p' for production, 'd' for dev)
MODE = os.getenv('MODE', 'dev').lower()
IS_PRODUCTION = MODE.startswith('p')

# MongoDB connection configuration
if IS_PRODUCTION:
    MONGODB_CONNECTION_STRING = os.getenv('MONGODB_PRODUCTION_STRING')
    if not MONGODB_CONNECTION_STRING:
        raise ValueError("MONGODB_PRODUCTION_STRING not found in .env for production mode")
else:
    # Default development connection (existing way)
    MONGODB_CONNECTION_STRING = "mongodb://localhost:27017"

# YCLIENTS timeout configurations
YCLIENTS_TIMEOUT = 10
YCLIENTS_MAX_RETRIES = 3
YCLIENTS_BACKOFF_FACTOR = 0.5