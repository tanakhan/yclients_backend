#!/bin/bash
# YCLIENTS Full Sync Cron Script
# This script can be scheduled in crontab to run the full sync automatically

# Change to the project directory (adjust path as needed)
cd /Users/baxter/projects/06_business/129_yclients_backend

# Activate virtual environment and run the full sync script
venv/bin/python3 yclients_full_sync.py >> /var/log/yclients_sync.log 2>&1

# Optional: Send email notification on failure (uncomment and configure)
# if [ $? -ne 0 ]; then
#     echo "YCLIENTS sync failed at $(date)" | mail -s "YCLIENTS Sync Error" your-email@example.com
# fi