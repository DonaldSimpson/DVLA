#!/bin/bash

# MOT Delta Loader Cron Script

# This script sets up the environment and runs delta_loader.py nightly

# Set strict error handling
set -euo pipefail

# Define script directory and log file
SCRIPT_DIR="/home/don/workspaces/mot_scripts"
LOG_FILE="/home/don/workspaces/mot_scripts/logs/delta_loader_cron.log"
DATE=$(date '+%Y-%m-%d %H:%M:%S')

# Create logs directory if it doesn't exist
mkdir -p "$(dirname "$LOG_FILE")"

# Function to log messages
log_message() {
    echo "[$DATE] $1" >> "$LOG_FILE"
    echo "[$DATE] $1"
}

# Function to handle errors
handle_error() {
    log_message "ERROR: Script failed at line $1"
    exit 1
}

# Set up error trap
trap 'handle_error $LINENO' ERR

# Start logging
log_message "Starting MOT Delta Loader cron job"

# Change to script directory
cd "$SCRIPT_DIR"
log_message "Changed to directory: $(pwd)"

# Source environment variables
if [ -f "./envsetup.sh" ]; then
    source ./envsetup.sh
    log_message "Sourced environment variables from envsetup.sh"
else
    log_message "ERROR: envsetup.sh not found in $SCRIPT_DIR"
    exit 1
fi

# Check if virtual environment exists, if not create it
if [ ! -d "venv" ]; then
    log_message "Creating virtual environment..."
    python3 -m venv venv
    log_message "Virtual environment created"
fi

# Activate virtual environment
source venv/bin/activate
log_message "Activated virtual environment"

# Upgrade pip and install/upgrade required packages
log_message "Installing/updating Python packages..."
pip install --upgrade pip --quiet
pip install --upgrade mysql-connector-python requests --quiet
log_message "Python packages installed/updated"

# Check if delta_loader.py exists
if [ ! -f "delta_loader.py" ]; then
    log_message "ERROR: delta_loader.py not found in $SCRIPT_DIR"
    exit 1
fi

# Run delta_loader.py
log_message "Starting delta_loader.py execution..."
python delta_loader.py

# Check exit status
if [ $? -eq 0 ]; then
    log_message "Delta loader completed successfully"
else
    log_message "ERROR: Delta loader failed with exit code $?"
    exit 1
fi

# Deactivate virtual environment
deactivate
log_message "Deactivated virtual environment"

log_message "MOT Delta Loader cron job completed successfully"

# Optional: Clean up old log files (keep last 30 days)
find "$(dirname "$LOG_FILE")" -name "delta_loader_cron.log.*" -mtime +30 -delete 2>/dev/null || true

# Rotate log file if it gets too large (>10MB)
if [ -f "$LOG_FILE" ] && [ $(stat -f%z "$LOG_FILE" 2>/dev/null || stat -c%s "$LOG_FILE" 2>/dev/null || echo 0) -gt 10485760 ]; then
    mv "$LOG_FILE" "${LOG_FILE}.$(date +%Y%m%d_%H%M%S)"
    log_message "Log file rotated due to size"
fi
