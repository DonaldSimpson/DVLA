#!/bin/bash

# Test script for MOT Delta Loader cron setup

# This script tests the environment and dependencies without running the full loader

set -euo pipefail

SCRIPT_DIR="/home/don/workspaces/mot_scripts"
DATE=$(date '+%Y-%m-%d %H:%M:%S')

echo "[$DATE] Testing MOT Delta Loader cron setup..."

# Test 1: Check directory
cd "$SCRIPT_DIR"
echo "[$DATE] ✅ Directory access: $(pwd)"

# Test 2: Check envsetup.sh
if [ -f "./envsetup.sh" ]; then
    source ./envsetup.sh
    echo "[$DATE] ✅ Environment variables loaded"
    echo "[$DATE]   - DB_HOST: ${DB_HOST:-'NOT SET'}"
    echo "[$DATE]   - DB_USER: ${DB_USER:-'NOT SET'}"
    echo "[$DATE]   - DVLA_CLIENT_ID: ${DVLA_CLIENT_ID:-'NOT SET'}"
else
    echo "[$DATE] ❌ envsetup.sh not found"
    exit 1
fi

# Test 3: Check Python3
if command -v python3 &> /dev/null; then
    echo "[$DATE] ✅ Python3 available: $(python3 --version)"
else
    echo "[$DATE] ❌ Python3 not found"
    exit 1
fi

# Test 4: Test virtual environment creation
if [ ! -d "venv" ]; then
    echo "[$DATE] Creating test virtual environment..."
    python3 -m venv venv
fi

source venv/bin/activate
echo "[$DATE] ✅ Virtual environment activated"

# Test 5: Test package installation
pip install --upgrade pip --quiet
pip install mysql-connector-python requests --quiet
echo "[$DATE] ✅ Python packages installed"

# Test 6: Check if delta_loader.py exists
if [ -f "delta_loader.py" ]; then
    echo "[$DATE] ✅ delta_loader.py found"
else
    echo "[$DATE] ❌ delta_loader.py not found"
    exit 1
fi

# Test 7: Test Python imports
python3 -c "
import mysql.connector
import requests
import os
print('[$DATE] ✅ Python imports successful')
"

deactivate
echo "[$DATE] ✅ Virtual environment deactivated"

echo "[$DATE] 🎉 All tests passed! Cron setup is ready."
