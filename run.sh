#!/bin/bash

# Set the script to exit immediately if a command exits with a non-zero status
set -e

# Install uv using pip
echo "Installing uv..."
python -m pip install --upgrade pip
pip install uv

# Install dependencies from requirements.txt using uv
echo "Installing dependencies with uv..."
uv pip install -r requirements.txt

echo "Starting Streamlit on PORT=$PORT"
python -m streamlit run app.py --server.port=$PORT --server.address 0.0.0.0