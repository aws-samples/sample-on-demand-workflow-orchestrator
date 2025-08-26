#!/bin/bash

# Copy library files for Docker Image Building
cp -r lib/python/* src/functions/execute_agent/

# Run CDK deployment
npx cdk deploy --all --require-approval never

# Clean up copied library files
find src/functions/execute_agent/ -maxdepth 1 -type f ! -name '__init__.py' ! -name 'index.py' ! -name 'requirements.txt' ! -name 'Dockerfile' -exec rm -v {} +