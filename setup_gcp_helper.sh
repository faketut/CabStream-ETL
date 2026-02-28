#!/bin/bash

# Configuration
PROJECT_ID="ringed-spirit-458615-p9"
CREDENTIALS_PATH="$(pwd)/credentials.json"

# Check if terraform is installed
if ! command -v terraform &> /dev/null
then
    echo "Terraform not found. Please install it first."
    exit 1
fi

# Set environment variables
export GOOGLE_APPLICATION_CREDENTIALS=$CREDENTIALS_PATH

# Change to terraform directory
cd nyc_taxi_pipeline/terraform

# Initialize
terraform init

# Apply
terraform apply -var="project_id=$PROJECT_ID"
