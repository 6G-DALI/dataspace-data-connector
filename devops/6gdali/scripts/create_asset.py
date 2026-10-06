import sys
import os
from pathlib import Path

import provider_helper as provider

# Load environment variables from .env file
try:
    from dotenv import load_dotenv

    # Load .env file from the same directory as this script
    env_path = Path(__file__).parent / '.env'
    load_dotenv(dotenv_path=env_path)
    print(f"Loaded configuration from: {env_path}")
except ImportError:
    print("Warning: python-dotenv not installed. Using environment variables or defaults.")
    print("Install with: pip install python-dotenv")

# Provider configuration
PROVIDER_DOMAIN = os.getenv('PROVIDER_DOMAIN')
PROVIDER_HTTP_PORT = int(os.getenv('PROVIDER_HTTP_PORT'))
PROVIDER_HTTP_MANAGEMENT_PORT = int(os.getenv('PROVIDER_HTTP_MANAGEMENT_PORT'))
PROVIDER_HTTP_PROTOCOL_PORT = int(os.getenv('PROVIDER_HTTP_PROTOCOL_PORT'))
PROVIDER_HTTP_CONTROL_PORT = int(os.getenv('PROVIDER_HTTP_CONTROL_PORT'))

provider_helper = provider.ProviderHelper(
    provider_base_url=PROVIDER_DOMAIN,
    provider_http_port=PROVIDER_HTTP_PORT,
    provider_http_management_port=PROVIDER_HTTP_MANAGEMENT_PORT,
    provider_http_protocol_port=PROVIDER_HTTP_PROTOCOL_PORT,
    provider_http_control_port=PROVIDER_HTTP_CONTROL_PORT
)

# Asset configuration
asset_name = os.getenv('ASSET_NAME')

data_address = {
    "type": "6GDaliTestbedExperiments",
    "endpoint": os.getenv('MINIO_ENDPOINT'),
    "bucketName": os.getenv('MINIO_BUCKET_NAME'),
    "accessKey": os.getenv('MINIO_ACCESS_KEY'),
    "secretKey": os.getenv('MINIO_SECRET_KEY'),
    "prefix": os.getenv('MINIO_PREFIX', '')
}

# Validate required credentials
if not data_address['accessKey'] or not data_address['secretKey']:
    raise ValueError(
        "MinIO credentials not found! Please set MINIO_ACCESS_KEY and MINIO_SECRET_KEY "
        "in .env file or environment variables."
    )

# ASSET CREATION
a = provider_helper.create_asset(asset_name, data_address=data_address)
print(f"response: {a}")

a = provider_helper.define_policy()
print(f"response: {a}")

a = provider_helper.create_contract_definitions()
print(f"response: {a}")
