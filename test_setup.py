#!/usr/bin/env python3
"""
Test script for DLT Hub Pipeline: Snowflake to HubSpot
Validates configuration and connections before running the full pipeline
"""

import os
import sys
from dotenv import load_dotenv
import dlt
from typing import Dict, Any

# Load environment variables
load_dotenv()


def test_environment_variables():
    """Test that all required environment variables are set"""
    print("🔍 Testing environment variables...")
    
    required_vars = {
        'SNOWFLAKE_ACCOUNT': 'Snowflake account URL',
        'SNOWFLAKE_USER': 'Snowflake username', 
        'SNOWFLAKE_DATABASE': 'Snowflake database name',
        'SNOWFLAKE_WAREHOUSE': 'Snowflake warehouse name',
        'HUBSPOT_API_KEY': 'HubSpot API key'
    }
    
    missing_vars = []
    for var, description in required_vars.items():
        value = os.getenv(var)
        if not value:
            missing_vars.append(f"{var} ({description})")
            print(f"❌ Missing: {var}")
        else:
            # Mask sensitive values
            if 'KEY' in var:
                masked_value = f"{value[:4]}{'*' * (len(value) - 4)}"
                print(f"✅ {var}: {masked_value}")
            else:
                print(f"✅ {var}: {value}")
    
    # Check if RSA key file exists
    if os.path.exists("rsa_key.p8"):
        print("✅ RSA_KEY_FILE: rsa_key.p8 found")
    else:
        missing_vars.append("RSA_KEY_FILE (rsa_key.p8 file in project root)")
        print("❌ Missing: rsa_key.p8 file")
    
    if missing_vars:
        print(f"\n❌ Environment test failed. Missing variables:")
        for var in missing_vars:
            print(f"   - {var}")
        return False
    
    print("✅ Environment variables test passed!")
    return True


def test_snowflake_connection():
    """Test Snowflake connection"""
    print("\n❄️ Testing Snowflake connection...")
    
    try:
        from dlt.sources.sql_database import sql_database
        from cryptography.hazmat.primitives import serialization
        
        # Read RSA private key
        with open("rsa_key.p8", "rb") as key_file:
            p_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,  # Use password if key is encrypted
            )
        
        credentials = {
            "drivername": "snowflake",
            "host": os.getenv("SNOWFLAKE_ACCOUNT"),
            "username": os.getenv("SNOWFLAKE_USER"),
            "private_key": p_key,  # Use private key instead of password
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
            "role": os.getenv("SNOWFLAKE_ROLE", "PUBLIC"),
        }
        
        # Test connection by creating a simple query
        source = sql_database(
            credentials=credentials, 
            schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
        )
        
        # Try to execute a simple query
        with source.engine.connect() as connection:
            result = connection.execute("SELECT CURRENT_TIMESTAMP() as test_time").fetchone()
            print(f"✅ Snowflake connection successful! Server time: {result[0]}")
            return True
            
    except FileNotFoundError:
        print("❌ RSA key file 'rsa_key.p8' not found in current directory")
        return False
    except Exception as e:
        print(f"❌ Snowflake connection failed: {str(e)}")
        return False


def test_hubspot_connection():
    """Test HubSpot API connection"""
    print("\n📞 Testing HubSpot connection...")
    
    try:
        import requests
        
        api_key = os.getenv("HUBSPOT_API_KEY")
        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
        
        # Test API access with account info endpoint
        response = requests.get(
            'https://api.hubapi.com/integrations/v1/me',
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ HubSpot connection successful!")
            print(f"   App ID: {data.get('appId', 'N/A')}")
            print(f"   Hub ID: {data.get('hubId', 'N/A')}")
            return True
        else:
            print(f"❌ HubSpot API error: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ HubSpot connection failed: {str(e)}")
        return False


def test_dlt_configuration():
    """Test DLT configuration"""
    print("\n⚙️ Testing DLT configuration...")
    
    try:
        # Create a test pipeline
        pipeline = dlt.pipeline(
            pipeline_name="test_pipeline",
            destination="duckdb",  # Use duckdb for testing
            dataset_name="test_dataset",
            dev_mode=True
        )
        
        print(f"✅ DLT pipeline created successfully!")
        print(f"   Pipeline name: {pipeline.pipeline_name}")
        print(f"   Destination: {pipeline.destination}")
        print(f"   Dataset: {pipeline.dataset_name}")
        return True
        
    except Exception as e:
        print(f"❌ DLT configuration failed: {str(e)}")
        return False


def test_pipeline_modules():
    """Test pipeline module imports"""
    print("\n📦 Testing pipeline modules...")
    
    success = True
    
    # Test minimal pipeline module  
    try:
        import minimal_pipeline
        print("✅ minimal_pipeline module imported successfully")
    except Exception as e:
        print(f"❌ Failed to import minimal_pipeline: {str(e)}")
        success = False
    
    return success


def test_direct_sync_setup():
    """Test direct sync configuration"""
    print("\n🔄 Testing direct sync setup...")
    
    try:
        from minimal_pipeline import get_snowflake_credentials
        
        # Test credentials function
        credentials = get_snowflake_credentials()
        required_fields = ['drivername', 'host', 'username', 'private_key', 'database', 'warehouse']
        
        missing_fields = [field for field in required_fields if not credentials.get(field)]
        
        if missing_fields:
            print(f"❌ Missing credential fields: {missing_fields}")
            return False
        
        print("✅ Direct sync configuration is valid!")
        print(f"   Database: {credentials.get('database')}")
        print(f"   Warehouse: {credentials.get('warehouse')}")
        print(f"   Authentication: RSA Key")
        return True
        
    except Exception as e:
        print(f"❌ Direct sync setup test failed: {str(e)}")
        return False


def run_full_test():
    """Run all tests"""
    print("🧪 Starting DLT Hub Pipeline Tests (Direct Sync)\n")
    print("=" * 50)
    
    tests = [
        ("Environment Variables", test_environment_variables),
        ("Pipeline Modules", test_pipeline_modules), 
        ("Direct Sync Setup", test_direct_sync_setup),
        ("DLT Configuration", test_dlt_configuration),
        ("Snowflake Connection", test_snowflake_connection),
        ("HubSpot Connection", test_hubspot_connection),
    ]
    
    results = {}
    for test_name, test_func in tests:
        results[test_name] = test_func()
        print()  # Add spacing between tests
    
    # Summary
    print("=" * 50)
    print("📊 Test Results Summary:")
    print("=" * 50)
    
    passed = sum(results.values())
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name:<25} {status}")
    
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Your direct sync pipeline is ready to run.")
        return True
    else:
        print(f"\n⚠️ {total - passed} test(s) failed. Please fix the issues before running the pipeline.")
        return False


if __name__ == "__main__":
    
    if len(sys.argv) > 1:
        test_name = sys.argv[1].lower()
        
        test_map = {
            'env': test_environment_variables,
            'snowflake': test_snowflake_connection,
            'hubspot': test_hubspot_connection,
            'dlt': test_dlt_configuration,
            'modules': test_pipeline_modules,
            'setup': test_direct_sync_setup
        }
        
        if test_name in test_map:
            test_map[test_name]()
        else:
            print("Available tests: env, snowflake, hubspot, dlt, modules, setup")
    else:
        run_full_test()