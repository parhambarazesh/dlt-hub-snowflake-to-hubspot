#!/usr/bin/env python3
"""
Enhanced DLT Pipeline: Snowflake to HubSpot
Direct connection: Snowflake → HubSpot API
"""

import os
import requests
import json
from typing import Dict, List, Any
from dotenv import load_dotenv

load_dotenv()


def extract_from_snowflake(table_name: str):
    """Extract data from Snowflake table using direct connection"""
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization
    
    print(f"📤 Extracting data from Snowflake table: {table_name}")
    
    # Read RSA private key
    with open("rsa_key.p8", "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
        )
    
    # Create direct Snowflake connection 
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=private_key,
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE", "PUBLIC")
    )
    
    cursor = conn.cursor()
    
    # Query the table
    query = f"SELECT * FROM {table_name} LIMIT 1000"  # Limit to avoid huge datasets
    cursor.execute(query)
    
    # Get column names
    columns = [desc[0] for desc in cursor.description]
    
    # Fetch all data
    rows = cursor.fetchall()
    
    # Convert to list of dictionaries
    data = [dict(zip(columns, row)) for row in rows]
    
    cursor.close()
    conn.close()
    
    print(f"✅ Extracted {len(data)} records from {table_name}")
    return data


def list_available_tables():
    """List all tables available in the current Snowflake database/schema"""
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization
    
    print("📋 Discovering available tables in Snowflake...")
    
    # Read RSA private key
    with open("rsa_key.p8", "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
        )
    
    # Create Snowflake connection
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=private_key,
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=os.getenv("SNOWFLAKE_ROLE", "PUBLIC")
    )
    
    cursor = conn.cursor()
    
    # Show current context
    print(f"📍 Current connection context:")
    print(f"   Database: {os.getenv('SNOWFLAKE_DATABASE')}")
    print(f"   Schema: {os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')}")
    print(f"   Role: {os.getenv('SNOWFLAKE_ROLE', 'PUBLIC')}")
    
    # List available schemas
    print("\n📂 Available schemas:")
    try:
        cursor.execute("SHOW SCHEMAS")
        schemas = cursor.fetchall()
        for i, schema in enumerate(schemas, 1):
            schema_name = schema[1]  # Schema name is in the second column
            print(f"  {i:2}. {schema_name}")
    except Exception as e:
        print(f"   ❌ Could not list schemas: {e}")
    
    # Try to find tables in current schema
    print(f"\n📋 Tables in current schema ({os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC')}):")
    try:
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        if tables:
            print(f"   Found {len(tables)} tables:")
            for i, table in enumerate(tables, 1):
                table_name = table[1]  # Table name is in the second column
                print(f"     {i:2}. {table_name}")
        else:
            print("   ⚠️ No tables found in current schema")
    except Exception as e:
        print(f"   ❌ Could not list tables: {e}")
    
    # Try to find tables across all schemas
    print(f"\n🔍 Searching for tables in all schemas in database {os.getenv('SNOWFLAKE_DATABASE')}...")
    try:
        query = f"""
        SELECT table_schema, table_name, table_type 
        FROM {os.getenv('SNOWFLAKE_DATABASE')}.information_schema.tables 
        WHERE table_catalog = '{os.getenv('SNOWFLAKE_DATABASE')}'
        ORDER BY table_schema, table_name
        LIMIT 50
        """
        cursor.execute(query)
        all_tables = cursor.fetchall()
        
        if all_tables:
            print(f"   Found {len(all_tables)} tables across all schemas:")
            current_schema = None
            for table in all_tables:
                schema_name, table_name, table_type = table
                if schema_name != current_schema:
                    print(f"\n   📁 Schema: {schema_name}")
                    current_schema = schema_name
                print(f"      • {table_name} ({table_type})")
        else:
            print("   ⚠️ No tables found in any schema")
            
    except Exception as e:
        print(f"   ❌ Could not search all schemas: {e}")
    
    cursor.close()
    conn.close()
    
    print(f"\n💡 To sync a table from a different schema, update SNOWFLAKE_SCHEMA in your .env file")
    print(f"   or use the full table name: schema_name.table_name")


def send_to_hubspot_contacts(contacts_data: List[Dict[str, Any]]):
    """Send contact data to HubSpot using optimized batch processing"""
    print(f"📞 Sending {len(contacts_data)} contacts to HubSpot using batch processing...")
    
    api_key = os.getenv("HUBSPOT_API_KEY")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # Step 1: Batch search for existing contacts by email
    print(f"🔍 Step 1: Batch searching for existing contacts...")
    emails_to_search = [contact.get("EMAIL") for contact in contacts_data if contact.get("EMAIL")]
    
    # Handle case where there are no emails
    if not emails_to_search:
        print("   ❌ No valid email addresses found in contact data")
        return
    
    existing_contacts = {}
    batch_size = 100  # HubSpot limit
    
    for i in range(0, len(emails_to_search), batch_size):
        batch_emails = emails_to_search[i:i+batch_size]
        
        # Use batch search API
        search_payload = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "email",
                    "operator": "IN",
                    "values": batch_emails
                }]
            }],
            "properties": ["email"],
            "limit": 100
        }
        
        try:
            search_url = "https://api.hubapi.com/crm/v3/objects/contacts/search"
            response = requests.post(search_url, headers=headers, json=search_payload, timeout=30)
            
            if response.status_code == 200:
                results = response.json().get("results", [])
                for result in results:
                    email = result["properties"].get("email")
                    if email:
                        existing_contacts[email] = result["id"]
                print(f"   Found {len(results)} existing contacts in batch {i//batch_size + 1}")
            else:
                print(f"❌ Batch search failed: {response.status_code} - {response.text[:200]}")
        except Exception as e:
            print(f"❌ Error in batch search: {str(e)}")
    
    print(f"📋 Found {len(existing_contacts)} existing contacts out of {len(contacts_data)}")
    
    # Step 2: Separate existing and new contacts
    contacts_to_update = []
    contacts_to_create = []
    
    for contact in contacts_data:
        email = contact.get("EMAIL")
        if not email:
            continue
            
        hubspot_properties = {
            "email": email,
            "firstname": contact.get("FIRSTNAME"),
            "lastname": contact.get("LASTNAME"), 
            "company": contact.get("COMPANY"),
            "phone": contact.get("PHONE"),
            "mobilephone": contact.get("MOBILEPHONE"),
            "jobtitle": contact.get("JOBTITLE"),
            "website": contact.get("WEBSITE"),
            "city": contact.get("CITY"),
            "state": contact.get("STATE"),
            "country": contact.get("COUNTRY"),
            "zip": contact.get("ZIP"),
            "lifecyclestage": contact.get("LIFECYCLESTAGE"),
            "hubspotscore": contact.get("HUBSPOTSCORE"),
        }
        # Remove None values and convert to strings
        hubspot_properties = {k: str(v) if v is not None else "" for k, v in hubspot_properties.items() if v is not None}
        
        if email in existing_contacts:
            contacts_to_update.append({
                "id": existing_contacts[email],
                "properties": hubspot_properties
            })
        else:
            contacts_to_create.append({
                "properties": hubspot_properties
            })
    
    success_count = 0
    updated_count = 0
    created_count = 0
    
    # Step 3: Batch update existing contacts
    if contacts_to_update:
        print(f"🔄 Step 3: Batch updating {len(contacts_to_update)} existing contacts...")
        for i in range(0, len(contacts_to_update), batch_size):
            batch = contacts_to_update[i:i+batch_size]
            
            try:
                update_payload = {"inputs": batch}
                update_url = "https://api.hubapi.com/crm/v3/objects/contacts/batch/update"
                response = requests.post(update_url, headers=headers, json=update_payload, timeout=30)
                
                if response.status_code == 200:
                    batch_results = response.json().get("results", [])
                    batch_success = len(batch_results)
                    success_count += batch_success
                    updated_count += batch_success
                    print(f"   ✅ Batch {i//batch_size + 1}: Updated {batch_success}/{len(batch)} contacts")
                else:
                    print(f"   ❌ Batch {i//batch_size + 1} update failed: {response.status_code} - {response.text[:200]}")
            except Exception as e:
                print(f"   ❌ Error updating batch {i//batch_size + 1}: {str(e)}")
    
    # Step 4: Batch create new contacts
    if contacts_to_create:
        print(f"➕ Step 4: Batch creating {len(contacts_to_create)} new contacts...")
        for i in range(0, len(contacts_to_create), batch_size):
            batch = contacts_to_create[i:i+batch_size]
            
            try:
                create_payload = {"inputs": batch}
                create_url = "https://api.hubapi.com/crm/v3/objects/contacts/batch/create"
                response = requests.post(create_url, headers=headers, json=create_payload, timeout=30)
                
                if response.status_code == 201:
                    batch_results = response.json().get("results", [])
                    batch_success = len(batch_results)
                    success_count += batch_success
                    created_count += batch_success
                    print(f"   ✅ Batch {i//batch_size + 1}: Created {batch_success}/{len(batch)} contacts")
                else:
                    print(f"   ❌ Batch {i//batch_size + 1} create failed: {response.status_code} - {response.text[:200]}")
            except Exception as e:
                print(f"   ❌ Error creating batch {i//batch_size + 1}: {str(e)}")
    
    print(f"📊 HubSpot contacts sync completed:")
    print(f"   ✅ Total successful: {success_count}/{len(contacts_data)}")
    print(f"   🔄 Updated existing: {updated_count}")
    print(f"   ➕ Created new: {created_count}")


def send_to_hubspot_deals(deals_data: List[Dict[str, Any]]):
    """Send deal data to HubSpot using API"""
    print(f"💰 Sending {len(deals_data)} deals to HubSpot...")
    
    api_key = os.getenv("HUBSPOT_API_KEY")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    url = "https://api.hubapi.com/crm/v3/objects/deals/batch"
    
    # Prepare batch payload for HubSpot
    inputs = []
    for deal in deals_data:
        # Map your Snowflake columns to HubSpot properties  
        hubspot_deal = {
            "properties": {
                "dealname": deal.get("dealname") or deal.get("deal_name") or f"Deal_{deal.get('id', 'unknown')}",
                "amount": deal.get("amount") or deal.get("deal_amount"),
                "closedate": deal.get("closedate") or deal.get("close_date"),
                "dealstage": deal.get("dealstage") or deal.get("deal_stage", "appointmentscheduled"),
                # Add more field mappings based on your Snowflake table structure
            }
        }
        # Remove None values and convert dates to HubSpot timestamp format
        hubspot_deal["properties"] = {k: v for k, v in hubspot_deal["properties"].items() if v is not None}
        inputs.append(hubspot_deal)
    
    # Send in batches of 100 (HubSpot limit)
    batch_size = 100
    success_count = 0
    
    for i in range(0, len(inputs), batch_size):
        batch = inputs[i:i+batch_size]
        payload = {"inputs": batch}
        
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 201:
                batch_results = response.json()
                success_count += len(batch_results.get("results", []))
                print(f"✅ Batch {i//batch_size + 1}: {len(batch)} deals sent successfully")
            else:
                print(f"❌ Batch {i//batch_size + 1} failed: {response.status_code} - {response.text[:200]}...")
                
        except Exception as e:
            print(f"❌ Batch {i//batch_size + 1} error: {str(e)}")
    
    print(f"📊 HubSpot deals sync completed: {success_count}/{len(deals_data)} successful")


def send_to_hubspot_companies(companies_data: List[Dict[str, Any]]):
    """Send company data to HubSpot using optimized batch processing"""
    print(f"🏢 Sending {len(companies_data)} companies to HubSpot using batch processing...")
    
    api_key = os.getenv("HUBSPOT_API_KEY")
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    # Step 1: Batch search for existing companies by name
    print(f"🔍 Step 1: Batch searching for existing companies...")
    company_names_to_search = [company.get("NAME") for company in companies_data if company.get("NAME")]
    
    # Handle case where there are no company names
    if not company_names_to_search:
        print("   ❌ No valid company names found in company data")
        return
    
    existing_companies = {}
    batch_size = 100  # HubSpot limit
    
    for i in range(0, len(company_names_to_search), batch_size):
        batch_names = company_names_to_search[i:i+batch_size]
        
        # Use batch search API
        search_payload = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "name",
                    "operator": "IN",
                    "values": batch_names
                }]
            }],
            "properties": ["name"],
            "limit": 100
        }
        
        try:
            search_url = "https://api.hubapi.com/crm/v3/objects/companies/search"
            response = requests.post(search_url, headers=headers, json=search_payload, timeout=30)
            
            if response.status_code == 200:
                results = response.json().get("results", [])
                for result in results:
                    name = result["properties"].get("name")
                    if name:
                        existing_companies[name] = result["id"]
                print(f"   Found {len(results)} existing companies in batch {i//batch_size + 1}")
            else:
                print(f"❌ Batch search failed: {response.status_code} - {response.text[:200]}")
        except Exception as e:
            print(f"❌ Error in batch search: {str(e)}")
    
    print(f"📋 Found {len(existing_companies)} existing companies out of {len(companies_data)}")
    
    # Step 2: Separate existing and new companies
    companies_to_update = []
    companies_to_create = []
    
    for company in companies_data:
        company_name = company.get("NAME")
        if not company_name:
            continue
            
        hubspot_properties = {
            "name": company_name,
            "domain": company.get("DOMAIN"), 
            "website": company.get("WEBSITE"),
            "phone": company.get("PHONE"),
            "city": company.get("CITY"),
            "state": company.get("STATE"),
            "country": company.get("COUNTRY"), 
            "zip": company.get("ZIP"),
            "industry": company.get("INDUSTRY"),
            "type": company.get("TYPE"),
            "numberofemployees": company.get("NUMBEROFEMPLOYEES"),
            "annualrevenue": company.get("ANNUALREVENUE"),
            "description": company.get("DESCRIPTION"),
        }
        # Remove None values and convert to strings
        hubspot_properties = {k: str(v) if v is not None else "" for k, v in hubspot_properties.items() if v is not None}
        
        if company_name in existing_companies:
            companies_to_update.append({
                "id": existing_companies[company_name],
                "properties": hubspot_properties
            })
        else:
            companies_to_create.append({
                "properties": hubspot_properties
            })
    
    success_count = 0
    updated_count = 0
    created_count = 0
    
    # Step 3: Batch update existing companies
    if companies_to_update:
        print(f"🔄 Step 3: Batch updating {len(companies_to_update)} existing companies...")
        for i in range(0, len(companies_to_update), batch_size):
            batch = companies_to_update[i:i+batch_size]
            
            try:
                update_payload = {"inputs": batch}
                update_url = "https://api.hubapi.com/crm/v3/objects/companies/batch/update"
                response = requests.post(update_url, headers=headers, json=update_payload, timeout=30)
                
                if response.status_code == 200:
                    batch_results = response.json().get("results", [])
                    batch_success = len(batch_results)
                    success_count += batch_success
                    updated_count += batch_success
                    print(f"   ✅ Batch {i//batch_size + 1}: Updated {batch_success}/{len(batch)} companies")
                else:
                    print(f"   ❌ Batch {i//batch_size + 1} update failed: {response.status_code} - {response.text[:200]}")
            except Exception as e:
                print(f"   ❌ Error updating batch {i//batch_size + 1}: {str(e)}")
    
    # Step 4: Batch create new companies
    if companies_to_create:
        print(f"➕ Step 4: Batch creating {len(companies_to_create)} new companies...")
        for i in range(0, len(companies_to_create), batch_size):
            batch = companies_to_create[i:i+batch_size]
            
            try:
                create_payload = {"inputs": batch}
                create_url = "https://api.hubapi.com/crm/v3/objects/companies/batch/create"
                response = requests.post(create_url, headers=headers, json=create_payload, timeout=30)
                
                if response.status_code == 201:
                    batch_results = response.json().get("results", [])
                    batch_success = len(batch_results)
                    success_count += batch_success
                    created_count += batch_success
                    print(f"   ✅ Batch {i//batch_size + 1}: Created {batch_success}/{len(batch)} companies")
                else:
                    print(f"   ❌ Batch {i//batch_size + 1} create failed: {response.status_code} - {response.text[:200]}")
            except Exception as e:
                print(f"   ❌ Error creating batch {i//batch_size + 1}: {str(e)}")
    
    print(f"📊 HubSpot companies sync completed:")
    print(f"   ✅ Total successful: {success_count}/{len(companies_data)}")
    print(f"   🔄 Updated existing: {updated_count}")
    print(f"   ➕ Created new: {created_count}")


def sync_table_to_hubspot(table_name: str):
    """Complete sync process: Snowflake → HubSpot API"""
    print(f"\n🔄 Starting sync for table: {table_name}")
    
    try:
        # Extract from Snowflake directly
        data = extract_from_snowflake(table_name)
        
        if not data:
            print(f"⚠️ No data found in table {table_name}")
            return False
        
        print(f"📊 Found {len(data)} records to sync")
        print(f"📋 Sample data preview: {data[0] if data else 'No data'}")
        
        # Send to HubSpot based on table type
        if 'CONTACTS' in table_name.upper() or table_name.lower() in ['contacts', 'contact']:
            send_to_hubspot_contacts(data)
        elif 'COMPANIES' in table_name.upper() or table_name.lower() in ['companies', 'company']:
            send_to_hubspot_companies(data) 
        elif table_name.lower() in ['deals', 'deal']:
            send_to_hubspot_deals(data)
        else:
            print(f"⚠️ Unknown table type: {table_name}. Please map it to contacts, deals, or companies.")
            print(f"Available columns: {list(data[0].keys()) if data else 'No columns'}")
            return False
        
        print(f"✅ {table_name}: Sync completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ {table_name}: FAILED - {str(e)}")
        import traceback
        print(f"📋 Full error: {traceback.format_exc()}")
        return False


def sync_all_tables():
    """Sync all specified tables"""
    
    # Define your table mappings here - GOLD schema tables
    tables_to_sync = [
        "HUBSPOT_CONTACTS_OUT_DT",      # Snowflake contacts table → HubSpot Contacts
        "HUBSPOT_COMPANIES_OUT_DT",     # Snowflake companies table → HubSpot Companies
    ]
    
    print("🚀 Starting Snowflake to HubSpot sync process...")
    print(f"📋 Tables to sync from GOLD schema: {', '.join(tables_to_sync)}")
    
    results = {}
    for table in tables_to_sync:
        results[table] = sync_table_to_hubspot(table)
    
    # Summary
    print("\n" + "="*50)
    print("📊 SYNC SUMMARY")
    print("="*50)
    for table, success in results.items():
        status = "✅ SUCCESS" if success else "❌ FAILED"
        print(f"{table:<25} {status}")
    
    successful = sum(results.values())
    total = len(results)
    print(f"\nOverall: {successful}/{total} tables synced successfully")
    
    return results


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "list" or command == "tables":
            # List available tables: python minimal_pipeline.py list
            list_available_tables()
        else:
            # Sync specific table: python minimal_pipeline.py contacts
            sync_table_to_hubspot(command)
    else:
        # Sync all tables
        sync_all_tables()