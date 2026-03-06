# Direct Snowflake to HubSpot Pipeline - Quick Setup

## 🚀 Quick Start (No Transformations)

Your data flows directly from Snowflake → HubSpot without any cleaning or transformations.

### 1. Setup Environment

```bash
cp .env.template .env
# Edit .env with your credentials
```

### 2. Configure Your Tables

Edit the pipeline file to specify your Snowflake table names:

**minimal_pipeline.py:**
```python
tables_to_sync = [
    "your_contacts_table",
    "your_deals_table", 
    "your_companies_table"
]
```

### 3. Run the Pipeline

```bash
# Sync all tables
python minimal_pipeline.py

# Sync specific table  
python minimal_pipeline.py contacts
python minimal_pipeline.py deals
```

### 4. Required Environment Variables

```bash
# Snowflake (RSA Key Authentication)
SNOWFLAKE_ACCOUNT=your_account.region.snowflakecomputing.com
SNOWFLAKE_USER=your_username
# No password - using rsa_key.p8 file for authentication
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_SCHEMA=your_schema
SNOWFLAKE_ROLE=your_role

# HubSpot
HUBSPOT_API_KEY=your_hubspot_private_app_token
```

**Note:** Make sure `rsa_key.p8` file is in the project root directory.

### 5. HubSpot Requirements

- Create a Private App in HubSpot
- Grant required scopes: `crm.objects.contacts.write`, `crm.objects.deals.write`, etc.
- Your Snowflake table columns should match HubSpot property names

## ✨ That's it!

Your data will flow directly from Snowflake tables to HubSpot objects without any modifications.