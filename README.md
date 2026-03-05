# DLT Hub Pipeline: Snowflake to HubSpot (Direct Transfer)

A simple, clean data pipeline using [DLT Hub](https://dlthub.com/) to transfer data directly from **Snowflake** to **HubSpot** without any transformations.

## 🚀 Features

- **Direct Transfer**: Data flows from Snowflake to HubSpot as-is
- **Simple & Clean**: Minimal code, easy to understand and maintain
- **Flexible Control**: Sync all tables at once or individual tables
- **Easy Setup**: Just configure table names and credentials

## 📁 Project Structure

```
dlt-hub-snowflake-to-hubspot/
├── minimal_pipeline.py        # Main pipeline - individual or all table sync
├── test_setup.py             # Connection testing
├── .env.template             # Environment variables template
├── requirements.txt          # Python dependencies
├── QUICK_START.md           # Quick setup guide
├── README.md               # This file
└── venv/                   # Python virtual environment
```

## 🛠 Setup Instructions

### 1. Configure Your Tables

Edit the pipeline file to specify your Snowflake table names:

**minimal_pipeline.py:**
```python
tables_to_sync = [
    "contacts",      # Your Snowflake contacts table
    "deals",         # Your Snowflake deals table  
    "companies"      # Your Snowflake companies table
]
```

### 2. Environment Setup

```bash
# Copy environment template
cp .env.template .env

# Edit .env file with your credentials
nano .env

# Ensure your RSA private key is in the project root
# The file should be named: rsa_key.p8
```

### 3. Configure Environment Variables

Update the `.env` file with your credentials:

```bash
# Snowflake Configuration (RSA Key Authentication)
SNOWFLAKE_ACCOUNT=your_account.region.snowflakecomputing.com
SNOWFLAKE_USER=your_snowflake_username
# Note: No password needed - using RSA key from rsa_key.p8 file
SNOWFLAKE_DATABASE=your_database_name
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_WAREHOUSE=your_warehouse_name
SNOWFLAKE_ROLE=your_role_name

# HubSpot Configuration  
HUBSPOT_API_KEY=your_hubspot_private_app_token
```

**Important**: Make sure your `rsa_key.p8` file is in the project root directory.

### 4. HubSpot API Setup

1. Go to HubSpot Settings → Integrations → Private Apps
2. Create a new Private App
3. Grant the following scopes:
   - `crm.objects.contacts.read`
   - `crm.objects.contacts.write` 
   - `crm.objects.deals.read`
   - `crm.objects.deals.write`
   - `crm.objects.companies.read`
   - `crm.objects.companies.write`
4. Copy the access token to `HUBSPOT_API_KEY`

### 5. Snowflake Setup

Ensure your Snowflake setup supports RSA key authentication:
- The specified user has RSA public key configured in Snowflake
- User has access to the specified database and schema
- Required warehouse for query execution
- Your tables (customize the table names in the pipeline file)

**RSA Key Setup in Snowflake:**
1. Generate your RSA key pair (you already have `rsa_key.p8`)
2. Extract the public key: `openssl rsa -in rsa_key.p8 -pubout -out rsa_key_pub.pub`
3. Set the public key in Snowflake: `ALTER USER your_username SET RSA_PUBLIC_KEY='your_public_key_content';`

## 🎯 Usage

### Sync All Tables

```bash
# Sync all configured tables at once
python minimal_pipeline.py
```

### Sync Individual Tables

```bash
# Sync specific table
python minimal_pipeline.py contacts
python minimal_pipeline.py deals
python minimal_pipeline.py companies
```

### Test Your Setup

```bash
# Test connections and configuration
python test_setup.py

# Test individual components
python test_setup.py snowflake  # Test Snowflake connection
python test_setup.py hubspot    # Test HubSpot connection
```

## 📊 Data Flow

### Simple Direct Transfer
**Snowflake Tables** → **HubSpot Objects**

- Your Snowflake table data flows directly to HubSpot without any modifications
- Table column names should match HubSpot property names
- All data types and values are preserved as-is

## 📋 Requirements

### Data Structure
- **Snowflake**: Your tables should have column names that match HubSpot properties
- **HubSpot**: Objects (contacts, deals, companies) receive data directly

### Example Table Structure

**Snowflake contacts table:**
```sql
email, firstname, lastname, company, phone
```

**Snowflake deals table:**
```sql
dealname, amount, closedate, dealstage
```

**Snowflake companies table:**
```sql
name, domain, industry, city, state
```

## 🧪 Testing

```bash
# Test all connections and setup
python test_setup.py

# Test individual components
python test_setup.py snowflake   # Test Snowflake connection
python test_setup.py hubspot     # Test HubSpot connection
python test_setup.py env         # Test environment variables
```

## 🔍 Troubleshooting

### Common Issues

1. **Snowflake Connection Errors**
   - Verify account URL format: `account.region.snowflakecomputing.com`
   - Check warehouse is running
   - Ensure user has required permissions

2. **HubSpot API Errors**
   - Verify API token has required scopes
   - Check rate limits (HubSpot free tier: 100 requests/10 seconds)
   - Ensure table column names match HubSpot property names

3. **Table/Column Errors**
   - Verify table names in pipeline files match your Snowflake tables
   - Check that column names match HubSpot property names
   - Ensure data types are compatible

### Debug Tips

Check the `.dlt` directory for detailed logs after running pipelines:
```bash
ls -la .dlt/
cat .dlt/pipeline.log
```

## 🔗 Resources

- [DLT Hub Documentation](https://dlthub.com/docs/)
- [Snowflake Python Connector](https://docs.snowflake.com/en/user-guide/python-connector.html)
- [HubSpot API Documentation](https://developers.hubspot.com/docs/api/overview)
- [DLT Hub Snowflake Source](https://dlthub.com/docs/sources/sql_database)
- [DLT Hub HubSpot Destination](https://dlthub.com/docs/destinations/hubspot)

---

**Simple & Direct Data Sync! 🎉**