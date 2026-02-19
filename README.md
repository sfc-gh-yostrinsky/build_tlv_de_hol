# Data Engineering Demo - Comprehensive Walkthrough

## Overview

This demo showcases 5 powerful Snowflake data engineering capabilities working together:

| Technology | Purpose | Output |
|------------|---------|--------|
| **Apache Iceberg Tables** | Open table format with full DML | 3 base tables (customers, products, orders) |
| **Dynamic Tables** | Auto-refreshing materialized views | Real-time hourly sales metrics |
| **dbt on Snowflake** | Native transformation framework | Customer Lifetime Value model |
| **Snowpark Connect** | PySpark APIs in Snowflake | Product category analysis |
| **Tasks DAG** | Parallel orchestration | Automated pipeline execution |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                   DATA ENGINEERING PIPELINE                                     │
├─────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                 │
│     ┌─────────────────┐         ┌─────────────────┐         ┌─────────────────┐                 │
│     │ CUSTOMERS_ICE   │         │  PRODUCTS_ICE   │         │   ORDERS_ICE    │  ← Iceberg      │
│     └────────┬────────┘         └────────┬────────┘         └────────┬────────┘    Tables       │
│              │                           │                           │                          │
│              └───────────────────────────┼───────────────────────────┘                          │
│                                          │                                                      │
│                                          ▼                                                      │
│                             ┌───────────────────────┐                                           │
│                             │    TASK_PIPELINE      │  ← Root Task (Serverless)                 │
│                             │        _ROOT          │    Scheduled hourly                       │
│                             └───────────┬───────────┘                                           │
│                                         │                                                       │
│              ┌──────────────────────────┼──────────────────────────┐                            │
│              │                          │                          │                            │
│              ▼                          ▼                          ▼                            │
│     ┌─────────────────┐        ┌─────────────────┐        ┌─────────────────┐                   │
│     │    DYNAMIC      │        │       dbt       │        │    NOTEBOOK     │                   │
│     │     TABLE       │        │      MODEL      │        │   (Snowpark     │                   │
│     │   (Refresh)     │        │    (Execute)    │        │    Connect)     │                   │
│     │                 │        │                 │        │                 │                   │
│     │  Hourly Sales   │        │    Customer     │        │    Product      │                   │
│     │    Metrics      │        │  Lifetime Value │        │    Category     │                   │
│     │                 │        │                 │        │    Analysis     │                   │
│     └────────┬────────┘        └────────┬────────┘        └────────┬────────┘                   │
│              │                          │                          │                            │
│              └──────────────────────────┼──────────────────────────┘                            │
│                                         │                                                       │
│                                         ▼                                                       │
│                             ┌───────────────────────────────┐                                   │
│                             │  TASK_PIPELINE_FINALIZER      │  ← Finalizer Task                 │
│                             └───────────────────────────────┘    Logs completion                │
│                                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Important: Demo Setup vs Customer Scenario

### For This Demo (Demo Setup)

We create **Snowflake-managed Iceberg tables** with synthetic data:
- `CATALOG = 'SNOWFLAKE'` - Snowflake manages the Iceberg catalog
- Full DML support (INSERT, UPDATE, DELETE, MERGE)
- Data stored in YOUR cloud storage in open Parquet/Iceberg format
- **Script:** `sql/00_demo_setup_iceberg.sql`

### For Customers (Production Scenario)

Customers typically have **existing Iceberg tables** from other engines (Spark, Trino, Flink):
- Use `METADATA_FILE_PATH` to point to existing metadata
- Or use **Catalog Integrations** (Glue, Open Catalog/Polaris) for auto-sync
- Read-only access in Snowflake (external engine manages writes)
- **Script:** `sql/01_customer_iceberg_scenario.sql`

```
┌───────────────────────────────────────────────────────────────────────────┐
│                    ICEBERG TABLE TYPES IN SNOWFLAKE                       │
├───────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  SNOWFLAKE-MANAGED (This Demo)         EXTERNALLY-MANAGED (Customers)     │
│  ──────────────────────────────        ───────────────────────────────    │
│  • CATALOG = 'SNOWFLAKE'               • METADATA_FILE_PATH = '...'       │
│  • Full DML support                    • Read-only in Snowflake           │
│  • Snowflake manages metadata          • External engine manages metadata │
│  • Time Travel supported               • Catalog Integration for auto-sync│
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

---

## File Structure

```
build_tlv_demo/
├── sql/
│   ├── 00_demo_setup_iceberg.sql        # Snowflake-managed Iceberg + sample data
│   ├── 01_external_iceberg_tables.sql   # Externally-managed Iceberg tables
│   ├── 02_dynamic_table.sql             # Dynamic Table for hourly metrics
│   ├── 03_dbt_deployment.sql            # dbt project deployment from workspace
│   ├── 04_notebook_deployment.sql       # Notebook project deployment from workspace
│   ├── 05_tasks_dag.sql                 # Task DAG orchestration (serverless)
│   └── 99_cleanup.sql                   # Complete cleanup script
├── dbt_ecommerce/
│   ├── dbt_project.yml
│   ├── profiles.yml                     # No env_var() or password fields!
│   └── models/
│       └── marts/
│           └── customer_lifetime_value.sql
├── notebooks/
│   ├── product_category_analysis.ipynb  # Snowpark Connect notebook
│   └── requirements.txt                 # pyspark, snowpark-connect[jvm]
└── README.md                            # This file
```

---

## Demo Execution Steps

### Step 0: Create Snowflake Workspace

This demo uses a Snowflake Workspace to store the dbt project and notebook files. Rather than uploading files manually, we'll create a workspace directly from the GitHub repository.

#### Create an API Integration

First, create an API integration that allows Snowflake to connect to GitHub:

```sql
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE API INTEGRATION tlv_build_git_integration
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/sfc-gh-yostrinsky')
    ENABLED = TRUE;
```

#### Create the Workspace

1. In Snowsight, navigate to **Projects → Workspaces**
2. Click **Create → From Git repository**
3. Fill in the dialog:
   - **Repository URL:** `https://github.com/sfc-gh-yostrinsky/build_tlv_de_hol`
   - **Workspace name:** `HOL`
   - **API Integration:** Select `tlv_build_git_integration`
   - **Database:** `TLV_BUILD_HOL`
   - **Schema:** `DATA_ENG_DEMO`
4. Select **Public repository** (or authenticate for private repos)
5. Click **Create**

Snowflake will clone the repository and create your workspace. Once complete, you'll see all files in the workspace:

```
sql/
├── 00_demo_setup_iceberg.sql
├── 01_external_iceberg_tables.sql
├── 02_dynamic_table.sql
├── 03_dbt_deployment.sql
├── 04_notebook_deployment.sql
├── 05_tasks_dag.sql
└── 99_cleanup.sql
dbt_ecommerce/
notebooks/
```

> **Note:** Throughout this demo, when we reference running a SQL file, you can open it directly from the workspace in Snowsight.

### Step 1: Create Iceberg Tables and Sample Data

**Run:** `sql/00_demo_setup_iceberg.sql` in Snowsight

**What it does:**
1. Creates External Volume pointing to S3
2. Creates Catalog Integration for Iceberg
3. Creates 3 Iceberg tables with `CATALOG = 'SNOWFLAKE'`
4. Populates synthetic e-commerce data (100 customers, 25 products, 1000 orders)

**Key talking point:**
> "Your data is stored in open Parquet format with Iceberg metadata. You own it, 
> no vendor lock-in. Any Iceberg-compatible engine can read it."

### Step 2: Create Dynamic Table

**Run:** `sql/02_dynamic_table.sql` in Snowsight

**What it does:**
- Creates `DT_HOURLY_SALES_METRICS` with `TARGET_LAG = '5 minutes'`
- Joins orders with products, aggregates by hour and category
- Automatically refreshes when source data changes

**Key talking point:**
> "Dynamic Tables are declarative pipelines. Define the WHAT, Snowflake handles the WHEN.
> No Airflow, no scheduling code, no orchestration headaches."

### Step 3: Deploy dbt Project

**Run:** `sql/03_dbt_deployment.sql` in Snowsight

This script:
1. Creates a dbt project from the workspace
2. Executes the project to build the `customer_lifetime_value` model

**Or deploy via CLI (terminal):**
```bash
snow dbt deploy dbt_ecommerce \
  --source ./dbt_ecommerce \
  --database TLV_BUILD_HOL \
  --schema DATA_ENG_DEMO
```

**Verify:**
```sql
SELECT * FROM TLV_BUILD_HOL.DATA_ENG_DEMO.CUSTOMER_LIFETIME_VALUE LIMIT 10;
```

**Key talking point:**
> "dbt runs INSIDE Snowflake - no external scheduler, no credentials in config files,
> native versioning and governance. Same dbt syntax you know, Snowflake execution."

### Step 4: Deploy Notebook Project

**Run:** `sql/04_notebook_deployment.sql` in Snowsight

This script:
1. Creates an External Access Integration for PyPI (required for pip packages)
2. Creates a Notebook Project from the workspace
3. Executes the notebook with Snowpark Connect

**Key talking point:**
> "Your data scientists can use familiar PySpark APIs. Processing happens inside Snowflake -
> no data movement, no Spark cluster to manage, same governance as SQL."

### Step 5: Set Up Tasks DAG

**Run:** `sql/05_tasks_dag.sql` in Snowsight

> **Note:** This script must be run in Snowsight, not via CLI, because it contains 
> multi-statement scripting blocks (BEGIN/END) that the CLI doesn't parse correctly.

**What it creates:**
```
TASK_PIPELINE_ROOT (serverless, scheduled hourly)
    │
    ├── TASK_REFRESH_DYNAMIC_TABLE (serverless) ────┐
    ├── TASK_RUN_DBT_MODEL (warehouse) ─────────────┼──→ TASK_PIPELINE_FINALIZER
    └── TASK_RUN_NOTEBOOK (serverless) ─────────────┘
```

**Task features demonstrated:**
- **Serverless execution** - No warehouse needed (except dbt which requires one)
- **Return values** - Each task returns record counts via `SYSTEM$SET_RETURN_VALUE`
- **Parallel execution** - Child tasks run simultaneously after root
- **Finalizer task** - Runs after ALL children complete, regardless of success/failure
- **Scripting blocks** - DECLARE/BEGIN/END for complex task logic
- **Auto-retry** - `TASK_AUTO_RETRY_ATTEMPTS = 2` for resilience

**Monitor execution:**
```sql
SELECT name, state, return_value, error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
))
ORDER BY scheduled_time DESC;
```

**Key talking point:**
> "Tasks can run serverless - no warehouse to manage, pay only for what you use.
> The DAG orchestrates everything: Dynamic Tables, dbt, and notebooks in parallel."

---

## Key Demo Scripts

### Customer Lifetime Value (dbt model)

```sql
-- RFM Analysis (Recency, Frequency, Monetary)
WITH customer_metrics AS (
    SELECT 
        customer_id,
        DATEDIFF('day', MAX(order_date), CURRENT_DATE()) AS recency_days,
        COUNT(DISTINCT order_id) AS frequency,
        SUM(quantity * unit_price) AS monetary
    FROM orders_ice
    GROUP BY customer_id
)
SELECT 
    *,
    NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
    NTILE(5) OVER (ORDER BY frequency) AS f_score,
    NTILE(5) OVER (ORDER BY monetary) AS m_score,
    (r_score + f_score + m_score) AS ltv_score,
    CASE 
        WHEN ltv_score >= 12 THEN 'Champions'
        WHEN ltv_score >= 9 THEN 'Loyal'
        WHEN ltv_score >= 6 THEN 'Potential'
        ELSE 'At Risk'
    END AS customer_tier
FROM customer_metrics
```

### Snowpark Connect Session Initialization

```python
from snowflake import snowpark_connect
from pyspark.sql import functions as F

# Initialize Snowpark Connect session
spark = snowpark_connect.init_spark_session()

# Read Iceberg tables as Spark DataFrames
orders_df = spark.table("TLV_BUILD_HOL.DATA_ENG_DEMO.ORDERS_ICE")
products_df = spark.table("TLV_BUILD_HOL.DATA_ENG_DEMO.PRODUCTS_ICE")
```

---

## Customer Questions & Answers

### "How is this different from regular tables?"

> "Iceberg tables store data in open Parquet format with Iceberg metadata. You can query
> the same data from Spark, Trino, or Flink without copying it. Zero vendor lock-in."

### "What about existing Iceberg data in our data lake?"

> "You have two options:
> 1. Point to your existing metadata file (`METADATA_FILE_PATH`)
> 2. Create a Catalog Integration with Glue or Open Catalog for auto-sync
> Either way, Snowflake reads your data where it lives."

### "Do I need to manage a Spark cluster for Snowpark Connect?"

> "No. Snowpark Connect runs inside Snowflake. You write PySpark code, it executes
> on Snowflake compute. No EMR, no Databricks clusters, just your warehouse."

### "How does dbt work without credentials in profiles.yml?"

> "When dbt runs inside Snowflake via `EXECUTE DBT PROJECT`, authentication is handled
> by your Snowflake session. No passwords or tokens in config files."

### "What's the difference between serverless and warehouse tasks?"

> "Serverless tasks use Snowflake-managed compute - no warehouse to configure, instant 
> scale, pay only for execution time. Use warehouses when you need specific sizing or 
> for operations that require them (like EXECUTE DBT PROJECT)."

---

## Outputs Created

| Object | Type | Description |
|--------|------|-------------|
| `CUSTOMERS_ICE` | Iceberg Table | 100 synthetic customers |
| `PRODUCTS_ICE` | Iceberg Table | 25 products across 4 categories |
| `ORDERS_ICE` | Iceberg Table | 1000 orders with status, discounts |
| `DT_HOURLY_SALES_METRICS` | Dynamic Table | Auto-refreshing hourly aggregates |
| `CUSTOMER_LIFETIME_VALUE` | Table (dbt) | RFM analysis with customer tiers |
| `PRODUCT_CATEGORY_ANALYSIS` | Table (Notebook) | Category performance metrics |
| `PIPELINE_RUN_LOG` | Table | Task execution history |

---

## Cleanup

Run `sql/99_cleanup.sql` in Snowsight:

```sql
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- DROP DATABASE (removes all schemas, tables, tasks, projects, stages inside)
DROP DATABASE IF EXISTS TLV_BUILD_HOL;

-- DROP ACCOUNT-LEVEL OBJECTS
DROP EXTERNAL ACCESS INTEGRATION IF EXISTS TLV_BUILD_HOL_PYPI_EAI;
DROP CATALOG INTEGRATION IF EXISTS tlv_iceberg_catalog_int;
DROP EXTERNAL VOLUME IF EXISTS tlv_datalake_s3_ev;

-- VERIFICATION
SHOW EXTERNAL VOLUMES LIKE '%tlv%';
SHOW CATALOG INTEGRATIONS LIKE '%iceberg%';
SHOW EXTERNAL ACCESS INTEGRATIONS LIKE '%tlv%';
```

---

## Troubleshooting

### "CLI fails with syntax errors on script 05"
The Snowflake CLI splits on semicolons, breaking multi-statement scripting blocks. Run `05_tasks_dag.sql` in Snowsight instead.

### "Invalid identifier 'CNT' in task"
When using `SYSTEM$SET_RETURN_VALUE`, build the message into a variable first, then use a bind variable:
```sql
msg := 'Count: ' || cnt::STRING;
CALL SYSTEM$SET_RETURN_VALUE(:msg);
```

### "Notebook execution in stored procedures is not supported"
`EXECUTE NOTEBOOK PROJECT` cannot be wrapped in scripting blocks. The notebook task must be a single statement.

### "dbt task fails with serverless"
`EXECUTE DBT PROJECT` requires a warehouse. Use `WAREHOUSE = COMPUTE_WH` instead of serverless for dbt tasks.

---

## Additional Resources

- [Iceberg Tables in Snowflake](https://docs.snowflake.com/en/user-guide/tables-iceberg)
- [Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-about)
- [dbt on Snowflake](https://docs.snowflake.com/en/developer-guide/snowflake-cli/dbt-commands/overview)
- [Snowpark Connect for Spark](https://docs.snowflake.com/en/developer-guide/snowpark/python/snowpark-connect)
- [Tasks](https://docs.snowflake.com/en/user-guide/tasks-intro)
- [Serverless Tasks](https://docs.snowflake.com/en/user-guide/tasks-intro#serverless-tasks)
