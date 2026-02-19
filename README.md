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
│     │ EXT_CUSTOMERS   │         │  EXT_PRODUCTS   │         │   EXT_ORDERS    │  ← Iceberg      │
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

### Step 1: Connect to External Iceberg Tables

**Run:** `sql/01_external_iceberg_tables.sql` in Snowsight

**What it does:**
1. Creates External Volume pointing to S3 (read-only)
2. Creates Catalog Integration for Iceberg (`CATALOG_SOURCE = OBJECT_STORE`)
3. Creates 3 externally-managed Iceberg tables pointing to existing metadata files

**Key talking point:**
> "Your data stays in your S3 bucket. Snowflake reads the Iceberg metadata to understand
> schema and file locations. No data copied, no ETL, zero vendor lock-in."

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
    FROM ext_orders
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
orders_df = spark.table("TLV_BUILD_HOL.EXTERNAL_ICEBERG.EXT_ORDERS")
products_df = spark.table("TLV_BUILD_HOL.EXTERNAL_ICEBERG.EXT_PRODUCTS")
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
| `EXT_CUSTOMERS` | Iceberg Table (external) | 100 customers from data lake |
| `EXT_PRODUCTS` | Iceberg Table (external) | 25 products across 4 categories |
| `EXT_ORDERS` | Iceberg Table (external) | 1000 orders with status, discounts |
| `DT_HOURLY_SALES_METRICS` | Dynamic Table | Auto-refreshing hourly aggregates |
| `CUSTOMER_LIFETIME_VALUE` | Table (dbt) | RFM analysis with customer tiers |
| `PRODUCT_CATEGORY_ANALYSIS` | Table (Notebook) | Category performance metrics |
| `PIPELINE_RUN_LOG` | Table | Task execution history |

---

## Additional Resources

- [Iceberg Tables in Snowflake](https://docs.snowflake.com/en/user-guide/tables-iceberg)
- [Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-about)
- [dbt on Snowflake](https://docs.snowflake.com/en/developer-guide/snowflake-cli/dbt-commands/overview)
- [Snowpark Connect for Spark](https://docs.snowflake.com/en/developer-guide/snowpark/python/snowpark-connect)
- [Tasks](https://docs.snowflake.com/en/user-guide/tasks-intro)
- [Serverless Tasks](https://docs.snowflake.com/en/user-guide/tasks-intro#serverless-tasks)
