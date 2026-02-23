# Step 6: Ask Business Questions with Cortex Code + Snowflake Intelligence

Now for the fun part! Let's use **Cortex Code (CoCo)** to create a **Snowflake Intelligence** semantic layer and answer business questions in natural language - using the tables we just built!

## Prerequisites: Enable Cross-Region Inference

Cortex Code requires access to Claude models. Since these models may not be available in your region, you need to enable **cross-region inference**. This should have been set up in Step 0.

## The 3 Tables We Built

| Table | Created By | Contains |
|-------|------------|----------|
| `CUSTOMER_LIFETIME_VALUE` | dbt | RFM scores, customer tiers, revenue metrics |
| `PRODUCT_CATEGORY_ANALYSIS` | Notebook | Category performance, profit margins, rankings |
| `DT_HOURLY_SALES_METRICS` | Dynamic Table | Real-time hourly sales aggregates |

## The Business Questions We'll Answer

| # | Business Question | Expected Answer |
|---|-------------------|-----------------|
| 1 | "Show me the top 2 Platinum tier customers by total revenue" | 2 specific customers with names and revenue |
| 2 | "Which product category has the best profit margin?" | Single category name with margin % |

---

## How to Do It (Interactive with CoCo)

### Part A: Create a Semantic View

Open Cortex Code in your IDE and type:

```
Create a semantic view for our e-commerce analytics that includes these 2 tables:
- TLV_BUILD_HOL.DATA_ENG_DEMO.CUSTOMER_LIFETIME_VALUE (customer tiers, RFM scores, revenue)
- TLV_BUILD_HOL.DATA_ENG_DEMO.PRODUCT_CATEGORY_ANALYSIS (category metrics, margins)

Business users will ask about top customers and product performance.
```

CoCo will guide you through creating a semantic view YAML file with proper dimensions, measures, and relationships.

### Part B: Create Snowflake Intelligence

Once your semantic view is deployed, ask CoCo:

```
Create a Snowflake Intelligence analyst using my semantic view
```

CoCo will help you create an Intelligence configuration that business users can query with natural language.

### Part C: Access Snowflake Intelligence

1. **In Snowsight:** Navigate to **AI & ML > Intelligence** in the left sidebar
2. **Find your agent:** Look for the agent CoCo created (e.g., `ECOMMERCE_ANALYST`)
3. **Open the chat interface:** Click on the agent to start asking questions

Alternatively, you can ask questions directly in Cortex Code by typing:
```
Ask my Snowflake Intelligence agent: <your question>
```

### Part D: Ask Your Questions!

Now the magic - in the Intelligence chat interface, ask these questions:

---

**Question 1: Top Customers**
```
Show me the top 2 Platinum tier customers by total revenue
```
*Expected: A table with 2 rows showing customer_name, total_revenue, customer_tier*

---

**Question 2: Product Performance**
```
Which product category has the best profit margin?
```
*Expected: A single answer like "Audio with 58% margin" or similar*

---

## Key Takeaway

> "Your business users don't need to know SQL. They ask questions in plain English,
> and Snowflake Intelligence translates it to optimized queries against your semantic model.
> The data engineers build the pipelines, the analysts just ask questions."

---

## Bonus: Test Without Intelligence

You can also ask CoCo directly to query these tables:

```
Query CUSTOMER_LIFETIME_VALUE and show me the top 2 Platinum customers by revenue
```

CoCo will write and execute the SQL for you, showing how the semantic model would answer the same question.
