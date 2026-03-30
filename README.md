# RetailPulse — End-to-End Retail Analytics Platform

> A complete retail merchandise analytics pipeline built on the modern enterprise data stack. Simulates how a merchandise planning team tracks store performance, inventory health, and category trends — from raw data ingestion to executive dashboards.

---

## Architecture

```
Kaggle CSV
    |
    v  [upload]
Azure Data Lake Storage Gen2  (retailpulse-raw container)
    |
    v  [read_files() SQL]
Azure Databricks
    |
    |-- Bronze  →  raw_inventory           (exact Delta copy, never modified)
    |-- Silver  →  inventory_cleaned       (cleaned + 6 business metrics)
    |               Liquid Clustering: store_id · sale_date · category
    |-- Gold    →  vw_store_kpi            (store performance by month)
    |           →  vw_inventory_exceptions (exception flags + priority scores)
    |           →  vw_category_trends      (weekly trends + forecast variance)
    |
    |-- Unity Catalog  (retailpulse.bronze / .silver / .gold)
    |-- AI/BI Genie    (natural language querying on gold layer)
    |
    v  [Databricks partner connector]
Power BI Dashboard
    5 pages · 22 visuals · 5 DAX measures
```

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Storage | Azure Data Lake Storage Gen2 | Raw file landing zone — untouched source of truth |
| Processing | Azure Databricks | Medallion architecture, Delta Lake, SQL transformation |
| Format | Delta Lake | ACID transactions, time travel, schema enforcement |
| Organization | Unity Catalog | 3-level governance: catalog.schema.table |
| Querying | AI/BI Genie | Natural language querying on gold views |
| Dashboards | Power BI | 5-page business dashboard via Databricks connector |

---

## Dataset

**Source:** [Retail Store Inventory Forecasting Dataset](https://www.kaggle.com/datasets/anirudhchauhan/retail-store-inventory-forecasting-dataset) by anirudhchauhan on Kaggle

| Column | Type | Description |
|---|---|---|
| Date | Date | Transaction date |
| Store ID | Text | Store identifier |
| Product ID | Text | SKU identifier |
| Category | Text | Product category |
| Region | Text | Geographic region |
| Inventory Level | Integer | Units currently on shelf |
| Units Sold | Integer | Units sold in period |
| Units Ordered | Integer | Units on order from supplier |
| Demand Forecast | Decimal | Predicted units to sell |
| Price | Decimal | Selling price per unit |
| Discount | Decimal | Discount applied (0.10 = 10%) |
| Weather Condition | Text | Weather during period |
| Holiday/Promotion | Integer | Promotion active (1) or not (0) |
| Competitor Pricing | Decimal | Competitor price point |
| Seasonality | Text | Season flag |

---

## Business Metrics Built from Scratch

All 6 metrics are calculated once in the Silver layer. Gold views aggregate — they never recalculate.

| Metric | Formula | What It Tells You |
|---|---|---|
| Revenue | Units Sold × Price × (1 - Discount) | Total income per record after discount |
| Sell-Through Rate | Units Sold / (Units Sold + Inventory) × 100 | % of available stock that sold. 65%+ = healthy |
| Days of Supply | (Inventory / Units Sold) × 7 | How many days current stock will last |
| Forecast Variance % | (Actual - Forecast) / Forecast × 100 | How accurate demand planning was |
| Price vs Competitor | Price - Competitor Pricing | Are we priced above or below competition |
| Inventory Status | CASE logic | Healthy / Stockout / Low Stock / Overstock Risk / Dead Stock |

---

## Dashboard Pages

| Page | Title | Source View | Business Question |
|---|---|---|---|
| 1 | Executive Summary | vw_store_kpi | How is the business performing overall? |
| 2 | Store Performance | vw_store_kpi | Which stores are top/bottom performers? |
| 3 | Inventory Tracker | vw_inventory_exceptions | Which products need allocation action now? |
| 4 | Category Trends | vw_category_trends | Which categories are growing or declining? |
| 5 | Store Monitoring | vw_store_kpi + vw_category_trends | Which stores are actively declining? |

### Key Visuals
- Treemap — revenue by region
- Scatter chart — revenue vs sell-through rate (bubble = inventory size)
- Exception table — color-coded by flag type with conditional formatting
- Allocation priority bar chart — ranked stores for restocking decisions
- Category heatmap — sell-through rate by category × week (Matrix visual)
- Promo vs non-promo line chart — promotional lift analysis

### DAX Measures
```
Stores_Need_Attention  = CALCULATE(DISTINCTCOUNT(store_id), performance_tier <> "Healthy")
Sell_Through_Rate      = DIVIDE(SUM(units_sold), SUM(units_sold) + SUM(inventory_level)) * 100
Overstock_Count        = CALCULATE(COUNTROWS(...), exception_flag = "Overstock Risk")
SlowMover_Count        = CALCULATE(COUNTROWS(...), exception_flag = "Slow Moving")
Stockout_Count         = CALCULATE(COUNTROWS(...), exception_flag = "Stockout")
```

---

## Key Business Insight

**A 30% average sell-through rate with near-zero forecast variance indicates an allocation problem — not a demand problem.**

When stores are selling roughly what was planned (low forecast variance) but only clearing 30% of available inventory (low sell-through), the issue is not that customers aren't buying. The issue is that the right products are being sent to the wrong stores. Reallocating slow-moving inventory from underperforming stores to high sell-through stores before season end reduces markdown exposure and protects margin.

---

## AI/BI Genie — Natural Language Queries

A Genie Space is configured on all 3 gold views. Sample questions:

- "Which stores have the lowest sell-through rate this month?"
- "Show me the top 10 stores by allocation priority score"
- "Which categories are declining week over week?"
- "Compare actual units sold vs demand forecast by category"
- "Which stores have overstock risk right now?"
- "Which regions have the highest average days of supply?"

---

## Repository Structure

```
RetailPulse/
├── sql/
│   ├── 01_setup.sql            # Catalog + schema creation
│   ├── 02_bronze.sql           # Raw data ingestion from ADLS
│   ├── 03_silver.sql           # Cleaning + business metric calculation
│   ├── 04_gold_views.sql       # 3 KPI views with window functions
│   └── 05_clustering.sql       # Liquid Clustering on silver table
├── powerbi/
│   └── RetailPulse.pbix        # Power BI dashboard file
├── docs/
│   └── architecture.png        # Architecture diagram
└── README.md
```

---

## How to Reproduce

1. Download the [Kaggle dataset](https://www.kaggle.com/datasets/anirudhchauhan/retail-store-inventory-forecasting-dataset)
2. Create an Azure free account → set up ADLS Gen2 → upload CSV
3. Create a Databricks Community Edition account
4. Run SQL files in `/sql/` in order (01 through 05)
5. Set up Unity Catalog and AI/BI Genie Space
6. Connect Power BI Desktop via Databricks partner connector
7. Open `RetailPulse.pbix` and refresh data

---

## Skills Demonstrated

`Azure ADLS Gen2` `Azure Databricks` `Delta Lake` `Medallion Architecture` `Unity Catalog`
`Liquid Clustering` `AI/BI Genie` `SQL Window Functions` `CTEs` `Power BI` `DAX`
`Retail Analytics` `Merchandise Planning` `KPI Dashboard Design` `Data Governance`

---

*Built as a portfolio project to demonstrate hands-on proficiency with the modern retail analytics stack.*
