# CODEBASE.md

_Generated: 2026-03-13T18:04:54.399823+00:00_

## Architecture Overview

This repository was analyzed as a mixed-codebase system with `8` modules, `5` import dependencies, `9` datasets, and `5` lineage transformations. The structural center of gravity is around `models/staging/stg_payments.sql`, `models/staging/stg_orders.sql`, `models/staging/stg_customers.sql`. The data layer appears to flow from discovered source nodes into downstream transformations and sink datasets captured in the lineage graph.

## Critical Path

Top modules by PageRank (highest structural influence):

1. `models/staging/stg_payments.sql`
   - Purpose: This module transforms raw payment data into a standardized format for downstream analytics. It converts payment amounts from cents to dollars and renames columns to follow consistent naming conventions, enabling accurate financial reporting and analysis of payment transactions across the business.
   - PageRank: `0.17612`
   - Change velocity (30d): `0`
2. `models/staging/stg_orders.sql`
   - Purpose: This module transforms raw order data into a staging format suitable for downstream analytics. It standardizes column names and prepares order-level information for business reporting and analysis.
   - PageRank: `0.17612`
   - Change velocity (30d): `0`
3. `models/staging/stg_customers.sql`
   - Purpose: This model transforms raw customer data into a standardized staging format for downstream analytics. It maps the source customer identifier to a consistent field name (customer_id) and preserves essential customer attributes (first_name, last_name) for business analysis. The model serves as a foundational data layer that enables reliable customer reporting and segmentation across the organization's data products.
   - PageRank: `0.13230`
   - Change velocity (30d): `0`
4. `dbt_project.yml`
   - Purpose: This module defines the configuration and structure for a data transformation project that processes e-commerce order and customer data. It provides the foundation for generating business intelligence reports and analytics that help understand customer behavior, order patterns, and sales performance in an online retail environment.
   - PageRank: `0.10309`
   - Change velocity (30d): `0`
5. `models/orders.sql`
   - Purpose: This module provides a comprehensive view of order payments by payment method, enabling the business to track revenue breakdown across credit card, coupon, bank transfer, and gift card transactions. It solves the business problem of understanding payment channel performance and total order value for financial reporting and analysis. The model serves as a foundational dataset for revenue attribution and payment method optimization decisions.
   - PageRank: `0.10309`
   - Change velocity (30d): `0`

## Data Sources & Sinks

### Sources
- `raw_payments`
- `raw_orders`
- `raw_customers`
- `jaffle_shop`

### Sinks
- `orders`
- `customers`
- `jaffle_shop`

## Known Debt

### Circular Dependencies
- No circular dependencies detected

### Documentation Drift
- No documentation drift flags recorded

## High-Velocity Files

Files changing most frequently in recent git history:
- `dbt_project.yml` — `0` changes
- `models/orders.sql` — `0` changes
- `models/customers.sql` — `0` changes
- `models/schema.yml` — `0` changes
- `models/staging/stg_payments.sql` — `0` changes
- `models/staging/stg_orders.sql` — `0` changes
- `models/staging/schema.yml` — `0` changes
- `models/staging/stg_customers.sql` — `0` changes

## Module Purpose Index

### analytics

- `models/orders.sql`
  - This module provides a comprehensive view of order payments by payment method, enabling the business to track revenue breakdown across credit card, coupon, bank transfer, and gift card transactions. It solves the business problem of understanding payment channel performance and total order value for financial reporting and analysis. The model serves as a foundational dataset for revenue attribution and payment method optimization decisions.
  - LOC: `56` | PageRank: `0.10309`
### core

- `models/schema.yml`
  - This module defines the schema for core business entities in the Jaffle Shop, establishing the data model for customers and orders that enables tracking of customer lifecycle and order fulfillment. It provides the foundational structure for analyzing customer behavior, order patterns, and revenue attribution across different payment methods, supporting business intelligence and operational decision-making.
  - LOC: `82` | PageRank: `0.10309`
- `models/staging/schema.yml`
  - This schema.yml file defines data quality tests for staging models that validate the integrity of raw customer, order, and payment data before it enters the data warehouse. By enforcing uniqueness, non-null constraints, and accepted value ranges, it ensures the foundational data is clean and reliable for downstream analytics and business reporting.
  - LOC: `31` | PageRank: `0.10309`
### customer

- `models/staging/stg_customers.sql`
  - This model transforms raw customer data into a standardized staging format for downstream analytics. It maps the source customer identifier to a consistent field name (customer_id) and preserves essential customer attributes (first_name, last_name) for business analysis. The model serves as a foundational data layer that enables reliable customer reporting and segmentation across the organization's data products.
  - LOC: `22` | PageRank: `0.13230`
- `models/customers.sql`
  - This model consolidates customer information, order history, and payment data to provide a comprehensive view of customer lifetime value. It enables business stakeholders to understand customer purchasing patterns, including first and most recent order dates, total order count, and cumulative payment amounts. This serves as a foundational dataset for customer analytics, marketing segmentation, and customer relationship management initiatives.
  - LOC: `69` | PageRank: `0.10309`
### transformation

- `models/staging/stg_orders.sql`
  - This module transforms raw order data into a staging format suitable for downstream analytics. It standardizes column names and prepares order-level information for business reporting and analysis.
  - LOC: `23` | PageRank: `0.17612`
- `models/staging/stg_payments.sql`
  - This module transforms raw payment data into a standardized format for downstream analytics. It converts payment amounts from cents to dollars and renames columns to follow consistent naming conventions, enabling accurate financial reporting and analysis of payment transactions across the business.
  - LOC: `25` | PageRank: `0.17612`
- `dbt_project.yml`
  - This module defines the configuration and structure for a data transformation project that processes e-commerce order and customer data. It provides the foundation for generating business intelligence reports and analytics that help understand customer behavior, order patterns, and sales performance in an online retail environment.
  - LOC: `26` | PageRank: `0.10309`
