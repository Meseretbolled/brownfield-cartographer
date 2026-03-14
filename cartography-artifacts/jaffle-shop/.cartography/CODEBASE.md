# CODEBASE.md — `brownfield-cartographer`

_Generated: 2026-03-14T15:02:46.254292+00:00_
_System type: dbt data transformation project_

## Architecture Overview

`brownfield-cartographer` is a **dbt data transformation project** comprising `32` modules connected by `11` import dependencies. The data layer contains `14` datasets across `13` tracked transformations. The structural centre of gravity is `models/staging/stg_supplies.sql`, `models/staging/stg_products.sql`, `models/staging/stg_orders.sql`. No circular dependencies detected. 7 module(s) flagged as potential dead code.

## Critical Path

Top modules by PageRank (highest structural influence):

1. `models/staging/stg_supplies.sql`
   - Purpose: Transforms raw supply data into structured staging tables
   - PageRank: `0.05770` | Domain: `transformation` | Velocity: `0` commits (90d)
2. `models/staging/stg_products.sql`
   - Purpose: Standardizes and cleans product data from the e-commerce source, converting pricing formats and adding boolean flags to identify food versus drink items.
   - PageRank: `0.05770` | Domain: `transformation` | Velocity: `0` commits (90d)
3. `models/staging/stg_orders.sql`
   - Purpose: Transforms raw order data into a structured format with standardized identifiers, converted pricing fields, and truncated timestamps for analysis.
   - PageRank: `0.05126` | Domain: `transformation` | Velocity: `0` commits (90d)
4. `models/staging/stg_locations.sql`
   - Purpose: Creates a clean store/location dimension table with standardized identifiers, tax rates, and opening dates for all retail locations.
   - PageRank: `0.04871` | Domain: `transformation` | Velocity: `0` commits (90d)
5. `models/marts/order_items.sql`
   - Purpose: Combines order item details with product information and supply costs to provide a complete view of individual order items including pricing, categorization, and fulfillment costs.
   - PageRank: `0.04228` | Domain: `transformation` | Velocity: `0` commits (90d)

## Data Sources & Sinks

### Sources
- `jaffle_shop`
- `/home/meseret/Desktop/brownfield-cartographer/jaffle-shop/models/staging/stg_supplies.sql`
- `/home/meseret/Desktop/brownfield-cartographer/jaffle-shop/models/staging/stg_order_items.sql`
- `/home/meseret/Desktop/brownfield-cartographer/jaffle-shop/models/staging/stg_products.sql`
- `/home/meseret/Desktop/brownfield-cartographer/jaffle-shop/models/staging/stg_orders.sql`
- `/home/meseret/Desktop/brownfield-cartographer/jaffle-shop/models/staging/stg_locations.sql`
- `/home/meseret/Desktop/brownfield-cartographer/jaffle-shop/models/staging/stg_customers.sql`
- `/home/meseret/Desktop/brownfield-cartographer/jaffle-shop/models/marts/metricflow_time_spine.sql`

### Sinks
- `metricflow_time_spine`
- `supplies`
- `customers`
- `products`
- `locations`
- `jaffle_shop`

## Known Debt

### Circular Dependencies
- No circular dependencies detected

### Documentation Drift
- No documentation drift flags recorded

## High-Velocity Files

Files with most commits in the last 90 days (likely pain points):
1. `package-lock.yml` — `0` commits
2. `packages.yml` — `0` commits
3. `dbt_project.yml` — `0` commits
4. `Taskfile.yml` — `0` commits
5. `models/staging/stg_supplies.sql` — `0` commits
6. `models/staging/__sources.yml` — `0` commits
7. `models/staging/stg_customers.yml` — `0` commits
8. `models/staging/stg_orders.yml` — `0` commits
9. `models/staging/stg_order_items.sql` — `0` commits
10. `models/staging/stg_locations.yml` — `0` commits

## Module Purpose Index

### configuration

- `dbt_project.yml`
  - Configures dbt project settings and structure
  - LOC: `38` | PageRank: `0.02633` | Complexity: `0.0`
- `macros/generate_schema_name.sql` ⚠️dead-code-candidate
  - Determines the appropriate database schema name for data assets based on their type and deployment environment to maintain organized data structure
  - LOC: `23` | PageRank: `0.02633` | Complexity: `1.0`
- `package-lock.yml`
  - Manages package dependencies for dbt project
  - LOC: `8` | PageRank: `0.02633` | Complexity: `0.0`
- `packages.yml`
  - Defines package dependencies for dbt project
  - LOC: `7` | PageRank: `0.02633` | Complexity: `0.0`
### ingestion

- `models/staging/__sources.yml`
  - Defines the source data structure for e-commerce operations, establishing the foundational data assets that power the Jaffle Shop's business intelligence.
  - LOC: `20` | PageRank: `0.02633` | Complexity: `0.0`
### orchestration

- `Taskfile.yml`
  - Automates dbt project setup and data generation tasks
  - LOC: `40` | PageRank: `0.02633` | Complexity: `0.0`
### serving

- `models/marts/orders.sql`
  - Creates an enriched order fact table with calculated order metrics and customer ordering patterns, enabling comprehensive order analysis and customer behavior tracking.
  - LOC: `77` | PageRank: `0.03752` | Complexity: `10.0`
- `models/marts/products.yml`
  - Defines a product dimension model that categorizes product attributes for analytical queries, supporting product-based reporting and analysis.
  - LOC: `26` | PageRank: `0.02633` | Complexity: `0.0`
### testing

- `models/marts/order_items.yml`
  - Tests the integrity of order item data relationships and supply cost calculations, ensuring accurate cost aggregation and data quality for downstream analytics.
  - LOC: `181` | PageRank: `0.02633` | Complexity: `0.0`
### transformation

- `models/staging/stg_products.sql`
  - Standardizes and cleans product data from the e-commerce source, converting pricing formats and adding boolean flags to identify food versus drink items.
  - LOC: `34` | PageRank: `0.05770` | Complexity: `4.0`
- `models/staging/stg_supplies.sql`
  - Transforms raw supply data into structured staging tables
  - LOC: `31` | PageRank: `0.05770` | Complexity: `4.0`
- `models/staging/stg_orders.sql`
  - Transforms raw order data into a structured format with standardized identifiers, converted pricing fields, and truncated timestamps for analysis.
  - LOC: `33` | PageRank: `0.05126` | Complexity: `4.0`
- `models/staging/stg_locations.sql`
  - Creates a clean store/location dimension table with standardized identifiers, tax rates, and opening dates for all retail locations.
  - LOC: `29` | PageRank: `0.04871` | Complexity: `4.0`
- `models/marts/order_items.sql`
  - Combines order item details with product information and supply costs to provide a complete view of individual order items including pricing, categorization, and fulfillment costs.
  - LOC: `66` | PageRank: `0.04228` | Complexity: `11.0`
- `models/staging/stg_customers.sql`
  - Transforms raw customer data from the e-commerce source system into a clean staging table with standardized customer identifiers and names.
  - LOC: `23` | PageRank: `0.03752` | Complexity: `4.0`
- `models/staging/stg_order_items.sql`
  - Transforms raw order item data into a standardized format linking orders to products for detailed sales and product performance analysis.
  - LOC: `22` | PageRank: `0.03532` | Complexity: `4.0`
- `macros/cents_to_dollars.sql` ⚠️dead-code-candidate
  - Provides a standardized method to convert currency values stored in cents to dollars for consistent financial reporting across different database systems
  - LOC: `21` | PageRank: `0.02633` | Complexity: `0.0`
- `models/marts/customers.sql` ⚠️dead-code-candidate
  - Creates a comprehensive customer profile by combining customer details with their order history, including lifetime spend, order counts, and customer type classification for segmentation analysis.
  - LOC: `58` | PageRank: `0.02633` | Complexity: `8.0`
- `models/marts/customers.yml`
  - Creates a customer data mart with key metrics like lifetime spend and order history for customer relationship management
  - LOC: `107` | PageRank: `0.02633` | Complexity: `0.0`
- `models/marts/locations.sql` ⚠️dead-code-candidate
  - Acts as a staging layer for location data, making geographic and store location information available for analysis and reporting.
  - LOC: `9` | PageRank: `0.02633` | Complexity: `3.0`
- `models/marts/locations.yml`
  - Establishes a location dimension table with tax rate information for regional analysis and reporting
  - LOC: `24` | PageRank: `0.02633` | Complexity: `0.0`
- `models/marts/metricflow_time_spine.sql` ⚠️dead-code-candidate
  - Generates a time spine covering 10 years for time-based aggregations and temporal analysis
  - LOC: `19` | PageRank: `0.02633` | Complexity: `3.0`
- `models/marts/orders.yml`
  - Provides an aggregated view of orders with key metrics like total amounts, tax calculations, and categorization of food vs drink items to support order analysis and financial reporting.
  - LOC: `183` | PageRank: `0.02633` | Complexity: `0.0`
- `models/marts/products.sql` ⚠️dead-code-candidate
  - Serves as a simple staging layer for product data, providing access to product attributes like names, prices, and item classifications for downstream analytics.
  - LOC: `9` | PageRank: `0.02633` | Complexity: `3.0`
- `models/marts/supplies.sql` ⚠️dead-code-candidate
  - Materializes a supplies table from staging data for downstream analytics
  - LOC: `9` | PageRank: `0.02633` | Complexity: `3.0`
- `models/marts/supplies.yml`
  - Provides a dimension table that maps supplies to products for inventory and supply chain analysis
  - LOC: `24` | PageRank: `0.02633` | Complexity: `0.0`
- `models/staging/stg_customers.yml`
  - Transforms raw customer data into a clean, standardized format with data quality validation to ensure reliable customer analytics and reporting.
  - LOC: `9` | PageRank: `0.02633` | Complexity: `0.0`
- `models/staging/stg_locations.yml`
  - Transforms raw store location data into a standardized format with temporal consistency checks to support location-based business operations and analysis.
  - LOC: `43` | PageRank: `0.02633` | Complexity: `0.0`
- `models/staging/stg_order_items.yml`
  - Transforms raw order item data into a structured format that links individual purchased items to their parent orders and provides pricing in both cents and dollars.
  - LOC: `16` | PageRank: `0.02633` | Complexity: `0.0`
- `models/staging/stg_orders.yml`
  - Transforms raw order data with validation checks to ensure financial accuracy and provides a trusted foundation for order-based business analysis.
  - LOC: `12` | PageRank: `0.02633` | Complexity: `0.0`
- `models/staging/stg_products.yml`
  - Creates a clean product catalog with standardized identifiers and pricing for all food and drink items available for ordering.
  - LOC: `9` | PageRank: `0.02633` | Complexity: `0.0`
- `models/staging/stg_supplies.yml`
  - Captures detailed supply cost records to track fluctuating supply expenses over time, enabling accurate cost accounting for supplies.
  - LOC: `12` | PageRank: `0.02633` | Complexity: `0.0`
