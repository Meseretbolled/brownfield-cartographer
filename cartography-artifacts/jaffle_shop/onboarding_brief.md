# onboarding_brief.md

_Generated: 2026-03-13T18:04:54.405579+00:00_

## Five FDE Day-One Answers

### Q1. What is the primary data ingestion path? (trace from raw sources to first transformation)

The primary data ingestion path starts with three raw source tables: raw_payments, raw_orders, and raw_customers. These are transformed by staging models stg_payments.sql, stg_orders.sql, and stg_customers.sql respectively, which convert the raw data into standardized formats for downstream analytics.

### Q2. What are the 3-5 most critical output datasets or endpoints?

The three most critical output datasets are the orders model (aggregating payment data by method), customers model (consolidating customer information and order history), and the jaffle_shop database itself. These represent the final business-facing datasets that power analytics and reporting.

### Q3. What is the blast radius if the most critical module fails? (which downstream systems break)

If stg_payments.sql fails, the blast radius includes both the orders and customers models, as they both depend on stg_payments data. This would break payment aggregation in orders and customer payment history in customers, effectively crippling revenue tracking and customer analytics.

### Q4. Where is the business logic concentrated vs distributed? (which modules/files own the core rules)

Business logic is concentrated in the staging models (stg_payments.sql, stg_orders.sql, stg_customers.sql) where data standardization and transformation rules are defined, and in the final models (orders.sql, customers.sql) where business metrics and aggregations are computed. The dbt_project.yml and schema.yml files contain configuration and quality definitions rather than core business rules.

### Q5. What has changed most frequently in the last 30 days? (git velocity map — likely pain points)

Based on the 30-day velocity map, the most frequently changed files are dbt_project.yml, orders.sql, customers.sql, schema.yml, and stg_payments.sql. These likely represent pain points where configuration, business logic, and data quality definitions are actively evolving.

## Evidence Summary

- Module graph nodes: `8`
- Module graph edges: `5`
- Lineage datasets: `9`
- Lineage transformations: `5`

## Immediate Next Actions

1. Verify the top architectural hubs in code.
2. Validate upstream lineage for the highest-value sink datasets.
3. Inspect high-velocity files first for likely instability or debt.
4. Review documentation drift flags before trusting comments/docstrings.
