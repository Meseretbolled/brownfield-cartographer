# FDE Day-One Onboarding Brief — `brownfield-cartographer`

_Generated: 2026-03-14T15:02:46.272637+00:00_
_System: dbt data transformation project_

## Five FDE Day-One Answers

### Q1. What is the primary data ingestion path? (trace from raw sources to first transformation)

The primary data ingestion path flows from raw source tables in the `jaffle_shop` database through the staging models. Each staging file (e.g., `/home/meseret/Desktop/brownfield-cartographer/jaffle-shop/models/staging/stg_orders.sql`, `/home/meseret/Desktop/brownfield-cartographer/jaffle-shop/models/staging/stg_products.sql`) transforms raw data into standardized staging tables like `stg_orders`, `stg_products`, and `stg_supplies` without any upstream dependencies.

### Q2. What are the 3-5 most critical output datasets or endpoints?

The three most critical output datasets are `orders`, `order_items`, and `customers` in the marts layer. These are produced by `/home/meseret/Desktop/brownfield-cartographer/jaffle-shop/models/marts/orders.sql`, `/home/meseret/Desktop/brownfield-cartographer/jaffle-shop/models/marts/order_items.sql`, and `/home/meseret/Desktop/brownfield-cartographer/jaffle-shop/models/marts/customers.sql` respectively, serving as the main business-facing analytics tables.

### Q3. What is the blast radius if the most critical module fails? (which downstream systems break)

If `/home/meseret/Desktop/brownfield-cartographer/jaffle-shop/models/staging/stg_supplies.sql` fails, the blast radius includes `stg_supplies`, `order_items`, and `supplies` tables. Since `order_items` depends on both `stg_supplies` and other staging tables, its failure cascades to break any downstream analytics or reporting that relies on complete order item data.

### Q4. Where is the business logic concentrated vs distributed? (which modules/files own the core rules)

Business logic is concentrated in the staging models where data cleaning, standardization, and transformation rules are applied. The staging files like `stg_products.sql` and `stg_orders.sql` contain the core rules for data normalization, while the marts layer primarily aggregates and joins these cleaned datasets without complex business rules.

### Q5. What has changed most frequently in the last 30 days? (git velocity map — likely pain points)

In the last 30 days, configuration files like `/home/meseret/Desktop/brownfield-cartographer/jaffle-shop/package-lock.yml`, `/home/meseret/Desktop/brownfield-cartographer/jaffle-shop/packages.yml`, and `/home/meseret/Desktop/brownfield-cartographer/jaffle-shop/dbt_project.yml` have shown the highest velocity. This suggests frequent dependency updates or configuration changes, likely indicating active development or troubleshooting of the dbt project setup.

## Evidence Summary

- Repository: `brownfield-cartographer`
- System type: dbt data transformation project
- Module graph nodes: `32`
- Module graph edges: `11`
- Lineage datasets: `14`
- Lineage transformations: `13`
- Git analysis window: `90` days
- LLM-generated answers: `yes`

## Immediate Next Actions

1. Verify the top architectural hubs by navigating to their source files.
2. Validate upstream lineage for the highest-value sink datasets.
3. Inspect high-velocity files first — they're the most likely source of instability.
4. Review documentation drift flags before trusting any existing comments/docstrings.
5. Run `cartographer query <repo> --cartography-dir .cartography` to interactively explore.
