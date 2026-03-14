# FDE Day-One Onboarding Brief — `ol-data-platform`

_Generated: 2026-03-14T17:14:37.481371+00:00_
_System: dbt data transformation project_

## Five FDE Day-One Answers

### Q1. What is the primary data ingestion path? (trace from raw sources to first transformation)

The primary data ingestion path starts with raw data sources like `user_course_roles`, `platforms`, and `legacy_edx_certificate_revision_mapping` (all in-degree=0), which flow into staging models such as `/home/meseret/Desktop/brownfield-cartographer/ol-data-platform/src/ol_dbt/models/staging/zendesk/stg__zendesk__ticket_field.sql` and `/home/meseret/Desktop/brownfield-cartographer/ol-data-platform/src/ol_dbt/models/staging/zendesk/stg__zendesk__organization.sql`. These staging models represent the first transformation layer, converting raw data into structured DBT models for downstream processing.

### Q2. What are the 3-5 most critical output datasets or endpoints?

The 3-5 most critical output datasets are the high-velocity reporting model `/home/meseret/Desktop/brownfield-cartographer/ol-data-platform/src/ol_dbt/models/reporting/_reporting__models.yml`, the migration model `/home/meseret/Desktop/brownfield-cartographer/ol-data-platform/src/ol_dbt/models/migration/edxorg_to_mitxonline_enrollments.sql`, and the dimensional model `/home/meseret/Desktop/brownfield-cartographer/ol-data-platform/src/ol_dbt/models/dimensional/dim_course_content.sql`. These outputs serve as the primary interfaces for analytics and data consumption across the platform.

### Q3. What is the blast radius if the most critical module fails? (which downstream systems break)

If the most critical module `/home/meseret/Desktop/brownfield-cartographer/ol-data-platform/src/ol_dbt/models/staging/mitxonline/stg__mitxonline__openedx__tracking_logs__user_activity.sql` fails, the blast radius would include all downstream models that depend on user activity tracking data, particularly the intermediate models like `/home/meseret/Desktop/brownfield-cartographer/ol-data-platform/src/ol_dbt/models/intermediate/mitxonline/int__mitxonline__users.sql` and reporting models. This would break analytics dashboards and any systems relying on user engagement metrics.

### Q4. Where is the business logic concentrated vs distributed? (which modules/files own the core rules)

Business logic is concentrated in the transformation models, particularly in the staging files like `/home/meseret/Desktop/brownfield-cartographer/ol-data-platform/src/ol_dbt/models/staging/mitxonline/stg__mitxonline__openedx__tracking_logs__user_activity.sql` which handle filtering, deduplication, and data cleaning rules. The logic is distributed across multiple platforms (MITx Online, edX, XPro, Residential) but centralized in the intermediate models that unify data from these sources.

### Q5. What has changed most frequently in the last 30 days? (git velocity map — likely pain points)

The most frequently changed files in the last 30 days are the reporting models `/home/meseret/Desktop/brownfield-cartographer/ol-data-platform/src/ol_dbt/models/reporting/_reporting__models.yml`, the migration model `/home/meseret/Desktop/brownfield-cartographer/ol-data-platform/src/ol_dbt/models/migration/edxorg_to_mitxonline_enrollments.sql`, and the docker-compose configuration `/home/meseret/Desktop/brownfield-cartographer/ol-data-platform/src/ol_dbt/docker-compose.yaml`. These represent active development areas and potential pain points requiring frequent updates.

## Evidence Summary

- Repository: `ol-data-platform`
- System type: dbt data transformation project
- Module graph nodes: `1106`
- Module graph edges: `902`
- Lineage datasets: `594`
- Lineage transformations: `589`
- Git analysis window: `90` days
- LLM-generated answers: `yes`

## Immediate Next Actions

1. Verify the top architectural hubs by navigating to their source files.
2. Validate upstream lineage for the highest-value sink datasets.
3. Inspect high-velocity files first — they're the most likely source of instability.
4. Review documentation drift flags before trusting any existing comments/docstrings.
5. Run `cartographer query <repo> --cartography-dir .cartography` to interactively explore.
