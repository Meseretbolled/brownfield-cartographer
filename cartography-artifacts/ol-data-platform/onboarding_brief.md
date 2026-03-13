# onboarding_brief.md

_Generated: 2026-03-13T18:26:02.143096+00:00_

## Five FDE Day-One Answers

### Q1. What is the primary data ingestion path? (trace from raw sources to first transformation)

[STATIC] Primary ingestion sources detected: user_course_roles, platforms, legacy_edx_certificate_revision_mapping, open_learning, /home/meseret/Documents/week-4/brownfield-cartographer/ol-data-platform/src/ol_dbt/models/staging/zendesk/stg__zendesk__ticket_field.sql.

### Q2. What are the 3-5 most critical output datasets or endpoints?

[STATIC] Critical output sinks: stg__micromasters__app__postgres__django_contenttype, stg__micromasters__app__postgres__ecommerce_usercoupon, stg__micromasters__app__postgres__ecommerce_couponinvoice, stg__bootcamps__app__postgres__django_contenttype, stg__bootcamps__app__postgres__ecommerce_orderaudit.

### Q3. What is the blast radius if the most critical module fails? (which downstream systems break)

[STATIC] Architectural hubs (highest blast radius): /home/meseret/Documents/week-4/brownfield-cartographer/ol-data-platform/src/ol_dbt/models/staging/mitxonline/stg__mitxonline__openedx__tracking_logs__user_activity.sql, /home/meseret/Documents/week-4/brownfield-cartographer/ol-data-platform/src/ol_dbt/models/staging/mitxresidential/stg__mitxresidential__openedx__tracking_logs__user_activity.sql, /home/meseret/Documents/week-4/brownfield-cartographer/ol-data-platform/src/ol_dbt/models/intermediate/mitxonline/int__mitxonline__users.sql.

### Q4. Where is the business logic concentrated vs distributed? (which modules/files own the core rules)

[STATIC] Business logic concentrated in top PageRank modules: /home/meseret/Documents/week-4/brownfield-cartographer/ol-data-platform/src/ol_dbt/models/staging/mitxonline/stg__mitxonline__openedx__tracking_logs__user_activity.sql, /home/meseret/Documents/week-4/brownfield-cartographer/ol-data-platform/src/ol_dbt/models/staging/mitxresidential/stg__mitxresidential__openedx__tracking_logs__user_activity.sql, /home/meseret/Documents/week-4/brownfield-cartographer/ol-data-platform/src/ol_dbt/models/intermediate/mitxonline/int__mitxonline__users.sql.

### Q5. What has changed most frequently in the last 30 days? (git velocity map — likely pain points)

[STATIC] Highest-velocity files (30d): /home/meseret/Documents/week-4/brownfield-cartographer/ol-data-platform/src/ol_dbt/models/reporting/_reporting__models.yml, /home/meseret/Documents/week-4/brownfield-cartographer/ol-data-platform/dg_deployments/reconcile_edxorg_partitions.py, /home/meseret/Documents/week-4/brownfield-cartographer/ol-data-platform/dg_projects/edxorg/edxorg/assets/edxorg_archive.py, /home/meseret/Documents/week-4/brownfield-cartographer/ol-data-platform/src/ol_superset/assets/charts/Content_Engagement_aa66927b-cc60-4950-8ad8-f79081736841.yaml, /home/meseret/Documents/week-4/brownfield-cartographer/ol-data-platform/src/ol_superset/assets/charts/Learners_Enrolled_37d70f20-6dcc-4237-921b-521dc43425a7.yaml.

## Evidence Summary

- Module graph nodes: `1106`
- Module graph edges: `902`
- Lineage datasets: `592`
- Lineage transformations: `588`

## Immediate Next Actions

1. Verify the top architectural hubs in code.
2. Validate upstream lineage for the highest-value sink datasets.
3. Inspect high-velocity files first for likely instability or debt.
4. Review documentation drift flags before trusting comments/docstrings.
