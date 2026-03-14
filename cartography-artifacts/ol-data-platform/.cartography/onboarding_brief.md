# FDE Day-One Onboarding Brief — `ol-data-platform`

_Generated: 2026-03-14T10:56:10.289311+00:00_
_System: dbt data transformation project_

## Five FDE Day-One Answers

### Q1. What is the primary data ingestion path? (trace from raw sources to first transformation)

The primary data ingestion path starts with raw sources like user_course_roles, platforms, legacy_edx_certificate_revision_mapping, and open_learning, then flows through staging models in the /home/meseret/Desktop/brownfield-cartographer/ol-data-platform/src/ol_dbt/models/staging/ directory, particularly the MITx Online, MITx residential, MITx pro, and edX.org tracking logs staging models which process user activity data.

### Q2. What are the 3-5 most critical output datasets or endpoints?

The most critical output datasets are the intermediate models like int__mitxonline__users.sql, int__mitx__programs.sql, and int__edxorg__mitx_user_activity.sql, plus the dimensional model dim_course_content.sql which consolidates course structure data from multiple platforms.

### Q3. What is the blast radius if the most critical module fails? (which downstream systems break)

If the most critical module /home/meseret/Desktop/brownfield-cartographer/ol-data-platform/src/ol_dbt/models/staging/mitxonline/stg__mitxonline__openedx__tracking_logs__user_activity.sql fails, all downstream systems that depend on MITx Online user activity data would break, including the intermediate int__mitxonline__users.sql model and any reporting or analytics that consume this data.

### Q4. Where is the business logic concentrated vs distributed? (which modules/files own the core rules)

Business logic appears concentrated in the staging models that process tracking logs and user activity data, with the most critical logic in the MITx Online, MITx residential, MITx pro, and edX.org tracking logs staging models, while transformation logic is more distributed across intermediate and dimensional models.

### Q5. What has changed most frequently in the last 30 days? (git velocity map — likely pain points)

The most frequently changing files in the last 30 days are the reporting models file /home/meseret/Desktop/brownfield-cartographer/ol-data-platform/src/ol_dbt/models/reporting/_reporting__models.yml and deployment scripts like dg_deployments/reconcile_edxorg_partitions.py, indicating these are likely pain points that require frequent updates.

## Evidence Summary

- Repository: `ol-data-platform`
- System type: dbt data transformation project
- Module graph nodes: `1106`
- Module graph edges: `902`
- Lineage datasets: `594`
- Lineage transformations: `589`
- Git analysis window: `30` days
- LLM-generated answers: `yes`

## Immediate Next Actions

1. Verify the top architectural hubs by navigating to their source files.
2. Validate upstream lineage for the highest-value sink datasets.
3. Inspect high-velocity files first — they're the most likely source of instability.
4. Review documentation drift flags before trusting any existing comments/docstrings.
5. Run `cartographer query <repo> --cartography-dir .cartography` to interactively explore.
