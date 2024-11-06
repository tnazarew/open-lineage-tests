# Description
This test scenario expects that after sending OL events to Dataplex

following entities will be created:
- 1 Process with `display_name` value `testColumnLevelLineage:open_lineage_integration_create_table`
- 1 Run with `display_name` value `019299c5-73cf-7069-a70f-74eb8070b577`
- 4 LineageEvent with `start_time` values:
  - `2024-10-17T09:18:26.825Z`
  - `2024-10-17T09:18:28.643Z`
  - `2024-10-17T09:18:29.267Z`
  - `2024-10-17T09:18:27.719Z`
