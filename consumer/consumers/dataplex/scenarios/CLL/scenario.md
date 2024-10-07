# Description
This test scenario expects that after sending OL events to Dataplex, following entities will be created
1 Process with `display_name` value `testColumnLevelLineage:open_lineage_integration_create_table`
1 Run with `display_name` value `019127df-0850-72bc-b214-b255290588a6`
4 LineageEvents with 
sources 
    `custom:file/tmp/cll_test/cll_source2`, `custom:file/tmp/cll_test/cll_source2`
target
    `custom:file/tmp/cll_test/tbl1`
