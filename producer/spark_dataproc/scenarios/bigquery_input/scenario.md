# Description
Scenario contains a spark job that reads from a bigquery table, filters the data, aggregates it and writes to a local file. 

# Entities

input entity is bigquery table 

`bigquery-public-data.samples.shakespeare`

output entity local csv file

`/tmp/my_shakespeare_output`

# Facets
Facets present in the events:
- ColumnLineageDatasetFacet
- DatasourceDatasetFacet
- JobTypeJobFacet
- LifecycleStateChangeDatasetFacet
- ParentRunFacet
- SQLJobFacet
- SchemaDatasetFacet
- SymlinksDatasetFacet
