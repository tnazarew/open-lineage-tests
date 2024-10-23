## Producers
## Facets Compatibility
|   Name   |run_event|jobType|parent|dataSource|processing_engine|sql|symlinks|schema|columnLineage|gcp_dataproc_spark|gcp_lineage|spark_properties|
|----------|---------|-------|------|----------|-----------------|---|--------|------|-------------|------------------|-----------|----------------|
|spark-mock|    +    |   +   |   +  |     +    |        +        | + |    +   |   +  |      +      |         +        |     +     |        +       |
| hive-mock|    +    |   -   |   -  |     -    |        -        | - |    -   |   -  |      -      |         -        |     -     |        -       |

## Lineage level support for spark-mock
|Datasource|Dataset|Column|Transformation|
|----------|-------|------|--------------|
|   hive   |   +   |   +  |       +      |
|   jdbc   |   +   |   +  |       -      |

## Lineage level support for hive-mock
|Datasource|Dataset|Column|Transformation|
|----------|-------|------|--------------|
|   hdfs   |   +   |   -  |       -      |

## Consumers
## Facets Compatibility
|    Name    |         run_event        |
|------------|--------------------------|
|marquez-mock|             -            |
|  dataplex  |above 1.22.0, below 1.23.0|

## Producers support for marquez-mock
|Producer|          Version         |
|--------|--------------------------|
|  spark |above 1.22.0, below 1.23.0|
## Producers support for dataplex
|Producer|          Version         |
|--------|--------------------------|
|  spark |above 1.22.0, below 1.23.0|
