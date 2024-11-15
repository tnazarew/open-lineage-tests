# Producers
## Facets Compatibility
|     Name     |run_event|jobType|parent|        dataSource        |processing_engine|sql|symlinks|          schema          |       columnLineage      |gcp_dataproc_spark|gcp_lineage|spark_properties|
|--------------|---------|-------|------|--------------------------|-----------------|---|--------|--------------------------|--------------------------|------------------|-----------|----------------|
|  spark-mock  |    +    |   +   |   +  |             +            |        +        | + |    +   |             +            |             +            |         +        |     +     |        +       |
|   hive-mock  |    +    |   -   |   -  |             -            |        -        | - |    -   |             -            |             -            |         -        |     -     |        -       |
|spark_dataproc|    -    |   -   |   -  |above 1.22.0, below 1.23.0|        -        | - |    -   |above 1.22.0, below 1.23.0|above 1.22.0, below 1.23.0|         -        |     -     |        -       |

## Lineage level support for spark-mock
|Datasource|Dataset|Column|Transformation|
|----------|-------|------|--------------|
|   hive   |   +   |   +  |       +      |
|   jdbc   |   +   |   +  |       -      |

## Lineage level support for hive-mock
|Datasource|Dataset|Column|Transformation|
|----------|-------|------|--------------|
|   hdfs   |   +   |   -  |       -      |

## Lineage level support for spark_dataproc
|Datasource|          Dataset         |          Column          |      Transformation      |
|----------|--------------------------|--------------------------|--------------------------|
| bigquery |above 1.22.0, below 1.23.0|above 1.22.0, below 1.23.0|above 1.22.0, below 1.23.0|

# Consumers
## Facets Compatibility
|    Name    |
|------------|
|marquez-mock|

## Producers support for marquez-mock
|Producer|          Version         |
|--------|--------------------------|
|  spark |above 1.22.0, below 1.23.0|
