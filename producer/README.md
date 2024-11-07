# Producer tests

## Producers

Each directory represents a producer and contains:

- `runner` directory with the validation logic (unlike producers where produced Openlineage events can be validated
  by generic component)
- `maintainers.json` file with the list of people responsible for the component
- `scenarios` directory containing scenario directories with following structure:
    - `config.json` file with the scenario configuration
    - `scenario.md` file with description of the scenario
    - `maintainers.json` file with the list of people responsible for the scenario

#### config

config file contains the metadata of the tests

```json
{
  "tests": [
    {
      "name": "name",
      "path": "path/to/file.json",
      "tags": {
        "facets": [
          "list",
          "of",
          "supported",
          "facets"
        ],
        "max_version": "9.99.9",
        "min_version": "0.0.1",
        "lineage_level": {
          "bigquery": [
            "dataset",
            "column",
            "transformation"
          ]
        }
      }
    }
  ]
}
```

Each entry in the config file represents a single test. `name`, `path` and `entity` are fields used to identify the
test.
The `tags` field is used to specify the different kinds of tags `min_version`, `max_version` are filtering tags,
if the version of OpenLineage used in tests in outside of the range specified by these values, the test will be skipped.
`producer` and `facets` are tags used to define what part of OpenLineage compatibility is being tested i.e. which facets
are supported by the consumer and how extensive the data lineage is supported.