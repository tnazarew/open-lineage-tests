# Generated files

## Report

Most important output of the test suite, contains the results of the tests for each component.

```json
[
  {
    "name": "name",
    "component_type": "component_type",
    "scenarios": [
      {
        "name": "name",
        "status": "SUCCESS",
        "tests": [
          {
            "name": "name",
            "status": "SUCCESS",
            "validation_type": "semantics",
            "entity_type": "openlineage",
            "details": [],
            "tags": {
              "facets": [
                "run_event"
              ],
              "lineage_level": {
                "source": [
                  "dataset", 
                  "column",
                  "transformation"
                ]
              }
            }
          }
        ]
      }
    ]
  }
]
```

## Compatibility table

Contains compatibility tables for producers and consumers.
For both producers and consumers there is a table for facets compatibility which contains information about which facets are supported by the component in which release of OpenLineage.

Additionally each component has its own table.

For producers it contains information about lineage level support for each datasource.

For consumers it contains information about compatibility with producers.

## Releases

Contains a list of all components and their latest tested release. During automatic run latest releases of components are checked against the list to see if new release has been made, if so the tests are run for updated component and the list is updated.

## Spec Versions

Contains list of tested OpenLineage spec and facets versions. During automatic run spec versions on the main branch of OpenLineage are checked against the list to see if new version has been made, if so the tests are run for updated spec version and the list is updated.