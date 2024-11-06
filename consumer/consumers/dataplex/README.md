# Dataplex

## Dataplex consumer

Dataplex consumer tests check the compatibility of the consumer with the OpenLineage events in two steps:
1.Send events to the consumer, check the response and in case of error report the error message
2.Check the state of the consumer API, compare it with the expected state using the `compare_events` script from common
scripts and report the differences

## Running locally

### Enable Dataplex in your project

Having dataplex enabled is required, check https://cloud.google.com/dataplex/docs/enable-api

### Get GCP credentials json

Before you run the script make sure that you have file json with google service account credentials
for more info go here: https://developers.google.com/workspace/guides/create-credentials

### Set Python version 3.11.*

To prevent any python version issues, ensure that you have python version `3.11.*`. You can set virtual environment with
pyenv, https://github.com/pyenv/pyenv?tab=readme-ov-file#installation

### Run the tests

To run dataproc tests locally use the command:

```commandline
./run_dataplex_tests.sh \
  --gcp-credentials-json-path path/to/google/credentials.json \
  --gcp-parent your/gcp/parent \
  --dump-api-state
```

gcp parent points to your dataplex instance, structure is `projects/<project-id>/locations/<location>`

This will generate dataplex-report.json file

### Get api state

If you would like to just get the API state into you can add `dump-api-state` flag, this will skip the sematic check and
just dump the API state to files

```commandline
./run_dataplex_tests.sh \
  --gcp-credentials-json-path path/to/google/credentials.json  \
  --consumer-dir path/to/consumer/dir \
  --scenario-dir path/to/scenario/dir \
  --gcp-parent projects/<project-id>/locations/<location> \
  --dump-api-state
```

