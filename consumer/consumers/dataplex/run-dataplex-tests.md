# Prerequisites

### Dataplex enabled
Having dataplex enabled is required, https://cloud.google.com/dataplex/docs/enable-api

### GCP credentials json
Before you run the script make sure that you have file json with google service account credentials
for more info go here: https://developers.google.com/workspace/guides/create-credentials

### Python 3.11.*
To prevent any python version issues, ensure that you have python version `3.11.*`. You can set virtual environment with pyenv, https://github.com/pyenv/pyenv?tab=readme-ov-file#installation

# Usage
To run dataproc tests locally use the command:

```commandline

./run-dataplex-tests.sh \
  --gcp-credentials-json-path path/to/google/credentials.json  \
  --gcp-parent your/gcp/parent\
  --dump-api-state
  
```

gcp parent points to your dataplex instance, structure is `projects/<project-id>/locations/<location>`

```commandline

./run-dataplex-tests.sh \
  --gcp-credentials-json-path path/to/google/credentials.json  \
  --consumer-dir path/to/consumer/dir \
  --scenario-dir path/to/scenario/dir \
  --gcp-parent projects/<project-id>/locations/<location> \
  --dump-api-state
  
```

This will generate dataplex-report.json file