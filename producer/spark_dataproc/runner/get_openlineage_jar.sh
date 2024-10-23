#!/bin/bash

# Installs OpenLineage jar
# onto a Cloud Dataproc cluster.

set -euxo pipefail

readonly VM_SPARK_JARS_DIR=/usr/lib/spark/jars
readonly OPENLINEAGE_SPARK_URL=$(/usr/share/google/get_metadata_value attributes/openlineage-spark-url || true)

if [[ -d ${OPENLINEAGE_SPARK_URL} ]]; then
    jar_url=${OPENLINEAGE_SPARK_URL}
else
    jar_url=gs://open-lineage-e2e/jars/openlineage-spark_2.12-1.23.0.jar
fi

gsutil cp -P "${jar_url}" "${VM_SPARK_JARS_DIR}/"
