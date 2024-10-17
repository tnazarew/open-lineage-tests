# Usage
To run dataproc tests locally use the command:
```commandline
./run-spark-dataproc-tests.sh 
  --gcp-credentials-json-path <path to your gcs credentails json see https://developers.google.com/workspace/guides/create-credentials>  \
  --gcs-transport-jar-path < path to gcs transport jar see https://mvnrepository.com/artifact/io.openlineage/transports-gcs> \
  --openlineage-directory < path to your local openlineage repository containing spec https://github.com/OpenLineage/OpenLineage> \
  --gcp-project <your gcp project> \
  --gcp-region <your gcp region> \
  --gcs-bucket <gcs bucket the script will work with> \
  --dataproc-cluster-name <name of your cluster>
```
