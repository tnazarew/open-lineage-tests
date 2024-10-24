python scripts/upload_file_to_gcs.py ~/Downloads/spark-3.5-bigquery-0.41.0.jar gs://open-lineage-e2e/dupa --credentials /home/dominik/dev/openlineage/gcp-open-lineage-testing-6d2aa0caca8e.json

exit_code=$?

if [ $exit_code -ne 0 ]; then
  echo "An error occurred during the upload process."
  exit $exit_code
else
echo "Script executed successfully."
# Continue with your workflow
fi
echo "uploaded_file=$(cat /tmp/SUCCESS)"