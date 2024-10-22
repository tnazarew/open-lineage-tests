#TODO add mention of setting up python 3.11
#TODO add mention about already having dataplex setup

python -m pip install --upgrade pip
pip install flake8 pytest
if [ -f consumer/consumers/dataplex/validator/requirements.txt ]; then pip install -r consumer/consumers/dataplex/validator/requirements.txt; fi


python consumer/consumers/dataplex/validator/validator.py \
  --credentials ${{ steps.gcp-auth.outputs.credentials_file_path }} \
  --consumer_dir consumer/consumers/dataplex \
  --scenario_dir consumer/scenarios/ \
  --parent projects/gcp-open-lineage-testing/locations/us