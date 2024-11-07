#!/bin/bash

# Define base path (can be provided as an argument, default is current directory)
BASE_PATH="${1:-.}"
missing_count=0

# Validation functions

# Validate consumer config.json structure
validate_consumer_config() {
    local config_file="$1"
    local valid_structure=$(jq -e '
        .tests and
        (all(.tests[]; .name and .path and .entity and
            (.tags | .facets and .producer)))' "$config_file")

    if [[ "$valid_structure" != "true" ]]; then
        echo "Invalid consumer config structure in: $config_file"
        missing_count=$((missing_count + 1))
    fi
}

# Validate producer config.json structure
validate_producer_config() {
    local config_file="$1"
    local valid_structure=$(jq -e '
        .tests and
        (all(.tests[]; .name and .path and
            (.tags | .facets and .lineage_level)))' "$config_file")

    if [[ "$valid_structure" != "true" ]]; then
        echo "Invalid producer config structure in: $config_file"
        missing_count=$((missing_count + 1))
    fi
}

# Search and validate consumer config files
for config_file in "$BASE_PATH"/consumer/consumers/*/scenarios/*/config.json; do
    [[ -f "$config_file" ]] || continue
    validate_consumer_config "$config_file"
done

# Search and validate producer config files
for config_file in "$BASE_PATH"/producer/*/scenarios/*/config.json; do
    [[ -f "$config_file" ]] || continue
    validate_producer_config "$config_file"
done

# Summary of results
if [[ $missing_count -eq 0 ]]; then
    echo "All config.json files have valid structures."
    exit 0
else
    echo "$missing_count config.json files have invalid structures."
    exit 1
fi
