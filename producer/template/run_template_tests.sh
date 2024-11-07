#!/bin/bash

################################################################################
########## THIS IS NOT RUNNABLE EXAMPLE, JUST TEMPLATE FOR SCRIPT ##############
##############################################################

# Help message function
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --openlineage-directory PATH        Path to openlineage repository directory (required)"
    echo "  --producer-output-events-dir PATH   Path to producer output events directory (default: /path/to/default/events/dir)"
    echo "  --openlineage-release VERSION       OpenLineage release version (default: default-openlineage-release)"
    echo "  --report-path PATH                  Path to report directory (default: /path/to/default/report)"
    echo "  -h, --help                          Show this help message and exit"
    echo ""
    echo "Example:"
    echo "  $0 --openlineage-directory /path/to/specs --producer-output-events-dir /path/to/events --openlineage-release 1.23.0 --report-path /path/to/report"
    exit 0
}

# Required variables (no defaults)
OPENLINEAGE_DIRECTORY=""

# Variables with default values
PRODUCER_OUTPUT_EVENTS_DIR=output
OPENLINEAGE_RELEASE=1.23.0
REPORT_PATH="../spark_template_report.json"

# If -h or --help is passed, print usage and exit
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    usage
fi

# Parse command line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --openlineage-directory) OPENLINEAGE_DIRECTORY="$2"; shift ;;
        --producer-output-events-dir) PRODUCER_OUTPUT_EVENTS_DIR="$2"; shift ;;
        --openlineage-release) OPENLINEAGE_RELEASE="$2"; shift ;;
        --report-path) REPORT_PATH="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; usage ;;
    esac
    shift
done

# Check required arguments
if [[ -z "$OPENLINEAGE_DIRECTORY" ]]; then
    echo "Error: Missing required arguments."
    usage
fi



OL_SPEC_DIRECTORIES=$OPENLINEAGE_DIRECTORY/spec/,$OPENLINEAGE_DIRECTORY/spec/facets/,$OPENLINEAGE_DIRECTORY/spec/registry/gcp/dataproc/facets,$OPENLINEAGE_DIRECTORY/spec/registry/gcp/lineage/facets

# fail if scenarios are not defined in scenario directory
[[ $(ls scenarios | wc -l) -gt 0 ]] || { echo >&2 "NO SCENARIOS DEFINED IN scenarios"; exit 1; }

mkdir -p "$PRODUCER_OUTPUT_EVENTS_DIR"

echo "RUNNING TEST SCENARIOS"

################################################################################
#
# RUN PRODUCER TEST SCENARIOS
#
################################################################################

echo "EVENT VALIDATION"

pip install -r ../../scripts/requirements.txt

python ../../scripts/validate_ol_events.py \
--event_base_dir="$PRODUCER_OUTPUT_EVENTS_DIR" \
--spec_dirs="$OL_SPEC_DIRECTORIES" \
--target="$REPORT_PATH" \
--component="spark_dataproc" \
--producer_dir=.

echo "EVENT VALIDATION FINISHED"
echo "REPORT CREATED IN $REPORT_PATH"