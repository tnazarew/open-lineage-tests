import json
import os
import argparse
import copy
from collections import defaultdict


def parse_arguments():
    """Parse command-line arguments for source and target paths."""
    parser = argparse.ArgumentParser(description="Process JSON facets and split into separate files.")
    parser.add_argument("source", help="Path to the source JSON file.")
    parser.add_argument("target", help="Path to the target output directory.")
    return parser.parse_args()


def load_json(file_path):
    """Load JSON data from a file."""
    with open(file_path, "r") as file:
        return json.load(file)


def create_output_base(data):
    """Create a base structure with common fields."""
    return {
        "eventType": data["eventType"],
        "job": {
            "namespace": data["job"]["namespace"],
            "name": data["job"]["name"]
        }
    }


def create_run_event(data, output_dir):
    """Create run_event.json with empty facet fields."""
    run_event_data = copy.deepcopy(data)

    # Empty the facets in each section
    run_event_data["run"]["facets"] = {}
    run_event_data["job"]["facets"] = {}

    for item in run_event_data.get("inputs", []):
        item["facets"] = {}
        item["inputFacets"] = {}
    for item in run_event_data.get("outputs", []):
        item["facets"] = {}
        item["outputFacets"] = {}

    write_facet_file(os.path.join(output_dir, "run_event_test.json"), run_event_data)


def collect_facets(section_data, section_name, facet_collection):
    """Collect facets from inputs or outputs and organize them in a dictionary."""
    for item in section_data:
        for facet_type in ["facets", "inputFacets", "outputFacets"]:
            if facet_type in item:
                for facet_name, facet_data in item[facet_type].items():
                    facet_info = {
                        "namespace": item["namespace"],
                        "name": item["name"],
                        facet_type: {
                            facet_name: facet_data
                        }
                    }
                    facet_collection[facet_name][section_name].append(facet_info)


def collect_single_facets(section_data, section_name, facet_collection):
    """Collect facets from run or job, treating them as single objects."""
    for facet_name, facet_data in section_data.items():
        facet_collection[facet_name][section_name] = {facet_name: facet_data}


def process_and_write_facets(facet_collection, output_base, output_dir):
    """Write aggregated facet data to separate files."""
    for facet_name, sections in facet_collection.items():
        output_data = output_base.copy()

        for section, content in sections.items():
            # Preserve arrays for inputs/outputs, but objects for run/job
            output_data[section] = content if isinstance(content, list) else {"facets": content}

        write_facet_file(os.path.join(output_dir, f"{facet_name}_test.json"), output_data)


def write_facet_file(file_path, data):
    """Write facet data to a JSON file."""
    with open(file_path, "w") as out_file:
        json.dump(data, out_file, indent=2)


def main():
    args = parse_arguments()
    source_file = args.source
    output_dir = args.target
    os.makedirs(output_dir, exist_ok=True)

    data = load_json(source_file)
    output_base = create_output_base(data)

    # Create run_event.json with empty facets
    create_run_event(data, output_dir)

    # Collect all facets from run, job, inputs, and outputs
    facet_collection = defaultdict(lambda: defaultdict(list))

    # Collect from run and job facets
    collect_single_facets(data["run"]["facets"], "run", facet_collection)
    collect_single_facets(data["job"]["facets"], "job", facet_collection)

    # Collect from inputs and outputs
    collect_facets(data.get("inputs", []), "inputs", facet_collection)
    collect_facets(data.get("outputs", []), "outputs", facet_collection)

    # Process and write facets to files
    process_and_write_facets(facet_collection, output_base, output_dir)

    print(f"Facet files and run_event.json have been created in the '{output_dir}' directory.")


if __name__ == "__main__":
    main()
