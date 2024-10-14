import argparse
import os
import json


def higher_than(v1, v2):
    if v2 is None:
        return True
    left = 1000000 * v1['major'] + 1000 * v1['minor'] + v1['patch']
    right = 1000000 * v2['major'] + 1000 * v2['minor'] + v2['patch']
    return left > right


def parse_id_field(data):
    if data.get('$id') is None:
        return None

    parts = data['$id'].split('/')
    version = parts[-2]
    name = parts[-1].strip('.json')
    major, minor, patch = version.split('-')
    return {name: {'major': major, 'minor': minor, 'patch': patch}}


def read_json_files(directory):
    json_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".json"):
                json_files.append(os.path.join(root, file))
    return json_files


def load_json(json_file):
    with open(json_file, 'r') as f:
        data = json.load(f)
    return data


def get_arguments():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('--spec_base', type=str, help="directory containing the reports")
    parser.add_argument('--versions_path', type=str,
                        help="comma separated list of directories containing spec and facets")
    parser.add_argument('--new_versions_path', type=str, help="directory storing producers")

    args = parser.parse_args()

    spec_base = args.spec_base
    versions_path = args.versions_path
    new_versions_path = args.new_versions_path

    return spec_base, versions_path, new_versions_path


def main():
    spec_base, versions_path, new_versions_path = get_arguments()
    latest = load_json(versions_path)

    parsed = [parsed for file in read_json_files(spec_base) if (parsed := parse_id_field(load_json(file)))]
    specs = {k: v for spec in parsed for k, v in spec.items()}
    if any(higher_than(v, latest.get(s)) for s, v in specs.items()):
        with open(new_versions_path, 'w') as f:
            json.dump(specs, f)


if __name__ == "__main__":
    main()
