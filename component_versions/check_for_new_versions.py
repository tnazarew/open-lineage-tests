import json
import os


def main():
    print(f"JSON file found at: {os.path.abspath('component_versions/versions.json')}")
    with open('component_versions/versions.json', 'r') as json_file:
        data = json.load(json_file)

    with open('list', 'w') as output_file:
        for e in data:
            if should_run(e):
                output_file.writelines(f"{e['name']}=true")


def should_run(e):
    return e['latest-version'] == ""


if __name__ == "__main__":
    main()

