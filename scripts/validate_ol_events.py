import argparse
import json
import os

from jsonschema.exceptions import best_match, by_relevance
from os import listdir
from os.path import isfile, join, isdir
from jsonschema import Draft202012Validator
from report import Test, Scenario, Component, Report
from compare_events import match


class OLSyntaxValidator:
    def __init__(self, schema_validators):
        self.schema_validators = schema_validators

    @staticmethod
    def is_custom_facet(facet):
        if facet.get('_schemaURL') is not None:
            return any(facet.get('_schemaURL').__contains__(f'defs/{facet_type}Facet') for facet_type in
                       ['Run', 'Job', 'Dataset', 'InputDataset', 'OutputDataset'])

    @classmethod
    def load_schemas(cls, paths):
        file_paths = [p for path in paths for file in listdir(path) if isfile((p := join(path, file)))]

        facet_schemas = [load_json(path) for path in file_paths if path.__contains__('Facet.json')]
        spec_schema = next(load_json(path) for path in file_paths if path.__contains__('OpenLineage.json'))

        schema_validators = {next(iter(schema['properties'])): Draft202012Validator(schema) for schema in facet_schemas}
        schema_validators['core'] = Draft202012Validator(spec_schema)
        return cls(schema_validators)

    def validate_entity(self, instance, schema_type):
        schema_validator = self.schema_validators.get(schema_type)
        if schema_validator is not None:
            errors = [error for error in schema_validator.iter_errors(instance)]
            if len(errors) == 0:
                return []
            else:
                return [f"{(e := best_match([error], by_relevance())).json_path}: {e.message}" for error in errors]
        elif self.is_custom_facet(instance.get(schema_type)):
            # facet type may be custom facet without available schema json file (defined only as class)
            return []
        else:
            return [f"$.{schema_type} facet type {schema_type} not recognized"]

    def validate(self, event):
        validation_result = []
        run_validation = self.validate_entity(event, 'core')
        run = self.validate_entity_map(event, 'run')
        job = self.validate_entity_map(event, 'job')
        inputs = self.validate_entity_array(event, 'inputs', 'facets')
        input_ifs = self.validate_entity_array(event, 'inputs', 'inputFacets')
        outputs = self.validate_entity_array(event, 'outputs', 'facets')
        output_ofs = self.validate_entity_array(event, 'outputs', 'outputFacets')

        validation_result.extend(run_validation)
        validation_result.extend(run)
        validation_result.extend(job)
        validation_result.extend(inputs)
        validation_result.extend(input_ifs)
        validation_result.extend(outputs)
        validation_result.extend(output_ofs)

        return validation_result

    def validate_entity_array(self, data, entity, generic_facet_type):
        return [e.replace('$', f'$.{entity}[{ind}]') for ind, i in enumerate(data[entity]) for k, v in
                i[generic_facet_type].items() for e in self.validate_entity({k: v}, k)]

    def validate_entity_map(self, data, entity):
        return [e.replace('$', f'$.{entity}') for k, v in data[entity]['facets'].items() for e in
                self.validate_entity({k: v}, k)]


class OLSemanticValidator:
    def __init__(self, expected_events):
        self.expected_events = expected_events

    def validate(self, events):
        tests = {}
        for name, event, tags in self.expected_events:
            details = self.validate_event(event, events)
            if details is None:
                details = ['one or more of .eventType, .job.name, .job.namespace not defined in expected event']
            named_details = [f"'{name}' {detail}" for detail in details]
            tests[name] = Test.simplified(name, 'semantics', 'openlineage', named_details, tags)
        return tests

    @staticmethod
    def validate_event(ee, events):
        if 'job' in ee and 'eventType' in ee and 'name' in ee['job'] and 'namespace' in ee['job']:
            found = [
                f"event with .eventType: {ee['eventType']}, .job.name: {ee['job']['name']} and .job.namespace: {ee['job']['namespace']} not found in result events"]
            for e in events.values():
                event_types_match = e['eventType'] == ee['eventType']
                names_match = e['job']['name'] == ee['job']['name']
                namespaces_match = e['job']['namespace'] == ee['job']['namespace']
                if event_types_match and names_match and namespaces_match and len(found) > 0:
                    found = match(ee, e)
            return found
        return None


def load_json(path):
    with open(path) as f:
        return json.load(f)


def get_event_identification(event, default):
    if 'job' in event and 'eventType' in event and 'name' in event['job'] and 'namespace' in event['job']:
        return f"{event['job']['namespace']}:{event['job']['name']}:{event['eventType']}"
    else:
        return default


def all_tests_succeeded(syntax_tests):
    return not any(t.status == "FAILURE" for t in syntax_tests.values())


def version_to_number(version):
    split = version.split('.')
    major = int(split[0])
    minor = int(split[0])
    patch = int(split[0])
    return major * 1000000 + minor * 1000 + patch


def release_between(release, min_version, max_version):
    max_ver = 999999999 if max_version is None else version_to_number(max_version)
    min_ver = 0 if min_version is None else version_to_number(min_version)
    rel = version_to_number(release)

    return min_ver <= rel <= max_ver


def get_expected_events(producer_dir, component, scenario_name, config, release):
    test_events = []
    for test in config['tests']:
        if release_between(release, test['tags'].get('min_version'), test['tags'].get('max_version')):
            filepath = join(producer_dir, component, 'scenarios', scenario_name, test['path'])
            body = load_json(filepath)
            test_events.append((test['name'], body, test['tags']))
    return test_events


def validate_scenario_syntax(result_events, validator):
    syntax_tests = {}
    for name, event in result_events.items():
        details = validator.validate(event)
        identification = get_event_identification(event, name)
        syntax_tests[identification] = Test(identification, "FAILURE" if len(details) > 0 else "SUCCESS",
                                            'syntax', 'openlineage', details, {})
    return syntax_tests


def get_config(producer_dir, component, scenario_name):
    with open(join(producer_dir, component, 'scenarios', scenario_name, 'config.json')) as f:
        return json.load(f)


def get_arguments():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('--event_base_dir', type=str, help="directory containing the reports")
    parser.add_argument('--spec_dirs', type=str, help="comma separated list of directories containing spec and facets")
    parser.add_argument('--producer_dir', type=str, help="directory storing producers")
    parser.add_argument('--component', type=str, help="component producing the validated events")
    parser.add_argument('--release', type=str, help="OpenLineage release used in generating events")
    parser.add_argument('--target', type=str, help="target file")

    args = parser.parse_args()

    event_base_dir = args.event_base_dir
    producer_dir = args.producer_dir
    target = args.target
    component = args.component
    release = args.release
    spec_dirs = args.spec_dirs.split(',')

    return event_base_dir, producer_dir, target, spec_dirs, component, release


def main():
    base_dir, producer_dir, target, spec_dirs, component, release = get_arguments()
    validator = OLSyntaxValidator.load_schemas(paths=spec_dirs)
    scenarios = {}
    for scenario_name in listdir(base_dir):
        scenario_path = join(base_dir, scenario_name)
        if isdir(scenario_path):
            config = get_config(producer_dir, component, scenario_name)
            expected = get_expected_events(producer_dir, component, scenario_name, config, release)
            result_events = {file: load_json(path) for file in listdir(scenario_path) if
                             isfile(path := join(scenario_path, file))}
            tests = validate_scenario_syntax(result_events, validator)

            if all_tests_succeeded(tests) and expected is not None and not component == 'scenarios':
                for name, res in OLSemanticValidator(expected).validate(result_events).items():
                    tests[name] = res

            scenarios[scenario_name] = Scenario.simplified(scenario_name, tests)
    report = Report({component: Component(component, 'producer', scenarios)})
    with open(target, 'w') as f:
        json.dump(report.to_dict(), f, indent=2)


if __name__ == "__main__":
    main()