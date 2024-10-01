import argparse
import json
import os

from jsonschema.exceptions import best_match, by_relevance
from os import listdir
from os.path import isfile, join, isdir
from jsonschema.validators import Draft202012Validator
from report import Test, Scenario, Component, Report


class OLValidator:
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

    def validate(self, instance, schema_type):
        schema_validator = self.schema_validators.get(schema_type)
        if schema_validator is not None:
            errors = [error for error in schema_validator.iter_errors(instance)]
            if len(errors) == 0:
                return []
            else:
                return [f"{(e := best_match([error], by_relevance())).json_path}: {e.message}" for error in errors]
        elif self.is_custom_facet(instance.get(schema_type)):
            return [f"$.{schema_type} facet type {schema_type} may be custom facet without available schema json file"]
        else:
            return [f"$.{schema_type} facet type {schema_type} not recognized"]


def load_json(path):
    with open(path) as f:
        return json.load(f)


def get_arguments():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('--event_base_dir', type=str, help="directory containing the reports")
    parser.add_argument('--spec_dirs', type=str, help="comma separated list of directories containing spec and facets")
    parser.add_argument('--component', type=str, help="component producing the validated events")
    parser.add_argument('--time', type=str, help="time of workflow execution", default="")

    args = parser.parse_args()

    event_base_dir = args.event_base_dir
    time = args.time
    component = args.component
    spec_dirs = args.spec_dirs.split(',')

    return event_base_dir, time, spec_dirs, component


def handle_ol_event(path, validator):
    with open(path) as f:
        data = json.load(f)
        validation_result = []
        run_validation = validator.validate(data, 'core')
        run = fun(data, validator, 'run')
        job = fun(data, validator, 'job')
        inputs = fun2(data, validator, 'inputs', 'facets')
        input_ifs = fun2(data, validator, 'inputs', 'inputFacets')
        outputs = fun2(data, validator, 'outputs', 'facets')
        output_ofs = fun2(data, validator, 'outputs', 'outputFacets')

        validation_result.extend(run_validation)
        validation_result.extend(run)
        validation_result.extend(job)
        validation_result.extend(inputs)
        validation_result.extend(input_ifs)
        validation_result.extend(outputs)
        validation_result.extend(output_ofs)

        return validation_result


def fun2(data, validator, entity, generic_facet_type):
    return [e.replace('$', f'$.{entity}[{ind}]') for ind, i in enumerate(data[entity]) for k, v in
            i[generic_facet_type].items() for e in validator.validate({k: v}, k)]


def fun(data, validator, entity):
    return [e.replace('$', f'$.{entity}') for k, v in data[entity]['facets'].items() for e in
            validator.validate({k: v}, k)]


def main():
    base_dir, time, spec_dirs, component = get_arguments()
    validator = OLValidator.load_schemas(paths=spec_dirs)
    scenarios = {}
    for s in listdir(base_dir):
        s_path = join(base_dir, s)
        if isdir(s_path):
            tests = {}
            for t in listdir(s_path):
                t_path = join(s_path, t)
                if isfile(t_path):
                    details = handle_ol_event(t_path, validator)
                    tests[t] = Test(t, "SUCCESS" if len(details) == 0 else "FAILURE", 'syntax', 'openlineage', details)
            scenarios[s] = Scenario(s, "FAILURE" if any(
                t.status == "FAILURE" for t in tests.values()) else "SUCCESS", tests)
    report = Report({component: Component(component, scenarios)})
    print(json.dumps(report.to_dict(), indent=2))


if __name__ == "__main__":
    main()
