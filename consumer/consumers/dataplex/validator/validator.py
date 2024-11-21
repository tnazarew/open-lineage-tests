import argparse
import json
import os
import time
from os.path import join

from proto import Message

from google.api_core.exceptions import InvalidArgument
from google.oauth2.service_account import Credentials
from google.cloud.datacatalog_lineage_v1 import LineageClient, SearchLinksRequest
from compare_events import match
from google.protobuf.json_format import ParseDict
from google.protobuf import struct_pb2
from scripts.compare_releases import release_between


class Validator:
    def __init__(self, client=None, consumer_dir=None, scenario_dir=None, parent=None, release=None):
        self.client = client
        self.consumer_dir = consumer_dir
        self.scenario_dir = scenario_dir
        self.parent = parent
        self.release = release

    def load_ol_events(self, scenario):
        return [{'name': entry.name, 'payload': ParseDict(json.load(open(entry.path, 'r')), struct_pb2.Struct())}
                for entry in os.scandir(f"{self.scenario_dir}/{scenario}/events") if entry.is_file()]

    def load_validation_events(self, scenario, config):
        d = {}
        scenario_dir = join(self.consumer_dir, "scenarios", scenario)
        for e in config['tests']:
            if release_between(self.release, e['tags'].get('min_version'), e['tags'].get('max_version')):
                name = e['name']
                path = e['path']
                entity = e['entity']
                tags = e['tags']
                d[name] = {'body': json.load(open(join(scenario_dir, path), 'r')), 'entity': entity, 'tags': tags}

        processes = {k: v for k, v in d.items() if v['entity'] == "process"}
        runs = {k: v for k, v in d.items() if v['entity'] == "run"}
        lineage_events = {k: v for k, v in d.items() if v['entity'] == "lineage_event"}
        return processes, runs, lineage_events

    def dump_api_state(self, scenario):
        dump_dir = join(self.consumer_dir, "scenarios", scenario, "api_state")
        processes_state, runs_state, events_state = self.get_api_state()
        try:
            os.mkdir(dump_dir)
        except FileExistsError:
            pass
        except PermissionError:
            print(f"Permission denied: Unable to create '{dump_dir}'.")
        except Exception as e:
            print(f"An error occurred: {e}")

        with open(join(dump_dir, "processes.json"), 'w') as f:
            json.dump(processes_state, f)
        with open(join(dump_dir, "runs.json"), 'w') as f:
            json.dump(runs_state, f)
        with open(join(dump_dir, "lineage_events.json"), 'w') as f:
            json.dump(events_state, f)

    def send_ol_events(self, scenario):
        events = self.load_ol_events(scenario)
        report = []
        for e in events:
            try:
                response = self.client.process_open_lineage_run_event(parent=self.parent, open_lineage=e['payload'])
                report.append(
                    {"status": "SUCCESS", 'validation_type': 'syntax', 'name': e['name'], 'entity_type': 'openlineage',
                     'tags': {}})
                time.sleep(0.1)
            except InvalidArgument as exc:
                report.append(
                    {"status": "FAILURE", 'validation_type': 'syntax', "details": exc.args[0], 'name': e['name'],
                     'entity_type': 'openlineage', 'tags': {}})
        return report

    def read_config(self, scenario):
        with open(join(self.consumer_dir, 'scenarios', scenario, "config.json"), 'r') as f:
            return json.load(f)

    def validate(self, scenario, dump):
        config = self.read_config(scenario)
        self.clean_up()
        report = self.send_ol_events(scenario)
        if not any(r['status'] == "FAILURE" for r in report):
            if dump:
                self.dump_api_state(scenario)
            else:
                report.extend(self.validate_api_state(scenario, config))

        self.clean_up()
        return {"name": scenario,
                "status": 'FAILURE' if any(r['status'] == "FAILURE" for r in report) else 'SUCCESS',
                "tests": report}

    def get_api_state(self):
        processes = [Message.to_dict(p) for p in self.client.list_processes(parent=self.parent)]
        runs = [Message.to_dict(r) for p in processes for r in self.client.list_runs(parent=p['name'])]
        lineage_events = [Message.to_dict(e) for r in runs for e in self.client.list_lineage_events(parent=r['name'])]

        links = []

        for le in lineage_events:
            for l in le["links"]:
                page_result = self.client.search_links(request=SearchLinksRequest(
                    source=l["source"], target=l["target"], parent=self.parent))
                for resp in page_result:
                    links.append(resp)



        return processes, runs, lineage_events

    def validate_api_state(self, scenario, config):
        processes_expected, runs_expected, events_expected = self.load_validation_events(scenario, config)
        processes_state, runs_state, events_state = self.get_api_state()
        report = []
        report.extend(self.compare_process_or_run(processes_expected, processes_state, 'process'))
        report.extend(self.compare_process_or_run(runs_expected, runs_state, 'run'))
        report.extend(self.compare_lineage_events(events_expected, events_state))

        return report

    def clean_up(self):
        processes = [x for x in self.client.list_processes(parent=self.parent)]
        for p in processes:
            self.client.delete_process(name=p.name)

    # processes and runs are matchable by entity name
    @staticmethod
    def compare_process_or_run(expected, result, entity_type):
        results = []
        for k, v in expected.items():
            details = []
            for exp in v['body']:
                entity_name = exp['name'].rsplit('/', 1)
                matched = next((proc for proc in result if proc['name'] == exp['name']), None)
                if matched is not None:
                    res = match(exp, matched, "")
                    details.extend([f"{entity_type} {entity_name}, {r}" for r in res])
                else:
                    details.append(f"{entity_type} {entity_name}, no matching entity")
            results.append({'entity_type': entity_type, 'status': 'SUCCESS' if len(details) == 0 else 'FAILURE',
                            'details': details, 'validation_type': 'semantics', 'name': k, 'tags': v['tags']})
        return results

    # lineage events can't be matched by name, so they're matched by equal start and end time
    @staticmethod
    def compare_lineage_events(expected, result):
        results = []
        for k, v in expected.items():
            details = []
            for exp in v['body']:
                entity_name = exp['start_time']
                matched = next((r for r in result if exp['start_time'] == r['start_time']), None)
                if matched is not None:
                    res = match(exp, matched, "")
                    details.extend([f"lineage event {entity_name}, {r}" for r in res])
                else:
                    details.append(f"event {entity_name}, no matching entity")
            results.append({'entity_type': 'event', 'status': 'SUCCESS' if len(details) == 0 else 'FAILURE',
                            'details': details, 'validation_type': 'semantics', 'name': k, 'tags': v['tags']})
        return results

    def __repr__(self):
        return (f"MyClass(client={self.client}, consumer_dir={self.consumer_dir}, "
                f"scenario_dir={self.scenario_dir}, parent={self.parent})")


def list_scenarios(consumer_dir):
    return [entry.name for entry in os.scandir(f"{consumer_dir}/scenarios") if entry.is_dir()]


def get_arguments():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('--credentials', type=str, help="credentials for GCP")
    parser.add_argument('--consumer_dir', type=str, help="Path to the consumer directory")
    parser.add_argument('--scenario_dir', type=str, help="Path to the scenario directory")
    parser.add_argument('--parent', type=str, help="Parent identifier")
    parser.add_argument('--release', type=str, help="OpenLineage release used in generating events")
    parser.add_argument("--dump", action='store_true', help="dump api state")

    args = parser.parse_args()

    credentials = Credentials.from_service_account_file(args.credentials)
    client = LineageClient(credentials=credentials)
    # dataplex_client = DataplexServiceClient(credentials=credentials)
    # metadata_service_client = MetadataServiceClient(credentials=credentials)

    consumer_dir = args.consumer_dir
    scenario_dir = args.scenario_dir
    parent = args.parent
    release = args.release
    dump = args.dump

    return consumer_dir, scenario_dir, parent, client, release, dump


def main():
    consumer_dir, scenario_dir, parent, client, release, dump = get_arguments()
    validator = Validator(client, consumer_dir, scenario_dir, parent, release)
    validator.validate("spark_dataproc_simple_producer_test", dump)




    # scenarios = list_scenarios(consumer_dir)
    # reports = [validator.validate(scenario, dump) for scenario in scenarios]
    # t = open('dataplex-report.json', 'w')
    # print(os.path.abspath(t.name))
    # json.dump([{"name": "dataplex", "component_type": "consumer", "scenarios": reports}], t, indent=2)


if __name__ == "__main__":
    main()
