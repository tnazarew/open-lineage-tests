import argparse
import json
import os

from google.api_core.exceptions import InvalidArgument
from google.oauth2.service_account import Credentials
from google.cloud.datacatalog_lineage_v1 import LineageClient
import proto
from ol_test import match
from google.protobuf.json_format import ParseDict
from google.protobuf import struct_pb2


class Validator:
    def __init__(self, client=None, consumer_dir=None, scenario_dir=None, parent=None):
        self.client = client
        self.consumer_dir = consumer_dir
        self.scenario_dir = scenario_dir
        self.parent = parent

    def load_ol_events(self, scenario):
        return [{'name': entry.name, 'payload': ParseDict(json.load(open(entry.path, 'r')), struct_pb2.Struct())}
                for entry in os.scandir(f"{self.scenario_dir}/{scenario}/events") if entry.is_file()]

    def load_validation_events(self, scenario):
        processes = json.load(open(f"{self.consumer_dir}/scenarios/{scenario}/validation/processes.json", 'r'))
        runs = json.load(open(f"{self.consumer_dir}/scenarios/{scenario}/validation/runs.json", 'r'))
        lineage_events = json.load(open(f"{self.consumer_dir}/scenarios/{scenario}/validation/lineage_events.json", 'r'))
        return processes, runs, lineage_events

    def send_ol_events(self, scenario):
        events = self.load_ol_events(scenario)
        report = []
        for e in events:
            try:
                response = self.client.process_open_lineage_run_event(parent=self.parent, open_lineage=e['payload'])
                report.append({"status": "SUCCESS", 'name': e['name']})
            except InvalidArgument as exc:
                report.append({"status": "FAILURE", 'validation': 'syntax', "details": exc.args[0], 'name': e['name']})
        return report

    def validate(self, scenario):
        self.clean_up()
        report = self.send_ol_events(scenario)
        if not any(r['status'] == "FAILURE" for r in report):
            report.extend( self.validate_api_state(scenario))
        for r in report:
            r['scenario'] = scenario
        self.clean_up()
        return report

    def get_api_state(self):
        processes = [proto.Message.to_dict(p) for p in self.client.list_processes(parent=self.parent)]
        runs = [proto.Message.to_dict(r) for p in processes for r in self.client.list_runs(parent=p['name'])]
        lineage_events = [proto.Message.to_dict(e) for r in runs for e in self.client.list_lineage_events(parent=r['name'])]
        return processes, runs, lineage_events

    def validate_api_state(self, scenario):
        processes_expected, runs_expected, events_expected = self.load_validation_events(scenario)
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
    def compare_process_or_run(self, expected, result, entity_type):
        d = {}
        for e in expected:
            d.setdefault(e['name'], {})['expected'] = e
        for r in result:
            d.setdefault(r['name'], {})['result'] = r
        results = []
        for k, v in d.items():
            result = ["no matching entity"]
            if v.__contains__('expected') and v.__contains__('result'):
                result = match(v['expected'], v['result'], "")
            results.append({'type': entity_type, 'status': 'SUCCESS' if len(result) == 0 else 'FAILURE',
                            'details': result, 'validation': 'semantics', 'name': k})
        return results

    # lineage events can't be matched by name, so they're matched by equal start and end time
    def compare_lineage_events(self, expected, result):
        d = {}
        for r in result:
            d.setdefault(r['name'], {})['result'] = r
        for e in expected:
            matching = next(
                (r for r in result if e['start_time'] == r['start_time'] and e['end_time'] == r['end_time']),
                None)
            if matching is not None:
                d[matching['name']]['expected'] = e
            else:
                d.setdefault(e['name'], {})['expected'] = e
        results = []
        for k, v in d.items():
            result = ["no matching entity"]
            if v.__contains__('expected') and v.__contains__('result'):
                result = match(v['expected']['links'], v['result']['links'], ".links")
            results.append({'type': 'event', 'status': 'SUCCESS' if len(result) == 0 else 'FAILURE',
                            'details': result, 'validation': 'semantics', 'name': k})
        return results

    def __repr__(self):
        return (f"MyClass(client={self.client}, consumer_dir={self.consumer_dir}, "
                f"scenario_dir={self.scenario_dir}, parent={self.parent})")


def list_scenarios(consumer_dir):
    return [entry.name for entry in os.scandir(f"{consumer_dir}/scenarios") if entry.is_dir()]


def get_arguments():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('--credentials', type=str, help="credentials for GCP", default=None)
    parser.add_argument('--consumer_dir', type=str, help="Path to the consumer directory", default=None)
    parser.add_argument('--scenario_dir', type=str, help="Path to the scenario directory", default=None)
    parser.add_argument('--parent', type=str, help="Parent identifier", default=None)

    args = parser.parse_args()

    client = LineageClient(credentials=Credentials.from_service_account_file(args.credentials))
    consumer_dir = args.consumer_dir
    scenario_dir = args.scenario_dir
    parent = args.parent

    return consumer_dir, scenario_dir, parent, client


def main():
    consumer_dir, scenario_dir, parent, client = get_arguments()
    validator = Validator(client, consumer_dir, scenario_dir, parent)
    scenarios = list_scenarios(consumer_dir)
    reports = [validator.validate(scenario) for scenario in scenarios]
    json.dump(reports, open('report.json', 'w'))


if __name__ == "__main__":
    main()


