import json
import os

from google.api_core.exceptions import InvalidArgument
from google.oauth2.service_account import Credentials
from google.cloud.datacatalog_lineage_v1 import LineageClient
import proto
from ol_test import match
from google.protobuf.json_format import ParseDict
from google.protobuf import struct_pb2
import configparser


config = configparser.ConfigParser()
config.read('config.ini')
default = config['DEFAULT']

CONSUMER_DIR = default['consumerDir']
SCENARIO_DIR = default['scenarioDir']
PARENT = default['parent']
CLIENT = LineageClient(credentials=Credentials.from_service_account_file(default['credentialFileLocation']))


def list_scenarios():
    return [entry.name for entry in os.scandir(f"{CONSUMER_DIR}/scenarios") if entry.is_dir()]


def load_ol_events(scenario):
    return [{'name': entry.name, 'payload': ParseDict(json.load(open(entry.path, 'r')), struct_pb2.Struct())} for entry
            in os.scandir(
            f"{SCENARIO_DIR}/{scenario}/events") if entry.is_file()]


def load_validation_events(scenario):
    processes = json.load(open(f"{CONSUMER_DIR}/scenarios/{scenario}/validation/processes.json", 'r'))
    runs = json.load(open(f"{CONSUMER_DIR}/scenarios/{scenario}/validation/runs.json", 'r'))
    lineage_events = json.load(open(f"{CONSUMER_DIR}/scenarios/{scenario}/validation/lineage_events.json", 'r'))
    return processes, runs, lineage_events


def clean_up():
    processes = [x for x in CLIENT.list_processes(parent=PARENT)]
    for p in processes:
        CLIENT.delete_process(name=p.name)


def send_events(scenario):
    events = load_ol_events(scenario)
    for e in events:
        try:
            response = CLIENT.process_open_lineage_run_event(parent=PARENT, open_lineage=e['payload'])
            e['report'] = {"status": "SUCCESS", "process": response.process, "run": response.run,
                           "lineage_events": [x for x in response.lineage_events]}
        except InvalidArgument as exc:
            e['report'] = {"status": "FAILURE", "code": exc.code, "details": exc.args[0]}
    return events


def get_api_state():
    processes = [proto.Message.to_dict(p) for p in CLIENT.list_processes(parent=PARENT)]
    runs = [proto.Message.to_dict(r) for p in processes for r in CLIENT.list_runs(parent=p['name'])]
    lineage_events = [proto.Message.to_dict(e) for r in runs for e in CLIENT.list_lineage_events(parent=r['name'])]
    return processes, runs, lineage_events


def validate_api_state(scenario):
    processes_expected, runs_expected, events_expected = load_validation_events(scenario)
    processes_result, runs_result, events_result = get_api_state()
    process_validation = compare_process_or_run(processes_expected, processes_result)
    run_validation = compare_process_or_run(runs_expected, runs_result)
    event_validation = compare_lineage_events(events_expected, events_result)

    return process_validation, run_validation, event_validation


# processes and runs are matchable by entity name
def compare_process_or_run(expected, result):
    d = {}
    for e in expected:
        d[e['name']] = {}
        d[e['name']]['expected'] = e
    for r in result:
        if not d.keys().__contains__(r['name']):
            d[r['name']] = {}
        d[r['name']]['result'] = r
    for v in d.values():
        v['val'] = False, "no matching entity"
        if v.__contains__('expected') and v.__contains__('result'):
            v['val'] = match(v['expected'], v['result'])
    return d


# lineage events can't be matched by name, so they're matched by equal start and end time
def compare_lineage_events(expected, result):
    d = {}
    for r in result:
        d[r['name']] = {}
        d[r['name']]['result'] = r
    for e in expected:
        matching = next((r for r in result if e['start_time'] == r['start_time'] and e['end_time'] == r['end_time']),
                        None)
        if matching is not None:
            d[matching['name']]['expected'] = e
        else:
            d[e['name']] = {}
            d[e['name']]['expected'] = e

    for v in d.values():
        v['val'] = False, "no matching entity"
        if v.__contains__('expected') and v.__contains__('result'):
            v['val'] = match(v['expected']['links'], v['result']['links'])
    return d


def validate(scenario):
    clean_up()
    syntax_report = send_events(scenario)

    if not any(event['report']['status'] == "FAILURE" for event in syntax_report):
        processes, runs, events = validate_api_state(scenario)
        print([(x, events[x]['val']) for x in events.keys() if not events[x]['val'][0]])
    # report = compile_report(syntax_report)
    clean_up()


def main():
    scenarios = list_scenarios()
    for scenario in scenarios:
        validate(scenario)


if __name__ == "__main__":
    # main()
    print('a')
