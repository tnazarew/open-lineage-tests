import json
from os import listdir
from os.path import isfile, join


# def get_arguments():
#     parser = argparse.ArgumentParser(description="")
#     parser.add_argument('--time', type=str, help="credentials for GCP", default=None)
#     parser.add_argument('--consumer_dir', type=str, help="Path to the consumer directory", default=None)
#     parser.add_argument('--scenario_dir', type=str, help="Path to the scenario directory", default=None)
#     parser.add_argument('--parent', type=str, help="Parent identifier", default=None)
#
#     args = parser.parse_args()
#
#     client = LineageClient(credentials=Credentials.from_service_account_file(args.credentials))
#     consumer_dir = args.consumer_dir
#     scenario_dir = args.scenario_dir
#     parent = args.parent
#
#     return consumer_dir, scenario_dir, parent, client
class Report:
    def __init__(self, components):
        self.components = components

    @classmethod
    def from_dict(cls, d):
        return cls({s['name']: Component.from_dict(s) for s in d})

    def get_new_failures(self, old):
        oc = old.components if old is not None and old.components is not None else {}
        return Report({k: nfc for k, v in self.components.items() if
                (nfc := v.get_new_failures(oc.setdefault(k, None))) is not None})

    def to_dict(self):
        return self.components.values()


class Component:

    def __init__(self, name, scenarios):
        self.name = name
        self.scenarios = scenarios

    @classmethod
    def from_dict(cls, d):
        return cls(d['name'], {s['name']: Scenario.from_dict(s) for s in d['scenarios']})

    def get_new_failures(self, old):
        os = old.scenarios if old is not None and old.scenarios is not None else {}
        nfs = {k: nfs for k, v in self.scenarios.items() if
               (nfs := v.get_new_failures(os.setdefault(k, None))) is not None}
        return Component(self.name, nfs) if any(nfs) else None

    def to_dict(self):
        return self.__dict__


class Scenario:
    def __init__(self, name, status, tests):
        self.name = name
        self.status = status
        self.tests = tests

    @classmethod
    def from_dict(cls, d):
        return cls(d['name'], d['status'], {t['name']: Test.from_dict(t) for t in d['tests']})

    def get_new_failures(self, old):
        if self.status == 'SUCCESS':
            return None
        ot = old.tests if old is not None and old.tests is not None else {}
        nft = {k: nft for k, v in self.tests.items() if (nft := v.get_new_failure(ot.setdefault(k, None))) is not None}
        return Scenario(self.name, self.status, nft) if any(nft) else None


class Test:
    def __init__(self, name, status, validation_type, entity_type, details):
        self.name = name
        self.status = status
        self.validation_type = validation_type
        self.entity_type = entity_type
        self.details = details

    @classmethod
    def from_dict(cls, d):
        return cls(d['name'], d['status'], d['validation_type'], d['entity_type'],
                   d['details'] if d.__contains__('details') else [])

    def get_new_failure(self, old):
        if self.status == 'FAILURE':
            if old is None or old.status == 'SUCCESS' or any(
                    d for d in self.details if not old.details.__contains__(d)):
                return self
        return None


def main():
    # print(f"{listdir('reports')} \n")
    # partial_reports = [join('reports', f) for f in listdir('reports') if
    #                    isfile(join('reports', f)) and f != "report.json"]
    # print(partial_reports)
    # new_reports = [Component(json.load(open(file))) for file in partial_reports]
    # new_report=[]
    # for report in new_reports:
    #     new_report.extend(report)
    last_report = json.load(open(
        '/Users/tomasznazarewicz/projects/open-lineage-tests/consumer/consumers/Dataplex/validator/dataplex-report.json',
        'r'))
    new_report = Report.from_dict(last_report)
    old_report = Report.from_dict([])

    failures = new_report.get_new_failures(old_report)
    print(json.dumps(failures.__dict__))
    # compare_reports(new_report, last_report)
    # json.dump({}, open('reports/retention-failures-report.json', 'w'))
    # json.dump({}, open('reports/updated-report.json', 'w'))


if __name__ == "__main__":
    main()
