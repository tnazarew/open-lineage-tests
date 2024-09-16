import argparse
import json
from os import listdir
from os.path import isfile, join


class Report:
    def __init__(self, components):
        self.components = components

    @classmethod
    def from_dict(cls, d):
        return cls({s['name']: Component.from_dict(s) for s in d})

    def get_new_failures(self, old):
        oc = old.components if old is not None and old.components is not None else {}
        return Report({k: nfc for k, v in self.components.items() if
                       (nfc := v.get_new_failures(oc.get(k))) is not None})

    def update(self, new):
        if len(self.components) == 0:
            self.components = new.components
        else:
            for k, v in new.components.items():
                if self.components.keys().__contains__(k):
                    self.components[k].update(v)
                else:
                    self.components[k] = v

    def to_dict(self):
        return [c.to_dict() for c in self.components.values()]


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
               (nfs := v.get_new_failures(os.get(k))) is not None}
        return Component(self.name, nfs) if any(nfs) else None

    def update(self, new):
        for k, v in new.scenarios.items():
            if self.scenarios.keys().__contains__(k):
                self.scenarios[k].update(v)
            else:
                self.scenarios[k] = v

    def to_dict(self):
        return {'name': self.name, 'scenarios': [c.to_dict() for c in self.scenarios.values()]}


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
        nft = {k: nft for k, v in self.tests.items() if (nft := v.get_new_failure(ot.get(k))) is not None}
        return Scenario(self.name, self.status, nft) if any(nft) else None

    def update(self, new):
        self.status = new.status
        for k, v in new.tests.items():
            if self.tests.keys().__contains__(k):
                self.tests[k].update(v)
            else:
                self.tests[k] = v

    def to_dict(self):
        return {'name': self.name, 'status': self.status, 'tests': [t.to_dict() for t in self.tests.values()]}


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

    def update(self, new):
        self.status = new.status
        self.details = new.details

    def to_dict(self):
        return {"name": self.name, "status": self.status, "validation_type": self.validation_type,
                "entity_type": self.entity_type, "details": self.details}


def get_arguments():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('--report_base_dir', type=str, help="directory containing the reports")
    parser.add_argument('--time', type=str, help="time of workflow execution", default="")

    args = parser.parse_args()

    report_base_dir = args.report_base_dir
    time = args.time

    return report_base_dir, time


def get_new_report(new_report_paths):
    reports = []
    for path in new_report_paths:
        reports.extend(json.load(open(path)))
    return Report.from_dict(reports)


def main():
    base_dir, time = get_arguments()
    new_report_paths = [path for f in listdir(base_dir) if
                        isfile((path := join(base_dir, f))) and f.__contains__("-report.json") and f != "report.json"]
    old_report_path = join(base_dir, "report.json")
    new_report = get_new_report(new_report_paths)
    old_report = Report.from_dict(json.load(open(old_report_path, 'r')))

    failures = new_report.get_new_failures(old_report)
    old_report.update(new_report)

    json.dump(failures.to_dict(), open(join(base_dir, 'retention-failures-report.json'), 'w'))
    json.dump(old_report.to_dict(), open(join(base_dir, 'updated-report.json'), 'w'))


if __name__ == "__main__":
    main()
