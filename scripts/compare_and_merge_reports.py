import argparse
import json
from report import Report
from os import listdir
from os.path import isfile, join


def get_arguments():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('--report_base_dir', type=str, help="directory containing the reports")

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
    json.dump(old_report.to_dict(), open(join(base_dir, 'updated-report.json'), 'w'), indent=2)


if __name__ == "__main__":
    main()
