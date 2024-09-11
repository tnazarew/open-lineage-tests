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


def main():
    print(f"{listdir('reports')} \n")
    partial_reports = [join('reports', f) for f in listdir('reports') if
                       isfile(join('reports', f) and f != "report.json")]
    print(partial_reports)
    last_report = open('reports/report.json', 'r')
    json.dump({}, open('reports/retention-failures-report.json', 'w'))
    json.dump({}, open('reports/updated-report.json', 'w'))


if __name__ == "__main__":
    main()
