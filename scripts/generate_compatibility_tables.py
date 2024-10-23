import argparse

from report import Report
import json

from py_markdown_table.markdown_table import markdown_table


def get_version_status(min_version, max_version):
    if min_version is None and max_version is None:
        return '+'
    elif min_version is not None and max_version is None:
        return f'above {min_version}'
    elif min_version is None and max_version is not None:
        return f'below {max_version}'
    else:
        return f'above {min_version}, below {max_version}'


def generate_facets_table(data):
    facets = set()

    # Collect all facets from all top-level keys
    for key, value in data.items():
        if 'facets' in value:
            facets.update(value['facets'].keys())

    # Prepare table header and rows
    table_data = []
    header = ['Name'] + sorted(facets)
    # table_data.append(dict(zip(header, header)))

    # Populate rows for each top-level key
    for key, value in data.items():
        row = {'Name': key}
        for facet in sorted(facets):
            if 'facets' in value and facet in value['facets']:
                facet_data = value['facets'][facet]
                status = get_version_status(facet_data['min_version'], facet_data['max_version'])
                row[facet] = status
            else:
                row[facet] = '-'
        table_data.append(row)

    table = markdown_table(table_data)
    table.set_params(row_sep="markdown", quote=False)
    return table.get_markdown()


def generate_lineage_table(data):
    producers = {}

    for key, value in data.items():
        if 'lineage_levels' in value:
            table_data = []
            for datasource, levels in value['lineage_levels'].items():
                row = {'Datasource': datasource}
                for level in ['dataset', 'column', 'transformation']:
                    if level in levels:
                        level_data = levels[level]
                        status = get_version_status(level_data['min_version'], level_data['max_version'])
                        row[level.capitalize()] = status
                    else:
                        row[level.capitalize()] = '-'
                table_data.append(row)
            table = markdown_table(table_data)
            table.set_params(row_sep="markdown", quote=False)
            producers[key] = table.get_markdown()
    return producers


def generate_producers_table(data):
    # Prepare table header and rows
    consumers = {}
    for key, value in data.items():
        if 'producers' in value:
            table_data = []
            for producer, producer_data in value['producers'].items():
                status = get_version_status(producer_data['min_version'], producer_data['max_version'])
                table_data.append({'Producer': producer, 'Version': status})
            table = markdown_table(table_data)
            table.set_params(row_sep="markdown", quote=False)
            consumers[key] = table.get_markdown()
    return consumers


def get_arguments():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('--report', type=str, help="path to report file")

    args = parser.parse_args()

    report_base_dir = args.report

    return report_base_dir


def main():
    report_path = get_arguments()
    with open(report_path, 'r') as c:
        report = json.load(c)
    consumer_report = Report.from_dict([e for e in report if e['component_type'] == 'consumer'])
    producer_report = Report.from_dict([e for e in report if e['component_type'] == 'producer'])
    consumer_tag_summary = consumer_report.get_tag_summary()
    producer_tag_summary = producer_report.get_tag_summary()
    consumer_facets_table = generate_facets_table(consumer_tag_summary)
    producers_tables = generate_producers_table(consumer_tag_summary)
    producer_facets_table = generate_facets_table(producer_tag_summary)
    lineage_level_tables = generate_lineage_table(producer_tag_summary)

    # Output the tables
    with open("output_tables.md", "w") as file:
        file.write("## Producers\n")
        file.write("## Facets Compatibility\n")
        file.write(producer_facets_table + "\n\n")

        for k, v in lineage_level_tables.items():
            file.write(f"## Lineage level support for {k}\n")
            file.write(v + "\n\n")

        file.write("## Consumers\n")
        file.write("## Facets Compatibility\n")
        file.write(consumer_facets_table + "\n\n")
        for k, v in producers_tables.items():
            file.write(f"## Producers support for {k}\n")
            file.write(v + "\n")


if __name__ == "__main__":
    main()
