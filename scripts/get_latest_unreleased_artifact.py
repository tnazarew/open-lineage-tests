import argparse
import sys
from os.path import join

import requests

BASE_URL = 'https://circleci.com/api/v2'
PROJECT_SLUG = 'gh/OpenLineage/OpenLineage'
WORKFLOW_NAME_TO_SEARCH = 'build'
JOB_NAMES_TO_SEARCH = ['release-integration-spark', 'release-integration-spark-extension-interfaces',
                       'release-client-java', 'release-integration-flink']


def get_pipelines(branch='main', page_token=None):
    url = f'{BASE_URL}/project/{PROJECT_SLUG}/pipeline?branch={branch}'
    if page_token:
        url += f"&page-token={page_token}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json().get('items', []), response.json().get('next_page_token')


def get_workflows(pipeline_id):
    return [wf for wf in get_from_url(f'{BASE_URL}/pipeline/{pipeline_id}/workflow') if
            wf['name'] == WORKFLOW_NAME_TO_SEARCH and wf['status'] == 'success']


def get_jobs_in_workflow(workflow_id):
    return [job for job in get_from_url(f'{BASE_URL}/workflow/{workflow_id}/job') if job['name'] in JOB_NAMES_TO_SEARCH]


def get_artifacts(job_number):
    artifacts = get_from_url(f'{BASE_URL}/project/{PROJECT_SLUG}/{job_number}/artifacts')
    return [art for art in artifacts if art['path'].endswith('SNAPSHOT.jar')]


def get_from_url(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json().get('items', [])


def get_latest_successful_workflow():
    page_token = None
    while True:
        pipelines, page_token = get_pipelines(page_token=page_token)
        for pipeline in pipelines:
            workflows = get_workflows(pipeline['id'])
            if len(workflows) > 0:
                return workflows[0]
        if not page_token:
            return None


def get_artifacts_list(workflow):
    jobs = [job for job in get_jobs_in_workflow(workflow['id']) if job['name'] in JOB_NAMES_TO_SEARCH]
    artifact_urls = [art['url'] for job in jobs for art in get_artifacts(job['job_number'])]

    return artifact_urls


def download_jar(path, url):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        filename = url.split('/')[-1]
        with open(join(path, filename), 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        return filename
    except Exception as e:
        print(f"Error downloading file: {e}")


def get_arguments():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument('--path', type=str, help="download directory")
    # parser.add_argument('--skip_spark', type=str, help="skip download of spark", default="false")
    # parser.add_argument('--skip_flink', type=str, help="skip download of flink", default="false")
    # parser.add_argument('--skip_java', type=str, help="skip download of java", default="false")
    # parser.add_argument('--skip_sql', type=str, help="skip download of sql", default="false")
    # parser.add_argument('--skip_extensions', type=str, help="skip download of extensions", default="false")
    # parser.add_argument('--skip_gcs', type=str, help="skip download of gcs", default="false")
    # parser.add_argument('--skip_gcp_lineage', type=str, help="skip download of gcp-lineage", default="false")
    # parser.add_argument('--skip_s3', type=str, help="skip download of s3", default="false")

    args = parser.parse_args()
    # skipped = { args.skip_flink
    # args.skip_flink
    # args.skip_spark
    # args.skip_extensions
    # args.skip_java
    # args.skip_gcp_lineage
    # args.skip_gcs
    # args.skip_s3
    #
    # skip-flink
    # skip-spark
    # skip-extensions
    # skip-java
    # skip-gcp-lineage
    # skip-gcs
    # skip-s3

    return args.path


def main():
    path = get_arguments()
    workflow = get_latest_successful_workflow()
    if workflow:
        artifacts_list = get_artifacts_list(workflow)
        for url in artifacts_list:
            download_jar(path, url)
    else:
        print('no workflow found')
        sys.exit(1)


if __name__ == '__main__':
    main()
