import json
import os
import re

import requests
from bs4 import BeautifulSoup
from packaging import version


def list_docker_tags(url):
    tags = []
    while url:
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f"Failed to fetch tags for {url} status code: {response.status_code}")

        data = response.json()
        tags.extend(tag['name'] for tag in data)
        url = data.get('next')
    return tags


def highest_version(url):
    response = requests.get(url)
    body = response.text
    soup = BeautifulSoup(body, 'html.parser')
    pre_tag = soup.find('pre', {'id': 'contents'})
    links = pre_tag.find_all('a')
    versions = [link.get('href').strip('/') for link in links if link.get('href').endswith('/') and not link.get('href').startswith('..') ]
    max_version = max(versions, key=version.parse)

    if max_version != "":
        return max_version
    else:
        return None


def get_version_to_run(e):
    if e['latest-version'] == "":
        return ""
    elif str(e['url']).__contains__('repo1.maven.org'):
        newest_version = highest_version(url=e['url'])
        if newest_version > e['latest-version']:
            return newest_version
    return None


def main():
    with open('generated-files/releases.json', 'r') as json_file:
        data = json.load(json_file)

    with open('generated-files/list', 'w') as output_file:
        for e in data:
            version_to_run = get_version_to_run(e)
            if version_to_run is not None:
                if version_to_run == '':
                    output_file.write(f"{e['name']}=true\n")
                else:
                    output_file.writelines([f"{e['name']}=true\n", f"{e['name']}_version={version_to_run}\n"])


if __name__ == "__main__":
    main()

