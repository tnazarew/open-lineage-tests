def max_version(v1, v2):
    if v1 is None or v2 is None:
        return None
    return v2 if version_to_number(v1) < version_to_number(v2) else v1


def min_version(v1, v2):
    if v1 is None or v2 is None:
        return None
    return v2 if version_to_number(v1) > version_to_number(v2) else v1


def release_between(release, min_version, max_version):
    max_ver = 999999999 if max_version is None else version_to_number(max_version)
    min_ver = 0 if min_version is None else version_to_number(min_version)
    rel = version_to_number(release)

    return min_ver <= rel <= max_ver


def version_to_number(version):
    split = version.split('.')
    major = int(split[0])
    minor = int(split[1])
    patch = int(split[2])
    return major * 1000000 + minor * 1000 + patch
