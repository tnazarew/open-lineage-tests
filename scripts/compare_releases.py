def max_version(v1, v2):
    if v1 is None or v2 is None:
        return None
    return v2 if version_to_number(v1) < version_to_number(v2) else v1


def min_version(v1, v2):
    if v1 is None or v2 is None:
        return None
    return v2 if version_to_number(v1) > version_to_number(v2) else v1


def version_between(v, v_min, v_max):
    ge_min = v_min is None or version_to_number(v) >= version_to_number(v_min)
    le_max = v_max is None or version_to_number(v) <= version_to_number(v_max)
    return ge_min and le_max


def version_to_number(version):
    split = version.split('.')
    major = int(split[0])
    minor = int(split[0])
    patch = int(split[0])
    return major * 1000000 + minor * 1000 + patch
