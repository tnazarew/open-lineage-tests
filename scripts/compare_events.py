# Copyright 2018-2024 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

import json
import logging
import os
import re
import uuid
from typing import Any, Optional
from urllib.parse import urlparse

from dateutil.parser import parse
from jinja2 import Environment

log = logging.getLogger(__name__)


def any(result: Any):
    return "true" if result is not None else "false"


def is_datetime(result: Any):
    try:
        parse(result)
        return "true"
    except Exception:
        pass
    return "false"


def is_uuid(result: Any):
    try:
        uuid.UUID(result)
        return "true"
    except Exception:
        pass
    return "false"


def env_var(var: str, default: Optional[str] = None) -> str:
    """The env_var() function. Return the environment variable named 'var'.
    If there is no such environment variable set, return the default.
    If the default is None, raise an exception for an undefined variable.
    """
    if var in os.environ:
        return os.environ[var]
    elif default is not None:
        return default
    else:
        msg = f"Env var required but not provided: '{var}'"
        raise Exception(msg)


def contains(result, pattern) -> str:
    return "true" if pattern in result else "false"


def not_match(result, pattern) -> str:
    return "true" if re.fullmatch(pattern, result) is None else "false"


def match(result, pattern) -> str:
    return "true" if re.fullmatch(pattern, result) is not None else "false"


def url_scheme_authority(url) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"


def url_path(url) -> str:
    return urlparse(url).path


def setup_jinja() -> Environment:
    env = Environment()
    env.globals["any"] = any
    env.globals["is_datetime"] = is_datetime
    env.globals["is_uuid"] = is_uuid
    env.globals["env_var"] = env_var
    env.globals["contains"] = contains
    env.globals["not_match"] = not_match
    env.globals["match"] = match
    env.filters["url_scheme_authority"] = url_scheme_authority
    env.filters["url_path"] = url_path
    return env


env = setup_jinja()


def diff(expected, result, prefix="") -> list:
    errors = []
    if isinstance(expected, dict):
        # Take a look only at keys present at expected dictionary
        for k, v in expected.items():
            expect_not_defined = isinstance(v, str) and v == '{{ not_defined }}'
            key_found = k in result
            if expect_not_defined and key_found:
                errors.append(f"Key {prefix}.{k} expected not to be defined but found")
            elif not key_found:
                errors.append(f"Key {prefix}.{k} missing")
            else:
                errors.extend(diff(v, result[k], f"{prefix}.{k}"))
    elif isinstance(expected, list):
        if len(expected) != len(result):
            errors.append(f"In {prefix}: Length does not match: expected {len(expected)} result: {len(result)}")
        else:
            for i, x in enumerate(expected):
                errors.extend(diff(x, result[i], f"{prefix}.[{i}]"))
    elif isinstance(expected, str):
        if "{{" in expected:
            # Evaluate jinja: in some cases, we want to check only if key exists, or if
            # value has the right type
            rendered = env.from_string(expected).render(result=result)
            if not (rendered == "true" or rendered == result):
                errors.append(f"In {prefix}: Rendered value {rendered} does not equal 'true' or {result}")
        elif expected != result:
            errors.append(f"In {prefix}: Expected value {expected} does not equal result {result}")
    elif expected != result:
        errors.append(f"In {prefix}: Object of type {type(expected)}: {expected} does not match {result}")

    return errors
