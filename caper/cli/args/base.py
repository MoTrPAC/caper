"""Base helpers for CLI argument dataclasses."""

from __future__ import annotations

import os
import re
from dataclasses import fields
from typing import TypeVar, Type
from argparse import Namespace

from autouri import AutoURI

T = TypeVar("T")

REGEX_DELIMITER_PARAMS = r',| '


def get_abspath(path: str | None) -> str | None:
    """
    Convert relative path to absolute, expand ~.
    
    This function is mainly used to make a command line argument an abspath
    since AutoURI module only works with abspath and full URIs
    (e.g. /home/there, gs://here/there).
    For example, "caper run toy.wdl --docker ubuntu:latest".
    AutoURI cannot recognize toy.wdl on CWD as a file path.
    It should be converted to an abspath first.
    To do so, use this function for local file path strings only (e.g. toy.wdl).
    Do not use this function for other non-local-path strings (e.g. --docker).
    """
    if path and not AutoURI(path).is_valid:
        return os.path.abspath(os.path.expanduser(path))
    return path


def split_delimited(value: str | None) -> list[str] | None:
    """Split comma or space-delimited string into list."""
    if value:
        return re.split(REGEX_DELIMITER_PARAMS, value)
    return None


def namespace_to_dataclass(ns: Namespace, cls: Type[T]) -> T:
    """
    Project an argparse Namespace onto a dataclass.
    
    Filters to only fields defined in the dataclass, ignoring
    extra Namespace attributes like _spec, command, etc.
    """
    # Use type: ignore because fields() accepts dataclass types
    field_names = {f.name for f in fields(cls)}  # type: ignore[arg-type]
    filtered = {k: v for k, v in vars(ns).items() if k in field_names}
    return cls(**filtered)

