"""Configuration loading for Caper CLI."""

from __future__ import annotations

import os
import tempfile
from argparse import ArgumentParser

from caper.arg_tool import update_parsers_defaults_with_conf
from caper.backward_compatibility import PARAM_KEY_NAME_CHANGE


def load_conf_defaults(conf_file: str) -> dict[str, str | bool | int | float | None]:
    """
    Load configuration defaults from file.

    Returns a flat dict of key-value pairs. Values are strings;
    type conversion happens when projecting to dataclasses.
    """
    conf_file = os.path.expanduser(conf_file)
    if not os.path.exists(conf_file):
        return {}

    # Use the existing read_from_conf function
    from caper.arg_tool import read_from_conf

    return read_from_conf(conf_file, conf_key_map=PARAM_KEY_NAME_CHANGE)


def apply_conf_defaults_to_parsers(
    parsers: list[ArgumentParser],
    conf_defaults: dict[str, str | bool | int | float | None],
) -> None:
    """
    Apply configuration defaults to all subparsers.

    Uses the existing update_parsers_defaults_with_conf function
    which handles type conversion based on parser defaults.
    """
    if not conf_defaults:
        return

    # We need to write conf_defaults to a temporary file to use
    # update_parsers_defaults_with_conf, or we can implement the logic directly.
    # For simplicity, let's write to a temp file and use the existing function.
    # But actually, we can just use update_parsers_defaults_with_conf directly
    # if we have the conf_file path. Let me check the flow...

    # Actually, since we already have conf_defaults as a dict, we can
    # use update_parsers_defaults_with_conf by creating a temp config file
    # or we can replicate its logic. Let's use a temp file approach for now.

    # Write defaults to a temporary config file
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.conf') as tmp:
        tmp.write('[defaults]\n')
        for key, value in conf_defaults.items():
            if value is not None:
                # Convert key back to hyphenated form for config file
                config_key = key.replace('_', '-')
                tmp.write(f'{config_key}={value}\n')
        tmp_path = tmp.name

    try:
        # Use existing function to apply defaults with proper type conversion
        update_parsers_defaults_with_conf(
            parsers=parsers,
            conf_file=tmp_path,
            conf_key_map=PARAM_KEY_NAME_CHANGE,
        )
    finally:
        # Clean up temp file
        os.unlink(tmp_path)

