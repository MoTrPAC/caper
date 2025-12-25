"""Configuration file loading and default merging for Caper CLI."""

from __future__ import annotations

import argparse
import os
from configparser import ConfigParser, MissingSectionHeaderError
from typing import Any


def load_conf_defaults(conf_file: str) -> dict[str, Any]:
    """
    Load config file as flat dict of string values.

    Args:
        conf_file: Path to config file

    Returns:
        Dictionary of config keys to string values (with hyphens converted to underscores)
    """
    conf_file = os.path.expanduser(conf_file)
    if not os.path.exists(conf_file):
        return {}

    config = ConfigParser()

    # Read config file, adding [defaults] section if missing
    with open(conf_file) as fp:
        content = fp.read()
        try:
            config.read_string(content)
        except MissingSectionHeaderError:
            # Add default section header if missing
            config.read_string(f'[defaults]\n{content}')

    # Extract all values from defaults section
    result: dict[str, str] = {}
    if config.has_section('defaults'):
        for key, value in config.items('defaults'):
            # Convert hyphens to underscores (argparse dest names use underscores)
            normalized_key = key.replace('-', '_')
            # Strip quotes from values
            result[normalized_key] = value.strip('"\'')

    return result


def apply_config_to_parser(parser: argparse.ArgumentParser, config: dict[str, Any]) -> None:
    """
    Apply config defaults to parser, with type conversion.

    This function introspects the parser's actions to determine the correct type
    for each config value, then applies them as defaults.

    Args:
        parser: argparse parser to apply defaults to
        config: Dictionary of config keys to string values
    """
    if not config:
        return

    # Build type map from parser actions
    type_map: dict[str, Any] = {}

    for action in parser._actions:  # noqa: SLF001
        if action.dest not in config:
            continue

        # Skip special actions
        if action.dest in ('help', 'version'):
            continue

        # If action has explicit type, use it
        if action.type:
            type_map[action.dest] = action.type
        # For store_true/store_false, convert to bool
        elif isinstance(action, (argparse._StoreTrueAction, argparse._StoreFalseAction)):  # noqa: SLF001
            type_map[action.dest] = _str_to_bool
        # Infer from default value type
        elif action.default is not None:
            default_type = type(action.default)
            if default_type in (bool, int, float, str):
                if default_type is bool:
                    type_map[action.dest] = _str_to_bool
                else:
                    type_map[action.dest] = default_type

    # Convert config values and apply
    converted: dict[str, Any] = {}
    for key, value in config.items():
        if key in type_map:
            try:
                converted[key] = type_map[key](value)
            except (ValueError, TypeError):
                # If conversion fails, use default value
                converted[key] = value
        else:
            # No type info, use default value
            converted[key] = value

    # Apply to parser defaults
    parser.set_defaults(**converted)


def _str_to_bool(value: str) -> bool:
    """Convert string to boolean for config file values."""
    return value.lower() in ('true', 'yes', '1', 'on')
