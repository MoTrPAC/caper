"""Core infrastructure for typed CLI arguments with embedded argparse metadata."""

from __future__ import annotations

import argparse
import os
import re
import types
import typing
from dataclasses import MISSING, dataclass, field, fields
from typing import TYPE_CHECKING, Any, TypeVar, get_args, get_origin, get_type_hints

from _typeshed import DataclassInstance

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from dataclasses import Field


T = TypeVar('T')

# Python 3.10+ supports both typing.Union and types.UnionType for X | Y syntax
_UNION_TYPES: tuple[type, ...] = (typing.Union, types.UnionType)


class ArgFieldError(ValueError):
    """Exception raised for invalid argument field definitions."""


@dataclass(frozen=True, slots=True)
class ArgMeta:
    """Metadata for a CLI argument field.

    Attributes:
        flags: Argument flags. Positional if first doesn't start with "-".
               MUST NOT be empty.
        help_text: Help text for --help output.
        action: argparse action (e.g., "store_true", "store_false", "count").
                REQUIRED for bool fields that should be flags.
        type_converter: Explicit type converter. If None, inferred from field
            annotation. Use for enums, Path, or custom converters.
        choices: Valid choices for the argument.
        nargs: argparse nargs. MUST be explicit for list fields - not auto-inferred.
        metavar: Display name in help text.
        required: Whether optional argument is required.
        normalize: Post-parse normalization function applied in __post_init__.
    """

    flags: tuple[str, ...]
    help_text: str = ''
    action: str | None = None
    type_converter: type | Callable[[str], Any] | None = None
    choices: Sequence[Any] | None = None
    nargs: str | int | None = None
    metavar: str | None = None
    required: bool = False
    normalize: Callable[[Any], Any] | None = None

    def __post_init__(self) -> None:
        """Validate metadata at definition time."""
        if not self.flags:
            msg = 'flags cannot be empty'
            raise ArgFieldError(msg)

    @property
    def is_positional(self) -> bool:
        """Return True if this is a positional argument (first flag has no - prefix)."""
        return len(self.flags) > 0 and not self.flags[0].startswith('-')


def _validate_action_default(action: str | None, default: Any) -> Any:
    """Validate and auto-set defaults for store_true/store_false actions."""
    if action == 'store_true':
        if default not in (None, False):
            msg = f"action='store_true' requires default=False, got {default!r}"
            raise ArgFieldError(msg)
        return False if default is None else default
    if action == 'store_false':
        if default not in (None, True):
            msg = f"action='store_false' requires default=True, got {default!r}"
            raise ArgFieldError(msg)
        return True if default is None else default
    return default


def arg(  # noqa: PLR0913
    *flags: str,
    help_text: str = '',
    default: T | None = None,
    action: str | None = None,
    type_converter: type | Callable[[str], Any] | None = None,
    choices: Sequence[Any] | None = None,
    nargs: str | int | None = None,
    metavar: str | None = None,
    required: bool = False,
    normalize: Callable[[Any], Any] | None = None,
    default_factory: Callable[[], T] | None = None,
) -> Any:
    """Define a CLI argument field with argparse metadata.

    This creates a dataclass field with embedded ArgMeta, enabling single-source
    definition of both the dataclass field AND the argparse argument.

    Usage:
        @dataclass
        class MyArgs:
            name: str = arg("-n", "--name", help_text="Your name", default="world")
            debug: bool = arg("-D", "--debug", help_text="Debug", action="store_true")
            files: list[str] = arg("files", help_text="Files", nargs="+",
                default_factory=list)

    Args:
        *flags: Argument flags. Positional if no "-" prefix.
        help_text: Help text.
        default: Default value. Use default_factory for mutable defaults.
        action: argparse action. REQUIRED for bool flags.
        type_converter: Type converter. Inferred if not provided.
        choices: Valid choices.
        nargs: REQUIRED for list fields - not auto-inferred.
        metavar: Display name.
        required: Mark optional arg as required.
        normalize: Normalization function for __post_init__.
        default_factory: Factory for mutable defaults.

    Raises:
        ArgFieldError: If flags empty or invalid action/default combination.
    """
    if not flags:
        msg = 'arg() requires at least one flag'
        raise ArgFieldError(msg)

    default = _validate_action_default(action, default)

    meta = ArgMeta(
        flags=flags,
        help_text=help_text,
        action=action,
        type_converter=type_converter,
        choices=choices,
        nargs=nargs,
        metavar=metavar,
        required=required,
        normalize=normalize,
    )

    if default_factory is not None:
        return field(default_factory=default_factory, metadata={'arg': meta})
    if default is not None:
        return field(default=default, metadata={'arg': meta})
    return field(default=None, metadata={'arg': meta})


def abspath(path: str | None) -> str | None:
    """Normalize to absolute path, expanding ~. Skips cloud URIs."""
    if not path:
        return path
    if path.startswith(('gs://', 's3://', 'http://', 'https://')):
        return path
    return os.path.abspath(os.path.expanduser(path))


def split_commas(value: str | None) -> list[str]:
    """Split comma/space-delimited string. Returns empty list if None."""
    if not value:
        return []
    return [v.strip() for v in re.split(r'[,\s]+', value.strip()) if v.strip()]


def get_arg_meta(f: Field[Any]) -> ArgMeta | None:
    """Extract ArgMeta from a dataclass field's metadata."""
    return f.metadata.get('arg') if f.metadata else None


def _is_optional_type(tp: Any) -> tuple[bool, type | None]:
    """Check if type is Optional[X] or X | None.

    Returns:
        Tuple of (is_optional, inner_type).
    """
    origin = get_origin(tp)
    if origin in _UNION_TYPES:
        args = get_args(tp)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1 and type(None) in args:
            return True, non_none[0]
    return False, None


def _is_list_type(tp: Any) -> tuple[bool, type | None]:
    """Check if type is list[X].

    Returns:
        Tuple of (is_list, element_type).
    """
    origin = get_origin(tp)
    if origin is list:
        args = get_args(tp)
        return True, args[0] if args else str
    return False, None


def _get_argparse_type(
    f: Field[Any], meta: ArgMeta, cls: type | None = None
) -> type | Callable[[str], Any] | None:
    """Determine the type converter for argparse.

    Priority:
    1. Explicit meta.type_converter
    2. Inferred from field annotation (but NOT for bool - action must be explicit)
    3. None (let argparse default to str)

    Args:
        f: Dataclass field
        meta: Argument metadata
        cls: Dataclass type (needed to resolve string annotations)
    """
    if meta.type_converter is not None:
        return meta.type_converter

    if meta.action:
        return None

    # Resolve string annotations to actual types
    ftype: Any = f.type
    if isinstance(ftype, str) and cls is not None:
        type_hints = get_type_hints(cls)
        ftype = type_hints.get(f.name, ftype)

    is_optional, inner = _is_optional_type(ftype)
    if is_optional and inner is not None:
        ftype = inner

    is_list, elem_type = _is_list_type(ftype)
    if is_list and elem_type is not None:
        return elem_type

    if ftype is bool:
        return None

    if ftype in (str, int, float):
        return ftype

    return None


def _add_action_or_type(
    kwargs: dict[str, Any], f: Field[Any], meta: ArgMeta, cls: type | None = None
) -> None:
    """Add action or type to kwargs.

    Automatically upgrades 'store_true' to BooleanOptionalAction for better config handling.
    """
    if meta.action == 'store_true':
        # Upgrade store_true to BooleanOptionalAction
        # This adds --no-{flag} capability and allows overriding config defaults
        kwargs['action'] = argparse.BooleanOptionalAction
    elif meta.action:
        kwargs['action'] = meta.action
    else:
        inferred_type = _get_argparse_type(f, meta, cls)
        if inferred_type is not None:
            kwargs['type'] = inferred_type


def _add_default(kwargs: dict[str, Any], f: Field[Any], meta: ArgMeta) -> None:
    """Add default value to kwargs for optional arguments."""
    if meta.is_positional:
        return

    # Special handling for BooleanOptionalAction
    # If using BooleanOptionalAction, we need an explicit default because it
    # defaults to None otherwise.
    if kwargs.get('action') is argparse.BooleanOptionalAction and 'default' not in kwargs:
        if f.default is not MISSING:
            kwargs['default'] = f.default
        else:
            # Default to False like store_true
            kwargs['default'] = False
        return

    if f.default is not MISSING:
        kwargs['default'] = f.default
    elif f.default_factory is not MISSING:
        kwargs['default'] = None


def _add_optional_meta(kwargs: dict[str, Any], meta: ArgMeta) -> None:
    """Add optional metadata fields to kwargs."""
    if meta.choices is not None:
        kwargs['choices'] = meta.choices
    if meta.nargs is not None:
        kwargs['nargs'] = meta.nargs
    if meta.metavar is not None:
        kwargs['metavar'] = meta.metavar
    if meta.required:
        kwargs['required'] = meta.required


def _build_argparse_kwargs(
    f: Field[Any], meta: ArgMeta, cls: type | None = None
) -> dict[str, Any]:
    """Build kwargs dict for argparse.add_argument()."""
    kwargs: dict[str, Any] = {'help': meta.help_text}

    if not meta.is_positional:
        kwargs['dest'] = f.name

    _add_action_or_type(kwargs, f, meta, cls)
    _add_default(kwargs, f, meta)
    _add_optional_meta(kwargs, meta)

    return kwargs


def add_field_to_parser(
    parser: argparse.ArgumentParser, f: Field[Any], cls: type | None = None
) -> None:
    """Add a single dataclass field to an argparse parser.

    Args:
        parser: Argparse parser to add field to
        f: Dataclass field
        cls: Dataclass type (needed to resolve string annotations)
    """
    meta = get_arg_meta(f)
    if meta is None:
        return

    kwargs = _build_argparse_kwargs(f, meta, cls)

    if meta.is_positional:
        parser.add_argument(meta.flags[0], **kwargs)
    else:
        parser.add_argument(*meta.flags, **kwargs)


def add_dataclass_args(parser: argparse.ArgumentParser, cls: type) -> None:
    """Add all ArgMeta fields from a dataclass (including inherited) to parser."""
    for f in fields(cls):
        add_field_to_parser(parser, f, cls)


D = TypeVar('D', bound=DataclassInstance)


def namespace_to_dataclass(ns: argparse.Namespace, cls: type[D]) -> D:
    """Convert argparse Namespace to typed dataclass instance.

    - Filters to only fields defined in cls
    - Handles default_factory fields that argparse left as None
    - __post_init__ will apply normalizers
    """
    field_info = {f.name: f for f in fields(cls)}
    kwargs: dict[str, Any] = {}

    for name, f in field_info.items():
        if hasattr(ns, name):
            value = getattr(ns, name)
            if value is None and f.default_factory is not MISSING:
                value = f.default_factory()
            kwargs[name] = value
        elif f.default is not MISSING:
            kwargs[name] = f.default
        elif f.default_factory is not MISSING:
            kwargs[name] = f.default_factory()

    return cls(**kwargs)


def apply_normalizers(instance: Any) -> None:
    """Apply normalize functions from ArgMeta to all field values.

    Call this in __post_init__ of composed dataclasses:
        def __post_init__(self):
            apply_normalizers(self)
            # ... additional custom normalization
    """
    for f in fields(instance):
        meta = get_arg_meta(f)
        if meta and meta.normalize:
            current = getattr(instance, f.name)
            if current is not None:
                normalized = meta.normalize(current)
                object.__setattr__(instance, f.name, normalized)


def dataclass_to_cli_args(
    instance: Any,
    include_fields: set[str] | None = None,
) -> list[str]:
    """Convert a dataclass instance back to CLI arguments list.

    Useful for reconstructing commands (e.g., HPC submission).

    Args:
        instance: Dataclass instance.
        include_fields: Optional set of field names to include. If None, includes all.

    Returns:
        List of CLI arguments strings.
    """
    cmd: list[str] = []

    for f in fields(instance):
        if include_fields is not None and f.name not in include_fields:
            continue

        meta = get_arg_meta(f)
        if not meta:
            continue

        value = getattr(instance, f.name)
        if value is None:
            continue

        # Handle Positional Arguments
        if meta.is_positional:
            if isinstance(value, list):
                cmd.extend(str(v) for v in value)
            else:
                cmd.append(str(value))
            continue

        # Handle Flags
        # Prefer long flag if available
        flag = next((fl for fl in meta.flags if fl.startswith('--')), meta.flags[0])

        if meta.action == 'store_true':
            # Handle BooleanOptionalAction logic (which we upgraded store_true to)
            if value:
                cmd.append(flag)
            else:
                # If value is False, we output --no-{flag} to ensure it's disabled
                # strictly, matching BooleanOptionalAction behavior.
                # Strip leading dashes
                clean_flag = flag.lstrip('-')
                cmd.append(f'--no-{clean_flag}')

        elif meta.action == 'store_false':
            # Not currently used, but logic would be inverted
            if not value:
                cmd.append(flag)
            else:
                clean_flag = flag.lstrip('-')
                cmd.append(f'--no-{clean_flag}')

        elif meta.action == 'append':
            if isinstance(value, list):
                for v in value:
                    cmd.append(flag)
                    cmd.append(str(v))

        elif meta.nargs in ('*', '+'):
            if isinstance(value, list) and value:
                cmd.append(flag)
                cmd.extend(str(v) for v in value)

        else:
            # Standard store (store, etc.)
            cmd.append(flag)
            cmd.append(str(value))

    return cmd
