# Caper CLI Refactor Plan

## Executive Summary

This document outlines a comprehensive refactor of Caper's command-line interface to achieve:

1. **Full static type safety** via typed dataclasses for each subcommand
2. **Single source of truth** via `field(metadata=...)` pattern with `arg()` helper
3. **Simplified dispatch** via a `CommandSpec` registry pattern
4. **Command aliases** for common operations (e.g., `ls` for `list`, `meta` for `metadata`)
5. **Cleaner HPC handling** without `sys.argv` manipulation
6. **Centralized normalization** via `normalize=` parameter on field definitions

The refactor maintains argparse as the CLI framework and is designed for incremental rollout.

---

## Table of Contents

1. [Current Pain Points](#current-pain-points)
2. [Target Architecture](#target-architecture)
3. [Core Implementation](#core-implementation)
4. [Mixin Dataclasses](#mixin-dataclasses)
5. [Composed Command Args](#composed-command-args)
6. [Command Registry](#command-registry)
7. [Config Merging](#config-merging)
8. [Main Dispatch Flow](#main-dispatch-flow)
9. [HPC Refactor](#hpc-refactor)
10. [Edge Cases Addressed](#edge-cases-addressed)
11. [Migration Strategy](#migration-strategy)
12. [Implementation Checklist](#implementation-checklist)

---

## Current Pain Points

1. **Untyped `Namespace`** - `args.wdl` could be anything; IDE/type checker has no idea
2. **Defensive `hasattr`/`getattr`** - 14+ `getattr(args, 'x', None)` calls in `cli.py`
3. **Scattered normalization** - `check_dirs()`, `check_db_path()`, `check_flags()` spread across functions
4. **HPC `sys.argv` manipulation** - Rewrites argv to convert `hpc submit` to `run`
5. **No command aliases** - Must type `troubleshoot`, `metadata`, `gcp_res_analysis`
6. **Double parsing** - Bootstrap parse for `--conf`, then full parse

---

## Target Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         main(argv)                               │
├─────────────────────────────────────────────────────────────────┤
│  1. Bootstrap parse for --conf                                   │
│  2. Load config defaults from file                               │
│  3. Build all subparsers from COMMANDS registry                  │
│  4. Apply config defaults to subparsers                          │
│  5. Full parse → Namespace                                       │
│  6. namespace_to_dataclass() → Typed Dataclass                   │
│  7. __post_init__() applies field normalizers                    │
│  8. Dispatch to handler(typed_args)                              │
└─────────────────────────────────────────────────────────────────┘
```

### Key Principles

1. **Single source of truth** - Each field defined ONCE via `arg()` with type, default, AND argparse metadata
2. **Composition via mixins** - Mixin dataclasses match current parent parser pattern
3. **Explicit over implicit** - `nargs`, `action`, `type` explicit when needed; no surprising auto-inference
4. **Normalization on fields** - `normalize=abspath` on field, applied in `__post_init__`
5. **Config integration** - Config file defaults applied BEFORE parse

---

## Core Implementation

### File Structure

```
caper/
├── cli/
│   ├── __init__.py          # Exports main()
│   ├── arg_field.py         # ArgMeta class + arg() helper + normalizers
│   ├── args/
│   │   ├── __init__.py      # Exports all dataclasses
│   │   ├── mixins.py        # CommonArgs, LocalizationArgs, etc.
│   │   ├── run.py           # RunArgs
│   │   ├── server.py        # ServerArgs
│   │   ├── submit.py        # SubmitArgs
│   │   ├── client.py        # ListArgs, MetadataArgs, etc.
│   │   ├── hpc.py           # HpcSubmitArgs, HpcListArgs, HpcAbortArgs
│   │   └── analysis.py      # GcpMonitorArgs, GcpResAnalysisArgs
│   ├── commands.py          # COMMANDS registry
│   ├── parser.py            # add_dataclass_args(), build functions
│   ├── handlers.py          # Handler functions
│   ├── dispatch.py          # main() and dispatch logic
│   └── config.py            # Config file loading + default merging
```

---

### ArgMeta Class

```python
# caper/cli/arg_field.py

from __future__ import annotations

import os
import re
import sys
import types
import typing
import argparse
from dataclasses import dataclass, field, fields, Field, MISSING
from typing import Any, Callable, Sequence, TypeVar, get_origin, get_args

T = TypeVar("T")

# Python 3.10+ has types.UnionType for X | Y syntax
_UNION_TYPES: tuple[type, ...] = (typing.Union,)
if sys.version_info >= (3, 10):
    _UNION_TYPES = (typing.Union, types.UnionType)


@dataclass(frozen=True, slots=True)
class ArgMeta:
    """
    Metadata for a CLI argument field.
    
    Attributes:
        flags: Argument flags. Positional if first doesn't start with "-".
               MUST NOT be empty.
        help: Help text for --help output.
        action: argparse action (e.g., "store_true", "store_false", "count", "append").
                REQUIRED for bool fields that should be flags.
        type: Explicit type converter. If None, inferred from field annotation.
              Use for enums, Path, or custom converters.
        choices: Valid choices for the argument.
        nargs: argparse nargs. MUST be explicit for list fields - not auto-inferred.
        metavar: Display name in help text.
        required: Whether optional argument is required.
        normalize: Post-parse normalization function applied in __post_init__.
    """
    flags: tuple[str, ...]
    help: str = ""
    action: str | None = None
    type: type | Callable[[str], Any] | None = None
    choices: Sequence[Any] | None = None
    nargs: str | int | None = None
    metavar: str | None = None
    required: bool = False
    normalize: Callable[[Any], Any] | None = None
    
    def __post_init__(self) -> None:
        """Validate metadata at definition time."""
        if not self.flags:
            raise ValueError("ArgMeta.flags cannot be empty")
        # Additional validation could go here
    
    @property
    def is_positional(self) -> bool:
        """True if this is a positional argument (first flag has no - prefix)."""
        return len(self.flags) > 0 and not self.flags[0].startswith("-")
```

---

### `arg()` Helper Function

```python
def arg(
    *flags: str,
    help: str = "",
    default: T | None = None,
    action: str | None = None,
    type: type | Callable[[str], Any] | None = None,
    choices: Sequence[Any] | None = None,
    nargs: str | int | None = None,
    metavar: str | None = None,
    required: bool = False,
    normalize: Callable[[Any], Any] | None = None,
    default_factory: Callable[[], T] | None = None,
) -> Any:
    """
    Define a CLI argument field with argparse metadata.
    
    This creates a dataclass field with embedded ArgMeta, enabling single-source
    definition of both the dataclass field AND the argparse argument.
    
    Usage:
        @dataclass
        class MyArgs:
            name: str = arg("-n", "--name", help="Your name", default="world")
            debug: bool = arg("-D", "--debug", help="Debug", action="store_true")
            files: list[str] = arg("files", help="Files", nargs="+", default_factory=list)
    
    Args:
        *flags: Argument flags. Positional if no "-" prefix.
        help: Help text.
        default: Default value. Use default_factory for mutable defaults.
        action: argparse action. REQUIRED for bool flags.
        type: Type converter. Inferred if not provided.
        choices: Valid choices.
        nargs: REQUIRED for list fields - not auto-inferred.
        metavar: Display name.
        required: Mark optional arg as required.
        normalize: Normalization function for __post_init__.
        default_factory: Factory for mutable defaults.
    
    Raises:
        ValueError: If flags empty or invalid action/default combination.
    """
    if not flags:
        raise ValueError("arg() requires at least one flag")
    
    # Validate store_true/store_false have correct defaults
    if action == "store_true" and default not in (None, False):
        raise ValueError(f"action='store_true' requires default=False, got {default!r}")
    if action == "store_false" and default not in (None, True):
        raise ValueError(f"action='store_false' requires default=True, got {default!r}")
    
    # Auto-set defaults for store_true/store_false
    if action == "store_true" and default is None:
        default = False
    if action == "store_false" and default is None:
        default = True
    
    meta = ArgMeta(
        flags=flags,
        help=help,
        action=action,
        type=type,
        choices=choices,
        nargs=nargs,
        metavar=metavar,
        required=required,
        normalize=normalize,
    )
    
    if default_factory is not None:
        return field(default_factory=default_factory, metadata={"arg": meta})
    elif default is not None:
        return field(default=default, metadata={"arg": meta})
    else:
        return field(default=None, metadata={"arg": meta})
```

---

### Common Normalizers

```python
def abspath(path: str | None) -> str | None:
    """Normalize to absolute path, expanding ~. Skips cloud URIs."""
    if not path:
        return path
    if path.startswith(("gs://", "s3://", "http://", "https://")):
        return path
    return os.path.abspath(os.path.expanduser(path))


def split_commas(value: str | None) -> list[str]:
    """Split comma/space-delimited string. Returns empty list if None."""
    if not value:
        return []
    return [v.strip() for v in re.split(r'[,\s]+', value.strip()) if v.strip()]
```

---

### Parser Building from Dataclass

```python
def _is_optional_type(tp: type) -> tuple[bool, type | None]:
    """
    Check if type is Optional[X] or X | None.
    Returns (is_optional, inner_type).
    Works with both typing.Union and types.UnionType (Python 3.10+).
    """
    origin = get_origin(tp)
    if origin in _UNION_TYPES:
        args = get_args(tp)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1 and type(None) in args:
            return True, non_none[0]
    return False, None


def _is_list_type(tp: type) -> tuple[bool, type | None]:
    """Check if type is list[X]. Returns (is_list, element_type)."""
    origin = get_origin(tp)
    if origin is list:
        args = get_args(tp)
        return True, args[0] if args else str
    return False, None


def _get_argparse_type(f: Field, meta: ArgMeta) -> type | Callable | None:
    """
    Determine type converter for argparse.
    
    Priority:
    1. Explicit meta.type
    2. Inferred from annotation (but NOT for bool - action must be explicit)
    3. None (argparse defaults to str)
    """
    if meta.type is not None:
        return meta.type
    
    if meta.action:  # Don't infer type if action is set
        return None
    
    ftype = f.type
    
    # Handle Optional[X] -> X
    is_optional, inner = _is_optional_type(ftype)
    if is_optional and inner is not None:
        ftype = inner
    
    # Handle list[X] -> X (type applies to elements)
    is_list, elem = _is_list_type(ftype)
    if is_list and elem is not None:
        return elem
    
    # DON'T auto-infer for bool - action must be explicit
    if ftype is bool:
        return None
    
    if ftype in (str, int, float):
        return ftype
    
    return None


def add_field_to_parser(parser: argparse.ArgumentParser, f: Field) -> None:
    """Add a single dataclass field to an argparse parser."""
    meta = f.metadata.get("arg") if f.metadata else None
    if meta is None:
        return
    
    kwargs: dict[str, Any] = {"help": meta.help, "dest": f.name}
    
    # Action
    if meta.action:
        kwargs["action"] = meta.action
    else:
        inferred_type = _get_argparse_type(f, meta)
        if inferred_type is not None:
            kwargs["type"] = inferred_type
    
    # Default - only for optional args
    if not meta.is_positional:
        if f.default is not MISSING:
            kwargs["default"] = f.default
        elif f.default_factory is not MISSING:
            # DON'T call factory here - would create shared mutable default
            # Let argparse use None; we handle it in namespace_to_dataclass
            kwargs["default"] = None
    
    # Pass through other metadata
    if meta.choices is not None:
        kwargs["choices"] = meta.choices
    if meta.nargs is not None:
        kwargs["nargs"] = meta.nargs
    if meta.metavar is not None:
        kwargs["metavar"] = meta.metavar
    if meta.required:
        kwargs["required"] = True
    
    # Add to parser
    if meta.is_positional:
        parser.add_argument(meta.flags[0], **kwargs)
    else:
        parser.add_argument(*meta.flags, **kwargs)


def add_dataclass_args(parser: argparse.ArgumentParser, cls: type) -> None:
    """Add all ArgMeta fields from a dataclass (including inherited) to parser."""
    for f in fields(cls):
        add_field_to_parser(parser, f)
```

---

### Namespace to Dataclass Conversion

```python
def namespace_to_dataclass(ns: argparse.Namespace, cls: type[T]) -> T:
    """
    Convert argparse Namespace to typed dataclass.
    
    - Filters to only cls fields
    - Handles default_factory fields that argparse left as None
    - __post_init__ will apply normalizers
    """
    field_info = {f.name: f for f in fields(cls)}
    kwargs: dict[str, Any] = {}
    
    for name, f in field_info.items():
        if hasattr(ns, name):
            value = getattr(ns, name)
            # If None and field has default_factory, call factory for fresh instance
            if value is None and f.default_factory is not MISSING:
                value = f.default_factory()
            kwargs[name] = value
        elif f.default is not MISSING:
            kwargs[name] = f.default
        elif f.default_factory is not MISSING:
            kwargs[name] = f.default_factory()
    
    return cls(**kwargs)


def apply_normalizers(instance: Any) -> None:
    """
    Apply normalize functions from ArgMeta to all field values.
    Call in __post_init__ of composed dataclasses.
    """
    for f in fields(instance):
        meta = f.metadata.get("arg") if f.metadata else None
        if meta and meta.normalize:
            current = getattr(instance, f.name)
            if current is not None:
                normalized = meta.normalize(current)
                object.__setattr__(instance, f.name, normalized)
```

---

## Mixin Dataclasses

Each mixin = one logical group of arguments. Fields defined ONCE with full metadata.

```python
# caper/cli/args/mixins.py

from dataclasses import dataclass
from ..arg_field import arg, abspath


@dataclass
class CommonArgs:
    """Common arguments for all commands."""
    conf: str = arg("-c", "--conf", help="Config file", default="~/.caper/default.conf")
    debug: bool = arg("-D", "--debug", help="Debug logging", action="store_true")
    gcp_service_account_key_json: str | None = arg(
        "--gcp-service-account-key-json", help="GCP SA key JSON", normalize=abspath
    )


@dataclass
class LocalizationArgs:
    """Cache/temp directory arguments."""
    local_loc_dir: str | None = arg("--local-loc-dir", "--tmp-dir", help="Local temp dir", normalize=abspath)
    gcp_loc_dir: str | None = arg("--gcp-loc-dir", help="GCS cache dir")
    aws_loc_dir: str | None = arg("--aws-loc-dir", help="S3 cache dir")


@dataclass
class BackendArgs:
    """Backend selection."""
    backend: str | None = arg("-b", "--backend", help="Backend to run workflow")
    dry_run: bool = arg("--dry-run", help="Validate but don't run", action="store_true")
    gcp_zones: str | None = arg("--gcp-zones", help="Comma-separated GCP zones")


@dataclass
class ServerClientArgs:
    """Server/client connection."""
    port: int = arg("--port", help="Server port", default=8000)
    no_server_heartbeat: bool = arg("--no-server-heartbeat", help="Disable heartbeat", action="store_true")
    server_heartbeat_file: str = arg("--server-heartbeat-file", help="Heartbeat file", 
                                      default="~/.caper/default_server_heartbeat", normalize=abspath)
    server_heartbeat_timeout: int = arg("--server-heartbeat-timeout", help="Timeout ms", default=120000)


@dataclass
class ClientArgs:
    """Client-only."""
    hostname: str = arg("--hostname", "--ip", help="Server hostname", default="localhost")


@dataclass
class SearchArgs:
    """Workflow search."""
    wf_id_or_label: list[str] = arg(
        "wf_id_or_label", help="Workflow IDs/labels", 
        nargs="*",  # EXPLICIT - allows 0+
        default_factory=list
    )


@dataclass
class SubmitIOArgs:
    """Submit I/O."""
    inputs: str | None = arg("-i", "--inputs", help="Inputs JSON", normalize=abspath)
    options: str | None = arg("-o", "--options", help="Options JSON", normalize=abspath)
    labels: str | None = arg("-l", "--labels", help="Labels JSON", normalize=abspath)
    imports: str | None = arg("-p", "--imports", help="Imports zip", normalize=abspath)
    str_label: str | None = arg("-s", "--str-label", help="Caper label")
    hold: bool = arg("--hold", help="Put on hold", action="store_true")
    docker: str | None = arg("--docker", help="Docker image", nargs="?")  # Flag or value
    singularity: str | None = arg("--singularity", help="Singularity image", nargs="?")
    conda: str | None = arg("--conda", help="Conda env", nargs="?")
    max_retries: int = arg("--max-retries", help="Task retries", default=0)
    ignore_womtool: bool = arg("--ignore-womtool", help="Ignore womtool", action="store_true")
    no_deepcopy: bool = arg("--no-deepcopy", help="No deepcopy", action="store_true")
    use_gsutil_for_s3: bool = arg("--use-gsutil-for-s3", help="gsutil for S3<->GCS", action="store_true")
    womtool: str | None = arg("--womtool", help="Womtool JAR", normalize=abspath)
    java_heap_womtool: str = arg("--java-heap-womtool", help="Womtool heap", default="1G")
```

---

## Composed Command Args

Commands compose mixins via inheritance. Only command-specific fields added.

```python
# caper/cli/args/run.py

from dataclasses import dataclass, field
from ..arg_field import arg, abspath, split_commas, apply_normalizers
from .mixins import CommonArgs, LocalizationArgs, BackendArgs, SubmitIOArgs, DatabaseArgs, CromwellArgs


@dataclass
class RunArgs(CommonArgs, LocalizationArgs, BackendArgs, SubmitIOArgs, DatabaseArgs, CromwellArgs):
    """Arguments for 'caper run'."""
    
    # Positional
    wdl: str = arg("wdl", help="WDL script", normalize=abspath)
    
    # Run-specific
    metadata_output: str | None = arg("-m", "--metadata-output", help="Metadata output dir", normalize=abspath)
    java_heap_run: str = arg("--java-heap-run", help="Cromwell heap", default="8G")
    local_out_dir: str = arg("--local-out-dir", "--out-dir", help="Output dir", default=".", normalize=abspath)
    gcp_prj: str | None = arg("--gcp-prj", help="GCP project")
    gcp_out_dir: str | None = arg("--gcp-out-dir", help="GCS output dir")
    aws_batch_arn: str | None = arg("--aws-batch-arn", help="AWS Batch ARN")
    aws_out_dir: str | None = arg("--aws-out-dir", help="S3 output dir")
    
    # Derived (not from CLI)
    gcp_zones_list: list[str] = field(default_factory=list, repr=False)
    
    def __post_init__(self) -> None:
        # Apply field normalizers
        apply_normalizers(self)
        
        # Derive loc dirs from out dirs
        if not self.local_loc_dir:
            self.local_loc_dir = f"{self.local_out_dir}/.caper_tmp"
        if self.gcp_out_dir and not self.gcp_loc_dir:
            self.gcp_loc_dir = f"{self.gcp_out_dir}/.caper_tmp"
        if self.aws_out_dir and not self.aws_loc_dir:
            self.aws_loc_dir = f"{self.aws_out_dir}/.caper_tmp"
        
        # Split delimited
        self.gcp_zones_list = split_commas(self.gcp_zones)
        
        # Validate mutual exclusion
        flags = [self.docker is not None, self.singularity is not None, self.conda is not None]
        if sum(flags) > 1:
            raise ValueError("--docker, --singularity, --conda are mutually exclusive")


@dataclass
class ListArgs(CommonArgs, LocalizationArgs, ServerClientArgs, ClientArgs, SearchArgs):
    """Arguments for 'caper list'."""
    format: str = arg("-f", "--format", help="Columns", default="id,status,name,str_label,user,parent,submission")
    hide_result_before: str | None = arg("--hide-result-before", help="Hide before date")
    show_subworkflow: bool = arg("--show-sub-workflow", help="Show subworkflows", action="store_true")
    
    def __post_init__(self) -> None:
        apply_normalizers(self)
```

---

## Command Registry

```python
# caper/cli/commands.py

from dataclasses import dataclass
from typing import Callable, Any

@dataclass
class Command:
    """Command definition."""
    name: str
    aliases: tuple[str, ...]
    help: str
    args_class: type
    handler: Callable[[Any], None]


COMMANDS: list[Command] = [
    Command("run", ("local", "exec"), "Run workflow", RunArgs, handle_run),
    Command("server", ("srv",), "Start server", ServerArgs, handle_server),
    Command("submit", ("sub",), "Submit to server", SubmitArgs, handle_submit),
    Command("list", ("ls",), "List workflows", ListArgs, handle_list),
    Command("metadata", ("meta", "md"), "Get metadata", MetadataArgs, handle_metadata),
    Command("troubleshoot", ("debug", "ts"), "Troubleshoot", TroubleshootArgs, handle_troubleshoot),
    Command("gcp_monitor", ("monitor",), "GCP monitoring", GcpMonitorArgs, handle_gcp_monitor),
    Command("cleanup", ("clean",), "Cleanup outputs", CleanupArgs, handle_cleanup),
    Command("hpc", ("cluster",), "HPC commands", None, None),  # Special: nested
]
```

---

## Config Merging

Config defaults applied BEFORE parsing, not after.

```python
# caper/cli/config.py

import os
from configparser import ConfigParser, MissingSectionHeaderError
from dataclasses import fields


def load_conf_defaults(conf_file: str) -> dict[str, Any]:
    """Load config file as flat dict. Values are strings."""
    conf_file = os.path.expanduser(conf_file)
    if not os.path.exists(conf_file):
        return {}
    
    config = ConfigParser()
    with open(conf_file) as fp:
        content = fp.read()
        try:
            config.read_string(content)
        except MissingSectionHeaderError:
            config.read_string(f"[defaults]\n{content}")
    
    result = {}
    for key, value in config.items("defaults"):
        result[key.replace("-", "_")] = value.strip("\"'")
    return result


def apply_config_to_parser(parser: argparse.ArgumentParser, config: dict[str, Any]) -> None:
    """Apply config defaults to parser. Type conversion via parser's type hints."""
    type_map = {}
    for action in parser._actions:
        if action.dest in config:
            if action.type:
                type_map[action.dest] = action.type
            elif isinstance(action.default, bool):
                type_map[action.dest] = lambda x: x.lower() in ("true", "yes", "1", "on")
            elif isinstance(action.default, int):
                type_map[action.dest] = int
            elif isinstance(action.default, float):
                type_map[action.dest] = float
    
    converted = {}
    for key, value in config.items():
        if key in type_map:
            converted[key] = type_map[key](value)
        else:
            converted[key] = value
    
    parser.set_defaults(**converted)
```

---

## Main Dispatch Flow

```python
# caper/cli/dispatch.py

def main(argv: list[str] | None = None) -> None:
    # 1. Bootstrap for --conf
    bootstrap = argparse.ArgumentParser(add_help=False)
    bootstrap.add_argument("-c", "--conf", default="~/.caper/default.conf")
    conf_ns, _ = bootstrap.parse_known_args(argv)
    
    # 2. Load config
    config = load_conf_defaults(conf_ns.conf)
    
    # 3. Build main parser
    parser = argparse.ArgumentParser(description="Caper CLI")
    parser.add_argument("-v", "--version", action="store_true")
    subparsers = parser.add_subparsers(dest="command")
    
    # 4. Build subparsers from registry
    command_map = {}
    for cmd in COMMANDS:
        if cmd.args_class is None:  # Special case (hpc)
            continue
        p = subparsers.add_parser(cmd.name, aliases=cmd.aliases, help=cmd.help)
        add_dataclass_args(p, cmd.args_class)
        apply_config_to_parser(p, config)
        command_map[cmd.name] = cmd
        for alias in cmd.aliases:
            command_map[alias] = cmd
    
    # Handle --help / no args
    if argv is None and len(sys.argv) == 1:
        parser.print_help()
        return
    
    # 5. Parse
    ns = parser.parse_args(argv)
    
    if ns.version:
        print(__version__)
        return
    
    # 6. Dispatch
    cmd = command_map.get(ns.command)
    if cmd is None:
        parser.error(f"Unknown command: {ns.command}")
    
    # 7. Convert to typed dataclass
    args = namespace_to_dataclass(ns, cmd.args_class)
    
    # 8. Call handler
    cmd.handler(args)
```

---

## HPC Refactor

No more `sys.argv` manipulation. HPC submit builds command from typed args.

```python
@dataclass
class HpcSubmitArgs(RunArgs):
    """HPC submit - extends RunArgs with HPC-specific fields."""
    leader_job_name: str = arg("--leader-job-name", help="Leader job name", required=True)
    slurm_leader_job_resource_param: str = arg("--slurm-leader-job-resource-param", help="SLURM resources")
    # ... other HPC fields
    
    def to_caper_run_command(self) -> list[str]:
        """Build 'caper run' command for HPC submission."""
        cmd = ["caper", "run", self.wdl]
        if self.inputs:
            cmd.extend(["-i", self.inputs])
        if self.backend:
            cmd.extend(["-b", self.backend])
        # ... etc
        return cmd


def handle_hpc_submit(args: HpcSubmitArgs) -> None:
    caper_cmd = args.to_caper_run_command()
    wrapper = get_hpc_wrapper(args.backend)
    stdout = wrapper.submit(args.leader_job_name, caper_cmd)
    print(stdout)
```

---

## Edge Cases Addressed

| Issue | Solution |
|-------|----------|
| **Union/Optional detection** | Check `origin in _UNION_TYPES` where `_UNION_TYPES = (typing.Union, types.UnionType)` on Python 3.10+ |
| **Bool inference** | DON'T auto-infer `store_true`. `action=` must be explicit for bool fields. |
| **List nargs** | DON'T auto-default to `nargs="*"`. Must be explicit per field. |
| **default_factory shared instance** | DON'T call factory at parser build time. Call in `namespace_to_dataclass()` per parse. |
| **store_true with wrong default** | Validate in `arg()`: `store_true` requires `default=False`. |
| **Config merging** | Apply config to parser BEFORE `parse_args()`, not after. |
| **Empty flags** | Validate `flags` not empty in `ArgMeta.__post_init__()`. |
| **Explicit type for enums/Path** | `type=` parameter in `ArgMeta` for cases inference can't handle. |

---

## Migration Strategy

### Phase 1: Core Infrastructure (2 days)
- [ ] Create `cli/arg_field.py` with `ArgMeta`, `arg()`, normalizers
- [ ] Create parser building functions
- [ ] Create `namespace_to_dataclass()`
- [ ] Unit tests for edge cases

### Phase 2: Mixin Dataclasses (2 days)
- [ ] Create all mixin dataclasses in `cli/args/mixins.py`
- [ ] Validate mixins match current `_add_*` functions
- [ ] Tests for field metadata

### Phase 3: Command Dataclasses (2 days)
- [ ] Create composed dataclasses: `RunArgs`, `ServerArgs`, etc.
- [ ] Implement `__post_init__` with normalization + validation
- [ ] Tests for each command's args

### Phase 4: Registry and Dispatch (1 day)
- [ ] Create `COMMANDS` registry
- [ ] Implement new `main()` with config integration
- [ ] Integration tests

### Phase 5: HPC Refactor (1 day)
- [ ] `HpcSubmitArgs.to_caper_run_command()`
- [ ] Delete `sys.argv` manipulation
- [ ] HPC tests

### Phase 6: Cleanup (1 day)
- [ ] Remove old `caper_args.py` code
- [ ] Add `py.typed`, mypy CI
- [ ] Update docs with aliases

**Total: ~9 days**

---

## Implementation Checklist

### Core
- [ ] `ArgMeta` class with validation
- [ ] `arg()` helper with action/default validation
- [ ] Union detection for both `typing.Union` and `types.UnionType`
- [ ] `add_field_to_parser()` without bool auto-inference
- [ ] `namespace_to_dataclass()` with fresh default_factory calls
- [ ] `apply_normalizers()`
- [ ] `abspath()`, `split_commas()` normalizers

### Mixins
- [ ] `CommonArgs`
- [ ] `LocalizationArgs`
- [ ] `BackendArgs`
- [ ] `ServerClientArgs`, `ClientArgs`
- [ ] `SearchArgs`
- [ ] `SubmitIOArgs`
- [ ] `DatabaseArgs`
- [ ] `CromwellArgs`
- [ ] `SchedulerArgs`

### Commands
- [ ] `RunArgs`
- [ ] `ServerArgs`
- [ ] `SubmitArgs`
- [ ] `ListArgs`, `MetadataArgs`, `TroubleshootArgs`
- [ ] `GcpMonitorArgs`, `GcpResAnalysisArgs`, `CleanupArgs`
- [ ] `HpcSubmitArgs`, `HpcListArgs`, `HpcAbortArgs`
- [ ] `InitArgs`

### Infrastructure
- [ ] `COMMANDS` registry
- [ ] `load_conf_defaults()`
- [ ] `apply_config_to_parser()`
- [ ] `main()` dispatch
- [ ] Handlers updated to accept typed args

### Tests
- [ ] Unit: `arg()` validation
- [ ] Unit: Union type detection
- [ ] Unit: `namespace_to_dataclass()` with factories
- [ ] Integration: Each command end-to-end
- [ ] Integration: Config file loading

---

## Appendix: Command Aliases

| Command | Aliases |
|---------|---------|
| `run` | `local`, `exec` |
| `server` | `srv` |
| `submit` | `sub` |
| `list` | `ls` |
| `metadata` | `meta`, `md` |
| `troubleshoot` | `debug`, `ts` |
| `gcp_monitor` | `monitor` |
| `gcp_res_analysis` | `gcp_res`, `res` |
| `cleanup` | `clean` |
| `hpc` | `cluster` |
| `hpc submit` | `sbatch`, `qsub`, `bsub` |
| `hpc list` | `ls` |
| `hpc abort` | `cancel` |
