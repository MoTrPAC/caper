"""Tests for `caper hpc` (leader job script generation and conf file handling)."""

import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from textwrap import dedent

import pytest

from caper import hpc
from caper.caper_args import get_parser_and_defaults
from caper.cli_hpc import make_caper_run_command_for_hpc_submit, subcmd_hpc
from caper.hpc import SlurmWrapper

BASH = shutil.which('bash')

ARGS_WITH_SPACES = [
    '/path/with space/hello_world.wdl',
    '--slurm-leader-job-resource-param',
    '-t 48:00:00 --mem 4G',
    '--str-label',
    'it\'s "quoted" $HOME `x`',
]

HPC_CONF = dedent(
    """\
    backend=slurm
    slurm-account=prj_gregor
    slurm-partition=batch
    slurm-leader-job-resource-param=-t 12:00:00 --mem 8G
    """
)


@pytest.fixture
def home_dir(tmp_path, monkeypatch):
    """HpcWrapper.submit() writes the leader job script in $HOME."""
    monkeypatch.setattr(hpc.Path, 'home', lambda: tmp_path)
    return tmp_path


@pytest.fixture
def capture_slurm(monkeypatch):
    """Capture the sbatch command line and the leader job script instead of running sbatch."""
    captured = {}

    def fake_run_command(_self, command):
        captured['command'] = command
        captured['script'] = Path(command[-1]).read_text()
        return 'Submitted batch job 1'

    monkeypatch.setattr(SlurmWrapper, '_run_command', fake_run_command)
    return captured


@pytest.fixture
def hpc_conf(tmp_path):
    conf = tmp_path / 'hpc.conf'
    conf.write_text(HPC_CONF)
    return str(conf)


@pytest.mark.skipif(BASH is None, reason='bash is required')
@pytest.mark.usefixtures('home_dir')
def test_leader_job_script_keeps_args_with_spaces(capture_slurm) -> None:
    """Each argument must reach the leader job unchanged (issue #13)."""
    command = ['printf', '%s\\n', *ARGS_WITH_SPACES]
    SlurmWrapper().submit('job', command)

    assert BASH is not None
    result = subprocess.run(  # noqa: S603
        [BASH, '-c', capture_slurm['script']],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.splitlines() == ARGS_WITH_SPACES


def test_make_caper_run_command_for_hpc_submit(monkeypatch) -> None:
    monkeypatch.setattr(sys, 'argv', ['caper', 'hpc', 'submit', *ARGS_WITH_SPACES])
    assert make_caper_run_command_for_hpc_submit() == ['caper', 'run', *ARGS_WITH_SPACES]


@pytest.mark.parametrize(
    'argv',
    [
        ['hpc', 'submit', 'x.wdl', '--leader-job-name', 'a'],
        ['hpc', 'list'],
        ['hpc', 'abort', '123'],
    ],
)
def test_hpc_subcommands_read_conf(hpc_conf, argv) -> None:
    """`caper hpc` subcommands must take backend etc. from the conf file (issue #12)."""
    parser, _ = get_parser_and_defaults(conf_file=hpc_conf)
    args = parser.parse_args(argv)
    assert args.backend == 'slurm'
    if argv[1] == 'submit':
        assert args.slurm_account == 'prj_gregor'
        assert args.slurm_partition == 'batch'
        assert args.slurm_leader_job_resource_param == '-t 12:00:00 --mem 8G'


@pytest.mark.usefixtures('home_dir')
def test_hpc_submit_end_to_end(hpc_conf, capture_slurm, monkeypatch) -> None:
    """Conf values go to sbatch and CLI args reach `caper run` unchanged."""
    cli_args = ['--conf', hpc_conf, '--leader-job-name', 'hello', *ARGS_WITH_SPACES]
    monkeypatch.setattr(sys, 'argv', ['caper', 'hpc', 'submit', *cli_args])
    parser, _ = get_parser_and_defaults(conf_file=hpc_conf)
    subcmd_hpc(parser.parse_args(sys.argv[1:]))

    sbatch = capture_slurm['command']
    # --slurm-leader-job-resource-param on the command line overrides the conf file value.
    assert sbatch[:5] == ['sbatch', '-t', '48:00:00', '--mem', '4G']
    # account and partition come from the conf file.
    assert sbatch[5:9] == ['-p', 'batch', '-A', 'prj_gregor']
    assert sbatch[-2] == 'CAPER_hello'

    script_lines = capture_slurm['script'].splitlines()
    assert script_lines[0] == '#!/bin/bash'
    assert shlex.split(script_lines[1]) == ['caper', 'run', *cli_args]
