#!/usr/bin/env python3
"""Defines shared fixtures and custom CLI options for Caper's tests."""

import os

import pytest

from caper.cromwell import Cromwell


def pytest_addoption(parser) -> None:
    parser.addoption('--ci-prefix', default='default_ci_prefix', help='Prefix for CI test.')
    parser.addoption(
        '--gcs-root',
        default='gs://motrpac-test-caper',
        help='GCS root path for CI test. ',
    )
    parser.addoption(
        '--cromwell',
        default=Cromwell.DEFAULT_CROMWELL,
        help='URI for Cromwell JAR. Local path is recommended.',
    )
    parser.addoption(
        '--womtool',
        default=Cromwell.DEFAULT_WOMTOOL,
        help='URI for Womtool JAR. Local path is recommended.',
    )
    parser.addoption('--gcp-prj', help='Project on Google Cloud Platform.')
    parser.addoption(
          '--gcp-service-account-key-json', help='JSON key file for GCP service account.'
      )
    parser.addoption(
        '--gcp-compute-service-account',
        help='Service account email to use for Google Compute Engine batch jobs.',
    )
    parser.addoption(
        '--debug-caper', action='store_true', help='Debug-level logging for CLI tests.'
    )


@pytest.fixture(scope='session')
def ci_prefix(request):
    return request.config.getoption('--ci-prefix').rstrip('/')


@pytest.fixture(scope='session')
def gcs_root(request):
    """GCS root to generate test GCS URIs on."""
    root = request.config.getoption('--gcs-root')
    if not root.startswith('gs://'):
        msg = f'GCS root must start with "gs://" but got {root}'
        raise ValueError(msg)
    return root.rstrip('/')


@pytest.fixture(scope='session')
def cromwell(request):
    return request.config.getoption('--cromwell')


@pytest.fixture(scope='session')
def womtool(request):
    return request.config.getoption('--womtool')


@pytest.fixture(scope='session')
def gcp_prj(request):
    project = request.config.getoption('--gcp-prj') or os.getenv('GOOGLE_CLOUD_PROJECT')
    if project is None:
        msg = 'Must supply --gcp-prj arg or set GOOGLE_CLOUD_PROJECT env variable'
        raise ValueError(msg)
    return project


@pytest.fixture(scope='session')
def gcp_service_account_key_json(request):
    return request.config.getoption('--gcp-service-account-key-json')


@pytest.fixture(scope='session')
def gcp_compute_service_account(request):
    return request.config.getoption('--gcp-compute-service-account')


@pytest.fixture(scope='session')
def debug_caper(request):
    return request.config.getoption('--debug-caper')


@pytest.fixture(scope='session')
def gcp_res_analysis_metadata(gcs_root) -> str:
    return f'{gcs_root}/resource_analysis/metadata.json'
