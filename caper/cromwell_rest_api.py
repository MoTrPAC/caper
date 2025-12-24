"""Cromwell REST API client."""

from __future__ import annotations

import fnmatch
import io
import logging
from collections.abc import Callable, Iterable
from http import HTTPStatus
from typing import Any, ParamSpec, TypedDict, TypeVar, cast
from uuid import UUID

import requests

from .cromwell_metadata import CromwellMetadata

logger = logging.getLogger(__name__)

T = TypeVar('T')
P = ParamSpec('P')

# Sometimes, Cromwell has taken up to 300 seconds to deliver requests
DEFAULT_REQUEST_TIMEOUT = 300


def requests_error_handler(func: Callable[P, T]) -> Callable[P, T | None]:
    """
    Re-raise ConnectionError with help message.

    Continue on HTTP 404 error (server is on but workflow doesn't exist).
    Otherwise, re-raise from None to hide nested tracebacks.
    """

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T | None:
        try:
            return func(*args, **kwargs)

        except requests.exceptions.HTTPError as err:
            if err.response.status_code == HTTPStatus.NOT_FOUND:
                logger.exception("Method/workflow/endpoint doesn't exist.")
                return None

            message = f'{err}\n\nCromwell server is on but got an HTTP error other than 404. '
            raise requests.exceptions.HTTPError(message) from None

        except requests.exceptions.ConnectionError as err:
            message = (
                'Failed to connect to Cromwell server. Check if Caper server is running. '
                'Also check if hostname and port are correct. '
            )
            if err.request:
                message += f'method={err.request.method}, url={err.request.url}'
            else:
                message += 'No request information available.'
            raise requests.exceptions.ConnectionError(message, request=err.request) from err

    return wrapper


def is_valid_uuid(workflow_id: str, version: int = 4) -> bool:
    """Validate Cromwell's UUID (lowercase only)."""
    if not isinstance(workflow_id, str):
        return False
    if not workflow_id.islower():
        return False

    try:
        UUID(workflow_id, version=version)
    except ValueError:
        return False
    return True


def has_wildcard(workflow_id_or_label: str | Iterable[str] | None) -> bool:
    """
    Check if string or any element in list/tuple has a wildcard (? or *).

    Args:
        workflow_id_or_label:
            Workflow ID (str) or label (str).
            Or array (list, tuple) of them.
    """
    if not workflow_id_or_label:
        return False
    if isinstance(workflow_id_or_label, Iterable) and not isinstance(workflow_id_or_label, str):
        return any(has_wildcard(val) for val in workflow_id_or_label)

    return '?' in workflow_id_or_label or '*' in workflow_id_or_label


class CromwellRestAPI:
    """Cromwell REST API client."""

    QUERY_URL = 'http://{hostname}:{port}'
    ENDPOINT_BACKEND = '/api/workflows/v1/backends'
    ENDPOINT_WORKFLOWS = '/api/workflows/v1/query'
    ENDPOINT_METADATA = '/api/workflows/v1/{wf_id}/metadata'
    ENDPOINT_LABELS = '/api/workflows/v1/{wf_id}/labels'
    ENDPOINT_SUBMIT = '/api/workflows/v1'
    ENDPOINT_ABORT = '/api/workflows/v1/{wf_id}/abort'
    ENDPOINT_RELEASE_HOLD = '/api/workflows/v1/{wf_id}/releaseHold'
    DEFAULT_HOSTNAME = 'localhost'
    DEFAULT_PORT = 8000

    def __init__(
        self,
        hostname: str = DEFAULT_HOSTNAME,
        port: int = DEFAULT_PORT,
        user: str | None = None,
        password: str | None = None,
    ) -> None:
        """
        Initialize the Cromwell REST API client.

        Args:
            hostname:
                The hostname of the Cromwell server.
            port:
                The port of the Cromwell server.
            user:
                The user to authenticate with the Cromwell server.
            password:
                The password to authenticate with the Cromwell server.
        """
        self._hostname = hostname
        self._port = port
        self._user = user
        self._password = password
        self._auth = (user, password) if user and password else None

    def submit(
        self,
        source: str,
        dependencies: str | None = None,
        inputs: str | None = None,
        options: str | None = None,
        labels: str | None = None,
        on_hold: bool = False,
    ) -> dict | None:
        """
        Submit a workflow.

        Returns:
            JSON Response from POST request submit a workflow
        """
        manifest: dict = {}
        with open(source) as fp:
            manifest['workflowSource'] = io.StringIO(fp.read())
        if dependencies:
            with open(dependencies, 'rb') as fp:
                manifest['workflowDependencies'] = io.BytesIO(fp.read())
        if inputs:
            with open(inputs) as fp:
                manifest['workflowInputs'] = io.StringIO(fp.read())
        else:
            manifest['workflowInputs'] = io.StringIO('{}')
        if options:
            with open(options) as fp:
                manifest['workflowOptions'] = io.StringIO(fp.read())
        if labels:
            with open(labels) as fp:
                manifest['labels'] = io.StringIO(fp.read())
        if on_hold:
            manifest['workflowOnHold'] = True

        r = self.__request_post(CromwellRestAPI.ENDPOINT_SUBMIT, manifest)
        logger.debug('submit: %s', r)
        return r

    def abort(
        self, workflow_ids: list[str] | None = None, labels: dict[str, str] | None = None
    ) -> list[dict] | None:
        """
        Abort workflows matching workflow IDs or labels.

        Returns:
            List of JSON responses from POST request
            for aborting workflows
        """
        valid_workflow_ids = self.find_valid_workflow_ids(workflow_ids=workflow_ids, labels=labels)
        if valid_workflow_ids is None:
            return None

        result = []
        for workflow_id in valid_workflow_ids:
            r = self.__request_post(CromwellRestAPI.ENDPOINT_ABORT.format(wf_id=workflow_id))
            result.append(r)
        logger.debug('abort: %s', result)
        return result

    def release_hold(
        self, workflow_ids: list[str] | None = None, labels: dict[str, str] | None = None
    ) -> list[dict] | None:
        """
        Release hold of workflows matching workflow IDs or labels.

        Returns:
            List of JSON responses from POST request
            for releasing hold of workflows
        """
        valid_workflow_ids = self.find_valid_workflow_ids(workflow_ids=workflow_ids, labels=labels)
        if valid_workflow_ids is None:
            return None

        result = []
        for workflow_id in valid_workflow_ids:
            r = self.__request_post(CromwellRestAPI.ENDPOINT_RELEASE_HOLD.format(wf_id=workflow_id))
            result.append(r)
        logger.debug('release hold: %s', result)
        return result

    def get_default_backend(self) -> str:
        """
        Retrieve default backend name.

        Returns:
            Default backend name
        """
        if not (backends := self.get_backends()):
            msg = "No backends found, unhandled failure since this shouldn't happen."
            raise RuntimeError(msg)
        return backends['defaultBackend']

    class _BackendResponse(TypedDict):
        """Represents JSON response from Cromwell REST API for backend response."""

        defaultBackend: str
        supportedBackends: list[str]

    def get_backends(self) -> _BackendResponse | None:
        """
        Retrieve available backend names and default backend name.

        Returns:
            JSON response with keys "defaultBackend" and "supportedBackends"
            Example: {"defaultBackend":"Local","supportedBackends":
                      ["Local","aws","gcp","pbs","sge","slurm"]}
        """
        return cast(
            'CromwellRestAPI._BackendResponse | None',
            self.__request_get(CromwellRestAPI.ENDPOINT_BACKEND),
        )

    def find_valid_workflow_ids(
        self,
        workflow_ids: list[str] | None = None,
        labels: dict[str, str] | None = None,
        exclude_subworkflow: bool = True,
    ) -> list[str] | None:
        """
        Checks if workflow ID in `workflow_ids` are already valid UUIDs (without wildcards).

        If so then we don't have to send the server a query to get matching workflow IDs.
        """
        if not labels and workflow_ids and all(is_valid_uuid(i) for i in workflow_ids):
            return workflow_ids
        workflows = self.find(
            workflow_ids=workflow_ids,
            labels=labels,
            exclude_subworkflow=exclude_subworkflow,
        )
        if not workflows:
            return None
        return [w['id'] for w in workflows]

    def get_metadata(
        self,
        workflow_ids: list[str] | None = None,
        labels: dict[str, str] | None = None,
        embed_subworkflow: bool = False,
    ) -> list[dict] | None:
        """
        Retrieve metadata for workflows matching workflow IDs or labels.

        Args:
            workflow_ids:
                List of workflows IDs to find workflows matched.
            labels:
                List of Caper's string labels to find workflows matched.
            embed_subworkflow:
                Recursively embed subworkflow's metadata in main
                workflow's metadata.
                This flag is to mimic behavior of Cromwell run mode with -m.
                Metadata JSON generated with Cromwell run mode
                includes all subworkflows embedded in main workflow's JSON file.
        """
        valid_workflow_ids = self.find_valid_workflow_ids(workflow_ids=workflow_ids, labels=labels)
        if valid_workflow_ids is None:
            return None

        result = []
        for workflow_id in valid_workflow_ids:
            params = {}
            if embed_subworkflow:
                params['expandSubWorkflows'] = True

            m = self.__request_get(
                CromwellRestAPI.ENDPOINT_METADATA.format(wf_id=workflow_id),
                params=params,
            )
            if m:
                cm = CromwellMetadata(m)
                result.append(cm.metadata)
        return result

    def get_labels(self, workflow_id: str) -> dict | None:
        """
        Get labels JSON for a specified workflow.

        Returns:
            Labels JSON for a workflow
        """
        if workflow_id is None or not is_valid_uuid(workflow_id):
            return None

        r = self.__request_get(CromwellRestAPI.ENDPOINT_LABELS.format(wf_id=workflow_id))
        if r is None:
            return None
        return r['labels']

    def get_label(self, workflow_id: str, key: str) -> str | None:
        """
        Get a label for a key in a specified workflow.

        Returns:
            Value for a specified key in labels JSON for a workflow
        """
        labels = self.get_labels(workflow_id)
        if labels is None:
            return None
        if key in labels:
            return labels[key]
        return None

    def update_labels(self, workflow_id: str, labels: dict[str, str]) -> dict | None:
        """Update labels for a specified workflow with a list of (key, val) tuples."""
        if workflow_id is None or labels is None:
            return None
        r = self.__request_patch(CromwellRestAPI.ENDPOINT_LABELS.format(wf_id=workflow_id), labels)
        logger.debug('update labels: %s', r)
        return r

    def find_with_wildcard(
        self,
        workflow_ids: list[str] | None = None,
        labels: list[tuple[str, str]] | None = None,
        exclude_subworkflow: bool = True,
    ) -> list[dict]:
        """
        Retrieves all workflows from Cromwell server, then finds workflows by ID or labels.

        Wildcards (? and *) are allowed for both parameters.
        """
        result = []

        if not workflow_ids and not labels:
            return result

        resp = self.__request_get(
            CromwellRestAPI.ENDPOINT_WORKFLOWS,
            params={
                'additionalQueryResultFields': 'labels',
                'includeSubworkflows': not exclude_subworkflow,
            },
        )

        if resp and resp['results']:
            for workflow in resp['results']:
                matched = False
                if 'id' not in workflow:
                    continue
                if workflow_ids:
                    for wf_id in workflow_ids:
                        if fnmatch.fnmatchcase(workflow['id'], wf_id):
                            result.append(workflow)
                            matched = True
                            break
                if matched:
                    continue
                if labels and 'labels' in workflow:
                    for k, v in labels:
                        v_ = workflow['labels'].get(k)
                        if not v_:
                            continue
                        if isinstance(v_, str) and isinstance(v, str):
                            # matching with wildcards for str values only
                            if fnmatch.fnmatchcase(v_, v):
                                result.append(workflow)
                                break
                        elif v_ == v:
                            result.append(workflow)
                            break
            logger.debug(
                'find with wildcard: workflow_ids=%s, labels=%s, result=%s',
                workflow_ids,
                labels,
                result,
            )

        return result

    def find_by_workflow_ids(
        self, workflow_ids: list[str] | None = None, exclude_subworkflow: bool = True
    ) -> list[dict]:
        """
        Finds workflows by exactly matching workflow IDs (UUIDs).

        Does OR search for a list of workflow IDs.
        Invalid UUID in `workflows_ids` will be ignored without warning.
        Wildcards (? and *) are not allowed.

        Args:
            workflow_ids:
                List of workflow ID (UUID) strings.
                Lower-case only (Cromwell uses lower-case UUIDs).
            exclude_subworkflow:
                Whether to exclude subworkflows.

        Returns:
            List of matched workflow JSONs.
        """
        if has_wildcard(workflow_ids):
            msg = f'Wildcards are not allowed in workflow_ids. ids={workflow_ids}'
            raise ValueError(msg)

        result = []
        if workflow_ids:
            # exclude invalid workflow UUIDs.
            workflow_ids = [wf_id for wf_id in workflow_ids if is_valid_uuid(wf_id)]
            resp = self.__request_get(
                CromwellRestAPI.ENDPOINT_WORKFLOWS,
                params={
                    'additionalQueryResultFields': 'labels',
                    'includeSubworkflows': not exclude_subworkflow,
                    'id': workflow_ids,
                },
            )
            if resp and resp['results']:
                result.extend(resp['results'])

            logger.debug('find by workflow ids: workflow_ids=%s, result=%s', workflow_ids, result)

        return result

    def find_by_labels(
        self, labels: list[tuple[str, str]] | None = None, exclude_subworkflow: bool = True
    ) -> list[dict]:
        """
        Finds workflows by exactly matching labels (key, value) tuples.

        Does OR search for a list of label key/value pairs. Wildcards (? and *) are not allowed.

        Args:
            labels:
                List of labels (key/value pairs).
            exclude_subworkflow:
                Whether to exclude subworkflows.

        Returns:
            List of matched workflow JSONs.
        """
        if has_wildcard(labels):
            msg = f'Wildcards are not allowed in labels. labels={labels}'
            raise ValueError(msg)

        result = []
        if labels:
            resp = self.__request_get(
                CromwellRestAPI.ENDPOINT_WORKFLOWS,
                params={
                    'additionalQueryResultFields': 'labels',
                    'includeSubworkflows': not exclude_subworkflow,
                    # reformat labels with `:` notation. exclude pairs with empty value.
                    'labelor': [f'{key}:{val}' for key, val in labels if val],
                },
            )
            if resp and resp['results']:
                result.extend(resp['results'])

            logger.debug('find by labels: labels=%s, result=%s', labels, result)

        return result

    def find(
        self,
        workflow_ids: list[str] | None = None,
        labels: list[tuple[str, str]] | None = None,
        exclude_subworkflow: bool = True,
    ) -> list[dict]:
        """
        Wrapper for the following three find functions.

        - find_with_wildcard
        - find_by_workflow_ids
        - find_by_labels

        Find workflows by matching workflow IDs or label (key, value) tuples.
        Does OR search for both parameters.
        Wildcards (? and *) in both parameters are allowed but Caper will
        retrieve a list of all workflows, which can lead to HTTP 503 of
        Cromwell server if there are many subworkflows and not `exclude_subworkflow`.

        Args:
            workflow_ids:
                List of workflow ID (UUID) strings.
                Lower-case only.
            labels:
                List of labels (key/value pairs).
            exclude_subworkflow:
                Exclude subworkflows.

        Returns:
            List of matched workflow JSONs.
        """
        wildcard_found_in_workflow_ids = has_wildcard(workflow_ids)
        wildcard_found_in_labels = has_wildcard([val for key, val in labels] if labels else None)
        if wildcard_found_in_workflow_ids or wildcard_found_in_labels:
            return self.find_with_wildcard(
                workflow_ids=workflow_ids,
                labels=labels,
                exclude_subworkflow=exclude_subworkflow,
            )

        result = []

        result_by_labels = self.find_by_labels(
            labels=labels, exclude_subworkflow=exclude_subworkflow
        )
        result.extend(result_by_labels)

        workflow_ids_found_by_labels = [workflow['id'] for workflow in result_by_labels]
        result.extend(
            [
                workflow
                for workflow in self.find_by_workflow_ids(
                    workflow_ids=workflow_ids, exclude_subworkflow=exclude_subworkflow
                )
                if workflow['id'] not in workflow_ids_found_by_labels
            ]
        )

        return result

    def __init_auth(self) -> None:
        """Init auth object."""
        if self._user is not None and self._password is not None:
            self._auth = (self._user, self._password)
        else:
            self._auth = None

    @requests_error_handler
    def __request_get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict:
        """
        Send a GET request to the Cromwell REST API.

        Returns:
            JSON response
        """
        url = CromwellRestAPI.QUERY_URL.format(hostname=self._hostname, port=self._port) + endpoint
        resp = requests.get(
            url,
            auth=self._auth,
            params=params,
            timeout=DEFAULT_REQUEST_TIMEOUT,
            headers={'accept': 'application/json'},
        )
        resp.raise_for_status()
        return resp.json()

    @requests_error_handler
    def __request_post(self, endpoint: str, manifest: dict[str, Any] | None = None) -> dict:
        """
        Send a POST request to the Cromwell REST API.

        Returns:
            JSON response
        """
        url = CromwellRestAPI.QUERY_URL.format(hostname=self._hostname, port=self._port) + endpoint
        resp = requests.post(
            url,
            files=manifest,
            auth=self._auth,
            timeout=DEFAULT_REQUEST_TIMEOUT,
            headers={'accept': 'application/json'},
        )
        resp.raise_for_status()
        return resp.json()

    @requests_error_handler
    def __request_patch(self, endpoint: str, data: dict[str, Any] | None = None) -> dict | None:
        """
        Send a PATCH request to the Cromwell REST API.

        Returns:
            JSON response
        """
        url = CromwellRestAPI.QUERY_URL.format(hostname=self._hostname, port=self._port) + endpoint
        resp = requests.patch(
            url,
            data=data,
            auth=self._auth,
            timeout=DEFAULT_REQUEST_TIMEOUT,
            headers={'accept': 'application/json', 'content-type': 'application/json'},
        )
        resp.raise_for_status()
        return resp.json()
