from __future__ import annotations

import fnmatch
import io
import logging
from uuid import UUID
from pathlib import Path

import requests
from requests.exceptions import HTTPError, ConnectionError as RequestsConnectionError

from .cromwell_metadata import CromwellMetadata
from typing import Any, Callable, TypeVar

F = TypeVar('F', bound=Callable[..., Any])

logger = logging.getLogger(__name__)


def requests_error_handler(func: F) -> F:
    """Re-raise ConnectionError with help message.
    Continue on HTTP 404 error (server is on but workflow doesn't exist).
    Otherwise, re-raise from None to hide nested tracebacks.
    """

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)

        except HTTPError as err:
            if err.response.status_code == 404:
                logger.exception("Workflow doesn't seem to exist.")
                return None
            message = (
                f'{err}\n\nCromwell server is on but got an HTTP error other than 404. '
            )
            raise HTTPError(message) from None

        except RequestsConnectionError as err:
            message = (
                f'{err}\n\n'
                'Failed to connect to Cromwell server. '
                'Check if Caper server is running. '
                'Also check if hostname and port are correct. '
                f'method={err.request.method if err.request else None}, '
                f'url={err.request.url if err.request else None}'
            )
            raise ConnectionError(message) from None

    return wrapper


def is_valid_uuid(workflow_id: str, version: int = 4) -> bool:
    """To validate Cromwell's UUID (lowercase only).
    This does not allow uppercase UUIDs.
    """
    if not isinstance(workflow_id, str):
        return False
    if not workflow_id.islower():
        return False

    try:
        UUID(workflow_id, version=version)
    except ValueError:
        return False
    return True


def has_wildcard(workflow_id_or_label: str | list[str] | tuple[str, ...] | None) -> bool:
    """Check if string or any element in list/tuple has
    a wildcard (? or *).

    Args:
        workflow_id_or_label:
            Workflow ID (str) or label (str).
            Or array (list, tuple) of them.
    """
    if workflow_id_or_label is None:
        return False
    if isinstance(workflow_id_or_label, (list, tuple)):
        return any(has_wildcard(val) for val in workflow_id_or_label)
    return '?' in workflow_id_or_label or '*' in workflow_id_or_label


class CromwellRestAPI:
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
        self._hostname = hostname
        self._port = port

        self._user = user
        self._password = password
        self.__init_auth()

    def submit(
        self,
        source: str,
        dependencies: str | None = None,
        inputs: str | None = None,
        options: str | None = None,
        labels: str | None = None,
        on_hold: bool = False,
    ) -> dict[str, Any]:
        """Submit a workflow.

        Returns:
            JSON Response from POST request submit a workflow
        """
        manifest = {}
        with Path(source).open() as fp:
            manifest['workflowSource'] = io.StringIO(fp.read())
        if dependencies:
            with Path(dependencies).open('rb') as fp:
                manifest['workflowDependencies'] = io.BytesIO(fp.read())
        if inputs:
            with Path(inputs).open() as fp:
                manifest['workflowInputs'] = io.StringIO(fp.read())
        else:
            manifest['workflowInputs'] = io.StringIO('{}')
        if options:
            with Path(options).open() as fp:
                manifest['workflowOptions'] = io.StringIO(fp.read())
        if labels:
            with Path(labels).open() as fp:
                manifest['labels'] = io.StringIO(fp.read())
        if on_hold:
            manifest['workflowOnHold'] = True

        r = self.__request_post(CromwellRestAPI.ENDPOINT_SUBMIT, manifest)
        logger.debug('submit: %s', r)
        return r

    def abort(self, workflow_ids: list[str] | None = None, labels: list[tuple[str, str]] | None = None) -> list[dict[str, Any]]:
        """Abort workflows matching workflow IDs or labels

        Returns:
            List of JSON responses from POST request
            for aborting workflows
        """
        valid_workflow_ids = self.find_valid_workflow_ids(
            workflow_ids=workflow_ids,
            labels=labels,
        )
        if valid_workflow_ids is None:
            return None

        result = []
        for workflow_id in valid_workflow_ids:
            r = self.__request_post(
                CromwellRestAPI.ENDPOINT_ABORT.format(wf_id=workflow_id),
            )
            result.append(r)
        logger.debug('abort: %s', result)
        return result

    def release_hold(self, workflow_ids: list[str] | None = None, labels: list[tuple[str, str]] | None = None) -> list[dict[str, Any]]:
        """Release hold of workflows matching workflow IDs or labels

        Returns:
            List of JSON responses from POST request
            for releasing hold of workflows
        """
        valid_workflow_ids = self.find_valid_workflow_ids(
            workflow_ids=workflow_ids,
            labels=labels,
        )
        if valid_workflow_ids is None:
            return None

        result = []
        for workflow_id in valid_workflow_ids:
            r = self.__request_post(
                CromwellRestAPI.ENDPOINT_RELEASE_HOLD.format(wf_id=workflow_id),
            )
            result.append(r)
        logger.debug('release_hold: %s', result)
        return result

    def get_default_backend(self) -> str:
        """Retrieve default backend name

        Returns:
            Default backend name
        """
        return self.get_backends()['defaultBackend']

    def get_backends(self) -> dict[str, Any]:
        """Retrieve available backend names and default backend name

        Returns:
            JSON response with keys "defaultBackend" and "supportedBackends"
            Example: {"defaultBackend":"Local","supportedBackends":
                      ["Local","aws","gcp","pbs","sge","slurm"]}
        """
        return self.__request_get(CromwellRestAPI.ENDPOINT_BACKEND)

    def find_valid_workflow_ids(
        self,
        workflow_ids: list[str] | None = None,
        labels: list[tuple[str, str]] | None = None,
        exclude_subworkflow: bool = True,
    ) -> list[str]:
        """Checks if workflow ID in `workflow_ids` are already valid UUIDs (without wildcards).
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

    def get_metadata(self, workflow_ids: list[str] | None = None, labels: list[tuple[str, str]] | None = None, embed_subworkflow: bool = False) -> list[dict[str, Any]]:
        """Retrieve metadata for workflows matching workflow IDs or labels

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
        valid_workflow_ids = self.find_valid_workflow_ids(
            workflow_ids=workflow_ids,
            labels=labels,
        )
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

    def get_labels(self, workflow_id: str) -> dict[str, Any]:
        """Get labels JSON for a specified workflow

        Returns:
            Labels JSON for a workflow
        """
        if workflow_id is None or not is_valid_uuid(workflow_id):
            return None

        r = self.__request_get(
            CromwellRestAPI.ENDPOINT_LABELS.format(wf_id=workflow_id),
        )
        if r is None:
            return None
        return r['labels']

    def get_label(self, workflow_id: str, key: str) -> str | None:
        """Get a label for a key in a specified workflow

        Returns:
            Value for a specified key in labels JSON for a workflow
        """
        labels = self.get_labels(workflow_id)
        if labels is None:
            return None
        if key in labels:
            return labels[key]
        return None

    def update_labels(self, workflow_id: str, labels: list[tuple[str, str]]) -> dict[str, Any] | None:
        """Update labels for a specified workflow with
        a list of (key, val) tuples
        """
        if workflow_id is None or labels is None:
            return None
        r = self.__request_patch(
            CromwellRestAPI.ENDPOINT_LABELS.format(wf_id=workflow_id),
            labels,
        )
        logger.debug('update_labels: %s', r)
        return r

    def find_with_wildcard(
        self,
        workflow_ids: list[str] | None = None,
        labels: list[tuple[str, str]] | None = None,
        exclude_subworkflow: bool = True,
    ) -> list[dict[str, Any]]:
        """Retrieves all workflows from Cromwell server.
        And then find matching workflows by ID or labels.
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
                'find_with_wildcard: workflow_ids=%s, '
                'labels=%s, result=%s',
                workflow_ids, labels, result,
            )

        return result

    def find_by_workflow_ids(self, workflow_ids: list[str] | None = None, exclude_subworkflow: bool = True) -> list[dict[str, Any]]:
        """Finds workflows by exactly matching workflow IDs (UUIDs).
        Does OR search for a list of workflow IDs.
        Invalid UUID in `workflows_ids` will be ignored without warning.
        Wildcards (? and *) are not allowed.

        Args:
            workflow_ids:
                List of workflow ID (UUID) strings.
                Lower-case only (Cromwell uses lower-case UUIDs).
        Returns:
            List of matched workflow JSONs.
        """
        if has_wildcard(workflow_ids):
            msg = f'Wildcards are not allowed in workflow_ids. ids={workflow_ids}'
            raise ValueError(
                msg,
            )

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

            logger.debug(
                'find_by_workflow_ids: workflow_ids=%s, result=%s',
                workflow_ids, result,
            )

        return result

    def find_by_labels(self, labels: list[tuple[str, str]] | None = None, exclude_subworkflow: bool = True) -> list[dict[str, Any]]:
        """Finds workflows by exactly matching labels (key, value) tuples.
        Does OR search for a list of label key/value pairs.
        Wildcards (? and *) are not allowed.

        Args:
            labels:
                List of labels (key/value pairs).
        Returns:
            List of matched workflow JSONs.
        """
        if has_wildcard(labels):
            msg = f'Wildcards are not allowed in labels. labels={labels}'
            raise ValueError(
                msg,
            )

        result = []
        if labels:
            # reformat labels with `:` notation. exclude pairs with empty value.
            labels = [f'{key}:{val}' for key, val in labels if val]
            resp = self.__request_get(
                CromwellRestAPI.ENDPOINT_WORKFLOWS,
                params={
                    'additionalQueryResultFields': 'labels',
                    'includeSubworkflows': not exclude_subworkflow,
                    'labelor': labels,
                },
            )
            if resp and resp['results']:
                result.extend(resp['results'])

            logger.debug(
                'find_by_labels: labels=%s, result=%s',
                labels, result,
            )

        return result

    def find(self, workflow_ids: list[str] | None = None, labels: list[tuple[str, str]] | None = None, exclude_subworkflow: bool = True) -> list[dict[str, Any]]:
        """Wrapper for the following three find functions.
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
        wildcard_found_in_labels = has_wildcard(
            [val for key, val in labels] if labels else None,
        )
        if wildcard_found_in_workflow_ids or wildcard_found_in_labels:
            return self.find_with_wildcard(
                workflow_ids=workflow_ids,
                labels=labels,
                exclude_subworkflow=exclude_subworkflow,
            )

        result = []

        result_by_labels = self.find_by_labels(
            labels=labels,
            exclude_subworkflow=exclude_subworkflow,
        )
        result.extend(result_by_labels)

        workflow_ids_found_by_labels = [workflow['id'] for workflow in result_by_labels]
        result.extend(
            [
                workflow
                for workflow in self.find_by_workflow_ids(
                    workflow_ids=workflow_ids,
                    exclude_subworkflow=exclude_subworkflow,
                )
                if workflow['id'] not in workflow_ids_found_by_labels
            ],
        )

        return result

    def __init_auth(self) -> None:
        """Init auth object"""
        if self._user is not None and self._password is not None:
            self._auth = (self._user, self._password)
        else:
            self._auth = None

    @requests_error_handler
    def __request_get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """GET request

        Returns:
            JSON response
        """
        url = (
            CromwellRestAPI.QUERY_URL.format(hostname=self._hostname, port=self._port)
            + endpoint
        )
        resp = requests.get(
            url,
            auth=self._auth,
            params=params,
            headers={'accept': 'application/json'},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    @requests_error_handler
    def __request_post(self, endpoint: str, manifest: dict[str, Any] | None = None) -> dict[str, Any]:
        """POST request

        Returns:
            JSON response
        """
        url = (
            CromwellRestAPI.QUERY_URL.format(hostname=self._hostname, port=self._port)
            + endpoint
        )
        resp = requests.post(
            url,
            files=manifest,
            auth=self._auth,
            headers={'accept': 'application/json'},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    @requests_error_handler
    def __request_patch(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        """POST request

        Returns:
            JSON response
        """
        url = (
            CromwellRestAPI.QUERY_URL.format(hostname=self._hostname, port=self._port)
            + endpoint
        )
        resp = requests.patch(
            url,
            data=data,
            auth=self._auth,
            headers={'accept': 'application/json', 'content-type': 'application/json'},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()
