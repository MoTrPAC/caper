"""Tests for GCP cost estimation feature."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from caper.caper_client import CaperClient
from caper.cli import subcmd_cost
from caper.cromwell_rest_api import CromwellRestAPI


SAMPLE_COST_RESPONSE = {
    'id': 'f9c26f2e-f550-4748-a650-5d0d4cab9f3a',
    'status': 'Succeeded',
    'cost': 1.23,
    'currency': 'USD',
    'errors': [],
}

SAMPLE_COST_RESPONSE_WITH_ERRORS = {
    'id': 'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
    'status': 'Running',
    'cost': 0.45,
    'currency': 'USD',
    'errors': ["Couldn't find valid vmCostPerHour for callA.-1.1"],
}


class TestGetCost:
    """Tests for CromwellRestAPI.get_cost()."""

    def test_get_cost_single_workflow(self) -> None:
        cra = CromwellRestAPI()
        with (
            patch.object(
                cra,
                'find_valid_workflow_ids',
                return_value=['f9c26f2e-f550-4748-a650-5d0d4cab9f3a'],
            ),
            patch.object(cra, '_CromwellRestAPI__request_get', return_value=SAMPLE_COST_RESPONSE),
        ):
            result = cra.get_cost(workflow_ids=['f9c26f2e-f550-4748-a650-5d0d4cab9f3a'])

        assert result is not None
        assert len(result) == 1
        assert result[0]['cost'] == 1.23
        assert result[0]['currency'] == 'USD'

    def test_get_cost_multiple_workflows(self) -> None:
        cra = CromwellRestAPI()
        ids = [
            'f9c26f2e-f550-4748-a650-5d0d4cab9f3a',
            'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
        ]
        with (
            patch.object(cra, 'find_valid_workflow_ids', return_value=ids),
            patch.object(
                cra,
                '_CromwellRestAPI__request_get',
                side_effect=[SAMPLE_COST_RESPONSE, SAMPLE_COST_RESPONSE_WITH_ERRORS],
            ),
        ):
            result = cra.get_cost(workflow_ids=ids)

        assert result is not None
        assert len(result) == 2

    def test_get_cost_no_valid_ids(self) -> None:
        cra = CromwellRestAPI()
        with patch.object(cra, 'find_valid_workflow_ids', return_value=None):
            result = cra.get_cost(workflow_ids=['invalid'])

        assert result is None

    def test_get_cost_skips_none_responses(self) -> None:
        cra = CromwellRestAPI()
        with (
            patch.object(
                cra,
                'find_valid_workflow_ids',
                return_value=['f9c26f2e-f550-4748-a650-5d0d4cab9f3a'],
            ),
            patch.object(cra, '_CromwellRestAPI__request_get', return_value=None),
        ):
            result = cra.get_cost(workflow_ids=['f9c26f2e-f550-4748-a650-5d0d4cab9f3a'])

        assert result is not None
        assert len(result) == 0


class TestCaperClientCost:
    """Tests for CaperClient.cost()."""

    def test_cost_delegates_to_rest_api(self) -> None:
        with patch.object(CaperClient, '__init__', lambda self, **kw: None):
            client = CaperClient()
            client._cromwell_rest_api = MagicMock()
            client._cromwell_rest_api.get_cost.return_value = [SAMPLE_COST_RESPONSE]

            result = client.cost(wf_ids_or_labels=['f9c26f2e-f550-4748-a650-5d0d4cab9f3a'])

        assert result == [SAMPLE_COST_RESPONSE]
        client._cromwell_rest_api.get_cost.assert_called_once()


class TestSubcmdCostFormatting:
    """Tests for subcmd_cost() output formatting."""

    def _make_args(self, cost_format: str = 'human') -> MagicMock:
        args = MagicMock()
        args.cost_format = cost_format
        args.wf_id_or_label = ['f9c26f2e-f550-4748-a650-5d0d4cab9f3a']
        return args

    def _make_client(self, cost_results: list[dict]) -> MagicMock:
        client = MagicMock()
        client.cost.return_value = cost_results
        return client

    def test_human_format_single_workflow(self, capsys: pytest.CaptureFixture) -> None:
        client = self._make_client([SAMPLE_COST_RESPONSE])
        args = self._make_args('human')

        subcmd_cost(client, args)

        out = capsys.readouterr().out
        assert 'f9c26f2e-f550-4748-a650-5d0d4cab9f3a' in out
        assert '$1.23' in out
        assert 'Succeeded' in out
        # Total line only shown for multiple workflows or when warnings present
        assert 'Total' not in out

    def test_human_format_with_warnings(self, capsys: pytest.CaptureFixture) -> None:
        client = self._make_client([SAMPLE_COST_RESPONSE, SAMPLE_COST_RESPONSE_WITH_ERRORS])
        args = self._make_args('human')

        subcmd_cost(client, args)

        out = capsys.readouterr().out
        assert 'Warnings' in out
        assert 'incomplete' in out.lower()
        assert 'vmCostPerHour' in out

    def test_tsv_format(self, capsys: pytest.CaptureFixture) -> None:
        client = self._make_client([SAMPLE_COST_RESPONSE])
        args = self._make_args('tsv')

        subcmd_cost(client, args)

        out = capsys.readouterr().out
        lines = out.strip().split('\n')
        assert lines[0] == 'workflow_id\tstatus\tcost\tcurrency\terrors'
        fields = lines[1].split('\t')
        assert fields[0] == 'f9c26f2e-f550-4748-a650-5d0d4cab9f3a'
        assert fields[2] == '1.23'

    def test_json_format(self, capsys: pytest.CaptureFixture) -> None:
        client = self._make_client([SAMPLE_COST_RESPONSE])
        args = self._make_args('json')

        subcmd_cost(client, args)

        out = capsys.readouterr().out
        parsed = json.loads(out)
        assert len(parsed) == 1
        assert parsed[0]['cost'] == 1.23

    def test_no_results(self) -> None:
        client = self._make_client(None)
        args = self._make_args('human')

        with pytest.raises(ValueError, match='no workflow'):
            subcmd_cost(client, args)
