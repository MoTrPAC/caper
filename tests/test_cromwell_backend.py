"""There are lots of UserDict-based classesi n caper/cromwell_backend.py
In this test, only the followings classes with public methods
will be tested.
    - CromwellBackendBase.

"""

from caper.cromwell_backend import CromwellBackendBase


def test_cromwell_backend_base_backend() -> None:
    """Test a property backend's getter, setter."""
    bb1 = CromwellBackendBase('test1')
    backend_dict = {'a': 1, 'b': '2'}

    bb1.backend = backend_dict
    assert bb1.backend == backend_dict


def test_cromwell_backend_base_merge_backend() -> None:
    bb1 = CromwellBackendBase('test1')
    bb1.backend = {'a': 1, 'b': '2'}
    backend_dict = {'c': 3.0, 'd': '4.0'}

    bb1.merge_backend(backend_dict)
    assert bb1.backend == {'a': 1, 'b': '2', 'c': 3.0, 'd': '4.0'}


def test_cromwell_backend_base_backend_config() -> None:
    bb1 = CromwellBackendBase('test1')
    bb1.backend = {'config': {'root': 'test/folder'}}
    assert bb1.backend_config == {'root': 'test/folder'}


def test_cromwell_backend_base_backend_config_dra() -> None:
    bb1 = CromwellBackendBase('test1')
    bb1.backend = {
        'config': {
            'root': 'test/folder',
            'default-runtime-attributes': {'docker': 'ubuntu:latest'},
        }
    }
    assert bb1.default_runtime_attributes == {'docker': 'ubuntu:latest'}


def test_cromwell_backend_gcp_with_network_and_subnetwork() -> None:
    """Test GCP backend with network and subnetwork specified."""
    from caper.cromwell_backend import CromwellBackendGcp

    gcp = CromwellBackendGcp(
        gcp_prj='test-project',
        gcp_out_dir='gs://test-bucket/output',
        gcp_network='my-vpc',
        gcp_subnetwork='my-subnet',
    )
    config = gcp.backend_config
    assert 'virtual-private-cloud' in config
    assert config['virtual-private-cloud']['network-name'] == 'my-vpc'
    assert config['virtual-private-cloud']['subnetwork-name'] == 'my-subnet'


def test_cromwell_backend_gcp_with_network_only() -> None:
    """Test GCP backend with only network specified."""
    from caper.cromwell_backend import CromwellBackendGcp

    gcp = CromwellBackendGcp(
        gcp_prj='test-project',
        gcp_out_dir='gs://test-bucket/output',
        gcp_network='my-vpc',
    )
    config = gcp.backend_config
    assert 'virtual-private-cloud' in config
    assert config['virtual-private-cloud']['network-name'] == 'my-vpc'
    assert 'subnetwork-name' not in config['virtual-private-cloud']


def test_cromwell_backend_gcp_with_subnetwork_only() -> None:
    """Test GCP backend with only subnetwork specified."""
    from caper.cromwell_backend import CromwellBackendGcp

    gcp = CromwellBackendGcp(
        gcp_prj='test-project',
        gcp_out_dir='gs://test-bucket/output',
        gcp_subnetwork='my-subnet',
    )
    config = gcp.backend_config
    assert 'virtual-private-cloud' in config
    assert config['virtual-private-cloud']['subnetwork-name'] == 'my-subnet'
    assert 'network-name' not in config['virtual-private-cloud']


def test_cromwell_backend_gcp_without_network_config() -> None:
    """Test GCP backend without network configuration (default behavior)."""
    from caper.cromwell_backend import CromwellBackendGcp

    gcp = CromwellBackendGcp(
        gcp_prj='test-project',
        gcp_out_dir='gs://test-bucket/output',
    )
    config = gcp.backend_config
    assert 'virtual-private-cloud' not in config
