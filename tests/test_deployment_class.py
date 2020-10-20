import pytest

from iguazioig.deployment_class import Deployment


def test_set_deployment_unsupported_exception():

    with pytest.raises(Exception) as exc_info:
        deployment = Deployment(api_version='not an api')

    assert 'not an api' in exc_info.value.args[0]


def test_api_020_inference_graph_loads():

    inference_graph = 'utils/api020/inference_graph.yaml'

    deployment = Deployment('0.2.0')
    deployed = deployment.deploy(inference_graph=inference_graph, dry_run=True)

    assert type(deployed) is list