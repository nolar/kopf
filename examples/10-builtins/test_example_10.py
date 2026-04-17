import os.path

import kopf.testing


def test_pods_reacted():

    # Run an operator and simulate some activity with the operated resource.
    # NB: not cluster-wide, since we do not want to block unrelated system pods from deletion.
    example_py = os.path.join(os.path.dirname(__file__), 'example.py')
    with kopf.testing.KopfCLI(
        ['run', '--verbose', '--standalone', '--namespace', 'default', example_py],
    ) as runner:
        runner.wait_for('watch-stream for pods.v1', timeout=5)

        name = _create_pod()
        runner.wait_for('Creation is processed', timeout=5)

        _delete_pod(name)
        runner.wait_for('Deleted, really deleted', timeout=2)

    # There are usually more than these messages, but we only check for the certain ones.
    assert f'[default/{name}] Creation is in progress:' in runner.output
    assert f'[default/{name}] === Pod killing happens in 30s.' in runner.output
    assert f'[default/{name}] Deletion is in progress:' in runner.output
    assert f'[default/{name}] === Pod killing is cancelled!' in runner.output


def _create_pod():
    import pykube
    api = pykube.HTTPClient(pykube.KubeConfig.from_file())
    with api.session:
        pod = pykube.Pod(api, {
            'apiVersion': 'v1',
            'kind': 'Pod',
            'metadata': {'generateName': 'kopf-pod-', 'namespace': 'default'},
            'spec': {
                'containers': [{
                    'name': 'the-only-one',
                    'image': 'busybox',
                    'command': ["sh", "-x", "-c", "sleep 1"],
                }]},
        })
        pod.create()
        return pod.name


def _delete_pod(name):
    import pykube
    api = pykube.HTTPClient(pykube.KubeConfig.from_file())
    with api.session:
        pod = pykube.Pod.objects(api, namespace='default').get_by_name(name)
        pod.delete()
