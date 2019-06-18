import os.path
import time

import pykube

import kopf.testing


def test_pods_reacted():

    example_py = os.path.join(os.path.dirname(__file__), 'example.py')
    with kopf.testing.KopfRunner(['run', '--verbose', example_py]) as runner:
        _create_pod()
        time.sleep(5)  # give it some time to react
        _delete_pod()
        time.sleep(1)  # give it some time to react

    assert runner.exception is None
    assert runner.exit_code == 0

    assert '[default/kopf-pod-1] Creation event:' in runner.stdout
    assert '[default/kopf-pod-1] === Pod killing happens in 30s.' in runner.stdout
    assert '[default/kopf-pod-1] Deletion event:' in runner.stdout
    assert '[default/kopf-pod-1] === Pod killing is cancelled!' in runner.stdout


def _create_pod():
    api = pykube.HTTPClient(pykube.KubeConfig.from_file())
    pod = pykube.Pod(api, {
        'apiVersion': 'v1',
        'kind': 'Pod',
        'metadata': {'name': 'kopf-pod-1', 'namespace': 'default'},
        'spec': {
            'containers': [{
                'name': 'the-only-one',
                'image': 'busybox',
                'command': ["sh", "-x", "-c", "sleep 1"],
            }]},
    })
    pod.create()


def _delete_pod():
    api = pykube.HTTPClient(pykube.KubeConfig.from_file())
    pod = pykube.Pod.objects(api, namespace='default').get_by_name('kopf-pod-1')
    pod.delete()
