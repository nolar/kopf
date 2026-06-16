import asyncio

# Import the operator's code, so that handlers register in the default registry.
import example  # noqa: F401  # type: ignore[import-unused]
import kmock
import kopf.testing
import pytest

# Optional: bind the mock server to a specific port, if we address it directly (we do not).
# pytestmark = pytest.mark.kmock(port=12345)


# Intercept all API calls to a locally mocked server.
# Both the operator's login handlers AND pykube-ng's client will use it.
@pytest.fixture(autouse=True)
def mocked_login(tmp_path, mocker, kmock: kmock.KubernetesEmulator) -> None:
    config = tmp_path / 'kubeconfig'
    config.write_text(f'''
        kind: Config
        current-context: self
        clusters:
          - name: self
            cluster:
              server: {kmock.url!s}
        contexts:
          - name: self
            context:
              cluster: self
    ''')
    mocker.patch.dict('os.environ', {'KUBECONFIG': str(config)})


async def test_deletion(kmock: kmock.KubernetesEmulator, caplog) -> None:
    # We want to see all logs for debugging.
    caplog.set_level(0)

    # Pre-populate the cluster with something.
    kmock.resources.load_bundled()
    kmock.resources['kopf.dev/v1/kopfexamples'].verbs = {'watch', 'list', 'fetch', 'patch'}
    kmock.resources['kopf.dev/v1/kopfexamples'].namespaced = True

    # Pre-populate with specific simulated objects.
    kmock.objects['kopf.dev/v1/kopfexamples', 'ns1', 'kex1'] = {
        'metadata': {'uid': '1234', 'name': 'kex1', 'namespace': 'ns1',
                     'resourceVersion': '1234'}}
    kmock.objects['v1/pods', 'ns1', 'pod1'] = {
        'metadata': {'uid': '2345', 'name': 'pod1', 'namespace': 'ns1',
                     'resourceVersion': '1234'}}

    # We do not want the auto-discovery too much, for simplicity.
    # If enabled, populate the namespaces resources too.
    settings = kopf.OperatorSettings()
    settings.scanning.disabled = True

    # Spawn an operator in the background in the same event loop as the test.
    # If looptime is used, this will take near-zero wall-clock time.
    async with kopf.testing.KopfTask(settings=settings, namespaces={'ns1'}):

        # Let it spawn all the watchers.
        await asyncio.sleep(1)

        # Simulate the external deletion by any means & tools.
        # Here, we use the client functionality of the mock server itself.
        await kmock.delete('/apis/kopf.dev/v1/namespaces/ns1/kopfexamples/kex1')

        # Give it some time to react (we do not have any other signals).
        await asyncio.sleep(5)

        # Assert the history of objects.
        # -1 is the soft-deletion marker (None)
        # -2 is the released object (patch: finalizer removed)
        # -3 is the last seen before the deletion (with the finalizer)
        pre_deletion = kmock.objects['kopf.dev/v1/kopfexamples', 'ns1', 'kex1'].history[-3]
        assert pre_deletion['metadata']['finalizers'] == ['kopf.zalando.org/KopfFinalizerMarker']

        # Assert the latest state of the objects is "soft-deleted".
        assert kmock.objects['kopf.dev/v1/kopfexamples', 'ns1', 'kex1'].deleted
        assert kmock.objects['v1/pods', 'ns1', 'pod1'].deleted

        # Assert the specific API requests (first filter for pods, then index by numbers).
        pods_reqs = kmock[kmock.resource('v1/pods')]
        assert pods_reqs[1].method == 'DELETE'
        assert pods_reqs[1].resource == 'v1/pods'
        assert pods_reqs[1].namespace == 'ns1'
        assert pods_reqs[1].name == 'pod1'
