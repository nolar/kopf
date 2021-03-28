import pytest

from kopf.toolkits.webhooks import ClusterDetector, WebhookAutoServer, WebhookAutoTunnel


# Reproducing the realistic environment would be costly and difficult,
# so we mock all external(!) libraries to return the results as we expect them.
# This reduces the quality of the tests, but makes them simple.
@pytest.fixture(autouse=True)
def pathmock(mocker, fake_vault, enforced_context, aresponses, hostname):
    mocker.patch('ssl.get_server_certificate', return_value='')
    mocker.patch('certvalidator.ValidationContext')
    validator = mocker.patch('certvalidator.CertificateValidator')
    pathmock = validator.return_value.validate_tls.return_value
    pathmock.first.issuer.native = {}
    pathmock.first.subject.native = {}
    return pathmock


async def test_no_detection(hostname, aresponses):
    aresponses.add(hostname, '/version', 'get', {'gitVersion': 'v1.2.3'})
    hostname = await ClusterDetector().guess_host()
    assert hostname is None


async def test_dependencies(hostname, aresponses, no_certvalidator):
    aresponses.add(hostname, '/version', 'get', {'gitVersion': 'v1.2.3'})
    with pytest.raises(ImportError) as err:
        await ClusterDetector().guess_host()
    assert "pip install certvalidator" in str(err.value)


async def test_minikube_via_issuer_cn(pathmock):
    pathmock.first.issuer.native = {'common_name': 'minikubeCA'}
    hostname = await ClusterDetector().guess_host()
    assert hostname == 'host.minikube.internal'


async def test_minikube_via_subject_cn(pathmock):
    pathmock.first.subject.native = {'common_name': 'minikube'}
    hostname = await ClusterDetector().guess_host()
    assert hostname == 'host.minikube.internal'


async def test_k3d_via_issuer_cn(pathmock):
    pathmock.first.issuer.native = {'common_name': 'k3s-ca-server-12345'}
    hostname = await ClusterDetector().guess_host()
    assert hostname == 'host.k3d.internal'


async def test_k3d_via_subject_cn(pathmock):
    pathmock.first.subject.native = {'common_name': 'k3s'}
    hostname = await ClusterDetector().guess_host()
    assert hostname == 'host.k3d.internal'


async def test_k3d_via_subject_org(pathmock):
    pathmock.first.subject.native = {'organization_name': 'k3s'}
    hostname = await ClusterDetector().guess_host()
    assert hostname == 'host.k3d.internal'


async def test_k3d_via_version_infix(hostname, aresponses):
    aresponses.add(hostname, '/version', 'get', {'gitVersion': 'v1.20.4+k3s1'})
    hostname = await ClusterDetector().guess_host()
    assert hostname == 'host.k3d.internal'


async def test_server_detects(responder, aresponses, hostname, caplog, assert_logs):
    caplog.set_level(0)
    aresponses.add(hostname, '/version', 'get', {'gitVersion': 'v1.20.4+k3s1'})
    server = WebhookAutoServer(insecure=True)
    async for _ in server(responder.fn):
        break  # do not sleep
    assert_logs(["Cluster detection found the hostname: host.k3d.internal"])


async def test_server_works(
        responder, aresponses, hostname, caplog, assert_logs):
    caplog.set_level(0)
    aresponses.add(hostname, '/version', 'get', {'gitVersion': 'v1.20.4'})
    server = WebhookAutoServer(insecure=True)
    async for _ in server(responder.fn):
        break  # do not sleep
    assert_logs(["Cluster detection failed, running a simple local server"])


async def test_tunnel_detects(responder, pyngrok_mock, aresponses, hostname, caplog, assert_logs):
    caplog.set_level(0)
    aresponses.add(hostname, '/version', 'get', {'gitVersion': 'v1.20.4+k3s1'})
    server = WebhookAutoTunnel()
    async for _ in server(responder.fn):
        break  # do not sleep
    assert_logs(["Cluster detection found the hostname: host.k3d.internal"])


async def test_tunnel_works(responder, pyngrok_mock, aresponses, hostname, caplog, assert_logs):
    caplog.set_level(0)
    aresponses.add(hostname, '/version', 'get', {'gitVersion': 'v1.20.4'})
    server = WebhookAutoTunnel()
    async for _ in server(responder.fn):
        break  # do not sleep
    assert_logs(["Cluster detection failed, using an ngrok tunnel."])
