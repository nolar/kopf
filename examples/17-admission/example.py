import pathlib
from typing import Dict, List

import kopf

ROOT = (pathlib.Path.cwd() / pathlib.Path(__file__)).parent.parent.parent


@kopf.on.startup()
def config(settings: kopf.OperatorSettings, **_):

    # Plain and simple local endpoint with an auto-generated certificate:
    settings.admission.server = kopf.WebhookServer()

    # Plain and simple local endpoint with with provided certificate (e.g. openssl):
    settings.admission.server = kopf.WebhookServer(certfile=ROOT/'cert.pem', pkeyfile=ROOT/'key.pem', port=1234)

    # K3d/K3s-specific server that supports accessing from inside of a VM (a generated certificate):
    settings.admission.server = kopf.WebhookK3dServer(cadump=ROOT/'ca.pem')

    # K3d/K3s-specific server that supports accessing from inside of a VM (a provided certificate):
    settings.admission.server = kopf.WebhookK3dServer(certfile=ROOT/'k3d-cert.pem', pkeyfile=ROOT/'k3d-key.pem', port=1234)

    # Minikube-specific server that supports accessing from inside of a VM (a generated certificate):
    settings.admission.server = kopf.WebhookMinikubeServer(port=1234, cadump=ROOT/'ca.pem')

    # Tunneling Kubernetes->ngrok->local server (anonymous, auto-loaded binary):
    settings.admission.server = kopf.WebhookNgrokTunnel(path='/xyz', port=1234)

    # Tunneling Kubernetes->ngrok->local server (registered users, pre-existing binary):
    settings.admission.server = kopf.WebhookNgrokTunnel(binary="/usr/local/bin/ngrok", token='...', )

    # Tunneling Kubernetes->ngrok->local server (registered users, pre-existing binary, specific region):
    settings.admission.server = kopf.WebhookNgrokTunnel(binary="/usr/local/bin/ngrok", region='eu')

    # Auto-detect the best server (K3d/Minikube/simple) strictly locally:
    settings.admission.server = kopf.WebhookAutoServer()

    # Auto-detect the best server (K3d/Minikube/simple) with external tunneling as a fallback:
    settings.admission.server = kopf.WebhookAutoTunnel()

    # The final configuration for CI/CD (overrides previous values):
    settings.admission.server = kopf.WebhookAutoServer()
    settings.admission.managed = 'auto.kopf.dev'


@kopf.on.validate('kex')
def authhook(headers: kopf.Headers, sslpeer: kopf.SSLPeer, warnings: List[str], **_):
    user_agent = headers.get('User-Agent', '(unidentified)')
    warnings.append(f"Accessing as user-agent: {user_agent}")
    if not sslpeer.get('subject'):
        warnings.append("SSL peer is not identified.")
    else:
        common_names = [val for key, val in sslpeer['subject'][0] if key == 'commonName']
        if common_names:
            warnings.append(f"SSL peer is {common_names[0]}.")
        else:
            warnings.append("SSL peer's common name is absent.")


@kopf.on.validate('kex')
def validate1(spec, dryrun, **_):
    if not dryrun and spec.get('field') == 'wrong':
        raise kopf.AdmissionError("Meh! I don't like it. Change the field.")


@kopf.on.validate('kex', field='spec.field', value='not-allowed')
def validate2(**_):
    raise kopf.AdmissionError("I'm too lazy anyway. Go away!", code=555)


@kopf.on.mutate('kex', labels={'somelabel': 'somevalue'})
def mutate1(patch: kopf.Patch, **_):
    patch.spec['injected'] = 123


# Marks for the e2e tests (see tests/e2e/test_examples.py):
# We do not care: pods can have 6-10 updates here.
E2E_SUCCESS_COUNTS = {}  # type: Dict[str, int]
