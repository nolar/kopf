import pathlib
from typing import Dict

import kopf


@kopf.on.startup()
def config(settings: kopf.OperatorSettings, **_):
    ROOT = (pathlib.Path.cwd() / pathlib.Path(__file__)).parent.parent.parent
    settings.admission.managed = 'auto.kopf.dev'
    settings.admission.server = kopf.WebhookK3dServer(cadump=ROOT/'ca.pem')
    ## Other options (see the docs):
    # settings.admission.server = kopf.WebhookServer()
    # settings.admission.server = kopf.WebhookServer(certfile=ROOT/'cert.pem', pkeyfile=ROOT/'key.pem', port=1234)
    # settings.admission.server = kopf.WebhookK3dServer(cadump=ROOT/'ca.pem')
    # settings.admission.server = kopf.WebhookK3dServer(certfile=ROOT/'k3d-cert.pem', pkeyfile=ROOT/'k3d-key.pem', port=1234)
    # settings.admission.server = kopf.WebhookMinikubeServer(port=1234, cadump=ROOT/'ca.pem', verify_cafile=ROOT/'client-cert.pem')
    # settings.admission.server = kopf.WebhookNgrokTunnel()
    # settings.admission.server = kopf.WebhookNgrokTunnel(binary="/usr/local/bin/ngrok", token='...', port=1234)
    # settings.admission.server = kopf.WebhookNgrokTunnel(binary="/usr/local/bin/ngrok", port=1234, path='/xyz', region='eu')


@kopf.on.validate('kex')
def authhook(headers, sslpeer, warnings, **_):
    # print(f'headers={headers}')
    # print(f'sslpeer={sslpeer}')
    if not sslpeer:
        warnings.append("SSL peer is not identified.")
    else:
        common_name = None
        for key, val in sslpeer['subject'][0]:
            if key == 'commonName':
                common_name = val
                break
        else:
            warnings.append("SSL peer's common name is absent.")
        if common_name is not None:
            warnings.append(f"SSL peer is {common_name}.")


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
