import certvalidator
import pytest

from kopf.toolkits.webhooks import WebhookServer


def test_missing_oscrypto(no_oscrypto):
    with pytest.raises(ImportError) as err:
        WebhookServer.build_certificate(['...'])
    assert "pip install certbuilder" in str(err.value)


def test_missing_certbuilder(no_certbuilder):
    with pytest.raises(ImportError) as err:
        WebhookServer.build_certificate(['...'])
    assert "pip install certbuilder" in str(err.value)


def test_certificate_generation():
    names = ['hostname1', 'hostname2', '001.002.003.004', '0:0:0:0:0:0:0:1']
    cert, pkey = WebhookServer.build_certificate(names)
    context = certvalidator.ValidationContext(extra_trust_roots=[cert])
    validator = certvalidator.CertificateValidator(cert, validation_context=context)
    path = validator.validate_tls('hostname1')
    assert len(path) == 1  # self-signed
    assert path.first.ca
    assert path.first.self_issued
    assert set(path.first.valid_domains) == {'hostname1', 'hostname2', '1.2.3.4', '::1'}
    assert set(path.first.valid_ips) == {'1.2.3.4', '::1'}


@pytest.mark.parametrize('hostnames, common_name', [
    (['hostname1', 'hostname2'], 'hostname1'),
    (['hostname2', 'hostname1'], 'hostname2'),
    (['1.2.3.4', 'hostname1'], 'hostname1'),
    (['1.2.3.4', '2.3.4.5'], '1.2.3.4'),
])
def test_common_name_selection(hostnames, common_name):
    cert, pkey = WebhookServer.build_certificate(hostnames)
    context = certvalidator.ValidationContext(extra_trust_roots=[cert])
    validator = certvalidator.CertificateValidator(cert, validation_context=context)
    path = validator.validate_tls(common_name)
    assert path.first.subject.native['common_name'] == common_name
