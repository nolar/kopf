import os.path
from datetime import datetime

import pytest

from kopf._cogs.helpers.certparser import parse_validity_from_pem


# Grab from random websites:
#   openssl s_client -showcerts -servername google.com -connect google.com:443 </dev/null | openssl x509 >google.pem
#
# Generate a self-signed certificate locally (using Kopf's internals):
#   from kopf._kits.webhooks import WebhookServer
#   server = WebhookServer()
#   certdata, pkeydata = server.build_certificate(['localhost'])
#   with open('self-signed.pem', 'wb') as f:
#       f.write(certdata)
#
# Then register them here with the proper values -- get them from a cert viewer
# or from the failed tests.
@pytest.mark.parametrize('cert_file, exp_not_before, exp_not_after', [
    ('fixtures/generated.pem', '2026-01-24 18:19:08', '2027-01-24 18:19:08'),
    ('fixtures/microsoft.pem', '2025-12-14 23:46:21', '2026-06-12 23:46:21'),
    ('fixtures/google.pem', '2025-12-29 19:51:06', '2026-03-23 19:51:05'),
])
def test_sslcert_validity_parser(cert_file, exp_not_before, exp_not_after):
    path = os.path.dirname(__file__)
    with open(os.path.join(path, cert_file), 'rb') as f:
        pem = f.read()
    not_before, not_after = parse_validity_from_pem(pem)
    assert not_before == datetime.fromisoformat(exp_not_before)
    assert not_after == datetime.fromisoformat(exp_not_after)



def test_ssl_cert_validity_parser_fails_on_invalid_cert():
    with pytest.raises(ValueError):
        parse_validity_from_pem(b'invalid')
