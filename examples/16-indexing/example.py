import pprint

import kopf


@kopf.index('pods')
def is_running(namespace, name, status, **_):
    return {(namespace, name): status.get('phase') == 'Running'}
    # {('kube-system', 'traefik-...-...'): True,
    #  ('kube-system', 'helm-install-traefik-...'): False,
    #    ...}


@kopf.index('pods')
def by_label(labels, name, **_):
    return {(label, value): name for label, value in labels.items()}
    # {('app', 'traefik'): ['traefik-...-...'],
    #  ('job-name', 'helm-install-traefik'): ['helm-install-traefik-...'],
    #  ('helmcharts.helm.cattle.io/chart', 'traefik'): ['helm-install-traefik-...'],
    #    ...}


@kopf.on.probe()  # type: ignore
def pod_count(is_running: kopf.Index, **_):
    return len(is_running)


@kopf.on.probe()  # type: ignore
def pod_names(is_running: kopf.Index, **_):
    return [name for _, name in is_running]


@kopf.timer('kex', interval=5)  # type: ignore
def intervalled(is_running: kopf.Index, by_label: kopf.Index, patch: kopf.Patch, **_):
    pprint.pprint(dict(by_label))
    patch.status['running-pods'] = [
        f"{ns}::{name}"
        for (ns, name), is_running in is_running.items()
        if ns in ['kube-system', 'default']
        if is_running
    ]


# Marks for the e2e tests (see tests/e2e/test_examples.py):
E2E_SUCCESS_COUNTS = {}  # type: ignore # we do not care: pods can have 6-10 updates here.
