import pprint
from typing import Any

import kopf


@kopf.index('pods')
def is_running(*, namespace: str | None, name: str, status: kopf.Status, **_: Any) -> Any:
    return {(namespace, name): status.get('phase') == 'Running'}
    # {('kube-system', 'traefik-...-...'): [True],
    #  ('kube-system', 'helm-install-traefik-...'): [False],
    #    ...}


@kopf.index('pods')
def by_label(labels: kopf.Labels, name: str, **_: Any) -> Any:
    return {(label, value): name for label, value in labels.items()}
    # {('app', 'traefik'): ['traefik-...-...'],
    #  ('job-name', 'helm-install-traefik'): ['helm-install-traefik-...'],
    #  ('helmcharts.helm.cattle.io/chart', 'traefik'): ['helm-install-traefik-...'],
    #    ...}


@kopf.on.probe()  # type: ignore
def pod_count(is_running: kopf.Index[tuple[str, str], list[bool]], **_: Any) -> int:
    return len(is_running)


@kopf.on.probe()  # type: ignore
def pod_names(is_running: kopf.Index[tuple[str, str], list[bool]], **_: Any) -> list[str]:
    return [name for _, name in is_running]


@kopf.timer('kex', interval=5)  # type: ignore
def intervalled(is_running: kopf.Index[tuple[str, str], list[bool]],
                by_label: kopf.Index[tuple[str, str], list[str]],
                patch: kopf.Patch, **_: Any) -> None:
    pprint.pprint(dict(by_label))
    patch.status['running-pods'] = [
        f"{ns}::{name}"
        for (ns, name), is_running in is_running.items()
        if ns in ['kube-system', 'default']
        if is_running
    ]


# Marks for the e2e tests (see tests/e2e/test_examples.py):
# We do not care: pods can have 6-10 updates here.
E2E_SUCCESS_COUNTS: dict[str, int] = {}
