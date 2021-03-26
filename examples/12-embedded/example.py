import asyncio
import contextlib
import threading
import time
from typing import Union

import kopf
import pykube


@kopf.on.create('kopfexamples')
def create_fn(memo: Union[kopf.Memo, object], **kwargs):
    print(memo.create_tpl.format(**kwargs))  # type: ignore


@kopf.on.delete('kopfexamples')
def delete_fn(memo: Union[kopf.Memo, object], **kwargs):
    print(memo.delete_tpl.format(**kwargs))  # type: ignore


def kopf_thread(
        ready_flag: threading.Event,
        stop_flag: threading.Event,
):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    with contextlib.closing(loop):

        kopf.configure(verbose=True)  # log formatting

        loop.run_until_complete(kopf.operator(
            ready_flag=ready_flag,
            stop_flag=stop_flag,
            memo=kopf.Memo(
                create_tpl="Hello, {name}!",
                delete_tpl="Good bye, {name}!",
            ),
        ))


def main(steps=3):

    # Start the operator and let it initialise.
    print(f"Starting the main app.")
    ready_flag = threading.Event()
    stop_flag = threading.Event()
    thread = threading.Thread(target=kopf_thread, kwargs=dict(
        stop_flag=stop_flag,
        ready_flag=ready_flag,
    ))
    thread.start()
    ready_flag.wait()

    # The operator is active: run the app's activity.
    for step in range(steps):
        print(f"Do the main app activity here. Step {step+1}/{steps}.")
        _create_object(step)
        time.sleep(1.0)
        _delete_object(step)
        time.sleep(1.0)

    # Ask the operator to terminate gracefully (can take a couple of seconds).
    print(f"Exiting the main app.")
    stop_flag.set()
    thread.join()


class KopfExample(pykube.objects.NamespacedAPIObject):
    version = "kopf.dev/v1"
    endpoint = "kopfexamples"
    kind = "KopfExample"


def _create_object(step):
    try:
        api = pykube.HTTPClient(pykube.KubeConfig.from_env())
        kex = KopfExample(api, dict(
            apiVersion='kopf.dev/v1',
            kind='KopfExample',
            metadata=dict(
                namespace='default',
                name=f'kopf-example-{step}',
            ),
        ))
        kex.create()
    except pykube.exceptions.HTTPError as e:
        if e.code in [409]:
            pass
        else:
            raise


def _delete_object(step):
    try:
        api = pykube.HTTPClient(pykube.KubeConfig.from_env())
        kex = KopfExample.objects(api, namespace='default').get_by_name(f'kopf-example-{step}')
        kex.delete()
    except pykube.exceptions.HTTPError as e:
        if e.code in [404]:
            pass
        else:
            raise


if __name__ == '__main__':
    main()
