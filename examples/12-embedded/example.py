import asyncio
import contextlib
import threading
import time

import kopf
import kubernetes.client.rest


@kopf.on.create('zalando.org', 'v1', 'kopfexamples')
def create_fn(**_):
    pass


@kopf.on.delete('zalando.org', 'v1', 'kopfexamples')
def delete_fn(**_):
    pass


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


def _create_object(step):
    try:
        api = kubernetes.client.CustomObjectsApi()
        api.create_namespaced_custom_object(
            group='zalando.org',
            version='v1',
            plural='kopfexamples',
            namespace='default',
            body=dict(
                apiVersion='zalando.org/v1',
                kind='KopfExample',
                metadata=dict(name=f'kopf-example-{step}'),
            ),
        )
    except kubernetes.client.rest.ApiException as e:
        if e.status in [409]:
            pass
        else:
            raise


def _delete_object(step):
    try:
        api = kubernetes.client.CustomObjectsApi()
        api.delete_namespaced_custom_object(
            group='zalando.org',
            version='v1',
            plural='kopfexamples',
            namespace='default',
            name=f'kopf-example-{step}',
            body={},
        )
    except kubernetes.client.rest.ApiException as e:
        if e.status in [404]:
            pass
        else:
            raise


if __name__ == '__main__':
    main()
