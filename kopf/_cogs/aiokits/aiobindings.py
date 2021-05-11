import asyncio


async def condition_chain(
        source: asyncio.Condition,
        target: asyncio.Condition,
) -> None:
    """
    A condition chain is a "clean" hack to attach one condition to another.

    It is a "clean" (not "dirty") hack to wake up the webhook configuration
    managers when either the resources are revised (as seen in the insights),
    or a new client config is yielded from the webhook server.
    """
    async with source:
        while True:
            await source.wait()
            async with target:
                target.notify_all()
