import asyncio
import logging

format = '[%(asctime)s] %(name)-20.20s [%(levelname)-8.8s] %(message)s'


def configure(debug=None, verbose=None, quiet=None):
    log_level = 'DEBUG' if debug or verbose else 'WARNING' if quiet else 'INFO'

    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter(format)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(log_level)

    # Configure the Kubernetes client defaults according to our settings.
    try:
        import kubernetes
    except ImportError:
        pass
    else:
        config = kubernetes.client.configuration.Configuration()
        config.logger_format = format
        config.logger_file = None  # once again after the constructor to re-apply the formatter
        config.debug = debug
        kubernetes.client.configuration.Configuration.set_default(config)

    # Kubernetes client is as buggy as hell: it adds its own stream handlers even in non-debug mode,
    # does not respect the formatting, and dumps too much of the low-level info.
    if not debug:
        logger = logging.getLogger("urllib3")
        del logger.handlers[1:]  # everything except the default NullHandler

    # Prevent the low-level logging unless in the debug verbosity mode. Keep only the operator's messages.
    logging.getLogger('urllib3').propagate = debug
    logging.getLogger('asyncio').propagate = debug
    logging.getLogger('kubernetes').propagate = debug

    loop = asyncio.get_event_loop()
    loop.set_debug(debug)
