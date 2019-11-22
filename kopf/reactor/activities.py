"""
An every-running tasks to keep the operator functional.

Consumes a credentials vault, and monitors that it has enough credentials.
When the credentials are invalidated (i.e. excluded), run the re-authentication
activity and populates with the new credentials (fully or partially).

The process is intentionally split into multiple packages:

* Authenticating background task (this module) is a part of the reactor,
  as it will not be able to run without up-to-date credentials,
  and since it initiates the reactor's activities and invokes the handlers.
* Vault are the data structures used mostly in the API clients wrappers
  (which are the low-level modules, so they cannot import the credentials
  from the high-level modules such as the reactor/engines).
* Specific authentication methods, such as the authentication piggybacking,
  belong to neither the reactor, nor the engines, nor the client wrappers.
"""
import logging
from typing import NoReturn

from kopf.reactor import causation
from kopf.reactor import handling
from kopf.reactor import lifecycles
from kopf.reactor import registries
from kopf.structs import credentials

logger = logging.getLogger(__name__)


async def authenticator(
        *,
        registry: registries.OperatorRegistry,
        vault: credentials.Vault,
) -> NoReturn:
    """ Keep the credentials forever up to date. """
    counter: int = 1 if vault else 0
    while True:
        await authenticate(
            registry=registry,
            vault=vault,
            _activity_title="Re-authentication" if counter else "Initial authentication",
        )
        counter += 1


async def authenticate(
        *,
        registry: registries.OperatorRegistry,
        vault: credentials.Vault,
        _activity_title: str = "Authentication",
) -> None:
    """ Retrieve the credentials once, successfully or not, and exit. """

    # Sleep most of the time waiting for a signal to re-auth.
    await vault.wait_for_emptiness()

    # Log initial and re-authentications differently, for readability.
    logger.info(f"{_activity_title} has been initiated.")

    activity_results = await handling.activity_trigger(
        lifecycle=lifecycles.all_at_once,
        registry=registry,
        activity=causation.Activity.AUTHENTICATION,
    )

    if activity_results:
        logger.info(f"{_activity_title} has finished.")
    else:
        logger.warning(f"{_activity_title} has failed: "
                       f"no credentials were retrieved from the login handlers.")

    # Feed the credentials into the vault, and unfreeze the re-authenticating clients.
    await vault.populate({str(handler_id): info for handler_id, info in activity_results.items()})
