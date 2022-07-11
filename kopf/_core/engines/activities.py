"""
Supporting tasks for startup/cleanup and to keep the operator functional.

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
from typing import Mapping, MutableMapping, NoReturn

from kopf._cogs.aiokits import aiotime
from kopf._cogs.configs import configuration
from kopf._cogs.structs import credentials, ephemera, ids
from kopf._core.actions import execution, lifecycles, progression
from kopf._core.intents import causes, registries

logger = logging.getLogger(__name__)


class ActivityError(Exception):
    """ An error in the activity, as caused by mandatory handlers' failures. """

    def __init__(
            self,
            msg: str,
            *,
            outcomes: Mapping[ids.HandlerId, execution.Outcome],
    ) -> None:
        super().__init__(msg)
        self.outcomes = outcomes


async def authenticator(
        *,
        registry: registries.OperatorRegistry,
        settings: configuration.OperatorSettings,
        indices: ephemera.Indices,
        vault: credentials.Vault,
        memo: ephemera.AnyMemo,
) -> NoReturn:
    """ Keep the credentials forever up to date. """
    counter: int = 0 if vault.is_empty() else 1
    while True:
        await authenticate(
            registry=registry,
            settings=settings,
            indices=indices,
            vault=vault,
            memo=memo,
            _activity_title="Re-authentication" if counter else "Initial authentication",
        )
        counter += 1


async def authenticate(
        *,
        registry: registries.OperatorRegistry,
        settings: configuration.OperatorSettings,
        indices: ephemera.Indices,
        vault: credentials.Vault,
        memo: ephemera.AnyMemo,
        _activity_title: str = "Authentication",
) -> None:
    """ Retrieve the credentials once, successfully or not, and exit. """

    # Sleep most of the time waiting for a signal to re-auth.
    await vault.wait_for_emptiness()

    # Log initial and re-authentications differently, for readability.
    logger.info(f"{_activity_title} has been initiated.")

    activity_results = await run_activity(
        lifecycle=lifecycles.all_at_once,
        registry=registry,
        settings=settings,
        activity=causes.Activity.AUTHENTICATION,
        indices=indices,
        memo=memo,
    )

    if activity_results:
        logger.info(f"{_activity_title} has finished.")
    else:
        logger.warning(f"{_activity_title} has failed: "
                       f"no credentials were retrieved from the login handlers.")

    # Feed the credentials into the vault, and unfreeze the re-authenticating clients.
    await vault.populate({str(handler_id): info for handler_id, info in activity_results.items()})


async def run_activity(
        *,
        lifecycle: execution.LifeCycleFn,
        registry: registries.OperatorRegistry,
        settings: configuration.OperatorSettings,
        activity: causes.Activity,
        indices: ephemera.Indices,
        memo: ephemera.AnyMemo,
) -> Mapping[ids.HandlerId, execution.Result]:
    logger = logging.getLogger(f'kopf.activities.{activity.value}')

    # For the activity handlers, we have neither bodies, nor patches, just the state.
    cause = causes.ActivityCause(
        logger=logger,
        activity=activity,
        settings=settings,
        indices=indices,
        memo=memo,
    )
    handlers = registry._activities.get_handlers(activity=activity)
    state = progression.State.from_scratch().with_handlers(handlers)
    outcomes: MutableMapping[ids.HandlerId, execution.Outcome] = {}
    while not state.done:
        current_outcomes = await execution.execute_handlers_once(
            lifecycle=lifecycle,
            settings=settings,
            handlers=handlers,
            cause=cause,
            state=state,
        )
        outcomes.update(current_outcomes)
        state = state.with_outcomes(current_outcomes)
        await aiotime.sleep(state.delay)

    # Activities assume that all handlers must eventually succeed.
    # We raise from the 1st exception only: just to have something real in the tracebacks.
    # For multiple handlers' errors, the logs should be investigated instead.
    exceptions = [outcome.exception
                  for outcome in outcomes.values()
                  if outcome.exception is not None]
    if exceptions:
        raise ActivityError("One or more handlers failed.", outcomes=outcomes) from exceptions[0]

    # If nothing has failed, we return identifiable results. The outcomes/states are internal.
    # The order of results is not guaranteed (the handlers can succeed on one of the retries).
    results = {handler_id: outcome.result
               for handler_id, outcome in outcomes.items()
               if outcome.result is not None}
    return results
