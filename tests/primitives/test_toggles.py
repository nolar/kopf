import asyncio

import pytest

from kopf._cogs.aiokits.aiotoggles import Toggle


async def test_created_as_off():
    toggle = Toggle()
    assert not toggle.is_on()
    assert toggle.is_off()


async def test_initialised_as_off():
    toggle = Toggle(False)
    assert not toggle.is_on()
    assert toggle.is_off()


async def test_initialised_as_on():
    toggle = Toggle(True)
    assert toggle.is_on()
    assert not toggle.is_off()


async def test_turning_on():
    toggle = Toggle(False)
    await toggle.turn_to(True)
    assert toggle.is_on()
    assert not toggle.is_off()


async def test_turning_off():
    toggle = Toggle(True)
    await toggle.turn_to(False)
    assert not toggle.is_on()
    assert toggle.is_off()


async def test_waiting_until_on_fails_when_not_turned_on(looptime):
    toggle = Toggle(False)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(toggle.wait_for(True), timeout=1.23)
    assert toggle.is_off()
    assert looptime == 1.23


async def test_waiting_until_off_fails_when_not_turned_off(looptime):
    toggle = Toggle(True)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(toggle.wait_for(False), timeout=1.23)
    assert toggle.is_on()
    assert looptime == 1.23


async def test_waiting_until_on_wakes_when_turned_on(looptime):
    toggle = Toggle(False)

    async def delayed_turning_on(delay: float):
        await asyncio.sleep(delay)
        await toggle.turn_to(True)

    asyncio.create_task(delayed_turning_on(9))
    await toggle.wait_for(True)

    assert toggle.is_on()
    assert looptime == 9


async def test_waiting_until_off_wakes_when_turned_off(looptime):
    toggle = Toggle(True)

    async def delayed_turning_off(delay: float):
        await asyncio.sleep(delay)
        await toggle.turn_to(False)

    asyncio.create_task(delayed_turning_off(9))
    await toggle.wait_for(False)

    assert toggle.is_off()
    assert looptime == 9


async def test_secures_against_usage_as_a_boolean():
    toggle = Toggle()
    with pytest.raises(NotImplementedError):
        bool(toggle)


async def test_repr_when_unnamed_and_off():
    toggle = Toggle(False)
    assert toggle.name is None
    assert repr(toggle) == "<Toggle: off>"


async def test_repr_when_unnamed_and_on():
    toggle = Toggle(True)
    assert toggle.name is None
    assert repr(toggle) == "<Toggle: on>"


async def test_repr_when_named_and_off():
    toggle = Toggle(False, name='xyz')
    assert toggle.name == 'xyz'
    assert repr(toggle) == "<Toggle: xyz: off>"


async def test_repr_when_named_and_on():
    toggle = Toggle(True, name='xyz')
    assert toggle.name == 'xyz'
    assert repr(toggle) == "<Toggle: xyz: on>"
