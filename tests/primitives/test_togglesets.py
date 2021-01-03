import asyncio

import pytest

from kopf.structs.primitives import Toggle, ToggleSet


async def test_created_as_off():
    toggleset = ToggleSet()
    assert len(toggleset) == 0
    assert set(toggleset) == set()
    assert Toggle() not in toggleset
    assert not toggleset.is_on()
    assert toggleset.is_off()


async def test_making_a_default_toggle():
    toggleset = ToggleSet()
    toggle = await toggleset.make_toggle()
    assert len(toggleset) == 1
    assert set(toggleset) == {toggle}
    assert toggle in toggleset
    assert Toggle() not in toggleset
    assert not toggleset.is_on()
    assert toggleset.is_off()


async def test_making_a_turned_off_toggle():
    toggleset = ToggleSet()
    toggle = await toggleset.make_toggle(False)
    assert len(toggleset) == 1
    assert set(toggleset) == {toggle}
    assert toggle in toggleset
    assert Toggle() not in toggleset
    assert not toggleset.is_on()
    assert toggleset.is_off()


async def test_making_a_turned_on_toggle():
    toggleset = ToggleSet()
    toggle = await toggleset.make_toggle(True)
    assert len(toggleset) == 1
    assert set(toggleset) == {toggle}
    assert toggle in toggleset
    assert Toggle() not in toggleset
    assert toggleset.is_on()
    assert not toggleset.is_off()


async def test_dropping_a_turned_off_toggle():
    toggleset = ToggleSet()
    toggle = await toggleset.make_toggle(False)
    await toggle.turn_to(True)
    await toggleset.drop_toggle(toggle)
    assert len(toggleset) == 0
    assert set(toggleset) == set()
    assert toggle not in toggleset
    assert not toggleset.is_on()
    assert toggleset.is_off()


async def test_dropping_a_turned_on_toggle():
    toggleset = ToggleSet()
    toggle = await toggleset.make_toggle(True)
    await toggleset.drop_toggle(toggle)
    assert len(toggleset) == 0
    assert set(toggleset) == set()
    assert toggle not in toggleset
    assert not toggleset.is_on()
    assert toggleset.is_off()


async def test_dropping_an_unexistent_toggle():
    toggleset = ToggleSet()
    toggle = Toggle()
    await toggleset.drop_toggle(toggle)
    assert len(toggleset) == 0
    assert set(toggleset) == set()
    assert toggle not in toggleset
    assert not toggleset.is_on()
    assert toggleset.is_off()


async def test_dropping_multiple_toggles():
    toggleset = ToggleSet()
    toggle1 = await toggleset.make_toggle(True)
    toggle2 = Toggle()
    await toggleset.drop_toggles([toggle1, toggle2])
    assert len(toggleset) == 0
    assert set(toggleset) == set()
    assert toggle1 not in toggleset
    assert toggle2 not in toggleset
    assert not toggleset.is_on()
    assert toggleset.is_off()


async def test_turning_a_toggle_on_turns_the_toggleset_on():
    toggleset = ToggleSet()
    toggle = await toggleset.make_toggle(False)
    assert not toggleset.is_on()
    assert toggleset.is_off()

    await toggle.turn_to(True)
    assert toggleset.is_on()
    assert not toggleset.is_off()


async def test_turning_a_toggle_off_turns_the_toggleset_off():
    toggleset = ToggleSet()
    toggle = await toggleset.make_toggle(True)
    assert toggleset.is_on()
    assert not toggleset.is_off()

    await toggle.turn_to(False)
    assert not toggleset.is_on()
    assert toggleset.is_off()


async def test_any_toggle_must_be_on_for_toggleset_to_be_on():
    toggleset = ToggleSet()
    toggle1 = await toggleset.make_toggle(False)
    toggle2 = await toggleset.make_toggle(False)
    assert not toggleset.is_on()
    assert toggleset.is_off()

    await toggle1.turn_to(True)
    assert toggleset.is_on()
    assert not toggleset.is_off()

    await toggle2.turn_to(True)
    assert toggleset.is_on()
    assert not toggleset.is_off()


async def test_all_toggles_must_be_off_for_toggleset_to_be_off():
    toggleset = ToggleSet()
    toggle1 = await toggleset.make_toggle(True)
    toggle2 = await toggleset.make_toggle(True)
    assert toggleset.is_on()
    assert not toggleset.is_off()

    await toggle1.turn_to(False)
    assert toggleset.is_on()
    assert not toggleset.is_off()

    await toggle2.turn_to(False)
    assert not toggleset.is_on()
    assert toggleset.is_off()


async def test_waiting_until_on_fails_when_not_turned_on():
    toggleset = ToggleSet()
    await toggleset.make_toggle(False)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(toggleset.wait_for(True), timeout=0.1)
    assert toggleset.is_off()


async def test_waiting_until_off_fails_when_not_turned_off():
    toggleset = ToggleSet()
    await toggleset.make_toggle(True)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(toggleset.wait_for(False), timeout=0.1)
    assert toggleset.is_on()


async def test_waiting_until_on_wakes_when_turned_on(timer):
    toggleset = ToggleSet()
    toggle = await toggleset.make_toggle(False)

    async def delayed_turning_on(delay: float):
        await asyncio.sleep(delay)
        await toggle.turn_to(True)

    with timer:
        asyncio.create_task(delayed_turning_on(0.05))
        await asyncio.wait_for(toggleset.wait_for(True), timeout=1.0)

    assert toggleset.is_on()
    assert timer.seconds < 0.5  # approx. 0.05 plus some code overhead


async def test_waiting_until_off_wakes_when_turned_off(timer):
    toggleset = ToggleSet()
    toggle = await toggleset.make_toggle(True)

    async def delayed_turning_off(delay: float):
        await asyncio.sleep(delay)
        await toggle.turn_to(False)

    with timer:
        asyncio.create_task(delayed_turning_off(0.05))
        await asyncio.wait_for(toggleset.wait_for(False), timeout=1.0)

    assert toggleset.is_off()
    assert timer.seconds < 0.5  # approx. 0.05 plus some code overhead


async def test_secures_against_usage_as_a_boolean():
    toggle = ToggleSet()
    with pytest.raises(NotImplementedError):
        bool(toggle)


async def test_repr_when_empty():
    toggleset = ToggleSet()
    assert repr(toggleset) == "set()"


async def test_repr_when_unnamed_and_off():
    toggleset = ToggleSet()
    await toggleset.make_toggle(False)
    assert repr(toggleset) == "{<Toggle: off>}"


async def test_repr_when_unnamed_and_on():
    toggleset = ToggleSet()
    await toggleset.make_toggle(True)
    assert repr(toggleset) == "{<Toggle: on>}"


async def test_repr_when_named_and_off():
    toggleset = ToggleSet()
    await toggleset.make_toggle(False, name='xyz')
    assert repr(toggleset) == "{<Toggle: xyz: off>}"


async def test_repr_when_named_and_on():
    toggleset = ToggleSet()
    await toggleset.make_toggle(True, name='xyz')
    assert repr(toggleset) == "{<Toggle: xyz: on>}"
