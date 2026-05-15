#!/usr/bin/env python3

import asyncio
from datetime import datetime
from datetime import timedelta
from functools import partial
import logging
import json
from typing import Callable

from aiomqtt import Client
import gpiod
from gpiod import Chip
from gpiod.line import Direction
from gpiod.line import Value
# from paho.mqtt.subscribeoptions import SubscribeOptions

import domio.config as config
from domio.typem import DomioException
from domio.utils import done_callback

_gpios = {}
_doorbell_button_task_handle = None
_doorbell_ring_task_handle = None
_last_on = datetime.now()
_mqtt_listen_task_handle = None
_running = False
_ups_task_handle = None

# logger initial setup
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


async def init():
    global _doorbell_button_task_handle
    global _mqtt_listen_task_handle
    global _running
    global _ups_task_handle

    chip = Chip(config.general.gpio_device)

    # configure raspberry pi gpios
    for gpio_name in dir(config.gpios):
        if gpio_name.startswith("__"):
            continue

        gpio = getattr(config.gpios, gpio_name)

        direction = gpio.direction
        kwargs = {"direction": direction}
        if gpio.active_low is not None:
            kwargs["active_low"] = gpio.active_low
        if gpio.bias is not None:
            kwargs["bias"] = gpio.bias
        if gpio.drive is not None:
            kwargs["drive"] = gpio.drive
        if gpio.debounce_period is not None:
            kwargs["debounce_period"] = gpio.debounce_period
        if gpio.edge_detection is not None:
            kwargs["edge_detection"] = gpio.edge_detection
        if gpio.output_value is not None:
            kwargs["output_value"] = gpio.output_value
        # TODO bias

        line_config = {gpio.gpio_num: gpiod.LineSettings(**kwargs)}

        _gpios[gpio.gpio_num] = chip.request_lines(
            consumer="domio",
            config=line_config
        )

    _running = True

    # configure doorbell button callback
    if _doorbell_button_task_handle is None:
        _doorbell_button_task_handle = asyncio.create_task(
            _wait_for_event(
                config.gpios.doorbell_button.gpio_num,
                __doorbell_button_callback,
            )
        )
        _doorbell_button_task_handle.add_done_callback(partial(done_callback, logger))

    # configure listen mqtt task
    if _mqtt_listen_task_handle is None:
        _mqtt_listen_task_handle = asyncio.create_task(_mqtt_listen_task())
        _mqtt_listen_task_handle.add_done_callback(partial(done_callback, logger))

    # configure ups
    if _ups_task_handle is None:
        _ups_task_handle = asyncio.create_task(_ups_task())
        _ups_task_handle.add_done_callback(partial(done_callback, logger))


async def _mqtt_publish(topic: str, payload=None):
    async with Client(config.mqtt.hostname, config.mqtt.port) as client:
        await client.publish(topic, payload=payload)


def __doorbell_button_callback():
    logger.info("door bell button pressed")
    asyncio.create_task(_mqtt_publish("home/doorbell/pressed"))


async def _ring_doorbell_task(number: int):
    global _doorbell_ring_task_handle

    logger.info("ring the bell")

    gpio_num = config.gpios.doorbell_output.gpio_num

    try:
        for s in range(number):
            set_value(gpio_num, True)
            logger.debug("ding")
            await asyncio.sleep(0.4)

            if not _running:
                return

            set_value(gpio_num, False)
            logger.debug("dong")
            await asyncio.sleep(1.2)

            # in case shutdown is requested while ringing the bell
            if not _running:
                return
    finally:
        set_value(gpio_num, False)
        _doorbell_ring_task_handle = None


async def _mqtt_listen_task():
    global _doorbell_ring_task_handle

    async with Client(config.mqtt.hostname, config.mqtt.port) as client:
        await client.subscribe("home/doorbell/ring")
        try:
            async for message in client.messages:
                # give up if the bell is already ringing
                if _doorbell_ring_task_handle is None:
                    # the task is run using the loop of the main thread enabling
                    # _doorbell_ring_task_handle to be tested without using a
                    # lock
                    payload = json.loads(message.payload.decode())
                    number = int(payload["number"])
                    _doorbell_ring_task_handle = asyncio.create_task(_ring_doorbell_task(number))
        except (asyncio.CancelledError, KeyboardInterrupt):
            pass


async def _ups_task():
    global _last_on

    ac220_old = True  # inactive state

    try:
        while _running:
            _ac220 = not get_value(config.gpios.ac220.gpio_num)

            if _ac220 != ac220_old:
                if _ac220:
                    logger.info("220V power supply is on")
                    set_value(config.gpios.buzzer.gpio_num, False)
                    await _mqtt_publish("home/mains/presence", "1")
                else:
                    logger.info("220V power supply is off")
                    set_value(config.gpios.buzzer.gpio_num, True)
                    await _mqtt_publish("home/mains/presence", "0")

            ac220_old = _ac220

            if _ac220:
                _last_on = datetime.now()
            else:
                if datetime.now() - _last_on > timedelta(minutes=5):
                    # shutdown after 5 minutes after lacking 220 V power
                    logger.debug("shutdown requested")
                    await asyncio.create_subprocess_shell(
                        "sudo shutdown -h +1",
                        shell=True,
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.DEVNULL
                    )

            await asyncio.sleep(1)

    except KeyboardInterrupt:
        return


async def close():
    global _doorbell_ring_task_handle
    global _gpios
    global _running
    global _mqtt_listen_task_handle
    global _ups_task_handle

    logger.debug("closing")

    _running = False

    # kill tasks
    if _doorbell_ring_task_handle is not None:
        try:
            await _doorbell_ring_task_handle
        except Exception:
            # task exceptions are handled by the done callback
            pass
        _doorbell_ring_task_handle = None

    if _mqtt_listen_task_handle is not None:
        try:
            _mqtt_listen_task_handle.cancel()
            await _mqtt_listen_task_handle
        except Exception:
            # task exceptions are handled by the done callback
            pass
        _mqtt_listen_task_handle = None

    if _ups_task_handle is not None:
        try:
            await _ups_task_handle
        except Exception:
            # task exceptions are handled by the done callback
            pass
        _ups_task_handle = None

    # set all raspberry pi's outputs to their rest value
    for gpio_name in dir(config.gpios):
        if gpio_name.startswith("__"):
            continue

        gpio = getattr(config.gpios, gpio_name)
        line = _gpios[gpio.gpio_num]
        if gpio.direction == Direction.OUTPUT and gpio.output_value is not None:
            line.set_value(gpio.gpio_num, gpio.output_value)
        line.release()

    _gpios = {}


def get_value(num: int) -> bool:
    try:
        line = _gpios[num]
    except IndexError as exc:
        raise DomioException(f"invalid gpio input number ({num})") from exc

    value = line.get_value(num)
    return value == Value.ACTIVE


def set_value(num: int, state: bool):
    try:
        line = _gpios[num]
    except IndexError as exc:
        raise DomioException(f"invalid gpio output number ({num})") from exc

    value = Value.ACTIVE if state else Value.INACTIVE
    line.set_value(num, value)


# see: https://stackoverflow.com/a/76769504
async def _wait_for_event(num_gpio: int, callback: Callable, *args, **kwargs):
    logger.info(f"task started ({num_gpio})")

    line = _gpios[num_gpio]
    loop = asyncio.get_running_loop()
    while _running:
        if await loop.run_in_executor(None, line.wait_edge_events, 1.0):
            # an event has been received
            try:
                line.read_edge_events(1)
            except Exception:
                # when the task is aborted
                continue

            callback(*args, **kwargs)

    logger.info(f"task stopped ({num_gpio})")


async def run(config_filename: str):
    config.read(config_filename)

    await init()

    await asyncio.sleep(1)

    await _ring_doorbell_task(5)

    await asyncio.sleep(20)


# main is used for test purpose as standalone
if __name__ == "__main__":
    import argparse
    import sys

    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter("%(asctime)s %(module)s %(levelname)s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", default="config.toml")
    args = parser.parse_args()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run(args.config))
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(close())
        loop.stop()
        logger.info("done")
