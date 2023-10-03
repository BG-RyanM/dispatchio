import asyncio
from typing import Dict, Union
import sys
import os

# I hate this, but it's how I got the import to actually work
current_dir = os.path.dirname(os.path.realpath(__file__))
base_dir = os.path.join(current_dir, "../")
base_dir = os.path.abspath(base_dir)
sys.path.append(base_dir)

from framework.message import Message, AsyncMessage
from framework.basic_listener import BasicMessageListener
from framework.dispatcher import Dispatcher

"""
This program demonstrates the idea of blocking messages, by showing a comparison between them and
non-blocking messages. The concept only applies to asynchronous messages.
"""


class Server(BasicMessageListener):
    def __init__(self, id: Union[str, int, None] = None):
        super().__init__(id)
        self.filtering_table.add_entry(
            {"message_type": ["A", "B", "C"]}, async_callback=self.custom_handle_message
        )

    async def custom_handle_message(self, message: AsyncMessage):
        """Callback that receives messages from client"""
        if message.message_type == "A":
            await asyncio.sleep(3.0)
            return "'A' response"
        elif message.message_type == "B":
            await asyncio.sleep(0.4)
            return "'B' response"
        elif message.message_type == "C":
            await asyncio.sleep(0.5)
            return "'C' response"


async def main():
    dispatcher = Dispatcher.get_instance()
    await dispatcher.register_listener(Server("server"))

    async def _await_replies(tasks):
        # Awaits expected replies from server, and prints results as they come in
        done_tasks = set()
        while len(done_tasks) < 3:
            for task in tasks:
                if task.done() and task not in done_tasks:
                    response = task.result()
                    print("Got response:", response)
                    done_tasks.add(task)
            await asyncio.sleep(0.1)

    # Non-blocking messages are sent one after another, but replies can happen in any order
    # (As illustrated by the slow reply to message "A")
    task1 = asyncio.create_task(
        dispatcher.send_message(
            message_type="A", destination_id="server", response_required=True
        )
    )
    task2 = asyncio.create_task(
        dispatcher.send_message(
            message_type="B", destination_id="server", response_required=True
        )
    )
    task3 = asyncio.create_task(
        dispatcher.send_message(
            message_type="C", destination_id="server", response_required=True
        )
    )

    tasks = [task1, task2, task3]
    print("Non-blocking message test, expected reply order is: B, C, A")
    await _await_replies(tasks)

    # A blocking message won't allow any other async message to be sent until a reply comes back
    # The other messages passed to send_message() are still queued up behind it, but not sent
    # until the blocking message receives its reply.
    task1 = asyncio.create_task(
        dispatcher.send_message(
            message_type="A",
            destination_id="server",
            response_required=True,
            is_blocking=True,
        )
    )
    task2 = asyncio.create_task(
        dispatcher.send_message(
            message_type="B",
            destination_id="server",
            response_required=True,
            is_blocking=True,
        )
    )
    task3 = asyncio.create_task(
        dispatcher.send_message(
            message_type="C",
            destination_id="server",
            response_required=True,
            is_blocking=True,
        )
    )

    tasks = [task1, task2, task3]
    print("Blocking message test, expected reply order is: A, B, C")
    # We expect replies in the order of: A, B, C -- the same order messages dispatched
    await _await_replies(tasks)

    await dispatcher.deregister_listener("server")

    active_channels, info = dispatcher.test_for_active_channels()
    if active_channels:
        print("Oops, there are still active channels. Info:", info)
    else:
        print("Test complete, no active channels")
    active_responses, info = dispatcher.test_for_active_awaited_responses()
    if active_responses:
        print("Oops, there are still active responses. Info:", info)
    else:
        print("Test complete, no active responses")
    registered_listeners, info = dispatcher.test_for_registered_listeners()
    if registered_listeners:
        print("Oops, there are still registered listeners. Info:", info)
    else:
        print("Test complete, no registered listeners")


asyncio.run(main())
