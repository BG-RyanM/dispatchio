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


class Server(BasicMessageListener):

    def __init__(self, id: Union[str, int, None] = None):
        super().__init__(id)
        self.filtering_table.add_entry({"message_type": ["A", "B", "C"]}, async_callback=self.handle_message)

    async def handle_message(self, message: AsyncMessage):
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
        done_tasks = set()
        while len(done_tasks) < 3:
            for task in tasks:
                if task.done() and task not in done_tasks:
                    response = task.result()
                    print("Got response:", response)
                    done_tasks.add(task)
            await asyncio.sleep(0.1)

    task1 = asyncio.create_task(dispatcher.send_message(message_type="A", destination_id="server",
                                                        response_required=True))
    task2 = asyncio.create_task(dispatcher.send_message(message_type="B", destination_id="server",
                                                        response_required=True))
    task3 = asyncio.create_task(dispatcher.send_message(message_type="C", destination_id="server",
                                                        response_required=True))

    tasks = [task1, task2, task3]
    print("Non-blocking:")
    await _await_replies(tasks)

    task1 = asyncio.create_task(dispatcher.send_message(message_type="A", destination_id="server",
                                                        response_required=True, is_blocking=True))
    task2 = asyncio.create_task(dispatcher.send_message(message_type="B", destination_id="server",
                                                        response_required=True, is_blocking=True))
    task3 = asyncio.create_task(dispatcher.send_message(message_type="C", destination_id="server",
                                                        response_required=True, is_blocking=True))

    tasks = [task1, task2, task3]
    print("Blocking:")
    await _await_replies(tasks)

    active_channels, info = dispatcher.test_for_active_channels()
    if active_channels:
        print("Oops, there are still active channels. Info:", info)
    else:
        print("Test complete, no active channels")


asyncio.run(main())
