import asyncio
from typing import Dict, Union
import sys
import os

# I hate this, but it's how I got the import to actually work
current_dir = os.path.dirname(os.path.realpath(__file__))
base_dir = os.path.join(current_dir, "../")
base_dir = os.path.abspath(base_dir)
sys.path.append(base_dir)

from framework.message import Message, AsyncMessage, MessageError
from framework.basic_listener import BasicMessageListener
from framework.dispatcher import Dispatcher

"""
Test to demonstrate that Dispatcher will clean itself up and shut down, even while message
responses are being awaited.
"""


class Server(BasicMessageListener):
    def __init__(self, id: Union[str, int, None] = None):
        super().__init__(id)
        self.filtering_table.add_entry(
            {"message_type": ["A", "B"]}, async_callback=self.custom_handle_message
        )

    async def custom_handle_message(self, message: AsyncMessage):
        """Callback that receives messages from client"""
        if message.message_type == "A":
            await asyncio.sleep(4.0)
            return "'A' response"
        elif message.message_type == "B":
            await asyncio.sleep(4.0)
            return "'B' response"


async def main():
    dispatcher = Dispatcher.get_instance()
    server = Server("server")
    await dispatcher.register_listener(server)
    await dispatcher.register_listener_in_group(server, "B group")

    # Send an invalid message and make sure it triggers an exception
    try:
        await dispatcher.dispatch_message(
            message_type="ZZZ", destination_id="server", group_id="B group"
        )
    except MessageError:
        print("Bad message produced exception as expected")
    else:
        print("Oops, bad message did not raise exception")

    # Dispatch message for which response is expected to take a long time, then
    # deregister listener. This shouldn't break anything.
    print("Sending message 'A'...")
    task = asyncio.create_task(
        dispatcher.dispatch_message(
            message_type="A", destination_id="server", response_required=True
        )
    )
    # Allow some time for message to come off queue and be sent
    await asyncio.sleep(1.0)
    await dispatcher.deregister_listener(server)
    reply = await task
    print("Got response to 'A' (expecting ''A' response'):", reply)

    # Same thing as above, but with group message
    print("Sending message 'B'...")
    task = asyncio.create_task(
        dispatcher.dispatch_message(
            message_type="B", group_id="B group", response_required=True
        )
    )
    # Allow some time for message to come off queue and be sent
    await asyncio.sleep(1.0)
    await dispatcher.deregister_listener_from_group(server, "B group")
    reply = await task
    print("Got response to 'B' (expecting 'None'):", reply)

    # Re-register listeners
    await dispatcher.register_listener(server)
    await dispatcher.register_listener_in_group(server, "B group")

    print("Sending 'A' and 'B' messages and shutting down...")
    asyncio.create_task(
        dispatcher.dispatch_message(
            message_type="A", destination_id="server", response_required=True
        )
    )
    asyncio.create_task(
        dispatcher.dispatch_message(
            message_type="B",
            group_id="B group",
            response_required=True,
            is_blocking=True,
        )
    )
    asyncio.create_task(
        dispatcher.dispatch_message(
            message_type="B", group_id="B group", response_required=True
        )
    )
    # Allow some time for messages to come off queue and be sent
    await asyncio.sleep(1.0)
    await dispatcher.shutdown()
    print("Successful shutdown")


asyncio.run(main())
