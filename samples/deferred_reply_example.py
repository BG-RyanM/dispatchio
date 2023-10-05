import asyncio
from typing import Dict, Union
import sys
import os

# I hate this, but it's how I got the import to actually work
current_dir = os.path.dirname(os.path.realpath(__file__))
base_dir = os.path.join(current_dir, "../")
base_dir = os.path.abspath(base_dir)
sys.path.append(base_dir)

from framework.listener import DeferredResponse
from framework.message import Message, AsyncMessage
from framework.basic_listener import BasicMessageListener
from framework.dispatcher import Dispatcher

"""
"""


class TestException(Exception):
    """Specific to this example only"""


class Server(BasicMessageListener):
    def __init__(self, id: Union[str, int, None] = None):
        super().__init__(id)
        self.filtering_table.add_entry(
            {"message_type": ["D", "X"]}, async_callback=self.custom_handle_message
        )
        self._need_response_to = asyncio.Queue()

    async def run(self):
        dispatcher = Dispatcher.get_instance()
        while True:
            msg_type, msg_id = await self._need_response_to.get()
            if msg_type == "D":
                # wait a bit, then reply
                await asyncio.sleep(2.0)
                await dispatcher.dispatch_message(
                    is_response=True, id=msg_id, content=msg_type + " response"
                )

    async def custom_handle_message(self, message: AsyncMessage):
        """Callback that receives messages from client"""
        if message.message_type == "D":
            await self._need_response_to.put((message.message_type, message.id))
            # Returning this tells the message sending function to keep waiting until a reply message
            # comes back. This reply message is sent by run() above. If anything else were
            # returned (including None), that value would go back to the sender as the reply.
            return DeferredResponse()
        elif message.message_type == "X":
            raise TestException("test exception")


async def main():
    dispatcher = Dispatcher.get_instance()
    server = Server("server")
    server_task = asyncio.create_task(server.run())
    await dispatcher.register_listener(server)

    # Do we get a reply to "D" message? We should.
    response = await dispatcher.dispatch_message(
        message_type="D", destination_id="server", response_required=True
    )
    print("got deferred reponse: ", response)

    try:
        await dispatcher.dispatch_message(
            message_type="X", destination_id="server", response_required=True
        )
    except TestException as te:
        print("got expected exception:", te)
    else:
        print("Oops, no exception")


asyncio.run(main())
