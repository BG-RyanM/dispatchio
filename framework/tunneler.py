from abc import ABC, abstractmethod
from typing import Union, Any
from asyncio import Queue, wait_for

from framework.listener import MessageListener


class MessageTunneler(MessageListener):

    def __init__(self):
        super().__init__()
        self._outgoing_queue = Queue()

    async def get_queued_outgoing_message(self, timeout: Union[float, int, None] = None):
        """
        Returns first item (earliest received) outgoing message to be returned to dispatcher.
        Waits, will return None if timeout.
        """
        try:
            message = await wait_for(self._outgoing_queue.get(), timeout=timeout)
        except TimeoutError:
            return None
        return message
