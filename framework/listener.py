from abc import ABC, abstractmethod
from typing import Union, Any
from asyncio import Queue, wait_for

from framework.message import Message


class DeferredResponse:
    """
    A Message Listener async message handling function returns this to inform the dispatcher
    that a reply will be coming later. Thus, the dispatcher send() function will keep waiting
    until the reply does come.
    """


class MessageListener(ABC):

    """
    Abstract Message Listener Base Class. Must be implemented by a subclass.

    See README for explanation of Message Listener. At the most basic level, a listener
    must implement handle_message_sync() for handling synchronous messages and
    handle_message() for async.

    handle_message_sync(): must always return a response, even if None. This will be
        passed back to sender.

    handle_message(): must return a response or a DeferredResponse object. The latter
        indicates to the dispatcher that it should wait for a reply.
    """

    def __init__(self):
        self._id: Union[str, int, None] = None
        self._queue = Queue()

    def get_id(self):
        """Returns ID of listener"""
        return self._id

    def set_id(self, id: Union[str, int]):
        """Sets ID of listener"""
        self._id = id

    @abstractmethod
    def handle_message_sync(self, message: Message) -> Any:
        """
        Handles a message synchronously.
        :param message: --
        :return: response, if any
        """

    @abstractmethod
    async def handle_message(self, message: Message) -> Any:
        """
        Handles a message asynchronously.
        :param message: --
        :return: response, if any, or DeferredResponse object
        """

    async def get_queued_message(self, timeout: Union[float, int, None] = None):
        """
        Returns first item (earliest received) message from Listener queue.
        Will return None if timeout.
        """
        try:
            message = await wait_for(self._queue.get(), timeout=timeout)
        except TimeoutError:
            return None
        return message
