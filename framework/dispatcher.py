from typing import List, Optional, Union, Dict, Literal, overload
from asyncio import Lock

from framework.listener import MessageListener
from framework.message import Message
from framework.exceptions import RegistrationError, DispatcherError

class Dispatcher:

    """
    Singleton class responsible for routing all messages within a process to MessageListeners that
    have been registered with Dispatcher.
    """

    _instance = None
    _initialized = False

    def __new__(cls):
        if Dispatcher._instance is None:
            Dispatcher._instance = object.__new__(cls)
        return Dispatcher._instance

    def __init__(self):
        if Dispatcher._initialized:
            return
        else:
            Dispatcher._initialized = True
        self._next_listener_id = 0
        self._next_message_id = 0
        self._listeners_by_id: Dict[Union[str, int], MessageListener] = {}
        self._listeners_by_group = {}
        self._lock = Lock()

    async def get_id(self):
        async with self._lock:
            id = self._next_listener_id
            self._next_listener_id += 1
            return id

    async def register_listener(self, listener: MessageListener):
        async with self._lock:
            if listener.get_id() is None:
                id = self._next_listener_id
                self._next_listener_id += 1
                listener.set_id(id)
            else:
                if self._listeners_by_id.get(listener.get_id()):
                    raise RegistrationError(f"Listener already registered with ID {listener.get_id()}")
                self._listeners_by_id[listener.get_id()] = listener

    async def deregister_listener(self, listener: MessageListener):
        async with self._lock:
            if listener.get_id() is not None:
                self._listeners_by_id.pop(listener.get_id())
            else:
                raise RegistrationError("Cannot deregister listener with no ID")

    async def register_listener_in_group(self, listener: MessageListener, group_name: Union[int, str]):
        async with self._lock:
            if self._listeners_by_group.get(group_name) is None:
                self._listeners_by_group[group_name] = []
            listener_list = self._listeners_by_group[group_name]
            for existing_listener in listener_list:
                if existing_listener is listener:
                    return
            listener_list.append(listener)

    async def deregister_listener_from_group(self, listener: MessageListener, group_name: Union[int, str]):
        async with self._lock:
            if self._listeners_by_group.get(group_name) is None:
                return
            new_list = []
            listener_list = self._listeners_by_group[group_name]
            for existing_listener in listener_list:
                if existing_listener is not listener:
                    new_list.append(existing_listener)
            self._listeners_by_group[group_name] = new_list

    def send_message_sync(self, *args, **kwargs):
        message = self._get_or_make_message(True, args, kwargs)
        # TODO: special stuff if have to go another dispatcher
        if message.destination_id is not None:
            listener = self._listeners_by_id.get(message.destination_id)
            if listener:
                # TODO: return type
                listener.handle_message_sync(message)
            else:
                raise DispatcherError(f"No listener registered for destination ID {message.destination_id}")
        elif message.group_id is not None:
            raise DispatcherError(f"Cannot send synchronous message to group")
        else:
            raise DispatcherError(f"Message has no destination or group ID")

    async def send_message(self, *args, **kwargs):
        message = self._get_or_make_message(False, args, kwargs)
        async with self._lock:
            # TODO: special stuff if have to go another dispatcher
            if message.destination_id is not None:
                listener = self._listeners_by_id.get(message.destination_id)
                if listener:
                    # TODO: return type
                    await listener.handle_message(message)
                else:
                    raise DispatcherError(f"No listener registered for destination ID {message.destination_id}")
            elif message.group_id is not None:
                listener_list = self._listeners_by_group.get(message.group_id)
                if listener_list:
                    for listener in listener_list:
                        try:
                            await listener.handle_message(message)
                        except Exception as e:
                            raise e
            else:
                raise DispatcherError(f"Message has no destination or group ID")

    def _get_or_make_message(self, is_sync: bool, *args, **kwargs):
        if len(args) == 1:
            # This should be the message
            message = args[0]
        elif kwargs.get("message"):
            message = kwargs.get("message")
        else:
            message = Message(**kwargs)
        if message.id == -1 and not is_sync:
            message.id = self._next_message_id
            self._next_message_id += 1
        return message
