import asyncio
from typing import List, Optional, Union, Dict, Any, Literal, overload
from asyncio import Lock, Queue, Event, Task, Future, CancelledError, TimeoutError
import logging

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

        self._logger = logging.getLogger(__name__)
        self._logger.info("Created message dispatcher")

        self._next_listener_id = 0
        self._next_message_id = 0
        self._listeners_by_id: Dict[Union[str, int], MessageListener] = {}
        self._listeners_by_group = {}
        self._lock = Lock()
        self._shutdown = False

        self._run_loop_task = asyncio.create_task(self._run_loop())

        self._outgoing_message_queue = Queue()
        # Holds async tasks that are waiting on reply from async callbacks
        # Maps message ID to task
        self._reply_task_table: Dict[int, Task] = {}
        # Maps message ID to Event triggered when reply comes in
        self._reply_event_table: Dict[int, Event] = {}
        # Maps message ID to reply object or to exception
        self._reply_table: Dict[int, Any] = {}

        # Similar idea to self._reply_task_table, but for group message
        self._group_reply_future_table: Dict[int, Future] = {}
        self._group_reply_event_table: Dict[int, Event] = {}
        # Exceptions only
        self._group_reply_table: Dict[int, Optional[Exception]] = {}

    @staticmethod
    def get_instance():
        return Dispatcher()

    async def get_id(self):
        async with self._lock:
            id = self._next_listener_id
            self._next_listener_id += 1
            return id

    async def register_listener(self, listener: MessageListener, auto_id: bool = False):
        """
        Registers a listener with dispatcher. If it hasn't already been given a (destination) ID,
        it will be assigned one.

        :param listener: --
        :param auto_id: if True, assign ID automatically
        :raise RegistrationError: if listener with ID already registered
        """
        async with self._lock:
            if listener.get_id() is None or auto_id:
                id = self._next_listener_id
                self._next_listener_id += 1
                listener.set_id(id)
            else:
                if self._listeners_by_id.get(listener.get_id()):
                    raise RegistrationError(f"Listener already registered with ID {listener.get_id()}")
            self._listeners_by_id[listener.get_id()] = listener
            self._logger.info(f"Registered listener with ID {listener.get_id()}")

    async def deregister_listener(self, listener: MessageListener):
        """
        Deregister listener with dispatcher. Has no effect if listener not registered.
        :param listener: --
        :raise: RegistrationError if listener has no ID
        """
        async with self._lock:
            if listener.get_id() is not None:
                self._listeners_by_id.pop(listener.get_id(), None)
                self._logger.info(f"Deregistered listener with ID {listener.get_id()}")
            else:
                raise RegistrationError("Cannot deregister listener with no ID")

    async def register_listener_in_group(self, listener: MessageListener, group_name: Union[int, str]):
        """
        Register a listener as subscribing to messages send to a particular group. Will not permit
        same listener to be registered twice.
        :param listener: --
        :param group_name: name of group ID listener is interested in
        :return:
        """
        async with self._lock:
            if self._listeners_by_group.get(group_name) is None:
                self._listeners_by_group[group_name] = []
            listener_list = self._listeners_by_group[group_name]
            for existing_listener in listener_list:
                if existing_listener is listener:
                    self._logger.warning(
                        f"Listener with ID {listener.get_id()} already registered in group {group_name}")
                    return
            listener_list.append(listener)
            self._logger.info(f"Registered listener with ID {listener.get_id()} in group {group_name}")

    async def deregister_listener_from_group(self, listener: MessageListener, group_name: Union[int, str]):
        """
        Unsubscribes listener to messages sent to a particular group.
        :param listener: --
        :param group_name: --
        """
        async with self._lock:
            if self._listeners_by_group.get(group_name) is None:
                return
            listener_list = self._listeners_by_group[group_name]
            new_list = []
            for existing_listener in listener_list:
                if existing_listener is not listener:
                    new_list.append(existing_listener)
                else:
                    self._logger.info(f"Deregistered listener with ID {listener.get_id()} from group {group_name}")
            self._listeners_by_group[group_name] = new_list

    @overload
    def send_message_sync(self, message: Union[Message, Dict],
                          message_type: Literal[""] = "",
                          id: Literal[-1] = -1,
                          source_id: Literal[None] = None,
                          destination_id: Literal[None] = None,
                          group_id: Literal[None] = None,
                          response_required: Literal[False] = False,
                          is_response: Literal[False] = False,
                          content: Literal[None] = None
                          ) -> Any:
        pass

    @overload
    def send_message_sync(self, message: Literal[None] = None,
                          message_type: str = "",
                          id: int = -1,
                          source_id: Union[str, int, None] = None,
                          destination_id: Union[str, int, None] = None,
                          group_id: Union[str, int, None] = None,
                          response_required: bool = False,
                          is_response: bool = False,
                          content: Any = None
                          ) -> Any:
        pass

    def send_message_sync(self, message: Union[Message, Dict, None] = None,
                          message_type: str = "",
                          id: int = -1,
                          source_id: Union[str, int, None] = None,
                          destination_id: Union[str, int, None] = None,
                          group_id: Union[str, int, None] = None,
                          response_required: bool = False,
                          is_response: bool = False,
                          content: Any = None
                          ) -> Any:
        """
        Sends a message to interested listeners synchronously
        :param message: a complete Message object or Dict version of. If given, all other params
            will be ignored. If not given, other params will be applied in building a new
            message.
        :param message_type: type of message, e.g. "GetUserInfo" or "MousePress"
        :param id: unique ID to give to message. If -1, ID will be assigned automatically
        :param source_id: source "address" of message. Generally, the same as a destination ID.
        :param destination_id: destination "address" of message
        :param group_id: if given, specifies group of listeners to send message copies to
        :param response_required: if True, a response must be given.
            TODO: more notes
        :param is_response: if True, this message is a response to another
        :param content: message content object. Should be cast-able to dict.
        :return: reply from other side, or None, if not given
        :raise: any exception encountered
        """
        return self._send_message_sync_impl(message=message, message_type=message_type, id=id, source_id=source_id,
                                            destination_id=destination_id, group_id=group_id,
                                            response_required=response_required, is_response=is_response,
                                            synchronous=True, content=content)

    @overload
    async def send_message(self, message: Union[Message, Dict],
                           message_type: Literal[""] = "",
                           id: Literal[-1] = -1,
                           source_id: Literal[None] = None,
                           destination_id: Literal[None] = None,
                           group_id: Literal[None] = None,
                           response_required: Literal[False] = False,
                           is_response: Literal[False] = False,
                           timeout: Optional[float] = None,
                           content: Literal[None] = None
                           ) -> Any:
        pass

    @overload
    async def send_message(self, message: Literal[None] = None,
                           message_type: str = "",
                           id: int = -1,
                           source_id: Union[str, int, None] = None,
                           destination_id: Union[str, int, None] = None,
                           group_id: Union[str, int, None] = None,
                           response_required: bool = False,
                           is_response: bool = False,
                           timeout: Optional[float] = None,
                           content: Any = None
                           ) -> Any:
        pass

    async def send_message(self, message: Union[Message, Dict, None] = None,
                           message_type: str = "",
                           id: int = -1,
                           source_id: Union[str, int, None] = None,
                           destination_id: Union[str, int, None] = None,
                           group_id: Union[str, int, None] = None,
                           response_required: bool = False,
                           is_response: bool = False,
                           timeout: Optional[float] = None,
                           content: Any = None
                           ) -> Any:
        """
        Sends a message to interested listeners asynchronously
        :param message: a complete Message object or Dict version of. If given, all other params
            will be ignored. If not given, other params will be applied in building a new
            message.
        :param message_type: type of message, e.g. "GetUserInfo" or "MousePress"
        :param id: unique ID to give to message. If -1, ID will be assigned automatically
        :param source_id: source "address" of message. Generally, the same as a destination ID.
        :param destination_id: destination "address" of message
        :param group_id: if given, specifies group of listeners to send message copies to
        :param response_required: if True, a response must be given.
            TODO: more notes
        :param is_response: if True, this message is a response to another
        :param timeout: if given, specifies how long to wait for response
        :param content: message content object. Should be cast-able to dict.
        :return: reply from other side, or None, if not given
        :raise TimeoutError: if timeout expires
        :raise: any exception encountered
        """
        return await self._queue_message_impl(message=message, message_type=message_type, id=id, source_id=source_id,
                                              destination_id=destination_id, group_id=group_id,
                                              response_required=response_required, is_response=is_response,
                                              synchronous=False, content=content, timeout=timeout)

    async def _run_loop(self):
        self._logger.info("In dispatcher run loop.")
        while not self._shutdown:
            # Is there an outgoing message on the queue? If so, send
            if not self._outgoing_message_queue.empty():
                message = await self._outgoing_message_queue.get()
                self._logger.debug(f"Dispatching message from queue: {message.get_info()}")
                # Will do work from behind lock
                await self._send_message_impl(message)

            async with self._lock:
                # Deal with any replies that have come back
                remove_ids = []
                for msg_id, task in self._reply_task_table.items():
                    if task.done():
                        self._logger.debug(f"Got response to message with ID {msg_id}")
                        try:
                            result = task.result()
                        except CancelledError:
                            result = None
                        except Exception as ex:
                            result = ex
                        self._reply_table[msg_id] = result
                        self._reply_event_table[msg_id].set()
                        remove_ids.append(msg_id)
                for msg_id in remove_ids:
                    try:
                        self._reply_task_table.pop(msg_id)
                    except KeyError:
                        pass

                # Deal with any group replies. We only care about exceptions.
                remove_ids = []
                for msg_id, future in self._group_reply_future_table.items():
                    if future.done():
                        self._logger.debug(f"Got responses to group message with ID {msg_id}")
                        result_list = await future
                        got_exception = False
                        for result in result_list:
                            if isinstance(result, Exception):
                                self._group_reply_table[msg_id] = result
                                got_exception = True
                                break
                        if not got_exception:
                            self._group_reply_table[msg_id] = None
                        self._group_reply_event_table[msg_id].set()
                        remove_ids.append(msg_id)
                for msg_id in remove_ids:
                    try:
                        self._group_reply_future_table.pop(msg_id)
                    except KeyError:
                        pass
                    except Exception as ex:
                        self._logger.exception(f"Unknown exception {ex}")

            await asyncio.sleep(0.000001)

    def _send_message_sync_impl(self, *args, **kwargs):
        message = self._get_or_make_message(args, kwargs)
        # TODO: special stuff if have to go another dispatcher
        if message.destination_id is not None:
            listener = self._listeners_by_id.get(message.destination_id)
            if listener:
                return listener.handle_message_sync(message)
            else:
                raise DispatcherError(f"No listener registered for destination ID {message.destination_id}")
        elif message.group_id is not None:
            raise DispatcherError(f"Cannot send synchronous message to group")
        else:
            raise DispatcherError(f"Message has no destination or group ID")

    async def _queue_message_impl(self, *args, **kwargs):
        timeout = kwargs.get("timeout", None)

        message = self._get_or_make_message(args, kwargs)
        msg_id = message.id
        is_group_msg = (message.destination_id is None) and (message.group_id is not None)
        async with self._lock:
            # The sender might or might not want a reply, but has to be informed about exceptions.
            # Create an event to signal when reply comes in.
            reply_event = asyncio.Event()
            if is_group_msg:
                self._group_reply_event_table[message.id] = reply_event
            else:
                self._reply_event_table[message.id] = reply_event

            self._logger.debug(f"Queueing outgoing message {message.get_info()}")
            await self._outgoing_message_queue.put(message)

        # Now, we wait on event
        if reply_event:
            try:
                await asyncio.wait_for(reply_event.wait(), timeout=timeout)
            except TimeoutError as te:
                self._logger.warning(f"Timed out for message {message.id}")
                raise te
            async with self._lock:
                if is_group_msg:
                    self._logger.debug(f"Received replies to group message {message.id}")
                    self._group_reply_event_table.pop(msg_id, None)
                    result = self._group_reply_table.pop(msg_id, None)
                else:
                    self._logger.debug(f"Received reply to message {message.id}")
                    self._reply_event_table.pop(msg_id, None)
                    result = self._reply_table.pop(msg_id, None)
                if isinstance(result, Exception):
                    raise result
                return result

    async def _send_message_impl(self, message: Message):
        async with self._lock:
            # TODO: special stuff if have to go another dispatcher
            if message.destination_id is not None:
                listener = self._listeners_by_id.get(message.destination_id)
                if listener:
                    # Create a task to wait for response, self._run_loop() will deal with eventual reply
                    reply_task = asyncio.create_task(listener.handle_message(message))
                    self._reply_task_table[message.id] = reply_task
                else:
                    raise DispatcherError(f"No listener registered for destination ID {message.destination_id}")
            elif message.group_id is not None:
                listener_list = self._listeners_by_group.get(message.group_id)
                if listener_list:
                    reply_tasks = []
                    for listener in listener_list:
                        reply_task = asyncio.create_task(listener.handle_message(message))
                        reply_tasks.append(reply_task)
                    group_future = asyncio.gather(*reply_tasks, return_exceptions=True)
                    self._group_reply_future_table[message.id] = group_future
            else:
                raise DispatcherError(f"Message has no destination or group ID")

    def _get_or_make_message(self, args, kwargs):
        self._logger.debug(f"Making message, args are {args}, kwargs are {kwargs}")
        is_sync = kwargs["synchronous"]
        if len(args) == 1:
            # This should be the message
            message = args[0]
        elif kwargs.get("message"):
            message = kwargs.get("message")
        else:
            kwargs.pop("message", None)
            kwargs.pop("timeout", None)
            message = Message(**kwargs)
        if message.id == -1 and not is_sync:
            message.id = self._next_message_id
            self._next_message_id += 1
        return message
