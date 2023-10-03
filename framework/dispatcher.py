import asyncio
from enum import IntEnum
from typing import List, Optional, Union, Dict, Tuple, Any, Literal, overload
from asyncio import Lock, Queue, Event, Task, Future, CancelledError, TimeoutError
import logging

import framework.listener
from framework.listener import DeferredResponse
from framework.basic_listener import MessageListener
from framework.message import Message, SyncMessage, AsyncMessage
from framework.exceptions import RegistrationError, DispatcherError


class ChannelTargetType(IntEnum):
    SINGLE_DESTINATION = 0
    GROUP = 1
    OTHER_PROCESS = 2
    REPLY = 3  # for replies only (no specific target)


class DispatcherChannel:
    """
    A `channel` represents a particular target that the Dispatcher knows about whether a destination ID,
    a group ID, or a process ID. There's also a channel for replies.
    """

    def __init__(self, target_type: ChannelTargetType, id: Union[str, int]):
        """
        :param target_type: single destination, group, or other process
        :param id: destination ID, group name, or process ID
        """
        self.target_type = target_type
        self.id = id
        self.outgoing_queue = Queue()
        # ID of any blocking message currently being waited on a response to, or None
        self.blocking_id = None
        self.listeners = []

    @property
    def currently_blocking(self):
        return self.blocking_id is not None

    def accepting_new_listeners(self) -> bool:
        if self.target_type == ChannelTargetType.GROUP:
            return True
        elif self.target_type == ChannelTargetType.SINGLE_DESTINATION:
            return len(self.listeners) == 0
        else:
            return False

    def get_info(self) -> str:
        """Gets human-readable info string about channel"""
        type_map = {
            ChannelTargetType.SINGLE_DESTINATION: "Destination ID",
            ChannelTargetType.GROUP: "Group ID",
            ChannelTargetType.OTHER_PROCESS: "Process ID",
            ChannelTargetType.REPLY: "Replies",
        }
        return f"{type_map[self.target_type]}: {self.id}"


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
        self._lock = Lock()
        self._shutdown = False

        self._run_loop_task = asyncio.create_task(self._run_loop())

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

        # maps {target_type : {ID: DispatcherChannel}}
        targets = [
            ChannelTargetType.GROUP,
            ChannelTargetType.SINGLE_DESTINATION,
            ChannelTargetType.OTHER_PROCESS,
            ChannelTargetType.REPLY,
        ]
        self._channel_map: Dict[
            ChannelTargetType, Dict[Union[str, int], DispatcherChannel]
        ] = {i: {} for i, target in enumerate(targets)}
        # Create a channel just for replies
        self._channel_map[ChannelTargetType.REPLY][0] = DispatcherChannel(
            ChannelTargetType.REPLY, 0
        )

        self._blocking_message_id_to_channel: Dict[
            Union[str, int], DispatcherChannel
        ] = {}

    @staticmethod
    def get_instance():
        return Dispatcher()

    def test_for_active_channels(self) -> Tuple[bool, Optional[str]]:
        """
        Returns (True, info string) if there are any channels that are blocking or have
        queued outgoing messages.
        """
        for _type, submap in self._channel_map.items():
            for target_id, channel in submap.items():
                if channel.currently_blocking:
                    return True, "blocked channel: " + channel.get_info()
                if not channel.outgoing_queue.empty():
                    return True, "channel has queued messages: " + channel.get_info()
        return False, None

    def test_for_active_awaited_responses(self) -> Tuple[bool, Optional[str]]:
        """
        Returns (True, info string) if a response is currently being awaited for any
        sent message.
        """
        if len(self._reply_event_table) > 0:
            return True, "reply events still active"
        if len(self._reply_task_table) > 0:
            return True, "reply tasks still active"
        if len(self._group_reply_event_table) > 0:
            return True, "group reply events still active"
        if len(self._group_reply_future_table) > 0:
            return True, "group reply futures still active"
        return False, None

    def test_for_registered_listeners(self) -> Tuple[bool, Optional[str]]:
        """
        Returns (True, info string) if any channel still has registered listeners.
        """
        for _type, submap in self._channel_map.items():
            for target_id, channel in submap.items():
                if len(channel.listeners) > 0:
                    return True, "channel still has listeners: " + channel.get_info()
        return False, None

    async def get_id(self):
        """Returns an unused message ID"""
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
                submap = self._channel_map[ChannelTargetType.SINGLE_DESTINATION]
                if submap.get(listener.get_id()):
                    raise RegistrationError(
                        f"Listener already registered with ID {listener.get_id()}"
                    )
            new_channel = DispatcherChannel(
                ChannelTargetType.SINGLE_DESTINATION, listener.get_id()
            )
            new_channel.listeners = [listener]
            submap[listener.get_id()] = new_channel
            self._logger.info(f"Registered listener with ID {listener.get_id()}")

    async def deregister_listener(self, listener: Union[MessageListener, int, str]):
        """
        Deregister listener with dispatcher. Has no effect if listener not registered.
        :param listener: listener object or ID of listener
        :raise: RegistrationError if listener has no ID
        """
        # TODO: probably don't want to deregister until safe
        if isinstance(listener, MessageListener):
            listener_id = listener.get_id()
        else:
            listener_id = listener

        async with self._lock:
            if listener_id is not None:
                submap = self._channel_map[ChannelTargetType.SINGLE_DESTINATION]
                submap.pop(listener_id, None)
                self._logger.info(f"Deregistered listener with ID {listener_id}")
            else:
                raise RegistrationError("Cannot deregister listener with no ID")

    async def register_listener_in_group(
        self, listener: MessageListener, group_name: Union[int, str]
    ):
        """
        Register a listener as subscribing to messages send to a particular group. Will not permit
        same listener to be registered twice.
        :param listener: --
        :param group_name: name of group ID listener is interested in
        :return:
        """
        async with self._lock:
            submap = self._channel_map[ChannelTargetType.GROUP]
            if submap.get(group_name) is None:
                new_channel = DispatcherChannel(ChannelTargetType.GROUP, group_name)
                submap[group_name] = new_channel
            listener_list = submap[group_name].listeners
            for existing_listener in listener_list:
                if existing_listener is listener:
                    self._logger.warning(
                        f"Listener with ID {listener.get_id()} already registered in group {group_name}"
                    )
                    return
            listener_list.append(listener)
            self._logger.info(
                f"Registered listener with ID {listener.get_id()} in group {group_name}"
            )

    async def deregister_listener_from_group(
        self, listener: MessageListener, group_name: Union[int, str]
    ):
        """
        Unsubscribes listener to messages sent to a particular group.
        :param listener: --
        :param group_name: --
        """
        async with self._lock:
            submap = self._channel_map[ChannelTargetType.GROUP]
            if submap.get(group_name) is None:
                return
            listener_list = submap[group_name].listeners
            new_list = []
            for existing_listener in listener_list:
                if existing_listener is not listener:
                    new_list.append(existing_listener)
                else:
                    self._logger.info(
                        f"Deregistered listener with ID {listener.get_id()} from group {group_name}"
                    )
            submap[group_name].listeners = new_list
            if len(new_list) == 0:
                submap.pop(group_name, None)

    async def deregister_listeners_from_group(self, group_name: Union[int, str]):
        """
        Unsubscribes all listeners to messages sent to a particular group.
        :param group_name: --
        """
        async with self._lock:
            submap = self._channel_map[ChannelTargetType.GROUP]
            if submap.get(group_name) is None:
                return
            submap[group_name].listeners = []
            self._logger.info(f"Deregistered all listeners from group {group_name}")
            submap.pop(group_name, None)

    @overload
    def send_message_sync(
        self,
        message: Union[SyncMessage, Dict],
        message_type: Literal[""] = "",
        id: Literal[-1] = -1,
        source_id: Literal[None] = None,
        destination_id: Literal[None] = None,
        group_id: Literal[None] = None,
        content: Literal[None] = None,
    ) -> Any:
        pass

    @overload
    def send_message_sync(
        self,
        message: Literal[None] = None,
        message_type: str = "",
        id: int = -1,
        source_id: Union[str, int, None] = None,
        destination_id: Union[str, int, None] = None,
        group_id: Union[str, int, None] = None,
        content: Any = None,
    ) -> Any:
        pass

    def send_message_sync(
        self,
        message: Union[SyncMessage, Dict, None] = None,
        message_type: str = "",
        id: int = -1,
        source_id: Union[str, int, None] = None,
        destination_id: Union[str, int, None] = None,
        group_id: Union[str, int, None] = None,
        content: Any = None,
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
        return self._send_message_sync_impl(
            message=message,
            message_type=message_type,
            id=id,
            source_id=source_id,
            destination_id=destination_id,
            group_id=group_id,
            synchronous=True,
            content=content,
        )

    @overload
    async def dispatch_message(
        self,
        message: Union[AsyncMessage, Dict],
        message_type: Literal[""] = "",
        id: Literal[-1] = -1,
        source_id: Literal[None] = None,
        destination_id: Literal[None] = None,
        group_id: Literal[None] = None,
        response_required: Literal[False] = False,
        is_response: Literal[False] = False,
        is_blocking: Literal[False] = False,
        timeout: Optional[float] = None,
        content: Literal[None] = None,
    ) -> Any:
        pass

    @overload
    async def dispatch_message(
        self,
        message: Literal[None] = None,
        message_type: str = "",
        id: int = -1,
        source_id: Union[str, int, None] = None,
        destination_id: Union[str, int, None] = None,
        group_id: Union[str, int, None] = None,
        response_required: bool = False,
        is_response: bool = False,
        is_blocking: bool = False,
        timeout: Optional[float] = None,
        content: Any = None,
    ) -> Any:
        pass

    async def dispatch_message(
        self,
        message: Union[AsyncMessage, Dict, None] = None,
        message_type: str = "",
        id: int = -1,
        source_id: Union[str, int, None] = None,
        destination_id: Union[str, int, None] = None,
        group_id: Union[str, int, None] = None,
        response_required: bool = False,
        is_response: bool = False,
        is_blocking: bool = False,
        timeout: Optional[float] = None,
        content: Any = None,
    ) -> Any:
        """
        Sends a message to interested listeners asynchronously. To avoid blocking, doesn't send
        right away, but queues for later.

        :param message: a complete Message object or Dict version of. If given, all other params
            will be ignored. If not given, other params will be applied in building a new
            message.
        :param message_type: type of message, e.g. "GetUserInfo" or "MousePress"
        :param id: unique ID to give to message. If -1, ID will be assigned automatically.
            Replies will use this same ID.
        :param source_id: source "address" of message. Generally, the same as a destination ID.
        :param destination_id: destination "address" of message
        :param group_id: if given, specifies group of listeners to send message copies to
        :param response_required: if True, a response must be given.
            TODO: more notes
        :param is_response: if True, this message is a response to another. It must be given
            same ID as message to which it is replying. Not necessary to specify a destination
            ID.
        :param is_blocking: if True, no other message will be sent to the recipient until
            a reply comes back. However, subsequent messages are still queued and will be
            dispatched when their turn comes.
        :param timeout: if given, specifies how long to wait for response
        :param content: message content object. Should be cast-able to dict.
        :return: reply from other side, or None, if not given
        :raise TimeoutError: if timeout expires
        :raise: any exception encountered
        """
        return await self._queue_message_impl(
            message=message,
            message_type=message_type,
            id=id,
            source_id=source_id,
            destination_id=destination_id,
            group_id=group_id,
            response_required=response_required,
            is_response=is_response,
            is_blocking=is_blocking,
            synchronous=False,
            content=content,
            timeout=timeout,
        )

    async def _run_loop(self):
        """
        Main workhorse function, runs continuously. Sends queued outgoing messages, and deals with
        futures/tasks waiting for message reply.

        Notes on potentially confusing handling of messages and responses (applies to async
        messages only):

        Order of events
        1. Message arrives in _queue_message_impl(). A reply event is created (triggered when reply
            comes in). The message goes onto an outgoing queue in appropriate channel.
        2. _queue_message_impl() then waits for the reply event to be triggered. When that happens,
            reply is returned to the caller. In the meantime, Step 3 applies.
        3. Message is eventually pulled from outgoing queue and given to _send_message_impl(),
            which creates a task or future to await reply. In the meantime, Step 4 applies.
        4. When task or future completes (checked in this function), results are stored and reply
            event is triggered.
        """
        self._logger.info("In dispatcher run loop.")
        while not self._shutdown:
            async with self._lock:
                # Is there an outgoing message on some channel's queue? If so, send
                for k, submap in self._channel_map.items():
                    for _id, channel in submap.items():
                        if channel.currently_blocking:
                            # Don't get anything from queue until blocking is done
                            pass
                        elif not channel.outgoing_queue.empty():
                            message: AsyncMessage = await channel.outgoing_queue.get()
                            # Should we block channel until reply comes back?
                            channel.blocking_id = (
                                message.get_dispatcher_target_id()
                                if message.is_blocking
                                else None
                            )
                            if channel.blocking_id is not None:
                                self._blocking_message_id_to_channel[
                                    message.id
                                ] = channel
                            self._logger.debug(
                                f"Dispatching message from queue: {message.get_info()}"
                            )
                            # Reply won't be awaited directly; a task will be created to wait
                            # If message is itself a reply, then there won't be any waiting for a reply to it
                            await self._send_message_impl(message)

                # Deal with any replies to single destination messages that have come back
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

                        handled = self._handle_message_reply(msg_id, result)
                        if handled:
                            remove_ids.append(msg_id)

                for msg_id in remove_ids:
                    try:
                        self._reply_task_table.pop(msg_id)
                    except KeyError:
                        pass

                # Deal with any group replies. We only care about exceptions; other replies are ignored,
                # including DeferredResponse
                remove_ids = []
                for msg_id, future in self._group_reply_future_table.items():
                    if future.done():
                        self._logger.debug(
                            f"Got responses to group message with ID {msg_id}"
                        )
                        # Overall result of future; will either be None or first exception encountered
                        result = None
                        reply_list = await future
                        for reply in reply_list:
                            if isinstance(reply, Exception):
                                result = reply
                                break

                        handled = self._handle_group_message_replies(msg_id, result)
                        if handled:
                            remove_ids.append(msg_id)

                for msg_id in remove_ids:
                    try:
                        self._group_reply_future_table.pop(msg_id)
                    except KeyError:
                        pass
                    except Exception as ex:
                        self._logger.exception(f"Unknown exception {ex}")

            await asyncio.sleep(0.00001)

    def _handle_message_reply(self, msg_id: int, reply: Any) -> bool:
        """
        Helper function called when message receives a reply. Returns true if some task
        is waiting on reply and will now get it.
        """
        if type(reply) is DeferredResponse:
            return False

        if not self._reply_event_table.get(msg_id):
            return False

        self._reply_table[msg_id] = reply
        self._reply_event_table[msg_id].set()

        # Unblock channel if it was being blocked
        channel = self._blocking_message_id_to_channel.get(msg_id)
        if channel:
            channel.blocking_id = None
            self._blocking_message_id_to_channel.pop(msg_id, None)
        return True

    def _handle_group_message_replies(self, msg_id: int, result: Any) -> bool:
        """
        Helper function called when a collection of group messages have received all
        their replies. Returns true if some future is waiting on replies and will now
        be able to be done.
        """
        if not self._group_reply_event_table.get(msg_id):
            # Nobody is waiting to find out about outcome
            return False

        self._group_reply_table[msg_id] = result
        self._group_reply_event_table[msg_id].set()

        # Unblock channel if it was being blocked
        channel = self._blocking_message_id_to_channel.get(msg_id)
        if channel:
            channel.blocking_id = None
            self._blocking_message_id_to_channel.pop(msg_id, None)

        return True

    def _send_message_sync_impl(self, *args, **kwargs):
        """
        Workhorse function for sending synchronous message and dealing with reply.
        """
        message = self._get_or_make_message(args, kwargs)
        # TODO: special stuff if have to go another dispatcher
        if message.destination_id is not None:
            submap = self._channel_map[ChannelTargetType.SINGLE_DESTINATION]
            channel = submap.get(message.destination_id)
            if channel:
                return channel.listeners[0].handle_message_sync(message)
            else:
                raise DispatcherError(
                    f"No listener registered for destination ID {message.destination_id}"
                )
        elif message.group_id is not None:
            submap = self._channel_map[ChannelTargetType.GROUP]
            channel = submap.get(message.group_id)
            if channel:
                for listener in channel.listeners:
                    listener.handle_message_sync(message)
            return None
        else:
            raise DispatcherError(f"Message has no destination or group ID")

    async def _queue_message_impl(self, *args, **kwargs):
        """
        Workhorse function for queuing asynchronous message and waiting for eventual reply.
        """
        timeout = kwargs.get("timeout", None)

        message = self._get_or_make_message(args, kwargs)
        msg_id = message.id
        is_group_msg = (message.destination_id is None) and (
            message.group_id is not None
        )
        is_reply_msg = message.is_response
        async with self._lock:
            # The sender might or might not want a reply, but has to be informed about exceptions.
            # Create an event to signal when reply comes in.
            reply_event = asyncio.Event()
            if is_reply_msg:
                submap = self._channel_map[ChannelTargetType.REPLY]
                id_to_use = 0
                msg_type = "Reply"
                # We don't want a reply to a reply
                reply_event = None
            elif is_group_msg:
                if message.response_required:
                    self._group_reply_event_table[message.id] = reply_event
                else:
                    # Sender doesn't care about reply -- no need to wait for one
                    reply_event = None
                submap = self._channel_map[ChannelTargetType.GROUP]
                id_to_use = message.group_id
                msg_type = "Group"
            else:
                if message.response_required:
                    self._reply_event_table[message.id] = reply_event
                else:
                    # Sender doesn't care about reply -- no need to wait for one
                    reply_event = None
                submap = self._channel_map[ChannelTargetType.SINGLE_DESTINATION]
                id_to_use = message.destination_id
                msg_type = "Single Destination"

            self._logger.debug(f"Queueing outgoing message {message.get_info()}")

            channel = submap.get(id_to_use)
            if channel:
                await channel.outgoing_queue.put(message)
            else:
                raise DispatcherError(
                    f"Nowhere to send message of type: {msg_type}, id: {id_to_use}"
                )

        # Now, we wait on event
        if reply_event:
            try:
                await asyncio.wait_for(reply_event.wait(), timeout=timeout)
            except TimeoutError as te:
                self._logger.warning(f"Timed out for message {message.id}")
                raise te
            async with self._lock:
                if is_group_msg:
                    self._logger.debug(
                        f"Received replies to group message {message.id}"
                    )
                    self._group_reply_event_table.pop(msg_id, None)
                    result = self._group_reply_table.pop(msg_id, None)
                else:
                    self._logger.debug(f"Received reply to message {message.id}")
                    self._reply_event_table.pop(msg_id, None)
                    result = self._reply_table.pop(msg_id, None)
                if isinstance(result, Exception):
                    raise result
                return result

    async def _send_message_impl(self, message: AsyncMessage):
        """
        Workhorse function for sending queued asynchronous messages. Doesn't wait for replies directly,
        but creates asyncio tasks to wait. To only be called from behind lock.
        """
        # TODO: special stuff if have to go another dispatcher

        if message.is_response:
            # This is a response to some sent message, so alert whoever's waiting on a reply
            # (Waiting in _queue_message_impl())
            handled = self._handle_message_reply(message.id, message.content)
            if handled:
                self._reply_task_table.pop(message.id)
        elif message.destination_id is not None:
            submap = self._channel_map[ChannelTargetType.SINGLE_DESTINATION]
            channel = submap.get(message.destination_id)
            if channel:
                # Create a task to wait for response, self._run_loop() will deal with eventual reply
                reply_task = asyncio.create_task(
                    channel.listeners[0].handle_message(message)
                )
                self._reply_task_table[message.id] = reply_task
            else:
                raise DispatcherError(
                    f"No listener registered for destination ID {message.destination_id}"
                )
        elif message.group_id is not None:
            submap = self._channel_map[ChannelTargetType.GROUP]
            channel = submap.get(message.group_id)
            if channel:
                reply_tasks = []
                listener_list = channel.listeners
                for listener in listener_list:
                    reply_task = asyncio.create_task(listener.handle_message(message))
                    reply_tasks.append(reply_task)
                group_future = asyncio.gather(*reply_tasks, return_exceptions=True)
                self._group_reply_future_table[message.id] = group_future
        else:
            raise DispatcherError(f"Message has no destination or group ID")

    def _get_or_make_message(self, args, kwargs):
        # Helper function
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
            kwargs.pop("synchronous", None)
            if is_sync:
                message = SyncMessage(**kwargs)
            else:
                message = AsyncMessage(**kwargs)
        if message.id == -1 and not is_sync:
            message.id = self._next_message_id
            self._next_message_id += 1
        return message
