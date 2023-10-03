from typing import List, Dict, Set, Tuple, Union, Optional, Callable, Awaitable, Any
from enum import IntEnum
from asyncio import Lock, Queue
import logging

from framework.listener import MessageListener
from framework.message import Message, SyncMessage, AsyncMessage
from framework.exceptions import ListenerError

_logger = logging.getLogger(__name__)


class FilterTargetType(IntEnum):
    CALLBACK = 0
    ASYNC_CALLBACK = 1
    QUEUE = 2
    LISTENER = 3
    NONE = 4


class FilterTarget:
    def __init__(
        self,
        type: FilterTargetType,
        target: Union[Callable[[Any], Any], Callable[[Any], Awaitable[Any]], None],
    ):
        self._type: FilterTargetType = type
        self._target: Union[
            Callable[[Any], Any], Callable[[Any], Awaitable[Any]], None
        ] = target

    @property
    def target(self):
        return self._target

    @property
    def type(self):
        return self._type


class FilterEntry:
    """Defines single row in filtering table. See FilteringTable."""

    def __init__(
        self,
        field_matches: Dict,
        custom_field_names: Set,
        filter_target: FilterTarget,
        priority: int,
    ):
        # Will be a dict mapping field names to list of matches. Applies to main message fields only.
        self._field_matches: Dict = field_matches
        # Will name any fields that are custom-only, apply to actual message content
        self._custom_field_names: Set = custom_field_names
        # Will describe what action to run if a match occurs
        self._filter_target: FilterTarget = filter_target
        # Order of entry in table
        self._priority = priority
        # Optional user-assigned filter name
        self._name: Optional[str] = None

    def test(self, message_obj: Union[SyncMessage, AsyncMessage]) -> bool:
        """
        Tests if message matches this filter entry, and it should be executed
        :param message: message as dict
        :return: True if match
        """
        message = message_obj.to_dict()
        matched_fields = set()
        for field, value in message.items():
            if (
                field in self._field_matches.keys()
                and field not in self._custom_field_names
            ):
                # Field being filtered for is in the message, so see if value is a match
                matches = self._field_matches[field]
                field_match = False
                if matches is None:
                    # This counts as a wildcard
                    field_match = True
                if isinstance(matches, list):
                    if value in matches:
                        field_match = True
                else:
                    if value == matches:
                        field_match = True
                if not field_match:
                    return False
                matched_fields.add(field)
        # TODO: add custom fields stuff
        return True

    def execute_sync(self, message: SyncMessage) -> Tuple[FilterTargetType, Any]:
        """
        Execute synchronous message handling pathway. Will raise exception if not available.

        :param message: --
        :return: tuple of (FilterTargetType, response from callback or None)
        :raise: any exception encountered or ListenerError
        """
        if self._filter_target.type == FilterTargetType.CALLBACK:
            return FilterTargetType.CALLBACK, self._filter_target.target(message)
        elif self._filter_target.type == FilterTargetType.ASYNC_CALLBACK:
            raise ListenerError(
                f"Cannot send synchronous message {message.to_dict()} to async callback"
            )
        elif self._filter_target.type == FilterTargetType.QUEUE:
            return FilterTargetType.QUEUE, None
        else:
            raise ListenerError(f"Cannot send synchronous message for some reason")

    async def execute(self, message: AsyncMessage) -> Tuple[FilterTargetType, Any]:
        """
        Execute asynchronous message handling pathway.

        :param message: --
        :return: tuple of (FilterTargetType, response from callback or None)
        :raise: any exception encountered or ListenerError
        """
        if self._filter_target.type == FilterTargetType.CALLBACK:
            return FilterTargetType.CALLBACK, self._filter_target.target(message)
        elif self._filter_target.type == FilterTargetType.ASYNC_CALLBACK:
            return FilterTargetType.ASYNC_CALLBACK, await self._filter_target.target(
                message
            )
        elif self._filter_target.type == FilterTargetType.QUEUE:
            pass
        return self._filter_target.type, None

    def is_sync_friendly(self):
        """Returns True if this FilterEntry can handle a synchronous message"""
        return self._filter_target.type in [
            FilterTargetType.CALLBACK,
            FilterTargetType.QUEUE,
        ]

    @property
    def priority(self):
        return self._priority

    @priority.setter
    def priority(self, val):
        self._priority = val

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, val):
        self._name = val


class FilteringTable:
    """
    See README for basic concept. The filtering table is used to decide what to do with a
    message based on matching rules applied to its fields.

    For example, we could use:
    {"message_type": ["X_Key", "Y_Key"]} to match messages whose type is "X_Key" or "Y_Key"

    Non-matching messages will pass by this filter to subsequent filters. Each entry specifies
    how to handle matching messages.
    """

    def __init__(self):
        self._entries: List[FilterEntry] = []
        # Maps filter entry name (if any) to FilterEntry
        self._name_lookup: Dict[str, FilterEntry] = {}
        # Applies to async messages only
        self._in_progress_message_count = 0

    def add_entry(
        self,
        field_matches: Dict[str, Any],
        custom_field_matches: Optional[Dict[str, Any]] = None,
        callback: Optional[Callable[[Any], Any]] = None,
        async_callback: Optional[Callable[[Any], Awaitable[Any]]] = None,
        priority=-1,
        name: Optional[str] = None,
    ):
        """
        Adds a filter entry to table.

        :param field_matches: Dict mapping field name to matches. A match can be a list or a single item.
        :param custom_field_matches: Same idea as above, but are custom fields in message content
            If a match is None, it counts as a wildcard matching any value of field.
        :param callback: callback to run
        :param async_callback: async callback to run
        :param priority: priority to assign to new filter, or -1 to assign automatically as lowest priority
            entry. If value is higher than highest value (lowest priority) in table, exception will be raised.
        :param name: optional name to give to entry
        :return: None
        """
        if self._in_progress_message_count > 0:
            raise ListenerError(
                "Can't add filtering table entry while messages being handled"
            )

        match_dict = {}
        custom_field_names = set()
        for f_name, matches in field_matches.items():
            match_dict[f_name] = matches
        if custom_field_matches:
            for f_name, matches in custom_field_matches.items():
                match_dict[f_name] = matches
                custom_field_names.add(f_name)
        target_type = (
            FilterTargetType.CALLBACK
            if callback
            else (
                FilterTargetType.ASYNC_CALLBACK
                if async_callback
                else FilterTargetType.QUEUE
            )
        )
        target_callback = (
            callback if callback else (async_callback if async_callback else None)
        )
        filter_target = FilterTarget(target_type, target_callback)
        filter_entry = FilterEntry(match_dict, custom_field_names, filter_target, -1)
        if name:
            filter_entry.name = name
            if self._name_lookup.get(name):
                raise ListenerError(f"Filter entry already exists with name {name}")
            self._name_lookup[name] = filter_entry
        if priority == -1 or priority == len(self._entries):
            filter_entry.priority = len(self._entries)
            self._entries.append(filter_entry)
        elif priority < 0 or priority > len(self._entries):
            raise ListenerError(f"Invalid filter entry priority {priority}")
        else:
            self._entries.insert(priority, filter_entry)
            for i in range(len(self._entries)):
                self._entries[i].priority = i

    def handle_message_sync(self, message: SyncMessage) -> Tuple[FilterTargetType, Any]:
        """
        Handles a message synchronously.
        :param message: --
        :return: (filter target type, result of action, if any)
        """
        for filter_entry in self._entries:
            if filter_entry.test(message):
                filter_type, result = filter_entry.execute_sync(message)
                return filter_type, result
        return FilterTargetType.NONE, None

    async def handle_message(
        self, message: AsyncMessage
    ) -> Tuple[FilterTargetType, Any]:
        """
        Handles a message asynchronously.
        :param message: --
        :return: (filter target type, result of action, if any)
        """
        for filter_entry in self._entries:
            if filter_entry.test(message):
                self._in_progress_message_count += 1
                filter_type, result = await filter_entry.execute(message)
                self._in_progress_message_count -= 1
                return filter_type, result
        return FilterTargetType.NONE, None


class BasicMessageListener(MessageListener):
    def __init__(self, id: Union[str, int, None] = None):
        super().__init__(id)
        self._table = FilteringTable()
        self._queue = Queue()

    def handle_message_sync(self, message: SyncMessage) -> Any:
        """
        Handles a message synchronously.
        :param message: --
        :return: response, if any
        """
        # For messages that require an instant response
        target_type, response = self._table.handle_message_sync(message)
        if target_type == FilterTargetType.QUEUE:
            self._queue.put_nowait(message)
        return response

    async def handle_message(self, message: AsyncMessage) -> Any:
        """
        Handles a message asynchronously.
        :param message: --
        :return: response, if any
        """
        # If there's an instant reply, return that to dispatcher/listener that called
        target_type, response = await self._table.handle_message(message)
        if target_type == FilterTargetType.QUEUE:
            await self._queue.put(message)
        elif target_type in [
            FilterTargetType.CALLBACK,
            FilterTargetType.ASYNC_CALLBACK,
        ]:
            return response
        return None

    @property
    def filtering_table(self):
        return self._table
