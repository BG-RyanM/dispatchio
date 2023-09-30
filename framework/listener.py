from typing import List, Dict, Set, Tuple, Union, Optional, Callable, Awaitable, Any
from enum import IntEnum
from asyncio import Lock, Queue

from framework.message import Message


class FilterTargetType(IntEnum):
    CALLBACK = 0
    ASYNC_CALLBACK = 1
    QUEUE = 2
    LISTENER = 3
    NONE = 4


class FilterTarget:

    def __init__(self, type: FilterTargetType, target: Union[Callable[[Any], Any], Callable[[Any], Awaitable[Any]], None]):
        self._type: FilterTargetType = type
        self._target: Union[Callable[[Any], Any], Callable[[Any], Awaitable[Any]], None] = target

    @property
    def target(self):
        return self._target

    @property
    def type(self):
        return self._type


class FilterEntry:

    def __init__(self, field_matches: Dict, custom_field_names: Set, filter_target: FilterTarget, priority: int):
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

    def test(self, message: Dict):
        matched_fields = set()
        for field, value in message:
            if field in self._field_matches.keys() and field not in self._custom_field_names:
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

    def execute_sync(self, message: Dict) -> Tuple[FilterTargetType, Any]:
        if self._filter_target.type == FilterTargetType.CALLBACK:
            # Return result of call
            pass
        else:
            # Must raise exception
            pass
        return self._filter_target.type, None

    async def execute(self, message: Dict) -> Tuple[FilterTargetType, Any]:
        if self._filter_target.type == FilterTargetType.CALLBACK:
            # Return result of call
            pass
        elif self._filter_target.type == FilterTargetType.ASYNC_CALLBACK:
            # Raise exception if instant response required
            # Return nothing
            pass
        elif self._filter_target.type == FilterTargetType.QUEUE:
            # Raise exception if instant response required
            pass
        return self._filter_target.type, None

    def is_sync_friendly(self):
        return self._filter_target.type in [FilterTargetType.CALLBACK]

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

    def __init__(self):
        self._entries: List[FilterEntry] = []
        # Maps filter entry name (if any) to FilterEntry
        self._name_lookup: Dict[str, FilterEntry] = {}

    def add_entry(self, field_matches: Dict[str, Any],
                  custom_field_matches: Optional[Dict[str, Any]] = None,
                  callback: Optional[Callable[[Any], Any]] = None,
                  async_callback: Optional[Callable[[Any], Awaitable[Any]]] = None,
                  priority = -1,
                  name: Optional[str] = None):
        """
        Adds an entry to table
        :param field_matches: Dict mapping field name to matches. A match can be a list or a single item.
            If a match is None, it counts as a wildcard matching any value of field.
        :param custom_field_matches: Same idea as above, but are custom fields in message content
        :param callback: callback to run
        :param async_callback: async callback to run
        :param priority: priority to assign to new filter, or -1 to assign automatically. If value is
            higher than highest value (lowest priority) in table, exception will be raised.
        :param name: optional name to give to entry
        :return: None
        """
        match_dict = {}
        custom_field_names = set()
        for f_name, matches in field_matches:
            match_dict[f_name] = matches
        if custom_field_matches:
            for f_name, matches in custom_field_matches:
                match_dict[f_name] = matches
                custom_field_names.add(f_name)
        target_type = FilterTargetType.CALLBACK if callback else \
            (FilterTargetType.ASYNC_CALLBACK if async_callback else FilterTargetType.QUEUE)
        target_callback = callback if callback else (async_callback if async_callback else None)
        filter_target = FilterTarget(target_type, target_callback)
        filter_entry = FilterEntry(match_dict, custom_field_names, filter_target, -1)
        if name:
            filter_entry.name = name
            if self._name_lookup.get(name):
                # TODO: raise exception
                pass
            self._name_lookup[name] = filter_entry
        if priority == -1 or priority == len(self._entries):
            filter_entry.priority = len(self._entries)
            self._entries.append(filter_entry)
        elif priority < 0 or priority > len(self._entries):
            # TODO: raise exception
            pass
        else:
            self._entries.insert(priority, filter_entry)
            for i in range(len(self._entries)):
                self._entries[i].priority = i

    def handle_message_sync(self, message: Union[Message, Dict]) -> Tuple[FilterTargetType, Any]:
        if isinstance(message, Message):
            message = message.to_dict()
        for filter_entry in self._entries:
            if filter_entry.test(message):
                filter_type, result = filter_entry.execute_sync(message)
                return filter_type, result
        return FilterTargetType.NONE, None

    async def handle_message(self, message: Union[Message, Dict]) -> Tuple[FilterTargetType, Any]:
        if isinstance(message, Message):
            message = message.to_dict()
        for filter_entry in self._entries:
            if filter_entry.test(message):
                filter_type, result = await filter_entry.execute(message)
                return filter_type, result
        return FilterTargetType.NONE, None


class MessageListener:

    def __init__(self):
        self._table = FilteringTable()
        self._lock = Lock()
        self._queue = Queue()

    def handle_message_sync(self, message: Message):
        # For messages that require an instant response
        pass

    async def handle_message(self, message: Message):
        with self._lock:
            # If there's an instant reply, return that to dispatcher
            pass