from typing import List, Optional, Union, Any, Dict
from abc import ABC, abstractmethod


class Message(ABC):
    def __init__(
        self,
        message_type: str = "",
        id: int = -1,
        source_id: Union[str, int, None] = None,
        destination_id: Union[str, int, None] = None,
        group_id: Union[str, int, None] = None,
        content: Any = None,
    ):
        self.message_type: str = message_type
        self.id: int = id
        self.source_id: Union[str, int, None] = source_id
        self.destination_id: Union[str, int, None] = destination_id
        self.group_id: Union[str, int, None] = group_id
        self.content = content
        self.synchronous = None

    @abstractmethod
    def to_dict(self):
        """Return copy as dict"""

    @staticmethod
    def make_copy(self):
        """Make a copy of Message"""

    def get_info(self):
        return (
            f"type: {self.message_type}, ID: {self.id}, source ID: {self.source_id}, "
            f"destination ID: {self.destination_id}, group ID: {self.group_id}"
        )

    def get_dispatcher_target_id(self):
        if self.destination_id is not None:
            return self.destination_id
        return self.group_id


class SyncMessage(Message):
    def __init__(
        self,
        message_type: str = "",
        id: int = -1,
        source_id: Union[str, int, None] = None,
        destination_id: Union[str, int, None] = None,
        group_id: Union[str, int, None] = None,
        content: Any = None,
    ):
        super().__init__(
            message_type=message_type,
            id=id,
            source_id=source_id,
            destination_id=destination_id,
            group_id=group_id,
            content=content,
        )
        self.synchronous = True

    def to_dict(self):
        return {
            "message_type": self.message_type,
            "id": self.id,
            "source_id": self.source_id,
            "destination_id": self.destination_id,
            "group_id": self.group_id,
            "content": self.content,
            "synchronous": self.synchronous,
        }

    @staticmethod
    def from_dict(self, the_dict: Dict[str, Any]):
        return SyncMessage(**the_dict)

    def make_copy(self):
        # TODO: copying content
        return SyncMessage(
            message_type=self.message_type,
            id=self.id,
            source_id=self.source_id,
            destination_id=self.destination_id,
            group_id=self.group_id,
        )


class AsyncMessage(Message):
    def __init__(
        self,
        message_type: str = "",
        id: int = -1,
        source_id: Union[str, int, None] = None,
        destination_id: Union[str, int, None] = None,
        group_id: Union[str, int, None] = None,
        response_required: bool = False,
        is_response: bool = False,
        is_blocking: bool = False,
        content: Any = None,
    ):
        super().__init__(
            message_type=message_type,
            id=id,
            source_id=source_id,
            destination_id=destination_id,
            group_id=group_id,
            content=content,
        )
        self.response_required = response_required
        self.is_response = is_response
        self.is_blocking = is_blocking
        self.synchronous = False

    def to_dict(self):
        return {
            "message_type": self.message_type,
            "id": self.id,
            "source_id": self.source_id,
            "destination_id": self.destination_id,
            "response_required": self.response_required,
            "is_response": self.is_response,
            "is_blocking": self.is_blocking,
            "group_id": self.group_id,
            "content": self.content,
            "synchronous": self.synchronous,
        }

    @staticmethod
    def from_dict(self, the_dict: Dict[str, Any]):
        return SyncMessage(**the_dict)

    def make_copy(self):
        # TODO: copying content
        return AsyncMessage(
            message_type=self.message_type,
            id=self.id,
            source_id=self.source_id,
            destination_id=self.destination_id,
            group_id=self.group_id,
            content=self.content,
            response_required=self.response_required,
            is_response=self.is_response,
            is_blocking=self.is_blocking,
        )
