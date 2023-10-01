from typing import List, Optional, Union, Any, Dict


class Message:

    def __init__(self, message_type: str = "",
                 id: int = -1,
                 source_id: Union[str, int, None] = None,
                 destination_id: Union[str, int, None] = None,
                 group_id: Union[str, int, None] = None,
                 response_required: bool = False,
                 is_response: bool = False,
                 synchronous: bool = False,
                 content: Any = None):
        self.message_type: str = message_type
        self.id: int = id
        self.source_id: Union[str, int, None] = source_id
        self.destination_id: Union[str, int, None] = destination_id
        self.group_id: Union[str, int, None] = group_id
        self.response_required = response_required
        self.is_response = is_response
        self.synchronous = synchronous
        self.content = content

    def to_dict(self):
        return {"message_type": self.message_type,
                "id": self.id,
                "source_id": self.source_id,
                "destination_id": self.destination_id,
                "group_id": self.group_id,
                "response_required": self.response_required,
                "is_response": self.is_response,
                "synchronous": self.synchronous,
                "content": self.content
                }

    @staticmethod
    def from_dict(self, the_dict: Dict[str, Any]):
        return Message(**the_dict)