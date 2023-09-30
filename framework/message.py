from typing import List, Optional, Union


class Message:

    def __init__(self):
        self.message_type: str = ""
        self.id: int = -1
        self.source_id: Union[str, int, None] = None
        self.destination_id: Union[str, int, None] = None
        self.group_id: Union[str, int, None] = None
        self.response_required = False
        self.is_response = False
        self.synchronous = False

    def to_dict(self):
        return {"message_type": self.message_type,
                "id": self.id,
                "source_id": self.source_id,
                "destination_id": self.destination_id,
                "group_id": self.group_id,
                "response_required": self.response_required,
                "is_response": self.is_response,
                "synchronous": self.synchronous
                }