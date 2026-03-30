from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class EnqueueRequest(_message.Message):
    __slots__ = ("order_id", "order_json")
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    ORDER_JSON_FIELD_NUMBER: _ClassVar[int]
    order_id: str
    order_json: str
    def __init__(self, order_id: _Optional[str] = ..., order_json: _Optional[str] = ...) -> None: ...

class EnqueueResponse(_message.Message):
    __slots__ = ("success", "reason", "queue_size")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    QUEUE_SIZE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    reason: str
    queue_size: int
    def __init__(self, success: bool = ..., reason: _Optional[str] = ..., queue_size: _Optional[int] = ...) -> None: ...

class DequeueRequest(_message.Message):
    __slots__ = ("executor_id",)
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    executor_id: str
    def __init__(self, executor_id: _Optional[str] = ...) -> None: ...

class DequeueResponse(_message.Message):
    __slots__ = ("success", "reason", "has_order", "order_id", "order_json", "queue_size")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    HAS_ORDER_FIELD_NUMBER: _ClassVar[int]
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    ORDER_JSON_FIELD_NUMBER: _ClassVar[int]
    QUEUE_SIZE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    reason: str
    has_order: bool
    order_id: str
    order_json: str
    queue_size: int
    def __init__(self, success: bool = ..., reason: _Optional[str] = ..., has_order: bool = ..., order_id: _Optional[str] = ..., order_json: _Optional[str] = ..., queue_size: _Optional[int] = ...) -> None: ...

class QueueStatusRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class QueueStatusResponse(_message.Message):
    __slots__ = ("queue_size",)
    QUEUE_SIZE_FIELD_NUMBER: _ClassVar[int]
    queue_size: int
    def __init__(self, queue_size: _Optional[int] = ...) -> None: ...
