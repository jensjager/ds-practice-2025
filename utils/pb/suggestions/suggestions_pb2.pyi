from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class VectorClock(_message.Message):
    __slots__ = ("transaction_verification", "fraud_detection", "suggestions")
    TRANSACTION_VERIFICATION_FIELD_NUMBER: _ClassVar[int]
    FRAUD_DETECTION_FIELD_NUMBER: _ClassVar[int]
    SUGGESTIONS_FIELD_NUMBER: _ClassVar[int]
    transaction_verification: int
    fraud_detection: int
    suggestions: int
    def __init__(self, transaction_verification: _Optional[int] = ..., fraud_detection: _Optional[int] = ..., suggestions: _Optional[int] = ...) -> None: ...

class OrderInitRequest(_message.Message):
    __slots__ = ("order_id", "order_json", "vector_clock")
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    ORDER_JSON_FIELD_NUMBER: _ClassVar[int]
    VECTOR_CLOCK_FIELD_NUMBER: _ClassVar[int]
    order_id: str
    order_json: str
    vector_clock: VectorClock
    def __init__(self, order_id: _Optional[str] = ..., order_json: _Optional[str] = ..., vector_clock: _Optional[_Union[VectorClock, _Mapping]] = ...) -> None: ...

class OrderInitResponse(_message.Message):
    __slots__ = ("success", "reason", "vector_clock")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    VECTOR_CLOCK_FIELD_NUMBER: _ClassVar[int]
    success: bool
    reason: str
    vector_clock: VectorClock
    def __init__(self, success: bool = ..., reason: _Optional[str] = ..., vector_clock: _Optional[_Union[VectorClock, _Mapping]] = ...) -> None: ...

class EventRequest(_message.Message):
    __slots__ = ("order_id", "vector_clock")
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    VECTOR_CLOCK_FIELD_NUMBER: _ClassVar[int]
    order_id: str
    vector_clock: VectorClock
    def __init__(self, order_id: _Optional[str] = ..., vector_clock: _Optional[_Union[VectorClock, _Mapping]] = ...) -> None: ...

class EventResponse(_message.Message):
    __slots__ = ("success", "reason", "event_name", "vector_clock")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    EVENT_NAME_FIELD_NUMBER: _ClassVar[int]
    VECTOR_CLOCK_FIELD_NUMBER: _ClassVar[int]
    success: bool
    reason: str
    event_name: str
    vector_clock: VectorClock
    def __init__(self, success: bool = ..., reason: _Optional[str] = ..., event_name: _Optional[str] = ..., vector_clock: _Optional[_Union[VectorClock, _Mapping]] = ...) -> None: ...

class SuggestedBook(_message.Message):
    __slots__ = ("book_id", "title", "author")
    BOOK_ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    AUTHOR_FIELD_NUMBER: _ClassVar[int]
    book_id: str
    title: str
    author: str
    def __init__(self, book_id: _Optional[str] = ..., title: _Optional[str] = ..., author: _Optional[str] = ...) -> None: ...

class SuggestionsEventResponse(_message.Message):
    __slots__ = ("success", "reason", "event_name", "vector_clock", "books")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    EVENT_NAME_FIELD_NUMBER: _ClassVar[int]
    VECTOR_CLOCK_FIELD_NUMBER: _ClassVar[int]
    BOOKS_FIELD_NUMBER: _ClassVar[int]
    success: bool
    reason: str
    event_name: str
    vector_clock: VectorClock
    books: _containers.RepeatedCompositeFieldContainer[SuggestedBook]
    def __init__(self, success: bool = ..., reason: _Optional[str] = ..., event_name: _Optional[str] = ..., vector_clock: _Optional[_Union[VectorClock, _Mapping]] = ..., books: _Optional[_Iterable[_Union[SuggestedBook, _Mapping]]] = ...) -> None: ...

class ClearOrderRequest(_message.Message):
    __slots__ = ("order_id", "final_vector_clock")
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    FINAL_VECTOR_CLOCK_FIELD_NUMBER: _ClassVar[int]
    order_id: str
    final_vector_clock: VectorClock
    def __init__(self, order_id: _Optional[str] = ..., final_vector_clock: _Optional[_Union[VectorClock, _Mapping]] = ...) -> None: ...

class ClearOrderResponse(_message.Message):
    __slots__ = ("success", "reason", "vector_clock")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    VECTOR_CLOCK_FIELD_NUMBER: _ClassVar[int]
    success: bool
    reason: str
    vector_clock: VectorClock
    def __init__(self, success: bool = ..., reason: _Optional[str] = ..., vector_clock: _Optional[_Union[VectorClock, _Mapping]] = ...) -> None: ...
