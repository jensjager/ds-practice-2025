from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class PingRequest(_message.Message):
    __slots__ = ("from_executor_id",)
    FROM_EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    from_executor_id: int
    def __init__(self, from_executor_id: _Optional[int] = ...) -> None: ...

class PingResponse(_message.Message):
    __slots__ = ("alive", "executor_id", "leader_id")
    ALIVE_FIELD_NUMBER: _ClassVar[int]
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    alive: bool
    executor_id: int
    leader_id: int
    def __init__(self, alive: bool = ..., executor_id: _Optional[int] = ..., leader_id: _Optional[int] = ...) -> None: ...

class ElectionRequest(_message.Message):
    __slots__ = ("candidate_id",)
    CANDIDATE_ID_FIELD_NUMBER: _ClassVar[int]
    candidate_id: int
    def __init__(self, candidate_id: _Optional[int] = ...) -> None: ...

class ElectionResponse(_message.Message):
    __slots__ = ("alive", "responder_id", "reason")
    ALIVE_FIELD_NUMBER: _ClassVar[int]
    RESPONDER_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    alive: bool
    responder_id: int
    reason: str
    def __init__(self, alive: bool = ..., responder_id: _Optional[int] = ..., reason: _Optional[str] = ...) -> None: ...

class CoordinatorRequest(_message.Message):
    __slots__ = ("leader_id",)
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    leader_id: int
    def __init__(self, leader_id: _Optional[int] = ...) -> None: ...

class CoordinatorResponse(_message.Message):
    __slots__ = ("acknowledged", "executor_id")
    ACKNOWLEDGED_FIELD_NUMBER: _ClassVar[int]
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    acknowledged: bool
    executor_id: int
    def __init__(self, acknowledged: bool = ..., executor_id: _Optional[int] = ...) -> None: ...

class StatusRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class StatusResponse(_message.Message):
    __slots__ = ("executor_id", "leader_id", "is_leader", "election_in_progress", "known_peer_count")
    EXECUTOR_ID_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    IS_LEADER_FIELD_NUMBER: _ClassVar[int]
    ELECTION_IN_PROGRESS_FIELD_NUMBER: _ClassVar[int]
    KNOWN_PEER_COUNT_FIELD_NUMBER: _ClassVar[int]
    executor_id: int
    leader_id: int
    is_leader: bool
    election_in_progress: bool
    known_peer_count: int
    def __init__(self, executor_id: _Optional[int] = ..., leader_id: _Optional[int] = ..., is_leader: bool = ..., election_in_progress: bool = ..., known_peer_count: _Optional[int] = ...) -> None: ...
