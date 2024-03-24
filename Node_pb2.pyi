from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class voteRequest(_message.Message):
    __slots__ = ("c_term", "c_id", "c_log_len", "c_log_term")
    C_TERM_FIELD_NUMBER: _ClassVar[int]
    C_ID_FIELD_NUMBER: _ClassVar[int]
    C_LOG_LEN_FIELD_NUMBER: _ClassVar[int]
    C_LOG_TERM_FIELD_NUMBER: _ClassVar[int]
    c_term: int
    c_id: int
    c_log_len: int
    c_log_term: int
    def __init__(self, c_term: _Optional[int] = ..., c_id: _Optional[int] = ..., c_log_len: _Optional[int] = ..., c_log_term: _Optional[int] = ...) -> None: ...

class voteResponse(_message.Message):
    __slots__ = ("term", "granted", "node_id")
    TERM_FIELD_NUMBER: _ClassVar[int]
    GRANTED_FIELD_NUMBER: _ClassVar[int]
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    term: int
    granted: bool
    node_id: int
    def __init__(self, term: _Optional[int] = ..., granted: bool = ..., node_id: _Optional[int] = ...) -> None: ...

class logRequest(_message.Message):
    __slots__ = ("l_id", "c_term", "pref_len", "pref_term", "suffix", "l_commit")
    L_ID_FIELD_NUMBER: _ClassVar[int]
    C_TERM_FIELD_NUMBER: _ClassVar[int]
    PREF_LEN_FIELD_NUMBER: _ClassVar[int]
    PREF_TERM_FIELD_NUMBER: _ClassVar[int]
    SUFFIX_FIELD_NUMBER: _ClassVar[int]
    L_COMMIT_FIELD_NUMBER: _ClassVar[int]
    l_id: int
    c_term: int
    pref_len: int
    pref_term: int
    suffix: _containers.RepeatedScalarFieldContainer[str]
    l_commit: int
    def __init__(self, l_id: _Optional[int] = ..., c_term: _Optional[int] = ..., pref_len: _Optional[int] = ..., pref_term: _Optional[int] = ..., suffix: _Optional[_Iterable[str]] = ..., l_commit: _Optional[int] = ...) -> None: ...

class logResponse(_message.Message):
    __slots__ = ("f_id", "term", "ack", "sucess")
    F_ID_FIELD_NUMBER: _ClassVar[int]
    TERM_FIELD_NUMBER: _ClassVar[int]
    ACK_FIELD_NUMBER: _ClassVar[int]
    SUCESS_FIELD_NUMBER: _ClassVar[int]
    f_id: int
    term: int
    ack: int
    sucess: bool
    def __init__(self, f_id: _Optional[int] = ..., term: _Optional[int] = ..., ack: _Optional[int] = ..., sucess: bool = ...) -> None: ...
