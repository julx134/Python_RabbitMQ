from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class Acknowledgement(_message.Message):
    __slots__ = ["ack"]
    ACK_FIELD_NUMBER: _ClassVar[int]
    ack: str
    def __init__(self, ack: _Optional[str] = ...) -> None: ...

class Pin(_message.Message):
    __slots__ = ["pin"]
    PIN_FIELD_NUMBER: _ClassVar[int]
    pin: str
    def __init__(self, pin: _Optional[str] = ...) -> None: ...
