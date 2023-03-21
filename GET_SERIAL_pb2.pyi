from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class PlaceHolder(_message.Message):
    __slots__ = ["place_holder"]
    PLACE_HOLDER_FIELD_NUMBER: _ClassVar[int]
    place_holder: str
    def __init__(self, place_holder: _Optional[str] = ...) -> None: ...

class SerialNumber(_message.Message):
    __slots__ = ["serial_no"]
    SERIAL_NO_FIELD_NUMBER: _ClassVar[int]
    serial_no: str
    def __init__(self, serial_no: _Optional[str] = ...) -> None: ...
