from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class Commands(_message.Message):
    __slots__ = ["commands"]
    COMMANDS_FIELD_NUMBER: _ClassVar[int]
    commands: str
    def __init__(self, commands: _Optional[str] = ...) -> None: ...

class RoverNum(_message.Message):
    __slots__ = ["rover_name"]
    ROVER_NAME_FIELD_NUMBER: _ClassVar[int]
    rover_name: str
    def __init__(self, rover_name: _Optional[str] = ...) -> None: ...
