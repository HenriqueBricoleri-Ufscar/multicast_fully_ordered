from dataclasses import dataclass

@dataclass(frozen=True)
class DataMessage:
    msg_id: str
    timestamp: int
    sender: str
    content: str


@dataclass(frozen=True)
class AckMessage:
    msg_id: str
    timestamp: int
    sender: str


type Packet = DataMessage | AckMessage
