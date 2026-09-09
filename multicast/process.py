from __future__ import annotations

import heapq
from collections import defaultdict
from collections.abc import Iterable
from typing import TYPE_CHECKING

from .messages import AckMessage, DataMessage, Packet

if TYPE_CHECKING:
    from .network import Network


type QueueEntry = tuple[int, str, str]


class Process:
    def __init__(
        self,
        pid: str,
        members: Iterable[str],
        network: Network,
    ) -> None:
        self.pid = pid
        self.members = frozenset(members)
        self.network = network

        if pid not in self.members:
            raise ValueError(f"Process {pid!r} is not a multicast group member")

        self.clock = 0
        self.sequence = 0

        # Heap de mensagens DATA, ordenado por timestamp, remetente e msg_id.
        self.queue: list[QueueEntry] = []

        # msg_id -> DataMessage
        self.messages: dict[str, DataMessage] = {}

        # msg_id -> processos que confirmaram o recebimento
        self.acks: defaultdict[str, set[str]] = defaultdict(set)

        self.delivered: list[str] = []
        self._delivered_ids: set[str] = set()

    # Relógio lógico de Lamport
    def tick(self) -> int:
        self.clock += 1
        return self.clock

    def receive_tick(self, received_timestamp: int) -> int:
        self.clock = max(self.clock, received_timestamp) + 1
        return self.clock

    # Envio
    def multicast(self, content: str) -> str:
        timestamp = self.tick()
        self.sequence += 1

        msg_id = f"{self.pid}:{self.sequence}"

        message = DataMessage(
            msg_id=msg_id,
            timestamp=timestamp,
            sender=self.pid,
            content=content,
        )

        print(
            f"[t={self.network.time:02d}] {self.pid} "
            f"MULTICAST {msg_id} timestamp={timestamp}"
        )

        self.network.multicast(self.pid, message)

        return msg_id

    # Recebimento
    def receive(self, packet: Packet) -> None:
        if isinstance(packet, DataMessage):
            self._receive_data(packet)
        elif isinstance(packet, AckMessage):
            self._receive_ack(packet)

    def _receive_data(self, message: DataMessage) -> None:
        self.receive_tick(message.timestamp)

        print(
            f"[t={self.network.time:02d}] {self.pid} received "
            f"DATA {message.msg_id} from {message.sender}"
        )

        if (
            message.msg_id not in self.messages
            and message.msg_id not in self._delivered_ids
        ):
            self.messages[message.msg_id] = message

            heapq.heappush(
                self.queue,
                (
                    message.timestamp,
                    message.sender,
                    message.msg_id,
                ),
            )

        ack_timestamp = self.tick()

        ack = AckMessage(
            msg_id=message.msg_id,
            timestamp=ack_timestamp,
            sender=self.pid,
        )

        self.network.multicast(self.pid, ack)
        self._try_deliver()

    def _receive_ack(self, ack: AckMessage) -> None:
        self.receive_tick(ack.timestamp)

        if ack.msg_id not in self._delivered_ids:
            self.acks[ack.msg_id].add(ack.sender)

        if ack.msg_id in self._delivered_ids:
            extra = "[already delivered]"
        elif ack.msg_id not in self.messages:
            extra = "[DATA not received]"
        else:
            extra = ""

        print(
            f"[t={self.network.time:02d}] {self.pid} received "
            f"ACK {ack.msg_id} from {ack.sender} {extra}"
        )

        self._try_deliver()

    # Entrega para a aplicação
    def _try_deliver(self) -> None:
        while self.queue:
            _, _, msg_id = self.queue[0]
            received_acks = self.acks[msg_id]

            if not self.members.issubset(received_acks):
                break

            heapq.heappop(self.queue)
            message = self.messages.pop(msg_id)
            self.acks.pop(msg_id, None)
            self._delivered_ids.add(msg_id)
            self.delivered.append(msg_id)

            print(
                f">>> {self.pid} DELIVERS "
                f"{message.msg_id}: {message.content!r}\n"
            )
