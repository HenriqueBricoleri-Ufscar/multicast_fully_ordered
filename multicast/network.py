from __future__ import annotations

import heapq
from collections import defaultdict
from collections.abc import Callable
from typing import TYPE_CHECKING

from .messages import Packet

if TYPE_CHECKING:
    from .process import Process


type Event = tuple[int, int, str, Packet]
type LatencyFunction = Callable[[str, str, Packet], int]


class Network:
    def __init__(
        self,
        latency_function: LatencyFunction | None = None,
    ) -> None:
        self.processes: dict[str, Process] = {}
        self.events: list[Event] = []
        self.time = 0
        self.counter = 0

        self.latest_delivery: defaultdict[tuple[str, str], int] = defaultdict(lambda: -1)

        self.latency_function = (
            latency_function
            if latency_function is not None
            else lambda _sender, _receiver, _packet: 1
        )

    def add_process(self, process: Process) -> None:
        if process.pid in self.processes:
            raise ValueError(f"Process {process.pid!r} is already registered")

        self.processes[process.pid] = process

    def send(self, sender: str, receiver: str, packet: Packet) -> None:
        if receiver not in self.processes:
            raise KeyError(f"Unknown receiver: {receiver!r}")

        delay = max(0, self.latency_function(sender, receiver, packet))
        desired_arrival = self.time + delay

        channel = (sender, receiver)

        arrival = max(desired_arrival, self.latest_delivery[channel] + 1)

        self.latest_delivery[channel] = arrival
        self.counter += 1

        heapq.heappush(
            self.events,
            (arrival, self.counter, receiver, packet),
        )

    def multicast(self, sender: str, packet: Packet) -> None:
        members = self.processes[sender].members
        # DATA e ACKs circulam apenas entre os membros cadastrados do grupo.
        for receiver in self.processes:
            if receiver in members:
                self.send(sender, receiver, packet)

    def run(self) -> None:
        while self.events:
            arrival, _, receiver, packet = heapq.heappop(self.events)
            self.time = arrival
            self.processes[receiver].receive(packet)
