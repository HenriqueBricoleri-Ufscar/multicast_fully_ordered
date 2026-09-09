from multicast import AckMessage, DataMessage, Network, Packet, Process


def latency(sender: str, receiver: str, packet: Packet) -> int:
    if (
        isinstance(packet, DataMessage)
        and packet.msg_id == "p1:1"
        and receiver == "p2"
    ):
        return 1

    if (
        isinstance(packet, DataMessage)
        and packet.msg_id == "p1:1"
        and receiver == "p3"
    ):
        return 8

    if (
        isinstance(packet, AckMessage)
        and packet.msg_id == "p1:1"
        and sender == "p2"
        and receiver == "p3"
    ):
        return 1

    return 2


def main() -> None:
    members = ["p1", "p2", "p3"]

    network = Network(latency_function=latency)
    processes: dict[str, Process] = {}

    for pid in members:
        process = Process(
            pid=pid,
            members=members,
            network=network,
        )

        processes[pid] = process
        network.add_process(process)

    processes["p1"].multicast("m1")
    processes["p2"].multicast("m2")

    network.run()

    print("\nFINAL ORDER")

    for pid in members:
        print(f"{pid}: {processes[pid].delivered}")

    orders = {tuple(process.delivered) for process in processes.values()}
    print(f"\nTotal order preserved: {len(orders) == 1}")


if __name__ == "__main__":
    main()
