import queue
import threading
from channel import FIFOChannel

# ProcessThread é uma classe que representa um processo em um sistema distribuído.
# Ela herda de threading.Thread e possui um identificador único (pid), um relógio lógico de Lamport, uma fila de mensagens recebidas, um conjunto de ACKs recebidos e uma lista de mensagens entregues.
# A classe também possui métodos para enviar mensagens em multicast, processar eventos recebidos, tentar entregar mensagens à aplicação e enviar mensagens da aplicação.    

class ProcessThread(threading.Thread):

  def __init__(self, pid: int, total_processes: int, all_inboxes: dict):
    super().__init__()
    self.pid = pid
    self.total = total_processes
    self.inbox = all_inboxes[pid]

    # Cria canais FIFO individuais para cada nó de destino
    self.channels = {
        target_pid: FIFOChannel(all_inboxes[target_pid])
        for target_pid in all_inboxes
    }
    for ch in self.channels.values():
      ch.start()

    # Estado local isolado
    self.clock = 0
    self.queue = []
    self.acks = {}  # msg_id -> set(pids)
    self.pending_acks = {}  # msg_id -> set(pids)
    self.delivered = []
    self.lock = threading.Lock()

  def broadcast(self, data: dict, delay_to_pid: int = 0, delay_sec: float = 0):
    """Envia mensagem para todos os membros do grupo respeitando a ordem FIFO por canal."""
    for target_pid, channel in self.channels.items():
      d = delay_sec if target_pid == delay_to_pid else 0
      channel.send(data, delay_sec=d)

  def try_deliver(self):
    """Critério de entrega: topo da fila e confirmação (ACK) de todos os processos."""
    while self.queue:
      head = self.queue[0]
      msg_id = head['msg_id']
      acks_count = len(self.acks.get(msg_id, set()))

      if acks_count == self.total:
        msg = self.queue.pop(0)
        self.delivered.append(msg_id)
        if msg_id in self.acks:
          del self.acks[msg_id]  # Limpeza de memória

        print(
            f"\n[P{self.pid}] >>> ENTREGA À APLICAÇÃO: '{msg_id}'"
            f" (ts={msg['ts']}, emissor=P{msg['sender']}, content='{msg['content']}')\n",
            flush=True,
        )
      else:
        break

  def process_event(self, data: dict):
    with self.lock:
      # Atualização do Relógio Lógico de Lamport
      self.clock = max(self.clock, data['ts']) + 1

      if data['type'] == 'MSG':
        msg_id = data['msg_id']

        # Drena ACKs que chegaram antes da mensagem
        initial_acks = self.pending_acks.pop(msg_id, set())
        self.acks[msg_id] = initial_acks

        # Insere e ordena por (timestamp, sender)
        self.queue.append(data)
        self.queue.sort(key=lambda m: (m['ts'], m['sender']))

        print(
            f"[P{self.pid}] Recebeu MSG '{msg_id}' (ts={data['ts']}, de"
            f" P{data['sender']}). Fila local: {[m['msg_id'] for m in self.queue]}"
            f' | ACKs acumulados: {sorted(list(initial_acks))}',
            flush=True,
        )

        # Dispara ACK em multicast
        self.clock += 1
        ack_packet = {
            'type': 'ACK',
            'msg_id': msg_id,
            'sender': self.pid,
            'ts': self.clock,
        }
        self.broadcast(ack_packet)

      elif data['type'] == 'ACK':
        msg_id = data['msg_id']
        acker = data['sender']

        is_in_queue = any(m['msg_id'] == msg_id for m in self.queue)

        if is_in_queue or msg_id in self.acks:
          self.acks.setdefault(msg_id, set()).add(acker)
          print(
              f"[P{self.pid}] Recebeu ACK de P{acker} para '{msg_id}'. Total"
              f' ACKs = {len(self.acks[msg_id])}/{self.total}',
              flush=True,
          )
        else:
          self.pending_acks.setdefault(msg_id, set()).add(acker)
          print(
              f"[P{self.pid}] [ATENÇÃO] Recebeu ACK de P{acker} para"
              f" '{msg_id}' ANTES da mensagem original!",
              flush=True,
          )

      self.try_deliver()

  def send_app_message(
      self,
      msg_id: str,
      content: str,
      delay_to_pid: int = 0,
      delay_sec: float = 0,
  ):
    with self.lock:
      self.clock += 1
      data = {
          'type': 'MSG',
          'msg_id': msg_id,
          'sender': self.pid,
          'ts': self.clock,
          'content': content,
      }
      print(
          f"[P{self.pid}] Disparando Multicast de '{msg_id}'"
          f' (ts={self.clock})...',
          flush=True,
      )
      self.broadcast(data, delay_to_pid=delay_to_pid, delay_sec=delay_sec)

  def run(self):
    while True:
      data = self.inbox.get()
      if data is None:
        break
      self.process_event(data)