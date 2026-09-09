import copy
import queue
import threading
import time

# FIFOChannel é uma classe que implementa um canal FIFO (First-In-First-Out) para enviar dados entre threads. 
# Ela herda de threading.Thread e possui um buffer interno para armazenar os dados a serem enviados. 
# O método send permite adicionar dados ao buffer com um atraso opcional, enquanto o método run processa os dados do buffer e os envia para a fila de destino.

class FIFOChannel(threading.Thread):

  def __init__(self, target_queue: queue.Queue):
    super().__init__(daemon=True)
    self.target_queue = target_queue
    self.buffer = queue.Queue()

  def send(self, data: dict, delay_sec: float = 0):
    self.buffer.put((copy.deepcopy(data), delay_sec))

  def run(self):
    while True:
      data, delay = self.buffer.get()
      if delay > 0:
        time.sleep(delay)
      self.target_queue.put(data)