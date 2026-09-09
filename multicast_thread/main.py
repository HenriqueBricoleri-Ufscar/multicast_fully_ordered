import queue
import time
from node import ProcessThread

TOTAL_PROCESSES = 3

if __name__ == '__main__':
  print('=== TESTE DO ALGORITMO DE MULTICAST TOTALMENTE ORDENADO ===\n')

  inboxes = {pid: queue.Queue() for pid in range(1, TOTAL_PROCESSES + 1)}
  nodes = {
      pid: ProcessThread(pid, TOTAL_PROCESSES, inboxes) for pid in inboxes
  }

  for node in nodes.values():
    node.start()

  # Teste: P1 envia m1 com atraso no canal P1 -> P3
  nodes[1].send_app_message(
      'm1', 'Payload de P1', delay_to_pid=3, delay_sec=2.5
  )

  time.sleep(4)

  # Teste: P2 envia m2
  nodes[2].send_app_message('m2', 'Payload de P2')

  time.sleep(3)

  # Finalização graciosa dos nós
  for inbox in inboxes.values():
    inbox.put(None)
  for node in nodes.values():
    node.join()

  print('\n=== VALIDAÇÃO DA ORDEM FINAL ENTREGUE À APLICAÇÃO ===')
  for pid, node in nodes.items():
    print(f'[P{pid}] Ordem de Entrega: {node.delivered}')