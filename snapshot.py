from collections import deque
import threading
import time
import random

class Process:
    def __init__(self, pid):
        self.pid = pid
        self.state = None  # Estado local do processo
        self.channels = {}  # Canais de entrada e saída
        self.snapshot_state = None  # Estado capturado no snapshot
        self.received_markers = set()  # Marcadores recebidos
        self.messages_received = {}  # Mensagens recebidas após o snapshot
        self.snapshot_started = False
        self.lock = threading.Lock()  # Lock para sincronização
        self.running = True  # Flag para controlar a execução das threads

    def set_state(self, state):
        self.state = state

    def add_channel(self, channel, direction):
        self.channels[channel] = direction
        if direction == 'in':
            self.messages_received[channel] = []

    def send_message(self, channel, message):
        if self.channels[channel] == 'out':
            with self.lock:
                print(f"Processo {self.pid} enviando mensagem: {message} pelo canal {channel}")
                channel.send(message)

    def receive_message(self, channel):
        if self.channels[channel] == 'in':
            message = channel.receive()
            if message == 'MARKER':
                self.handle_marker(channel)
            elif message is not None:
                with self.lock:
                    print(f"Processo {self.pid} recebeu mensagem: {message} pelo canal {channel}")
                    if self.snapshot_started:
                        self.messages_received[channel].append(message)
            return message

    def handle_marker(self, channel):
        with self.lock:
            if not self.snapshot_started:
                # Primeiro marcador recebido: iniciar snapshot
                self.snapshot_started = True
                self.snapshot_state = self.state
                self.received_markers.add(channel)
                print(f"Processo {self.pid} iniciou snapshot. Estado capturado: {self.snapshot_state}")
                # Enviar marcadores pelos canais de saída
                for ch, dir in self.channels.items():
                    if dir == 'out':
                        self.send_message(ch, 'MARKER')
            else:
                # Já iniciou o snapshot: gravar estado do canal
                self.received_markers.add(channel)
                print(f"Processo {self.pid} recebeu marcador pelo canal {channel}. Estado do canal: {self.messages_received[channel]}")

    def start_snapshot(self):
        with self.lock:
            self.snapshot_started = True
            self.snapshot_state = self.state
            print(f"Processo {self.pid} iniciou snapshot. Estado capturado: {self.snapshot_state}")
            # Enviar marcadores pelos canais de saída
            for ch, dir in self.channels.items():
                if dir == 'out':
                    self.send_message(ch, 'MARKER')

    def stop(self):
        """Método para parar a execução das threads."""
        self.running = False

class Channel:
    def __init__(self):
        self.queue = deque()
        self.lock = threading.Lock()

    def send(self, message):
        with self.lock:
            self.queue.append(message)

    def receive(self):
        with self.lock:
            if self.queue:
                return self.queue.popleft()
            return None

# Função para enviar mensagens em um processo
def sender_process(process, channel, num_messages):
    for i in range(num_messages):
        time.sleep(random.uniform(0.1, 0.5))  # Simula atraso na rede
        process.send_message(channel, f"Mensagem {i+1} do processo {process.pid}")

# Função para receber mensagens em um processo
def receiver_process(process, channel):
    while process.running:
        time.sleep(random.uniform(0.1, 0.5))  # Simula atraso na rede
        process.receive_message(channel)

# Exemplo de uso
if __name__ == "__main__":
    # Criação dos canais
    c1 = Channel()  # Canal de p1 para p2
    c2 = Channel()  # Canal de p2 para p1

    # Criação dos processos
    p1 = Process(1)
    p2 = Process(2)

    # Configuração dos canais
    p1.add_channel(c1, 'out')
    p1.add_channel(c2, 'in')
    p2.add_channel(c1, 'in')
    p2.add_channel(c2, 'out')

    # Definindo estados iniciais
    p1.set_state("Estado inicial de p1")
    p2.set_state("Estado inicial de p2")

    # Threads para enviar e receber mensagens
    sender_thread_p1 = threading.Thread(target=sender_process, args=(p1, c1, 5))
    sender_thread_p2 = threading.Thread(target=sender_process, args=(p2, c2, 5))
    receiver_thread_p1 = threading.Thread(target=receiver_process, args=(p1, c2))
    receiver_thread_p2 = threading.Thread(target=receiver_process, args=(p2, c1))

    # Iniciando as threads
    sender_thread_p1.start()
    sender_thread_p2.start()
    receiver_thread_p1.start()
    receiver_thread_p2.start()

    # Aguardando um pouco antes de iniciar o snapshot
    time.sleep(1)

    # Iniciando o snapshot a partir de p1
    p1.start_snapshot()

    # Aguardando as threads de envio terminarem
    sender_thread_p1.join()
    sender_thread_p2.join()

    # Parando as threads de recebimento
    p1.stop()
    p2.stop()

    # Aguardando as threads de recebimento terminarem
    receiver_thread_p1.join(timeout=2)
    receiver_thread_p2.join(timeout=2)

    # Verificando estados capturados
    print("\nResultado do Snapshot:")
    print(f"Estado capturado de p1: {p1.snapshot_state}")
    print(f"Estado capturado de p2: {p2.snapshot_state}")
    print(f"Mensagens recebidas por p1 no canal c2: {p1.messages_received[c2]}")
    print(f"Mensagens recebidas por p2 no canal c1: {p2.messages_received[c1]}")