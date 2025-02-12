import threading
import time
import queue

class Channel:
    def __init__(self):
        self.queue = queue.Queue()
        self.recording = False
        self.state = []

    def send(self, message):
        self.queue.put(message)

    def receive(self):
        if not self.queue.empty():
            return self.queue.get()
        return None

    def start_recording(self):
        self.recording = True
        self.state = []

    def stop_recording(self):
        self.recording = False

class Process(threading.Thread):
    def __init__(self, pid, incoming_channel, outgoing_channel):
        super().__init__()
        self.pid = pid
        self.incoming_channel = incoming_channel
        self.outgoing_channel = outgoing_channel
        self.state = f"Estado inicial de p{self.pid}"
        self.received_messages = []
        self.has_snapshot = False

    def send_message(self, message):
        print(f"Processo {self.pid} enviando mensagem: {message} pelo canal {self.outgoing_channel}")
        self.outgoing_channel.send(message)

    def receive_message(self):
        message = self.incoming_channel.receive()
        if message:
            if message == "MARKER":
                self.handle_marker()
            else:
                print(f"Processo {self.pid} recebeu mensagem: {message} pelo canal {self.incoming_channel}")
                if not self.has_snapshot:
                    self.received_messages.append(message)
                if self.incoming_channel.recording:
                    self.incoming_channel.state.append(message)

    def handle_marker(self):
        if not self.has_snapshot:
            self.start_snapshot()
        else:
            print(f"Processo {self.pid} recebeu marcador pelo canal {self.incoming_channel}. Estado do canal: {self.incoming_channel.state}")
        self.incoming_channel.stop_recording()

    def start_snapshot(self):
        print(f"Processo {self.pid} iniciou snapshot. Estado capturado: {self.state}")
        self.has_snapshot = True
        self.incoming_channel.start_recording()
        self.outgoing_channel.send("MARKER")

    def run(self):
        time.sleep(1)
        self.send_message(f"Mensagem 1 do processo {self.pid}")
        time.sleep(1)
        self.receive_message()
        time.sleep(1)

        if self.pid == 1:
            self.start_snapshot()

        time.sleep(1)
        self.send_message(f"Mensagem 2 do processo {self.pid}")
        time.sleep(1)
        self.receive_message()
        time.sleep(1)

        print(f"Processo {self.pid} terminou de enviar mensagens.")

def main():
    channel1 = Channel()
    channel2 = Channel()

    process1 = Process(1, channel2, channel1)
    process2 = Process(2, channel1, channel2)

    process1.start()
    process2.start()

    process1.join()
    process2.join()

    print("\nResultado do Snapshot:")
    print(f"Estado capturado de p1: {process1.state}")
    print(f"Estado capturado de p2: {process2.state}")
    print(f"Mensagens recebidas por p1 no canal c2: {process1.received_messages}")
    print(f"Mensagens recebidas por p2 no canal c1: {process2.received_messages}")

if __name__ == "__main__":
    main()
    input("Pressione Enter para sair...")
