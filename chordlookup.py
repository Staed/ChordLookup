# CS425 MP2
import socket
import threading
import sys
from threading import Thread

identifier = 0
keys[256]

class chordlookup(object):
    def __init__(self, input):
        self._Queue= Queue.Queue()

    def start(self):
        self.t_listen=threading.Thread(target=self.listen)
        self.t_listen.start()
        self.t_send=threading.Thread(target=self.send)
        self.t_send.start()
        
    def listen(self):   # Coordinator Thread
        while True:
            message, addr = self.s_listen.recvfrom(1024)
                
    def send(self):
        while True:
            message = self._Queue.get()

if __name__ == '__main__':
    if len(sys.argv) <2:
        sys.argv.append(raw_input('Central Server> Please choose the consistency model:\n 1.Linearizability\n 2.Sequential consistency\n 3.Eventual consistency, W=1, R=1\n 4.Eventual consistency, W=2, R=2\n Central Server>'))
    usc = Central_server(sys.argv[1])
    usc.start()
