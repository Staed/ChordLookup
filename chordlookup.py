# CS425 MP2
# Kelsey, Chester, 2015
import socket
import threading
from threading import Thread
import sys
from sys import stdin

identifier = 0
keys[256]
threads = []

class chordlookup(object):
    def __init__(self, input):
        selfIP = "127.0.0.1"
        defaultPort = 8000
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((UDP_IP, UDP_PORT))

    def start(self):
        self.t_coord=threading.Thread(target=self.coordinator)
        self.t_coord.start()

        for thread in threads:
            thread.join()
        self.t_coord.join()
        
    def coordinator(self):   # Coordinator Thread
        exitFlag = False
        while not(exitFlag):
            # message, addr = self.s_listen.recvfrom(1024)
            userinput = stdin.readline()
            cmdP = userInput.split(" ")

            if cmdP[0] == "join":   # join p
                thread = threading.Thread(target=node)
                thread.start()
                threads.append(thread)
            if cmdP[0] == "find":   # find p k
            if cmdP[0] == "leave":  # leave p
            if cmdP[0] == "show":
                if cmdP[1] == "all":    # show all
                else:                   # show p
                    self.sendto


    def node(self):
        # Stuff
                
if __name__ == '__main__':
    if len(sys.argv) <2:
        sys.argv.append(raw_input('Central Server> Please choose the consistency model:\n 1.Linearizability\n 2.Sequential consistency\n 3.Eventual consistency, W=1, R=1\n 4.Eventual consistency, W=2, R=2\n Central Server>'))
    usc = Central_server(sys.argv[1])
    usc.start()
