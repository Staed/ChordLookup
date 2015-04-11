# CS425 MP2
# Kelsey, Chester, 2015
import socket
import threading
from threading import Thread
import sys
from sys import stdin

identifier = 0
keys = [None] * 256

threads = [None] * 256
defaultPort = 8000

#node class
class node(object):
    def _init_ (self, identifier_from_coord):
        self.t_listen = None
        self.identifier = identifier_coord
        self.fingertable = intervalTable()
        self.port = defaultPort + self.identifier
        self.selfIP="127.0.0.1"
        self.sock_listen = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_send = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_listen.bind((self.selfIP, self.port))
    
    def start(self):
        #notify coordinator the node been created
        message = "ack "+identifier 
        send(message, defaultPort)
        self.t_listen=threading.Thread(target=self.listen)
        self.t_listen.start()
        
    def listen(self):
        while True:
            message, addr = self.sock_listen.recvfrom(1024)
            print message
            if not message:
                 continue
             
            message = message.split(" ")
            if(message[0] == "find"):
                continue
            elif(message[0]=="leave"):
                continue
            elif(message[0]=="show"):
                continue
            elif(message[0]=="show all"):
                continue
    
    def send(self, message, port):
        self.sock_send.sendto(message, (self.selfIP,port))
    
    
    
#fingertable class 
class intervalTable:
    def _init_ (self):
        self.node=0
        self.successor=0
        self.predecesoor=0
        self.interval=(0,0)
        #TO-ADD char str[INET_ADDRSTRLEN];
    



#coordinator class
class chordlookup(object):
    def __init__(self, input):
        global defaultPort
        for i in range(0, 256):
            keys[i] = i

        selfIP = "127.0.0.1"
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((selfIP, defaultPort))

    def start(self):
        self.t_coord=threading.Thread(target=self.coordinator)
        self.t_coord.start()

        threads[0] = threading.Thread(target=node)  # Create the default node

        for thread in threads:
            thread.join()
        self.t_coord.join()
        
    def coordinator(self):   # Coordinator Thread
        global defaultPort
        exitFlag = False

        while not(exitFlag):
            userinput = stdin.readline()
            cmdP = userInput.split(" ")
            cmdP[0] = cmdP[0].lower()

            if cmdP[0] == "join":       # join p
                # @TODO[Kelsey] Check if thread P already exists
                thread = threading.Thread(target=node)
                thread.start()
                threads[i] = thread

            elif cmdP[0] == "find":       # find p k
                sock.sendto("find" + cmdP[2], (selfIP, defaultPort + int(cmdP[1])))
                data, addr = sock.recvfrom(1024)

                # dissect data for location of k (the identifier of a node

            elif cmdP[0] == "leave":      # leave p
                sock.sendto("leave", (selfIP, defaultPort + int(cmdP[1])))
                data, addr = sock.recvfrom(1024)

                dataS = data.split(" ")
                if dataS[0].lower() == "left":
                    print "Node " + dataS[1] + " has left the network.\n"

            elif cmdP[0] == "show":
                countMsg = 1
                if cmdP[1] == "all":    # show all
                    countMsg = 256
                    for i in range(0, 256):
                        sock.sendto("show", (selfIP, defaultPort + i))
                else:                   # show p
                    sock.sendto("show", (selfIP, defaultPort + i))

                while countMsg > 0:     # While we still expect a result
                    data, addr = sock.recvfrom(1024)
                    # @TODO[Kelsey] Format data
                    print data
                    countMsg -= 1

            elif cmdP[0] == "exit":
                exitFlag = True

if __name__ == '__main__':
    chordLookup.start()
