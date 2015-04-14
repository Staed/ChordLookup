# CS425 MP2
# Kelsey, Chester, 2015
import socket
import threading
from threading import Thread
import sys
from sys import stdin

identifier = 0
#keys = [None] * 256

threads = [None] * 256
defaultPort = 8103

#node class
class node(object):

    def __init__ (self, identifier_from_coord):
        self.t_listen = None
        self.identifier = identifier_from_coord
        self.fingertable = None
        self.port = defaultPort + int(self.identifier)
        self.selfIP = "127.0.0.1"
        self.sock_listen = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_send = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_listen.bind((self.selfIP, self.port))
        self.keys = [None]*256
        #print "node initiated"
        
    def start(self):
        #notify coordinator the node been created
        message = "ack " + self.identifier
        self.send(message, defaultPort-1)
        self.fingertable = intervalTable()
        #print "node " + self.identifier + " ack message send back to coordinator"
        self.t_listen=threading.Thread(target=self.listen)
        self.t_listen.start()
        
        #[Chester] potential some communication with coordinator before call join function.
        self.join(self.identifier)
        
        
        
    def listen(self):
        #print "node " + self.identifier + " listening"
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
    
    def join(self,node):
        #[Chester]node 0 will be directly initialized, other nodes may depends on other info
        self.fingertable.initialize(node)
        self.update_others()
        
    def update_others(self):
        #[Chester]function that updates other nodes' fingertable by passing message.
        pass
    
    
#fingertable class 
class intervalTable:
    def __init__ (self):
        self.node=0
        self.successor=0
        self.predecesoor=0
        self.start=[None]*8
        self.interval_lower=[None]*8
        self.interval_upper=[None]*8
        self.start_successor=[None]*8
        #print "fingertable initiated"
        #TO-ADD char str[INET_ADDRSTRLEN];
        
    def initialize(self, node):
        print "test1"
        if(int(node) == 0):
            print "test2"
            for i in range (1,9):
                self.start[i-1] = 2**(i-1) % 256
            for i in range (1,9):
                self.interval_lower[i-1] = self.start[i-1]
            for i in range (1,8):
                self.interval_upper[i-1] = self.start[i]
            self.interval_upper[7] = 0
            for i in range (1,9):
                self.start_successor[i-1] = 0
            self.print_table()
    
    #debugging function
    def print_table(self):
        print "node: " + str(self.node)
        print "successor: " + str(self.successor)
        for i in range (1,9):
            print "start: " + str(self.start[i-1]) +" "+ "interval: " + str(self.interval_lower[i-1]) + " " + str(self.interval_upper[i-1]) +" "+ "successor: " + str(self.start_successor[i-1])
    



#coordinator class
class chordlookup(object):
    def __init__(self):
        global defaultPort
        #for i in range(0, 256):
        #    keys[i] = i

        selfIP = "127.0.0.1"
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((selfIP, defaultPort-1))
        self.t_listen=threading.Thread(target=self.listen)
        self.t_listen.start()

    def start(self):
        self.t_coord=threading.Thread(target=self.coordinator)
        self.t_coord.start()


        defaultNode = node("0")
        threads[0] = threading.Thread(target=defaultNode.start).start()  # Create the default node

        #for thread in threads:
        #    thread.join()
        #self.t_coord.join()

    def listen(self):
        while True:
            message, addr = self.sock.recvfrom(1024)
            print "[RECV] " + message
            if not message:
                 continue
        
    def coordinator(self):   # Coordinator Thread
        global defaultPort
        exitFlag = False

        while not(exitFlag):
            userinput = stdin.readline()
            cmdP = userinput.split(" ")
            cmdP[0] = cmdP[0].strip()
            cmdP[0] = cmdP[0].lower()
            
            if cmdP[0] == "join":       # join p
                # @TODO[Kelsey] Check if thread P already exists
                nS = node(cmdP[1])
                thread = threading.Thread(target=nS.start)
                thread.start()
                threads[int(cmdP[1])] = thread

            elif cmdP[0] == "find":       # find p k
                self.sock.sendto("find" + cmdP[2], (selfIP, defaultPort + int(cmdP[1])))
                #data, addr = self.sock.recvfrom(1024)

                # dissect data for location of k (the identifier of a node

            elif cmdP[0] == "leave":      # leave p
                self.sock.sendto("leave", (selfIP, defaultPort + int(cmdP[1])))
                #data, addr = self.sock.recvfrom(1024)

                #dataS = data.split(" ")
                #if dataS[0].lower() == "left":
                #    print "Node " + dataS[1] + " has left the network.\n"

            elif cmdP[0] == "show":
                countMsg = 1
                if cmdP[1] == "all":    # show all
                    countMsg = 256
                    for i in range(0, 256):
                        self.sock.sendto("show", (selfIP, defaultPort + i))
                else:                   # show p
                    self.sock.sendto("show", (selfIP, defaultPort + i))

                #while countMsg > 0:     # While we still expect a result
                #    data, addr = self.sock.recvfrom(1024)
                #    # @TODO[Kelsey] Format data
                #    print data
                #    countMsg -= 1
            
            #debugging function
            
            
            elif cmdP[0] == "thread":
                print "active thread as following:"
                for i in range(0, 256):
                    if (threads[i] != None):
                        print "Active node %d" % i
                        
            
            elif cmdP[0] == "exit":
                exitFlag = True

if __name__ == '__main__':
    cl = chordlookup()
    cl.start()
