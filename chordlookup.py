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
defaultPort = 8000

#node class
class node(object):

    def __init__ (self, identifier_from_coord):
        self.t_listen = None
        self.identifier = int(identifier_from_coord)
        self.fingertable = None
        self.port = defaultPort + self.identifier
        self.selfIP = "127.0.0.1"
        self.sock_listen = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_send = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_listen.bind((self.selfIP, self.port))
        self.keys = [None]*256
        #print "node initiated"
        
    def start(self):
        #notify coordinator the node been created
        message = "ack " + str(self.identifier)
        self.send(message, defaultPort-1)
        self.fingertable = intervalTable()
        #print "node " + self.identifier + " ack message send back to coordinator"
        self.t_listen=threading.Thread(target=self.listen)
        self.t_listen.start()
        if(self.identifier == 0):
            for i in range (0,256):
                self.keys[i]=i
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
            size = len(message)
            if(message[0] == "find"):   # This is actually find_successor
                id = message[1]
                if size > 2:
                    reqId = message[2]
                else:
                    reqId = self.identifier
                #threading.Thread(target=self.find_predecessor, args=(id, self.identifier))
                self.find_predecessor(id, reqId)
            elif(message[0]=="resfind"):    # Process find results
                value = message[1]
                #n_prime is just a node number, need communication to that node for information retrieval 
                n_prime = message[size-1]
                message_comm = "successor"
            elif(message[0]=="leave"):
            #send successor all local keys
                print "node " + str(self.identifier) + " leaving"
                #send all the keys to successor
                rtr_msg = "leavekey"
                for i in range (0,256):
                    if (self.keys[i]!=None):
                        rtr_msg = rtr_msg + " " + str(self.keys[i])
                print self.fingertable.successor
                self.send(rtr_msg, defaultPort + self.fingertable.successor)
                #ack coordinator who is the successor and its own node number
                rtr_msg = "leavesuccessor " + str(self.fingertable.successor)+ " " + str(self.identifier) 
                self.send(rtr_msg, defaultPort-1)
                self.sock_listen.close()         
                #kill the thread,might be some problem
                break
            #handle some predecessor leave. take keys
            elif(message[0]=="leavekey"):
            #pair handle, put keys into local set from a leaving predecessor 
                if(size > 1):
                    for i in range (0, size-2):
                        self.keys[message[i+1]] = message[i+1]
            
            elif(message[0]=="leaveupdate"):
                next_successor = message[2]
                leave_node = message[1]
                for i in range (0,8):
                    if(self.fingertable.start_successor[i]==int(leave_node)):
                         self.fingertable.start_successor[i] = int(next_successor)
                self.fingertable.successor = self.fingertable.start_successor[0]
               # if(self.identifier == 4):
                #    self.fingertable.print_table()
                if(self.identifier == 2):
                    self.fingertable.print_table()
                
            elif(message[0]=="show"):
                rtr_msg = "ackshow"
                for i in range (0,256):
                    if (self.keys[i]!=None):
                        rtr_msg = rtr_msg + " " + str(self.keys[i])
                self.send(rtr_msg, defaultPort-1)   
                
                
            elif(message[0]=="show all"):
                continue
    
    def send(self, message, port):
        self.sock_send.sendto(message, (self.selfIP,port))
    
    def join(self,node):
        #[Chester]node 0 will be directly initialized, other nodes may depends on other info
        self.fingertable.initialize(node)
        self.update_others()
        
    def update_others(self):        #       @TODO[Kelsey]
        #[Chester]function that updates other nodes' fingertable by passing message.
        pass
    
    def find_predecessor(self, id, reqId):
        #[Chester]I don't know if this is right
        print "Entering Find_Predecessor"
        n_prime = self.identifier
        n_prime_successor = self.fingertable.successor
        n_prime_identifier = self.identifier
        n_prime_start_successor = self.fingertable.start_successor
        #while(id >= n_prime_successor or id <= n_prime_identifier):                         # @TODO[Kelsey] FIX THIS, Never exits
        #    n_prime = self.closest_preceding_finger(n_prime, id, n_prime_start_successor) 

        if (id >= n_prime_successor or id <= n_prime_indentifier):
            self.send("find " + str(id) + " " + str(reqId), defaultPort + self.closest_preceding_finger(n_prime, id, n_prime_start_successor))
        else:
            result_string = "resfind " + str(id) + " " + str(n_prime)
            self.send(result_string, defaultPort + reqId)

            #the function only return a node number, need communication to that node and retrieve successor information.
    
    def closest_preceding_finger(self, node, id, start_successor):
        for i in range (7,-1,-1):
            if(node< start_successor[i] < id):
                return start_successor
        return node
    
#fingertable class 
class intervalTable:
    def __init__ (self):
        self.node=0
        self.successor=0
        self.predecessor=0
        self.start=[None]*8
        self.interval_lower=[None]*8
        self.interval_upper=[None]*8
        self.start_successor=[None]*8
        #print "fingertable initiated"
        #TO-ADD char str[INET_ADDRSTRLEN];
        
    def initialize(self, node):
    
        if(node == 0):
            self.node=0
            self.successor=2
            self.predecessor=4
            for i in range (1,9):
                self.start[i-1] = node + 2**(i-1) % 256
            for i in range (1,9):
                self.interval_lower[i-1] = self.start[i-1]
            for i in range (1,8):
                self.interval_upper[i-1] = self.start[i]
            self.interval_upper[7] = node
            for i in range (1,9):
                self.start_successor[i-1] = 2
            self.print_table()
            
        if(node == 2):
            self.node=2
            self.successor=4
            self.predecessor=0
            for i in range (1,9):
                self.start[i-1] = node + 2**(i-1) % 256
            for i in range (1,9):
                self.interval_lower[i-1] = self.start[i-1]
            for i in range (1,8):
                self.interval_upper[i-1] = self.start[i]
            self.interval_upper[7] = node
            for i in range (1,9):
                self.start_successor[i-1] = 4
            self.print_table()
        
        if(node == 4):
            self.node=4
            self.successor=0
            self.predecessor=2
            for i in range (1,9):
                self.start[i-1] = node + 2**(i-1) % 256
            for i in range (1,9):
                self.interval_lower[i-1] = self.start[i-1]
            for i in range (1,8):
                self.interval_upper[i-1] = self.start[i]
            self.interval_upper[7] = node
            for i in range (1,9):
                self.start_successor[i-1] = 0
            self.print_table()
    
    #debugging function
    def print_table(self):
        print "node: " + str(self.node)
        print "successor: " + str(self.successor)
        print "predecessor: " + str(self.predecessor)
        for i in range (1,9):
            print "start: " + str(self.start[i-1]) +" "+ "interval: " + str(self.interval_lower[i-1]) + " " + str(self.interval_upper[i-1]) +" "+ "successor: " + str(self.start_successor[i-1])
    



#coordinator class
class chordlookup(object):
    def __init__(self):
        global defaultPort
        #for i in range(0, 256):
        #    keys[i] = i

        self.selfIP = "127.0.0.1"
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.selfIP, defaultPort-1))
        self.t_listen=threading.Thread(target=self.listen)
        self.t_listen.start()

    def start(self):
        self.t_coord=threading.Thread(target=self.coordinator)
        self.t_coord.start()


        defaultNode = node("0")
        thread = threading.Thread(target=defaultNode.start)
        thread.start()
        threads[0] = thread
        print threads[0]

        #for thread in threads:
        #    thread.join()
        #self.t_coord.join()

    def listen(self):
        while True:
            message, addr = self.sock.recvfrom(1024)
            #print "[RECV] " + message
            if not message:
                 continue
            message = message.split(" ")
            size = len(message)
            message[0] = message[0].strip()
            
            if(message[0] == "ack"):
                print "[RECV] " + message[1]
            
            elif(message[0] == "ackshow"):
                print "[RECV] show key value" 
                for i in range (0, size-2):
                    print "[RECV]: " + message[i+1]
            
            elif(message[0] == "leavesuccessor"):
                next_successor = message[1]
                leave_node = message[2]
                #threads[int(leave_node)].join()
                threads[int(leave_node)] = None
                msg = "leaveupdate "+ leave_node +" "+ next_successor
                for i in range (0,256):
                    if(threads[i]!=None):
                        self.sock.sendto(msg, (self.selfIP, defaultPort+i ))
                        
                        
                
            #TO-DO send next successor to all active node in order to update fingertable.  
        
    def coordinator(self):   # Coordinator Thread
        global defaultPort
        exitFlag = False

        while not(exitFlag):
            userinput = stdin.readline()
            cmdP = userinput.split(" ")
            size = len(cmdP)
            cmdP[0] = cmdP[0].strip()
            cmdP[0] = cmdP[0].lower()
            
            if cmdP[0] == "join":       # join p
                # @TODO[Kelsey] Check if thread P already exists
                nS = node(cmdP[1])
                thread = threading.Thread(target=nS.start)
                thread.start()
                threads[int(cmdP[1])] = thread

            elif cmdP[0] == "find":       # find p k
                self.sock.sendto("find " + cmdP[2], (self.selfIP, defaultPort + int(cmdP[1])))
                #data, addr = self.sock.recvfrom(1024)

                # dissect data for location of k (the identifier of a node

            elif cmdP[0] == "leave":      # leave p
                self.sock.sendto("leave", (self.selfIP, defaultPort + int(cmdP[1])))
                #data, addr = self.sock.recvfrom(1024)

                #dataS = data.split(" ")
                #if dataS[0].lower() == "left":
                #    print "Node " + dataS[1] + " has left the network.\n"

            elif cmdP[0] == "show":
                countMsg = 1
                if cmdP[1] == "all":    # show all
                    countMsg = 256
                    for i in range(0, 256):
                        self.sock.sendto("show", (self.selfIP, defaultPort + i))
                else:
                    #show p, [Chester]edited
                    node_number = cmdP[1].strip()
                    if(threads[int(node_number)]!=None):                  
                        self.sock.sendto("show", (self.selfIP, defaultPort + int(node_number)))
                    else:
                        print "the node doesn't exist!"

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
