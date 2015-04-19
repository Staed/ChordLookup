# CS425 MP2
# Kelsey, Chester, 2015
import socket
import threading
from threading import Thread
import sys
from sys import stdin
import math
#import cPickle as pickle

import time

identifier = 0
#keys = [None] * 256

threads = [None] * 256
defaultPort = 8270
show_all_msg = [None] * 256
msg_count = 0;
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
        #if(self.identifier == 2):
        #    self.keys[1]=1
        #    self.keys[2]=2
        #if(self.identifier == 4):
        #    self.keys[3]=3
        #    self.keys[4]=4
        #[Chester] potential some communication with coordinator before call join function.
            self.fingertable.initialize(self.identifier)

        
        
    def listen(self):
        #print "node " + self.identifier + " listening"
        while True:
            msg, addr = self.sock_listen.recvfrom(1024)
            print msg
            if not msg:
                 continue
             
            message = msg.split(" ")
            size = len(message)
            message[0] = message[0].strip()

            if(message[0] == "join"):
                self.join(self.identifier)
            
            
            
            elif(message[0] == "find"):   # This is actually find_successor
                print message
                id = message[1].strip()
                if size > 2:
                    reqId = message[2].strip()
                else:
                    reqId = self.identifier
                #threading.Thread(target=self.find_predecessor, args=(id, self.identifier))
                self.find_predecessor(id, reqId)
            elif(message[0]=="resfind"):    # Process find results
                nodevalue = message[2].strip()
                #n_prime is just a node number, need communication to that node for information retrieval 
                n_prime = message[size-1].strip()
                print "Found at " + str(nodevalue)
                
            elif(message[0]=="leave"):
            #send successor all local keys
                print "node " + str(self.identifier) + " leaving"
                #send all the keys to successor
                rtr_msg = "leavekey" 
                for i in range (0,256):
                    if (self.keys[i]!=None):
                        rtr_msg = rtr_msg + " " + str(self.keys[i])
                print   rtr_msg
                self.send(rtr_msg, defaultPort + self.fingertable.successor)
                #ack coordinator who is the successor and its own node number
                rtr_msg = "leavesuccessor " + str(self.fingertable.successor)+ " " + str(self.identifier) 
                self.send(rtr_msg, defaultPort-1)
                
                rtr_msg = "updatepredeccessor" + " " + str(self.fingertable.predecessor)
                self.send(rtr_msg, defaultPort + self.fingertable.successor)
                
                self.sock_listen.close()         
                #kill the thread,might be some problem
                break
            #handle some predecessor leave. take keys
            elif(message[0]=="leavekey" or message[0] == "joinUpdate"):
            #pair handle, put keys into local set from a leaving predecessor 
                if(size > 1):
                    for i in range (0, size-1):
                        self.keys[int(message[i+1])] = int(message[i+1])
                       
            elif(message[0]=="updatepredeccessor"):
                self.fingertable.predecessor = int(message[1])

            elif(message[0]=="askpredecessor"):
                            
                message[1] = message[1].strip()
                msg = "ackask "+ str(self.fingertable.predecessor)
                self.send(msg, defaultPort+int(message[1]))
                self.fingertable.predecessor = int(message[1])

            
            
            elif(message[0]=="leaveupdate"):
                next_successor = message[2].strip()
                leave_node = message[1].strip()
                for i in range (0,8):
                    if(self.fingertable.start_successor[i]==int(leave_node)):
                         self.fingertable.start_successor[i] = int(next_successor)
                self.fingertable.successor = self.fingertable.start_successor[0]
               # if(self.identifier == 4):
                #    self.fingertable.print_table()
                if(self.identifier == 2):
                    self.fingertable.print_table()
                
            elif(message[0]=="show"):
            #doesn't really have to send back to coordinator. Could just print locally
                rtr_msg = "Node " + str(self.identifier) + ": "
                for i in range (0,256):
                    if (self.keys[i]!=None):
                        rtr_msg = rtr_msg + " " + str(self.keys[i])
                print rtr_msg
                
                
            elif(message[0]=="showall"):
                rtr_msg = "ackshowall"
                for i in range (0,256):
                    if (self.keys[i]!=None):
                        rtr_msg = rtr_msg + " " + str(self.keys[i])
                rtr_msg = rtr_msg + " " + str(self.identifier)
                self.send(rtr_msg, defaultPort-1) 
                
            elif(message[0]=="table"):
                self.fingertable.print_table()
                
            elif(message[0]=="updateFinger"):
                self.update_finger_table(int(message[1].strip()), int(message[2].strip()))
            
            elif(message[0]=="joinTransfer"):
                if(self.identifier > self.fingertable.predecessor):
                    rtr_msg = "joinUpdate"
                    for i in range (self.identifier+1, 255):
                        if (self.keys[i]!=None):
                            rtr_msg = rtr_msg + " " + str(self.keys[i])
                            self.keys[i]=None
                    for i in range (0, self.fingertable.predecessor+1):
                        if (self.keys[i]!=None):
                            rtr_msg = rtr_msg + " " + str(self.keys[i])
                            self.keys[i]=None
                    self.send(rtr_msg, defaultPort+self.fingertable.predecessor)
                else:
                    rtr_msg = "joinUpdate"
                    for i in range (self.identifier+1, self.fingertable.predecessor+1):
                        if (self.keys[i]!=None):
                            rtr_msg = rtr_msg + " " + str(self.keys[i])
                            self.keys[i]=None
                    self.send(rtr_msg, defaultPort+self.fingertable.predecessor)
                    
           # elif message[0]=="upFinger":
           #     self.update_finger_table(int(message[1]), int(message[2]))
           # elif message[0]=="reqFinger":
                #fingerStr = pickle.dumps(self.fingertable, -1)
                #self.send("initFinger " + fingerStr, defaultPort + int(message[1]))
           #     self.send("initFinger " + str(self.fingertable.predecessor), defaultPort + int(message[1]))
           # elif message[0]=="initFinger":
                #successorId = self.fingertable.successor
                #self.fingertable = pickle.loads(msg[11:]) #msg.replace("initFinger ", "", 1))
                #print self.fingertable.start_successor
                #print len(self.fingertable.start_successor)
            #    self.fingertable.predecessor = int(message[1])
            #    self.init_finger_table
    
    def send(self, message, port):
        global msg_count
        msg_count=msg_count+1
        self.sock_send.sendto(message, (self.selfIP,port))
    
    def join(self, nodeId):
        #[Chester]node 0 will be directly initialized, other nodes may depends on other info
            #or int(nodeId) == 2 or int(nodeId == 4):
            self.fingertable.initialize(nodeId)
            #reqString = "reqFinger " + str(successorId)
            #self.send(reqString, defaultPort + int(successorId))
            self.init_finger_table(nodeId)
            print "init done. doing update others"
            self.update_others(nodeId)
            print "update others done. doing key transfer"
            #time.sleep(5)
            self.send("joinTransfer", defaultPort + self.fingertable.successor)
            
            
    def init_finger_table(self, nodeId):    # @TODO Fix this whole function
        msg = "find "+ str(self.fingertable.start[0])+" "+str(nodeId)
        self.send(msg,defaultPort)
        while(True):
            msg, addr = self.sock_listen.recvfrom(1024)
            if not msg:
                 continue
            else:
                print "[find_predecessor] :" + msg
                break
        msg = msg.split(" ")
        msg[2] = msg[2].strip()
        self.fingertable.successor = int(msg[2])
        self.fingertable.start_successor[0] = int(msg[2])
        message = "askpredecessor "+ str(self.identifier)
        self.send(message, defaultPort + int(self.fingertable.successor))
        while(True):
            msg, addr = self.sock_listen.recvfrom(1024)
            print msg
            if not msg:
                 continue
            else:
                print "[ask_predecessor] :" + msg
                break
        msg = msg.split(" ")
        self.fingertable.predecessor = int(msg[1].strip())
        for i in range (0, 7):
            print "I am in loop: " + str(i) + " " + str(nodeId) + " "+ str(self.fingertable.start[i+1]) + " " + str(self.fingertable.start_successor[i])
            if(int(nodeId) <= int(self.fingertable.start[i+1]) < int(self.fingertable.start_successor[i])):
                self.fingertable.start_successor[i+1] = self.fingertable.start_successor[i]
                print "I am here for loading message in round: " + str(i)
            else:
                #self.find_predecessor(self.fingertable.start[i+1], nodeId)
                print "I am here for sending message in round: " + str(i)
                msg = "find "+ str(self.fingertable.start[i+1])+" "+str(nodeId)
                self.send(msg,defaultPort)
                while(True):
                    msg, addr = self.sock_listen.recvfrom(1024)
                    print msg
                    if not msg:
                        continue
                    else:
                        print "[find_predecessor] :" + msg
                        break
                msg = msg.split(" ")
                print "[HERE] "+ msg[2].strip() + " i is :" + str(i)
                self.fingertable.start_successor[i+1] = msg[2].strip()
                #if not(self.fingertable.start[i+1] <= self.fingertable.start_successor[i+1] <= nodeId):
                #    self.fingertable.start_successor[i+1] = nodeId
        #for i in range (1,9):
        #    self.start[i-1] = node + 2**(i-1) % 256
        #for i in range (1,9):
        #    self.interval_lower[i-1] = self.start[i-1]
        #for i in range (1,8):
        #    self.interval_upper[i-1] = self.start[i]
        #self.interval_upper[7] = node
        #for i in range (1,9):
        #    self.start_successor[i-1] = 0
        #self.print_table()

        #self.fingertable.start_successor[1] = nodeId.self.find_predecessor(self.fingertable.start)
        #self.fingertable.predecessor = (self.fingertable.successor).predecessor
        #(self.fingertable.successor).predessor = self.identifier

        #for i in range(1, 255):
        #    if self.identifier <= self.fingertable.start_successor[i+1].start < self.fingertable.start_successor[i]:
        #        self.fingertable.start_successor[i+1] = self.fingertable.start_successor[i]
        #    else:
        #        self.fingertable.start_successor[i+1] = nodeId.self.find_predecessor(self.fingertable.start_successor[i+1])
        
    def update_others(self, nodeId):
            i=0
       
                        

            #p = int(msg[3].strip())
            message = "updateFinger "+ str(nodeId) + " " + str(i)
            p = self.fingertable.predecessor
            self.send(message, defaultPort + p)
        #earlyExit = False
        #for i in range(1, 256):
        #    current = self.identifier - math.pow(2, i - 1)
        #    if current < 0:
        #        current += 256
        #        earlyExit = True
        #    p = self.serial_find_predecessor(current)
        #    if not(p == None):
        #        self.send("upFinger " + str(self.identifier) + " " + str(i), defaultPort + int(p))
        #    if earlyExit:
        #        break

    def update_finger_table(self, s, i):
        print "update finger table at: " + str(self.identifier)
        print "update s is: " + str(s) + " i is : " + str(i)
        if(s == self.identifier):
            return
        for j in range (0, 8):
            #if(0 <= self.fingertable.start_successor[j] <= self.identifier):
            #    temp = self.fingertable.start_successor[j] + 256
            #else:
            #    temp = self.fingertable.start_successor[j]
        
            #if (self.identifier+1) <= s < temp:
            #    self.fingertable.start_successor[i] = s
            #    p = self.fingertable.predecessor
            #    if(self.identifier != p):
            #        message = "updateFinger "+ str(s) + " " + str(i)
            #        self.send(message, defaultPort + p)
            if self.fingertable.interval_upper[j] < self.fingertable.interval_lower[j]:
                temp = 256 +self.fingertable.interval_upper[j]
            else:
                temp = self.fingertable.interval_upper[j]
            if(self.fingertable.interval_lower[j]<= s):
                if(self.fingertable.start_successor[j]==0 or (self.fingertable.start_successor[j]>s)):
                    self.fingertable.start_successor[j] = s                   
        self.fingertable.successor = self.fingertable.start_successor[0]
        p = self.fingertable.predecessor
        message = "updateFinger "+ str(s) + " " + str(i)
        self.send(message, defaultPort + p)

       # while(True):
       #     msg, addr = self.sock_listen.recvfrom(1024)
       #     print msg
       #     if not msg:
       #          continue
       #     else:
       #         
       #         print "[ask_predecessor] :" + msg
       #         msg = msg.split(" ")
       #         size = len(msg)
       #         if(msg[0]=="ackask"):
       #             break#

       #         elif(msg[0]=="find"):
       #             id = msg[1].strip()
       #             if size > 2:
       #                 reqId = msg[2].strip()
       #             else:
       #                 reqId = self.identifier
                #threading.Thread(target=self.find_predecessor, args=(id, self.identifier))
       #             self.find_predecessor(id, reqId)
       #             break
       #         elif(msg[0]=="updateFinger"):
       #             self.update_finger_table(int(msg[1].strip()), int(msg[2].strip()))
       #             break

       #     self.fingertable.predecessor = int(msg[1].strip())
        
            #if not(p == 0):
             #   self.send("upFinger " + str(s) + " " + str(i), defaultPort + int(p))
    
    def serial_find_predecessor(self, id):
        n_prime = self.identifier
        n_prime_successor = self.fingertable.successor
        n_prime_start_successor = self.fingertable.start_successor
        while id <= n_prime_successor and id > n_prime:
            n_prime = self.closest_preceding_finger(n_prime, id, n_prime_start_successor)
        return n_prime   


    def find_predecessor(self, id, reqId):
        n_prime = self.identifier
        n_prime_successor = self.fingertable.successor
        n_prime_identifier = self.identifier
        n_prime_start_successor = self.fingertable.start_successor
        print str(id) + " np:" + str(n_prime) + " ps:" + str(n_prime_successor) + " pi:" + str(n_prime_identifier)
        temp_id = int(id)
        if n_prime_successor < n_prime_identifier:
            n_prime_successor = int(n_prime_successor) + 256
            if(int(id) < self.identifier):
                temp_id = temp_id +256
        if int(id) == int(n_prime_identifier):
            result_string = "resfind " + str(id) + " " + str(self.identifier) + " " + str(self.identifier)
            self.send(result_string, defaultPort + int(reqId))
        
        elif (int(temp_id) > int(n_prime_identifier) and int(temp_id) <= int(n_prime_successor)) or (int(n_prime) == int(n_prime_successor)):
            result_string = "resfind " + str(id) + " " + str(self.fingertable.successor) + " " + str(self.identifier)
            self.send(result_string, defaultPort + int(reqId))
        
        else:
            #time.sleep(1)
            self.send("find " + str(id) + " " + str(reqId), defaultPort + int(self.closest_preceding_finger(n_prime, id, n_prime_start_successor)))
            #the function only return a node number, need communication to that node and retrieve successor information.
    
    def closest_preceding_finger(self, node, id, start_successor):
        print node
        
        if int(id) < int(node):
            temp_id = int(id) + 256         
            for i in range (7,-1,-1):
                if(0 <= int(start_successor[i]) and int(start_successor[i])<int(id)):
                    temp_start_successor = int(start_successor[i])+256
                else:
                    temp_start_successor = int(start_successor[i])   # @TODO What is this supposed to be?
                if(int(node) < temp_start_successor and temp_start_successor < int(temp_id)):
                    print "cpf returns " + str(start_successor[i])
                    return start_successor[i]
            return node
        else:
            for i in range (7,-1,-1):   # @TODO What is this supposed to be?
                if(int(node) < int(start_successor[i]) and int(start_successor[i]) < int(id)):
                    print "cpf returns " + str(start_successor[i])
                    return start_successor[i]
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
            self.successor=0
            self.predecessor=0
            for i in range (1,9):
                self.start[i-1] = (node + 2**(i-1)) % 256
            for i in range (1,9):
                self.interval_lower[i-1] = self.start[i-1]
            for i in range (1,8):
                self.interval_upper[i-1] = self.start[i]
            self.interval_upper[7] = node
            for i in range (1,9):
                self.start_successor[i-1] = 0

            self.print_table()
            
        else:
            self.node = node
            for i in range (1,9):
                self.start[i-1] = (node + 2**(i-1)) % 256
            for i in range (1,9):
                self.interval_lower[i-1] = self.start[i-1]
            for i in range (1,8):
                self.interval_upper[i-1] = self.start[i]
            self.interval_upper[7] = node
        #if(node == 2):
        #    self.node=2
        #    self.successor=4
        #    self.predecessor=0
        #    for i in range (1,9):
        #        self.start[i-1] = node + 2**(i-1) % 256
        #    for i in range (1,9):
        #        self.interval_lower[i-1] = self.start[i-1]
        #    for i in range (1,8):
        #        self.interval_upper[i-1] = self.start[i]
        #    self.interval_upper[7] = node
        #    for i in range (1,3):
        #        self.start_successor[i-1] = 4
        #    for i in range (3,9):
        #        self.start_successor[i-1]=0
        #    self.print_table()
        
       # if(node == 4):
       #     self.node=4
       #     self.successor=0
       #     self.predecessor=2
       #     for i in range (1,9):
       #         self.start[i-1] = node + 2**(i-1) % 256
       #     for i in range (1,9):
       #         self.interval_lower[i-1] = self.start[i-1]
       #     for i in range (1,8):
       #        self.interval_upper[i-1] = self.start[i]
       #     self.interval_upper[7] = node
       #     for i in range (1,9):
       #        self.start_successor[i-1] = 0
       #     self.print_table()
    
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
        self.selfIP = "127.0.0.1"
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind((self.selfIP, defaultPort-1))
        self.t_listen=threading.Thread(target=self.listen)
        self.t_listen.start()
        self.num_node = 1

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
        reply_ctr =0
        global msg_count
        while True:
            
            message, addr = self.sock.recvfrom(1024)
            #print "[RECV] " + message
            if not message:
                 continue
            msg = message.split(" ")
            size = len(msg)
            msg[0] = msg[0].strip()
            
            if(msg[0] == "ack"):
                print "[RECV] " + msg[1]
            
            elif(msg[0] == "leavesuccessor"):
                next_successor = msg[1]
                leave_node = msg[2]
                #threads[int(leave_node)].join()
                threads[int(leave_node)] = None
                send_msg = "leaveupdate "+ leave_node +" "+ next_successor
                for i in range (0,256):
                    if(threads[i]!=None):

                        msg_count=msg_count+1
                        self.sock.sendto(send_msg, (self.selfIP, defaultPort+i ))
            
            elif(msg[0] == "ackshowall"):
                reply_ctr = reply_ctr + 1
                pos = int(msg[size-1])
                show_all_msg[pos] = message
                if(self.num_node == reply_ctr):
                    for i in range (0, 256):
                        if(show_all_msg[i]!=None):
                            print_msg = show_all_msg[i].split(" ")
                            size = len(print_msg)
                            keys =""
                            for i in range (1, size-1):
                                keys = keys + " " + print_msg[i]
                            print "Node "+ print_msg[size-1] +":"+ keys
                    reply_ctr = 0
                    for i in range (0,256):
                        show_all_msg[i]=None 
                        

                        
                
            #TO-DO send next successor to all active node in order to update fingertable.  
        
    def coordinator(self):   # Coordinator Thread
        global defaultPort
        global msg_count
        exitFlag = False

        while not(exitFlag):
            userinput = stdin.readline()
            cmdP = userinput.split(" ")
            size = len(cmdP)
            cmdP[0] = cmdP[0].strip()
            if(size>1):
                cmdP[1] = cmdP[1].strip()
            cmdP[0] = cmdP[0].lower()
            
            if cmdP[0] == "join":     
                self.num_node= self.num_node+1

                if(threads[int(cmdP[1])]!=None):
                    print "The node is already in the network"
                else:                 
                    nS = node(cmdP[1])
                    thread = threading.Thread(target=nS.start)
                    thread.start()
                    threads[int(cmdP[1])] = thread
                    self.sock.sendto("join", (self.selfIP,defaultPort + int(cmdP[1])))

            elif cmdP[0] == "find":       # find p k
                if(threads[int(cmdP[1])]==None):
                    print "the node doesn't exist!"
                
                if(int(cmdP[2].strip())<0 or int(cmdP[2].strip())>256):
                    print "the key doesn't exist!"
                
                else:
                    global msg_count
                    msg_count=msg_count+1
                    self.sock.sendto("find " + cmdP[2], (self.selfIP, defaultPort + int(cmdP[1])))

                # dissect data for location of k (the identifier of a node

            elif cmdP[0] == "leave":
                if(threads[int(cmdP[1])]!=None):
                    self.num_node= self.num_node-1 
                    msg_count=msg_count+1     # leave p
                    self.sock.sendto("leave", (self.selfIP, defaultPort + int(cmdP[1])))
                else:
                    print "the node doesn't exist!"

            elif cmdP[0] == "show":
                countMsg = 1
                if cmdP[1] == "all":    # show all
                    countMsg = 256
                    for i in range(0, 256):
                        if(threads[i]!=None):
                            msg_count=msg_count+1
                            self.sock.sendto("showall", (self.selfIP, defaultPort + i))
                else:
                    #show p, [Chester]edited
                    node_number = cmdP[1].strip()
                    if(threads[int(node_number)]!=None):
                        msg_count=msg_count+1                  
                        self.sock.sendto("show", (self.selfIP, defaultPort + int(node_number)))
                    else:
                        print "the node doesn't exist!"
            
            #debugging function
            elif cmdP[0] == "table":
                node_number = cmdP[1].strip()
                if(threads[int(node_number)]!=None):
                    msg_count=msg_count+1                  
                    self.sock.sendto("table", (self.selfIP, defaultPort + int(node_number)))
                else:
                    print "the node doesn't exist!"
            
            elif cmdP[0] == "thread":
                print "active thread as following:"
                for i in range(0, 256):
                    if (threads[i] != None):
                        print "Active node %d" % i
                        
            elif cmdP[0] == "message":
                print "The total message number so far is: " + str(msg_count)
            
            elif cmdP[0] == "exit":
                exitFlag = True

if __name__ == '__main__':
    cl = chordlookup()
    cl.start()
