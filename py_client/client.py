# -*- coding: utf8 -*-
import sys
import socket
from pipe_pb2 import Route
from pipe_pb2 import Message
from pipe_pb2 import User
from pipe_pb2 import _ROUTE_PATH
from pipe_pb2 import MessagesResponse
from pipe_pb2 import NetworkDiscoveryPacket
from encoder_decoder import LengthFieldProtoEncoder, LengthFieldProtoDncoder
from network_discovery import NetworkDiscover
from multiprocessing import Process
import urllib2
import time
# import datetime


# This is currently port and IP oof Nginx server
TCP_IP = 'http://10.0.0.10'
TCP_PORT = 4467
BUFFER_SIZE = 4 * 1024 * 1024

class MessageClient:
    def __init__(self, buffer_size):
        self.buffer_size = buffer_size
        self.encoder = LengthFieldProtoEncoder()
        self.decoder = LengthFieldProtoDncoder()
        self.network_discover = NetworkDiscover()
        self.s = None
        self.socket_connect = None
        self.uname = None

        self.host = "10.0.0.3"
        self.port = 4168
        
        try:
#             self.network_discover.connectUDP()
#             self.network_discover.sendNetworkDiscoveryPacket()
#             self.route = self.network_discover.receiveNetworkDiscoveryPacket()
#             self.host = self.route.networkDiscoveryPacket.nodeAddress
#             self.port = self.route.networkDiscoveryPacket.nodePort
            print ("Connecting to server ", self.host, self.port)
        except Exception as ex:
            print "Ex-udp-network-discover -" 
            import traceback
            exc_info = sys.exc_info()
            traceback.print_exception(*exc_info)
        finally:
            self.connect()


    def __get_server_socket_address(self):
        socket_address = urllib2.urlopen(TCP_IP + ":" + str(TCP_PORT)).read()
        print socket_address
        return socket_address

    def connect(self):
        self.s = socket.socket()
        self.s.connect((self.host, self.port))
        print "client connected to server: " + self.__get_server_path()

        self.uname = raw_input("Please enter your username: ")

        # listen to incoming data on a new thread
        thread = Process(target = self.listen)
        thread.start()

        self.create_user(self.uname)
   
    def listen(self):
        while True:
            try:
                data = self.s.recv(self.buffer_size)
                if (len(data) == 0):
                    print "connection has been closed with: " + self.__get_server_path()
                    self.close()
                    break

                route = self.decoder.decode(data)
                if route.path == route.MESSAGE:
                    self.print_message_received(route.message)
                elif route.path == route.MESSAGES_RESPONSE:
                    messages_response = route.messagesResponse
                    for msg in messages_response.messages:
                        self.print_message_received(msg)

            except socket.timeout:
                self.close()
                print "connection timed out with: " + self.__get_server_path()
                break
            except KeyboardInterrupt:
                self.close()
                print "connection closed with: " + self.__get_server_path()
                break

    def print_message_received(self, msg):
        print msg.timestamp + ": new message received from " + msg.senderId + ", message: " + msg.payload

    def send_message(self, senderId, payload, receiverId):
        route = Route()
        route.id = 1
        route.path = route.MESSAGE

        #creating message object
        message = Message()
        message.type = message.SINGLE
        message.senderId = senderId
        message.payload = payload
        ts = time.time()
        message.receiverId = receiverId
        message.timestamp = str(ts)
        message.status = message.ACTIVE
        message.action = message.POST

        #merge message to route object
        route.message.MergeFrom(message)
        
        self.send(route)

    def create_user(self, username):
        
        route = Route()
        route.id = 1
        route.path = route.MESSAGE
        
        userDetail = User()
        userDetail.uname = username
        ts = time.time()
        userDetail.recentActiveTime = str(ts)
        userDetail.action = userDetail.REGISTER
        
        route.user.MergeFrom(userDetail)
        
        self.send(route)

    def send(self, route):
        message = self.encoder.encode(route)
        self.s.send(message)

    def close(self):
        self.s.close()
        self.s = None

    def __get_server_path(self):
        return self.host + ":" + str(self.port)


    def start_cli(self):
        try:
            while True:
                cmd = raw_input("Example message - @username message:\n")

                if not self.s:
                    print "sorry socket not connected, please try again"
                else:

                    receiver_id, message = self.__parse_message_command(cmd)
                    if receiver_id is not None and message is not None:
                        for i in range(0,100):    
                            self.send_message(self.uname, message + str(i), receiver_id)
                        print "sent message to " + receiver_id + " - "  + message
                    else:
                        print "cannot send message, invalid format"
        except KeyboardInterrupt:
            sys.exit(1)

    def __parse_message_command(self, cmd):
        try:
            words = cmd.split(" ")
            receiver_id = words[0]
            if not receiver_id.startswith("@"):
                return (None, None)

            receiver_id = receiver_id[1:]
            message = ' '.join([str(x) for x in words[1:]])

            return (receiver_id, message)
        except:
            return (None, None)


mc = MessageClient(BUFFER_SIZE)
mc.start_cli()
