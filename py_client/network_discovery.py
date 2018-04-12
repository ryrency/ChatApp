import socket

from pipe_pb2 import Route
from pipe_pb2 import NetworkDiscoveryPacket
from encoder_decoder import LengthFieldProtoEncoder, LengthFieldProtoDncoder

UDP_IP = "255.255.255.255"
UDP_PORT = 8888
SECRET_KEY = 'secret'

class NetworkDiscover:

    def __init__(self):
        self.encoder = LengthFieldProtoEncoder()
        self.decoder = LengthFieldProtoDncoder()
        self.socket_connect = None
        self.route = None

    def connectUDP(self):
        self.socket_connect = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        self.socket_connect.bind(('', UDP_PORT))
        self.socket_connect.setsockopt(socket.SOL_SOCKET,socket.SO_BROADCAST,1)
        print "connectUDP: Request to server: " + self.__get_udp_path()


    def sendNetworkDiscoveryPacket(self, ip_address = socket.gethostbyname(socket.gethostname()), ip_port = UDP_PORT):
        print ("IP address sent in discovery packet - " + ip_address)
        route = Route()
        route.id = 2
        route.path = route.NETWORK_DISCOVERY
        
        networkDiscoveryPacket = NetworkDiscoveryPacket()
        networkDiscoveryPacket.mode = networkDiscoveryPacket.REQUEST
        networkDiscoveryPacket.sender = networkDiscoveryPacket.END_USER_CLIENT
#         networkDiscoveryPacket.groupTag
#         networkDiscoveryPacket.nodeId
        networkDiscoveryPacket.nodeAddress = ip_address
        networkDiscoveryPacket.nodePort = ip_port
        networkDiscoveryPacket.secret = SECRET_KEY
        
        route.networkDiscoveryPacket.MergeFrom(networkDiscoveryPacket)
        message = self.encoder.encode(route, msg_type='udp')
        self.socket_connect.sendto(message,(UDP_IP, UDP_PORT))

    def receiveNetworkDiscoveryPacket(self):
        while 1:
            msg, server = self.socket_connect.recvfrom(1024)
            print server
            
            if (server != (socket.gethostbyname(socket.gethostname()), UDP_PORT)) and msg:
               break
                       
        self.socket_connect.close()   
        decodedMsg = self.decoder.decode(msg)
        return decodedMsg

    def __get_udp_path(self):
        return UDP_IP + ":" +  str(UDP_PORT)
