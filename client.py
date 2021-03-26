import threading
import socket
import sys

class Client(object):
    def __init__(self,ip_address,port):
        self.port = port
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.connect((ip_address,port))

    def send_message(self,message):
        try:
            self.client_socket.send(message.encode('utf-8'))
        except socket.error:
            print("Message sending failed!")
            sys.exit()

    def receive_message(self):
        try:
            answer = self.client_socket.recv(1024)
        except socket.error:
            print("Message was not received!")
            sys.exit()
        else:
            return answer

    def close_socket(self):
        try:
            self.client_socket.send('close socket'.encode('utf-8'))
        except socket.error:
            print("Closing of socket failed")
            sys.exit()
        else:
            self.client_socket.close()
