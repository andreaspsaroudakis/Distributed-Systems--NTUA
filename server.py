import sys
import socket
import threading
from hashlib import sha1
from help_scripts import getIP
from client import Client

class Server(object):
    def __init__(self,port,bootstrap_ip,bootstrap_port,num_replicas):             # h porta toy, h ip toy bootstrap, h port toy bootstrap kai to k

        self.my_port = int(port)
        self.my_ip = getIP()

        self.bootstrap_ip = bootstrap_ip
        self.bootstrap_port = int(bootstrap_port)

        self.num_replicas = int(num_replicas)

        self.server_id = self.my_ip + ":" + str(self.my_port)
        self.my_hash = sha1(self.server_id.encode('utf-8')).hexdigest()

        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.my_ip,self.my_port))
        self.server_socket.settimeout(1)
        self.server_socket.listen(10)

        self.previous_ip = self.my_ip
        self.previous_port = self.my_port
        self.previous_hash = self.my_hash

        self.next_ip = self.my_ip
        self.next_port = self.my_port
        self.next_hash = self.my_hash

        self.my_data = {}
        self.connection_thread = threading.Thread(target=self.connection)
        self.thread_list = []

        self.stop_connection = False

        self.commands = {
                           'join': self.join_approval,
                           'next': self.update_previous,
                           'previous': self.update_next,
                           'insert_linearizability': self.insert_linearizability,
                           'insert_eventual': self.insert_eventual,
                           'query_linearizability': self.query_linearizability,
                           'query_eventual': self.query_eventual,
                           'depart': self.depart,
                           'delete': self.delete_data,
                           'overlay': self.overlay,
                           'replication': self.add_replicas,
                           'replication_lazy': self.add_replicas_lazy,
                           'give_data': self.give_data,
                           'belongs_to_you': self.belongs_to_you,
                           'has_replica': self.has_replica,
                           'is_last_replica': self.is_last_replica,
                           'get_network_size': self.get_network_size,
                           'delete_excess_replica': self.delete_excess_replica,
                           'print_chord_data': self.print_chord_data
                        }

    def receive_data(self,conn):
        while True:
            try:
                data = conn.recv(1024)
                data = data.decode()
            except socket.error:
                print("Data was not received")
            else:
                if not data:
                    break
                else:
                    if data != 'close socket':
                        close_socket = False
                        function = self.commands.get(data.split(":")[0])
                        answer = function(data)
                    else:
                        close_socket = True
                        answer = "Socket was closed"
                    try:
                        if answer:
                            conn.send(answer.encode('utf-8'))
                        if close_socket:
                            conn.close()
                            break
                    except socket.error:
                        print("Answer failed")

    def connection(self):
        print("Server started listening on {}".format(self.server_id))
        while True:
            if self.stop_connection:
                break
            else:
                try:
                    conn, _ = self.server_socket.accept()
                except socket.timeout:
                    pass
                else:
                    self.thread_list.append(threading.Thread(target=self.receive_data, args=(conn,)))
                    self.thread_list[-1].start()

    def belongs_to_me(self,hash):
        return (self.previous_hash < hash <= self.my_hash) or (hash <= self.my_hash <= self.previous_hash) or (self.my_hash <= self.previous_hash <= hash)

    def join_approval(self,data):
        _, hash = data.split(":")
        if self.belongs_to_me(hash):
            answer = ':'.join((self.previous_ip, str(self.previous_port), self.previous_hash, self.my_ip, str(self.my_port), self.my_hash))
        else:
            answer = self.send_next(data)
        return answer

    def join_chord(self):
        client = Client(self.bootstrap_ip,self.bootstrap_port)
        client.send_message('join:{}'.format(self.my_hash))
        self.previous_ip, self.previous_port, self.previous_hash, self.next_ip, self.next_port, self.next_hash = client.receive_message().decode().split(":")
        self.previous_port = int(self.previous_port)
        self.next_port = int(self.next_port)
        client.close_socket()
        self.inform_previous()
        self.inform_next()
        network_size = self.send_next('get_network_size:1:{}'.format(self.my_hash))
        self.send_next('give_data:{}'.format(network_size))

    def get_network_size(self,data):
        #data=get_network_size:num_of_nodes:starter_hash
        _, num_of_nodes, starter_hash = data.split(":")
        num_of_nodes = str(int(num_of_nodes) + 1)
        if self.next_hash == starter_hash:
            answer = num_of_nodes
        else:
            answer = self.send_next('get_network_size:{}:{}'.format(num_of_nodes,starter_hash))
        return answer

    def inform_previous(self):
        client = Client(self.previous_ip,self.previous_port)
        client.send_message('next:{}:{}:{}'.format(self.my_ip,self.my_port,self.my_hash))
        answer = client.receive_message().decode()
        client.close_socket()
        return answer

    def inform_next(self):
        client = Client(self.next_ip,self.next_port)
        client.send_message('previous:{}:{}:{}'.format(self.my_ip,self.my_port,self.my_hash))
        answer = client.receive_message().decode()
        client.close_socket()
        return answer

    def update_previous(self,data):
        _, self.next_ip, self.next_port, self.next_hash = data.split(":")
        self.next_port = int(self.next_port)
        answer = "Update of previous node was successful"
        return answer

    def update_next(self,data):
        _, self.previous_ip, self.previous_port, self.previous_hash = data.split(":")
        self.previous_port = int(self.previous_port)
        answer = "Update of next node was successful"
        return answer

    def send_next(self,data):
        client = Client(self.next_ip,self.next_port)
        client.send_message(data)
        answer = client.receive_message().decode()
        client.close_socket()
        return answer

    def send_previous(self,data):
        client = Client(self.previous_ip,self.previous_port)
        client.send_message(data)
        answer = client.receive_message().decode()
        client.close_socket()
        return answer

    def add_replicas(self,data):
        #data=replication:key:value:num_replicas:starter_hash
        temp = data.split(":")[1:]
        keys, value, num_replicas, starter_hash = temp[:-3], temp[-3], temp[-2], temp[-1]
        if len(keys) == 1:
            key = keys[0]
        else:
            key = ":".join(keys)
        key_hash = sha1(key.encode('utf-8')).hexdigest()
        key_value = self.my_data.get(key_hash)
        if key_value == None or key_value[1] != value:                          # Den to exw h to exw alla me palio value
            self.my_data[key_hash] = (key,value)
            num_replicas = str(int(num_replicas)-1)
            print("Node {} added a replica of key {}".format(self.server_id,key))
        if num_replicas == '0' or self.next_hash == starter_hash:               # Stamataw an den exw alla antigrafa na prosthesw h an tha sxhmatistei meta kyklos
            answer = "Key-value pair ({},{}) was inserted successfully".format(key,value)
        else:
            answer = self.send_next('replication:{}:{}:{}:{}'.format(key,value,num_replicas,starter_hash))
        return answer

    def add_replicas_lazy(self,data):
        #data=replication_lazy:key:value:num_replicas:starter_hash
        temp = data.split(":")[1:]
        keys, value, num_replicas, starter_hash = temp[:-3], temp[-3], temp[-2], temp[-1]
        if len(keys) == 1:
            key = keys[0]
        else:
            key = ":".join(keys)
        key_hash = sha1(key.encode('utf-8')).hexdigest()
        key_value = self.my_data.get(key_hash)
        if key_value == None or key_value[1] != value:                          # Den to exw h to exw alla me palio value
            self.my_data[key_hash] = (key,value)
            num_replicas = str(int(num_replicas)-1)
            print("Node {} added a replica of key {}".format(self.server_id,key))
        if num_replicas == '0' or self.next_hash == starter_hash:               # Stamataw an den exw alla antigrafa na prosthesw h an tha sxhmatistei meta kyklos
            answer = "Key-value pair ({},{}) was inserted successfully".format(key,value)
        else:
            data = 'replication_lazy:{}:{}:{}:{}'.format(key,value,num_replicas,starter_hash)
            self.thread_list.append(threading.Thread(target=self.send_next, args=(data,)))
            self.thread_list[-1].start()
            answer = "Sent data to next"
        return answer

    def belongs_to_you(self,data):
        _, key_hash = data.split(":")
        if self.belongs_to_me(key_hash):
            answer = 'True'
        else:
            answer = 'False'
        return answer

    def has_replica(self,data):
        #data=hash_replica:key_hash
        _, key_hash = data.split(":")
        if self.my_data.get(key_hash) != None:                                  # To exw
            answer = 'True'
        else:
            answer = 'False'
        return answer

    def is_last_replica(self,data):
        #data=is_last_replica:key
        _, key = data.split(":")
        key_hash = sha1(key.encode('utf-8')).hexdigest()
        next_has_replica = self.send_next('has_replica:{}'.format(key_hash))
        belongs_to_next = self.send_next('belongs_to_you:{}'.format(key_hash))
        if next_has_replica == 'True' and belongs_to_next == 'False':                                           # DEN eimai to teleytaio antigrafo
            answer = self.send_next(data)
        else:                                                                # Eimai to teleytaio replica sthn alysida
            del self.my_data[key_hash]
            print("Last chain replica of key {} was deleted from node {}".format(key,self.server_id))
            answer = "Last replica was deleted"
        return answer

    def delete_excess_replica(self,data):
        #data=delete_excess_replica:key:remaining_replicas
        _, key, remaining_replicas = data.split(":")
        key_hash = sha1(key.encode('utf-8')).hexdigest()
        if remaining_replicas != '0':
            answer = self.send_next('delete_excess_replica:{}:{}'.format(key,int(remaining_replicas)-1))
        else:
            del self.my_data[key_hash]
            print("Last chain replica of key {} was deleted from node {}".format(key,self.server_id))
            answer = "Excess replica deleted"
        return answer

    def give_data(self,data):
        #data=give_data:network_size
        _, network_size = data.split(":")
        network_size = int(network_size)
        if network_size <= self.num_replicas:
            if bool(self.my_data):
                for key, value in self.my_data.values():
                    self.send_previous('replication:{}:{}:1:{}'.format(key,value,self.my_hash))
                print("Node {} gave all his (key,value) pairs to his previous node".format(self.server_id))
                answer = "Gave all (key,value) pairs"
            else:
                print("Node {} has no data to give to his previous node".format(self.server_id))
                answer = "No data to give"
        else:
            if bool(self.my_data):
                for key_value in list(self.my_data.values()):
                    key_hash = sha1(key_value[0].encode('utf-8')).hexdigest()
                    if self.belongs_to_me(key_hash):
                        print("Key {} still belongs to node {}".format(key_value[0],self.server_id))
                        answer = "Key still belongs to me"
                        continue
                    else:
                        self.send_previous('replication:{}:{}:1:{}'.format(key_value[0],key_value[1],self.my_hash))
                        belongs_to_previous = self.send_previous('belongs_to_you:{}'.format(key_hash))
                        if belongs_to_previous == 'True':
                            if self.num_replicas == 1:                          # No replication
                                del self.my_data[key_hash]
                                print("Node {} deleted key {}".format(self.server_id,key_value[0]))
                                answer = "(Key,value) pair was deleted"
                            else:
                                remaining_replicas = self.num_replicas - 2
                                answer = self.send_next('delete_excess_replica:{}:{}'.format(key_value[0],remaining_replicas))
                        else:                                                       # Eimai kapoy mesa sthn alysida
                            next_has_replica = self.send_next('has_replica:{}'.format(key_hash))
                            belongs_to_next = self.send_next('belongs_to_you:{}'.format(key_hash))
                            if next_has_replica == 'True' and belongs_to_next == 'False':
                                answer = self.send_next('is_last_replica:{}'.format(key_value[0]))
                            else:                                           # Found last replica
                                del self.my_data[key_hash]
                                print("Last chain replica of key {} was deleted from node {}".format(key_value[0],self.server_id))
                                answer = "Last replica was deleted"
            else:
                print("Node {} has no data to give to his previous node".format(self.server_id))
                answer = "No data to give"
        return answer

    def insert_linearizability(self,data):
        temp = data.split(":")[1:]
        keys, value = temp[:-1], temp[-1]
        if len(keys) == 1:
            key = keys[0]
        else:
            key = ":".join(keys)
        key_hash = sha1(key.encode('utf-8')).hexdigest()
        key_value = self.my_data.get(key_hash)
        if key_value != None and key_value[1] == value:
            answer = "Key-value pair already exists"
        elif self.belongs_to_me(key_hash):
            print("Successor node for key {} is {}".format(key,self.server_id))
            self.my_data[key_hash] = (key,value)
            remaining_replicas = self.num_replicas - 1
            if remaining_replicas != 0:
                answer = self.send_next('replication:{}:{}:{}:{}'.format(key,value,remaining_replicas,self.my_hash))
            else:
                answer = "Key-value pair ({},{}) was inserted successfully".format(key,value)
        else:
            answer = self.send_next(data)
        return answer

    def insert_eventual(self,data):
        temp = data.split(":")[1:]
        keys, value = temp[:-1], temp[-1]
        if len(keys) == 1:
            key = keys[0]
        else:
            key = ":".join(keys)
        key_hash = sha1(key.encode('utf-8')).hexdigest()
        key_value = self.my_data.get(key_hash)
        if key_value != None and key_value[1] == value:
            answer = "Key-value pair already exists"
        elif self.belongs_to_me(key_hash):
            print("Successor node for key {} is {}".format(key,self.server_id))
            self.my_data[key_hash] = (key,value)
            remaining_replicas = self.num_replicas - 1
            if remaining_replicas != 0:
                data = 'replication_lazy:{}:{}:{}:{}'.format(key,value,remaining_replicas,self.my_hash)
                self.thread_list.append(threading.Thread(target=self.send_next, args=(data,)))
                self.thread_list[-1].start()
            answer = "Key-value pair ({},{}) was inserted successfully".format(key,value)
        else:
            answer = self.send_next(data)
        return answer

    def delete_data(self,data):
        #data=delete:key:remaining_replicas:starter_hash
        _, key, remaining_replicas, starter_hash = data.split(":")
        key_hash = sha1(key.encode('utf-8')).hexdigest()
        key_value = self.my_data.get(key_hash)
        if key_value != None:                                                   # To exw
            del self.my_data[key_hash]
            print("Key {} was deleted from node {}".format(key,self.server_id))
            remaining_replicas = str(int(remaining_replicas)-1)
            if remaining_replicas != '0' and self.next_hash != starter_hash:
                answer = self.send_next('delete:{}:{}:{}'.format(key,remaining_replicas,starter_hash))
            else:
                answer = "Data was deleted"
        else:
            answer = self.send_next(data)
        return answer

    def query_linearizability(self,data):
        #data='query_linearizability:key:num_replicas'
        _, key, num_replicas = data.split(":")
        key_hash = sha1(key.encode('utf-8')).hexdigest()
        if num_replicas != 'None':                                              # Komvoi poy exoyn antigrafa meta ton prwteyon
            next_has_replica = self.send_next('has_replica:{}'.format(key_hash))
            belongs_to_next = self.send_next('belongs_to_you:{}'.format(key_hash))
            if next_has_replica == 'True' and belongs_to_next == 'False':       # Kapoios apo toys endiamesoys komvoys poy katexoyn replicas
                print("Node {} has replica of key {}".format(self.server_id,key))
                answer = self.send_next(data)
            else:                                                               # Komvos me to teleytaio replica
                key_value = self.my_data.get(key_hash)
                print("Node {} has last replica of key {}".format(self.server_id,key))
                answer = "Node with id {} has key {} with value {}".format(self.server_id,key,key_value[1])
        elif self.belongs_to_me(key_hash):
            key_value = self.my_data.get(key_hash)
            if key_value != None:                                               # To exw
                print("Key {} belongs to node {}".format(key,self.server_id))
                if self.num_replicas > 1:                                        # Paizoun replicas
                    remaining_replicas = self.num_replicas - 1
                    answer = self.send_next('query_linearizability:{}:{}'.format(key,remaining_replicas))
                else:
                    answer = "Node with id {} has key {} with value {}".format(self.server_id,key,key_value[1])
            else:                                                               # Anhkei se emena alla den to exw
                answer = "Key {} does not exist".format(key)
        else:
            answer = self.send_next(data)
        return answer

    def query_eventual(self,data):
        #data='query_eventual:key'
        _, key = data.split(":")
        key_hash = sha1(key.encode('utf-8')).hexdigest()
        key_value = self.my_data.get(key_hash)
        if key_value != None:                                                   # To exw
            answer = "Node with id {} has key {} with value {}".format(self.server_id,key,key_value[1])
        else:                                                                   # Den to exw
            if self.belongs_to_me(key_hash):                                    # Den to exw kai eimai o successor
                answer = "Key {} does not exist".format(key)
            else:                                                               # Den to exw kai eimai enas tyxaios komvos
                answer = self.send_next(data)
        return answer

    def depart(self,data):
        #data='depart'                                                          # Leipoun ta locks
        if self.my_hash != self.next_hash:
            for data in self.my_data.values():
                self.send_next('replication:{}:{}:1:{}'.format(data[0],data[1],self.my_hash))
            self.send_next('previous:{}:{}:{}'.format(self.previous_ip,self.previous_port,self.previous_hash))
            self.send_previous('next:{}:{}:{}'.format(self.next_ip,self.next_port,self.next_hash))
        self.stop_connection = True
        self.connection_thread.join()
        answer = "Server {} departed".format(self.server_id)
        return answer

    def print_chord_data(self, data):
        starter_hash = data.split(":")[1]                                       # Leipoun ta locks
        server_data = []
        for key_value in self.my_data.values():
            server_data.append(key_value)
        data = data + ":" + self.server_id + ":" + str(server_data)
        if self.my_hash == self.next_hash or self.next_hash == starter_hash:
            answer = data
        else:
            answer = self.send_next(data)
        return answer

    def overlay(self,data):
        #data=overlay:starter_hash:list_of_server_ids
        starter_hash = data.split(":")[1]
        if self.my_hash == starter_hash:
            if self.my_hash != self.next_hash:
                answer = self.send_next(data)
            else:
                answer = "Only node with id {} exists in overlay network".format(self.server_id)
        elif self.next_hash != starter_hash:
            data = data + ":" + self.server_id
            answer = self.send_next(data)
        else:
            answer = data + ":" + self.server_id
        return answer

    def get_port(self):
        return self.my_port

    def get_hash(self):
        return self.my_hash
